/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.oozie.command.coord;

import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.command.bundle.BundleStatusUpdateXCommand;
import org.apache.oozie.command.wf.KillXCommand;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.KillTransitionXCommand;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.dependency.DependencyChecker;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.BatchQueryExecutor.UpdateEntry;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.executor.jpa.CoordJobGetActionsNotCompletedJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.StatusUtils;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class CoordKillXCommand extends KillTransitionXCommand {

    private final String jobId;
    private CoordinatorJobBean coordJob;
    private List<CoordinatorActionBean> actionList;
    private JPAService jpaService = null;
    private CoordinatorJob.Status prevStatus = null;

    public CoordKillXCommand(String id) {
        super("coord_kill", "coord_kill", 2);
        this.jobId = ParamChecker.notEmpty(id, "id");
    }

    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    public String getEntityKey() {
        return this.jobId;
    }

    @Override
    public String getKey() {
        return getName() + "_" + this.jobId;
    }

    @Override
    protected void loadState() throws CommandException {
        try {
            jpaService = Services.get().get(JPAService.class);

            if (jpaService != null) {
                this.coordJob = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB_SUSPEND_KILL, jobId);
                //Get actions which are not succeeded, failed, timed out or killed
                this.actionList = jpaService.execute(new CoordJobGetActionsNotCompletedJPAExecutor(jobId));
                prevStatus = coordJob.getStatus();
                LogUtils.setLogInfo(coordJob);
            }
            else {
                throw new CommandException(ErrorCode.E0610);
            }
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        // if namespace 0.1 is used and backward support is true, SUCCEEDED coord job can be killed
        if (StatusUtils.isV1CoordjobKillable(coordJob)) {
            return;
        }
        if (coordJob.getStatus() == CoordinatorJob.Status.SUCCEEDED
                || coordJob.getStatus() == CoordinatorJob.Status.FAILED
                || coordJob.getStatus() == CoordinatorJob.Status.DONEWITHERROR
                || coordJob.getStatus() == CoordinatorJob.Status.KILLED
                || coordJob.getStatus() == CoordinatorJob.Status.IGNORED) {
            LOG.info("CoordKillXCommand not killed - job either finished SUCCEEDED, FAILED, KILLED, DONEWITHERROR"
                    + " or IGNORED,job id = " + jobId + ", status = " + coordJob.getStatus());
            throw new PreconditionException(ErrorCode.E1020, jobId);
        }
    }

    private void updateCoordAction(CoordinatorActionBean action, boolean makePending) {
        CoordinatorAction.Status prevStatus = action.getStatus();
        action.setStatus(CoordinatorActionBean.Status.KILLED);
        if (makePending) {
            action.incrementAndGetPending();
        } else {
            // set pending to false
            action.setPending(0);
        }
        if (EventHandlerService.isEnabled() && prevStatus != CoordinatorAction.Status.RUNNING
                && prevStatus != CoordinatorAction.Status.SUSPENDED) {
            CoordinatorXCommand.generateEvent(action, coordJob.getUser(), coordJob.getAppName(), null);
        }
        action.setLastModifiedTime(new Date());
        updateList.add(new UpdateEntry<CoordActionQuery>(CoordActionQuery.UPDATE_COORD_ACTION_STATUS_PENDING_TIME,action));
    }

    @Override
    public void killChildren() throws CommandException {
        if (actionList != null) {
            for (CoordinatorActionBean action : actionList) {
                // queue a WorkflowKillXCommand to delete the workflow job and actions
                if (action.getExternalId() != null) {
                    queue(new KillXCommand(action.getExternalId()));
                    // As the kill command for children is queued, set pending flag for coord action to be true
                    updateCoordAction(action, true);
                    LOG.debug(
                            "Killed coord action = [{0}], new status = [{1}], pending = [{2}] and queue KillXCommand for [{3}]",
                            action.getId(), action.getStatus(), action.getPending(), action.getExternalId());
                }
                else {
                    // As killing children is not required, set pending flag for coord action to be false
                    updateCoordAction(action, false);
                    LOG.debug("Killed coord action = [{0}], current status = [{1}], pending = [{2}]",
                            action.getId(), action.getStatus(), action.getPending());
                }
                String pushMissingDeps = action.getPushMissingDependencies();
                if (pushMissingDeps != null) {
                    CoordPushDependencyCheckXCommand.unregisterMissingDependencies(
                            Arrays.asList(DependencyChecker.dependenciesAsArray(pushMissingDeps)), action.getId());
                }
            }
        }
        coordJob.setDoneMaterialization();
        coordJob.setLastModifiedTime(new Date());
        LOG.debug("Killed coord actions for the coordinator=[{0}]", jobId);
    }

    @Override
    public void notifyParent() throws CommandException {
        // update bundle action
        if (coordJob.getBundleId() != null) {
            BundleStatusUpdateXCommand bundleStatusUpdate = new BundleStatusUpdateXCommand(coordJob, prevStatus);
            bundleStatusUpdate.call();
        }
    }

    @Override
    public void updateJob() throws CommandException {
        updateList.add(new UpdateEntry<CoordJobQuery>(CoordJobQuery.UPDATE_COORD_JOB_STATUS_PENDING_TIME, coordJob));
    }

    @Override
    public void performWrites() throws CommandException {
        try {
            BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(null, updateList, null);
        }
        catch (JPAExecutorException e) {
            throw new CommandException(e);
        }
    }

    @Override
    public Job getJob() {
        return coordJob;
    }

}
