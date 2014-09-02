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

import java.util.Date;
import java.util.List;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.SuspendTransitionXCommand;
import org.apache.oozie.command.bundle.BundleStatusUpdateXCommand;
import org.apache.oozie.command.wf.SuspendXCommand;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.BatchQueryExecutor.UpdateEntry;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.executor.jpa.CoordJobGetActionsRunningJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.InstrumentUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.StatusUtils;

/**
 * Suspend coordinator job and actions.
 *
 */
public class CoordSuspendXCommand extends SuspendTransitionXCommand {
    private final String jobId;
    private CoordinatorJobBean coordJob;
    private JPAService jpaService;
    private boolean exceptionOccured = false;
    private CoordinatorJob.Status prevStatus = null;

    public CoordSuspendXCommand(String id) {
        super("coord_suspend", "coord_suspend", 1);
        this.jobId = ParamChecker.notEmpty(id, "id");
    }

    @Override
    public String getEntityKey() {
        return jobId;
    }

    @Override
    public String getKey() {
        return getName() + "_" + jobId;
    }

    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    protected void loadState() throws CommandException {
        super.eagerLoadState();
        try {
            jpaService = Services.get().get(JPAService.class);
            if (jpaService != null) {
                this.coordJob = CoordJobQueryExecutor.getInstance()
                        .get(CoordJobQuery.GET_COORD_JOB_SUSPEND_KILL, this.jobId);
                prevStatus = coordJob.getStatus();
            }
            else {
                throw new CommandException(ErrorCode.E0610);
            }
        }
        catch (Exception ex) {
            throw new CommandException(ErrorCode.E0603, ex.getMessage(), ex);
        }
        LogUtils.setLogInfo(this.coordJob);
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        super.eagerVerifyPrecondition();
        if (coordJob.getStatus() == CoordinatorJob.Status.SUCCEEDED
                || coordJob.getStatus() == CoordinatorJob.Status.FAILED
                || coordJob.getStatus() == CoordinatorJob.Status.KILLED
                || coordJob.getStatus() == CoordinatorJob.Status.IGNORED) {
            LOG.info("CoordSuspendXCommand is not going to execute because "
                    + "job finished or failed or killed, id = " + jobId + ", status = " + coordJob.getStatus());
            throw new PreconditionException(ErrorCode.E0728, jobId, coordJob.getStatus().toString());
        }
    }

    @Override
    public void suspendChildren() throws CommandException {
        try {
            //Get all running actions of a job to suspend them
            List<CoordinatorActionBean> actionList = jpaService
                    .execute(new CoordJobGetActionsRunningJPAExecutor(jobId));
            for (CoordinatorActionBean action : actionList) {
                // queue a SuspendXCommand
                if (action.getExternalId() != null) {
                    queue(new SuspendXCommand(action.getExternalId()));
                    updateCoordAction(action);
                    LOG.debug(
                            "Suspend coord action = [{0}], new status = [{1}], pending = [{2}] and queue SuspendXCommand for [{3}]",
                            action.getId(), action.getStatus(), action.getPending(), action.getExternalId());
                }
                else {
                    updateCoordAction(action);
                    LOG.debug(
                            "Suspend coord action = [{0}], new status = [{1}], pending = [{2}] and external id is null",
                            action.getId(), action.getStatus(), action.getPending());
                }

            }
            LOG.debug("Suspended coordinator actions for the coordinator=[{0}]", jobId);
        }
        catch (XException ex) {
            exceptionOccured = true;
            throw new CommandException(ex);
        }
        finally {
            if (exceptionOccured) {
                coordJob.setStatus(CoordinatorJob.Status.FAILED);
                coordJob.resetPending();
                LOG.debug("Exception happened, fail coordinator job id = " + jobId + ", status = "
                        + coordJob.getStatus());
                updateList.add(new UpdateEntry<CoordJobQuery>(CoordJobQuery.UPDATE_COORD_JOB_STATUS_PENDING_TIME, coordJob));
           }
        }
    }

    @Override
    public void notifyParent() throws CommandException {
        // update bundle action
        if (this.coordJob.getBundleId() != null) {
            BundleStatusUpdateXCommand bundleStatusUpdate = new BundleStatusUpdateXCommand(coordJob, prevStatus);
            bundleStatusUpdate.call();
        }
    }

    @Override
    public void updateJob() {
        InstrumentUtils.incrJobCounter(getName(), 1, getInstrumentation());
        coordJob.setLastModifiedTime(new Date());
        coordJob.setSuspendedTime(new Date());
        LOG.debug("Suspend coordinator job id = " + jobId + ", status = " + coordJob.getStatus() + ", pending = " + coordJob.isPending());
        updateList.add(new UpdateEntry<CoordJobQuery>(CoordJobQuery.UPDATE_COORD_JOB_STATUS_PENDING_TIME, coordJob));
    }

    @Override
    public void performWrites() throws CommandException {
        try {
            BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(null, updateList, null);
        }
        catch (JPAExecutorException jex) {
            throw new CommandException(jex);
        }
    }

    private void updateCoordAction(CoordinatorActionBean action) {
        action.setStatus(CoordinatorActionBean.Status.SUSPENDED);
        action.incrementAndGetPending();
        action.setLastModifiedTime(new Date());
        updateList.add(new UpdateEntry<CoordActionQuery>(CoordActionQuery.UPDATE_COORD_ACTION_STATUS_PENDING_TIME, action));
    }

    @Override
    public Job getJob() {
        return coordJob;
    }

    /**
     * Transit job to suspended from running or to prepsuspended from prep.
     *
     * @see org.apache.oozie.command.TransitionXCommand#transitToNext()
     */
    @Override
    public void transitToNext() {
        if (coordJob == null) {
            coordJob = (CoordinatorJobBean) this.getJob();
        }
        if (coordJob.getStatus() == Job.Status.PREP) {
            coordJob.setStatus(Job.Status.PREPSUSPENDED);
            coordJob.setStatus(StatusUtils.getStatus(coordJob));
        }
        else if (coordJob.getStatus() == Job.Status.RUNNING) {
            coordJob.setStatus(Job.Status.SUSPENDED);
        }
        else if (coordJob.getStatus() == Job.Status.RUNNINGWITHERROR || coordJob.getStatus() == Job.Status.PAUSEDWITHERROR) {
            coordJob.setStatus(Job.Status.SUSPENDEDWITHERROR);
        }
        else if (coordJob.getStatus() == Job.Status.PAUSED) {
            coordJob.setStatus(Job.Status.SUSPENDED);
        }
        else if (coordJob.getStatus() == Job.Status.PREPPAUSED) {
            coordJob.setStatus(Job.Status.PREPSUSPENDED);
        }
        coordJob.setPending();
    }

}
