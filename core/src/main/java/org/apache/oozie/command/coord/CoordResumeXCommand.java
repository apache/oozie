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
import org.apache.oozie.command.ResumeTransitionXCommand;
import org.apache.oozie.command.bundle.BundleStatusUpdateXCommand;
import org.apache.oozie.command.wf.ResumeXCommand;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.BatchQueryExecutor.UpdateEntry;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.executor.jpa.CoordJobGetActionsSuspendedJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.InstrumentUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;

/**
 * Resume coordinator job and actions.
 *
 */
public class CoordResumeXCommand extends ResumeTransitionXCommand {
    private final String jobId;
    private CoordinatorJobBean coordJob = null;
    private JPAService jpaService = null;
    private boolean exceptionOccured = false;
    CoordinatorJob.Status prevStatus;

    public CoordResumeXCommand(String id) {
        super("coord_resume", "coord_resume", 1);
        this.jobId = ParamChecker.notEmpty(id, "id");
    }

    @Override
    public String getEntityKey() {
        return jobId;
    }

    @Override
    public String getKey() {
        return getName() + "_" + this.jobId;
    }

    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    protected void loadState() throws CommandException {
        jpaService = Services.get().get(JPAService.class);
        if (jpaService == null) {
            throw new CommandException(ErrorCode.E0610);
        }
        try {
            coordJob = jpaService.execute(new CoordJobGetJPAExecutor(jobId));
        }
        catch (JPAExecutorException e) {
            throw new CommandException(e);
        }
        setJob(coordJob);
        prevStatus = coordJob.getStatus();
        LogUtils.setLogInfo(coordJob);
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (coordJob.getStatus() != CoordinatorJob.Status.SUSPENDED && coordJob.getStatus() != CoordinatorJob.Status.SUSPENDEDWITHERROR && coordJob.getStatus() != Job.Status.PREPSUSPENDED) {
            throw new PreconditionException(ErrorCode.E1100, "CoordResumeXCommand not Resumed - "
                    + "job not in SUSPENDED/SUSPENDEDWITHERROR/PREPSUSPENDED state, job = " + jobId);
        }
    }

    @Override
    public void updateJob() {
        InstrumentUtils.incrJobCounter(getName(), 1, getInstrumentation());
        coordJob.setSuspendedTime(null);
        coordJob.setLastModifiedTime(new Date());
        LOG.debug("Resume coordinator job id = " + jobId + ", status = " + coordJob.getStatus() + ", pending = "
                + coordJob.isPending());
        updateList.add(new UpdateEntry<CoordJobQuery>(CoordJobQuery.UPDATE_COORD_JOB_STATUS_PENDING_TIME, coordJob));
    }

    @Override
    public void resumeChildren() throws CommandException {
        try {
            // Get all suspended actions to resume them
            List<CoordinatorActionBean> actionList = jpaService.execute(new CoordJobGetActionsSuspendedJPAExecutor(
                    jobId));

            for (CoordinatorActionBean action : actionList) {
                if (action.getExternalId() != null) {
                    // queue a ResumeXCommand
                    queue(new ResumeXCommand(action.getExternalId()));
                    updateCoordAction(action);
                    LOG.debug(
                            "Resume coord action = [{0}], new status = [{1}], pending = [{2}] and queue ResumeXCommand for [{3}]",
                            action.getId(), action.getStatus(), action.getPending(), action.getExternalId());
                }
                else {
                    updateCoordAction(action);
                    LOG.debug(
                            "Resume coord action = [{0}], new status = [{1}], pending = [{2}] and external id is null",
                            action.getId(), action.getStatus(), action.getPending());
                }
            }

        }
        catch (XException ex) {
            exceptionOccured = true;
            throw new CommandException(ex);
        }
        finally {
            if (exceptionOccured) {
                coordJob.setStatus(CoordinatorJob.Status.FAILED);
                coordJob.resetPending();
                LOG.warn("Resume children failed so fail coordinator, coordinator job id = " + jobId + ", status = "
                        + coordJob.getStatus());
                updateList.add(new UpdateEntry<CoordJobQuery>(CoordJobQuery.UPDATE_COORD_JOB_STATUS_PENDING_TIME,
                        coordJob));
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
    public void performWrites() throws CommandException {
        try {
            BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(null, updateList, null);
            if (EventHandlerService.isEnabled()) {
                // good enough to set event start time as coord's last modified time
                // updated when set to running
                generateEvents(coordJob, coordJob.getLastModifiedTime());
            }
        }
        catch (JPAExecutorException e) {
            throw new CommandException(e);
        }
    }

    private void updateCoordAction(CoordinatorActionBean action) {
        action.setStatus(CoordinatorActionBean.Status.RUNNING);
        action.incrementAndGetPending();
        action.setLastModifiedTime(new Date());
        updateList.add(new UpdateEntry<CoordActionQuery>(CoordActionQuery.UPDATE_COORD_ACTION_STATUS_PENDING_TIME, action));
    }

    @Override
    public Job getJob() {
        return coordJob;
    }
}
