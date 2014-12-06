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

package org.apache.oozie.command.bundle;

import java.util.Date;
import java.util.List;

import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.Job;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.SuspendTransitionXCommand;
import org.apache.oozie.command.coord.CoordSuspendXCommand;
import org.apache.oozie.executor.jpa.BundleActionQueryExecutor.BundleActionQuery;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.BundleActionQueryExecutor;
import org.apache.oozie.executor.jpa.BundleJobQueryExecutor;
import org.apache.oozie.executor.jpa.BundleJobQueryExecutor.BundleJobQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.BatchQueryExecutor.UpdateEntry;
import org.apache.oozie.util.InstrumentUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.LogUtils;

public class BundleJobSuspendXCommand extends SuspendTransitionXCommand {
    private final String jobId;
    private List<BundleActionBean> bundleActions;
    private BundleJobBean bundleJob;

    public BundleJobSuspendXCommand(String id) {
        super("bundle_suspend", "bundle_suspend", 1);
        this.jobId = ParamChecker.notEmpty(id, "id");
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#getJob()
     */
    @Override
    public Job getJob() {
        return bundleJob;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#notifyParent()
     */
    @Override
    public void notifyParent() throws CommandException {
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#setJob(org.apache.oozie.client.Job)
     */
    @Override
    public void setJob(Job job) {
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.SuspendTransitionXCommand#performWrites()
     */
    @Override
    public void performWrites() throws CommandException {
        try {
            BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(null, updateList, null);
        }
        catch (JPAExecutorException e) {
            throw new CommandException(e);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    public String getEntityKey() {
        return this.jobId;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#isLockRequired()
     */
    @Override
    protected boolean isLockRequired() {
        return true;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#loadState()
     */
    @Override
    protected void loadState() throws CommandException {
        try {
            bundleJob = BundleJobQueryExecutor.getInstance().get(BundleJobQuery.GET_BUNDLE_JOB, jobId);
        }
        catch (Exception Ex) {
            throw new CommandException(ErrorCode.E0604, jobId);
        }

        try {
            bundleActions = BundleActionQueryExecutor.getInstance().getList(
                    BundleActionQuery.GET_BUNDLE_ACTIONS_STATUS_UNIGNORED_FOR_BUNDLE, bundleJob.getId());
        }
        catch (Exception Ex) {
            throw new CommandException(ErrorCode.E1311, jobId);
        }

        LogUtils.setLogInfo(bundleJob);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (bundleJob.getStatus() == Job.Status.SUCCEEDED || bundleJob.getStatus() == Job.Status.FAILED
                || bundleJob.getStatus() == Job.Status.KILLED || bundleJob.getStatus() == Job.Status.DONEWITHERROR) {
            LOG.info("BundleJobSuspendXCommand is not going to execute because job either succeeded, failed, killed, or donewitherror; id = "
                            + jobId + ", status = " + bundleJob.getStatus());
            throw new PreconditionException(ErrorCode.E1312, jobId, bundleJob.getStatus().toString());
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#updateJob()
     */
    @Override
    public void updateJob() {
        InstrumentUtils.incrJobCounter("bundle_suspend", 1, null);
        bundleJob.setSuspendedTime(new Date());
        bundleJob.setLastModifiedTime(new Date());

        LOG.debug("Suspend bundle job id = " + jobId + ", status = " + bundleJob.getStatus() + ", pending = " + bundleJob.isPending());
        updateList.add(new UpdateEntry<BundleJobQuery>(BundleJobQuery.UPDATE_BUNDLE_JOB_STATUS_PENDING_SUSP_MOD_TIME, bundleJob));
    }

    @Override
    public void suspendChildren() throws CommandException {
        for (BundleActionBean action : this.bundleActions) {
            if (action.getStatus() == Job.Status.RUNNING || action.getStatus() == Job.Status.RUNNINGWITHERROR
                    || action.getStatus() == Job.Status.PREP || action.getStatus() == Job.Status.PAUSED
                    || action.getStatus() == Job.Status.PAUSEDWITHERROR) {
                // queue a CoordSuspendXCommand
                if (action.getCoordId() != null) {
                    queue(new CoordSuspendXCommand(action.getCoordId()));
                    updateBundleAction(action);
                    LOG.debug("Suspend bundle action = [{0}], new status = [{1}], pending = [{2}] and queue CoordSuspendXCommand for [{3}]",
                            action.getBundleActionId(), action.getStatus(), action.getPending(), action.getCoordId());
                } else {
                    updateBundleAction(action);
                    LOG.debug("Suspend bundle action = [{0}], new status = [{1}], pending = [{2}] and coord id is null",
                            action.getBundleActionId(), action.getStatus(), action.getPending());
                }

            }
        }
        LOG.debug("Suspended bundle actions for the bundle=[{0}]", jobId);
    }

    private void updateBundleAction(BundleActionBean action) {
        if (action.getStatus() == Job.Status.PREP) {
            action.setStatus(Job.Status.PREPSUSPENDED);
        }
        else if (action.getStatus() == Job.Status.RUNNING) {
            action.setStatus(Job.Status.SUSPENDED);
        }
        else if (action.getStatus() == Job.Status.RUNNINGWITHERROR) {
            action.setStatus(Job.Status.SUSPENDEDWITHERROR);
        }
        else if (action.getStatus() == Job.Status.PAUSED) {
            action.setStatus(Job.Status.SUSPENDED);
        }
        else if (action.getStatus() == Job.Status.PAUSEDWITHERROR) {
            action.setStatus(Job.Status.SUSPENDEDWITHERROR);
        }

        action.incrementAndGetPending();
        action.setLastModifiedTime(new Date());
        updateList.add(new UpdateEntry<BundleActionQuery>(
                BundleActionQuery.UPDATE_BUNDLE_ACTION_STATUS_PENDING_MODTIME, action));
    }
}
