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
import org.apache.oozie.command.ResumeTransitionXCommand;
import org.apache.oozie.command.coord.CoordResumeXCommand;
import org.apache.oozie.executor.jpa.BulkUpdateInsertJPAExecutor;
import org.apache.oozie.executor.jpa.BundleActionsGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.InstrumentUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;

public class BundleJobResumeXCommand extends ResumeTransitionXCommand {

    private final String bundleId;
    private JPAService jpaService = null;
    private BundleJobBean bundleJob;
    private List<BundleActionBean> bundleActions;

    /**
     * Constructor to create the Bundle Resume Command.
     *
     * @param jobId : Bundle Id
     */
    public BundleJobResumeXCommand(String jobId) {
        super("bundle_resume", "bundle_resume", 1);
        this.bundleId = ParamChecker.notNull(jobId, "BundleId");
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.ResumeTransitionXCommand#resumeChildren()
     */
    @Override
    public void resumeChildren() {
        for (BundleActionBean action : bundleActions) {
            if (action.getStatus() == Job.Status.SUSPENDED || action.getStatus() == Job.Status.SUSPENDEDWITHERROR || action.getStatus() == Job.Status.PREPSUSPENDED) {
                // queue a CoordResumeXCommand
                if (action.getCoordId() != null) {
                    queue(new CoordResumeXCommand(action.getCoordId()));
                    updateBundleAction(action);
                    LOG.debug("Resume bundle action = [{0}], new status = [{1}], pending = [{2}] and queue CoordResumeXCommand for [{3}]",
                                    action.getBundleActionId(), action.getStatus(), action.getPending(), action
                                            .getCoordId());
                }
                else {
                    updateBundleAction(action);
                    LOG.debug("Resume bundle action = [{0}], new status = [{1}], pending = [{2}] and coord id is null",
                                    action.getBundleActionId(), action.getStatus(), action.getPending());
                }
            }
        }
        LOG.debug("Resume bundle actions for the bundle=[{0}]", bundleId);
    }

    private void updateBundleAction(BundleActionBean action) {
        if (action.getStatus() == Job.Status.PREPSUSPENDED) {
            action.setStatus(Job.Status.PREP);
        }
        else if (action.getStatus() == Job.Status.SUSPENDED) {
            action.setStatus(Job.Status.RUNNING);
        }
        else if (action.getStatus() == Job.Status.SUSPENDEDWITHERROR) {
            action.setStatus(Job.Status.RUNNINGWITHERROR);
        }
        action.incrementAndGetPending();
        action.setLastModifiedTime(new Date());
        updateList.add(action);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#notifyParent()
     */
    @Override
    public void notifyParent() throws CommandException {

    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#updateJob()
     */
    @Override
    public void updateJob() {
        InstrumentUtils.incrJobCounter("bundle_resume", 1, null);
        bundleJob.setSuspendedTime(null);
        bundleJob.setLastModifiedTime(new Date());
        LOG.debug("Resume bundle job id = " + bundleId + ", status = " + bundleJob.getStatus() + ", pending = " + bundleJob.isPending());
        updateList.add(bundleJob);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.ResumeTransitionXCommand#performWrites()
     */
    @Override
    public void performWrites() throws CommandException {
        try {
            jpaService.execute(new BulkUpdateInsertJPAExecutor(updateList, null));
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
        return bundleId;
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
        jpaService = Services.get().get(JPAService.class);
        if (jpaService == null) {
            throw new CommandException(ErrorCode.E0610);
        }

        try {
            bundleJob = jpaService.execute(new BundleJobGetJPAExecutor(bundleId));
            bundleActions = jpaService.execute(new BundleActionsGetJPAExecutor(bundleId));
        }
        catch (Exception Ex) {
            throw new CommandException(ErrorCode.E0604, bundleId);
        }

        LogUtils.setLogInfo(bundleJob, logInfo);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (bundleJob.getStatus() != Job.Status.SUSPENDED && bundleJob.getStatus() != Job.Status.SUSPENDEDWITHERROR && bundleJob.getStatus() != Job.Status.PREPSUSPENDED) {
            throw new PreconditionException(ErrorCode.E1100, "BundleResumeCommand not Resumed - "
                    + "job not in SUSPENDED/SUSPENDEDWITHERROR/PREPSUSPENDED state " + bundleId);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#getJob()
     */
    @Override
    public Job getJob() {
        return bundleJob;
    }
}
