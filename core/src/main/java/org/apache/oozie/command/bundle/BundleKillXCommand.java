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
import org.apache.oozie.XException;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.KillTransitionXCommand;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.coord.CoordKillXCommand;
import org.apache.oozie.executor.jpa.BulkUpdateInsertJPAExecutor;
import org.apache.oozie.executor.jpa.BundleActionsGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;

public class BundleKillXCommand extends KillTransitionXCommand {
    private final String jobId;
    private BundleJobBean bundleJob;
    private List<BundleActionBean> bundleActions;
    private JPAService jpaService = null;

    public BundleKillXCommand(String jobId) {
        super("bundle_kill", "bundle_kill", 1);
        this.jobId = ParamChecker.notEmpty(jobId, "jobId");
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    public String getEntityKey() {
        return jobId;
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
    public void loadState() throws CommandException {
        try {
            jpaService = Services.get().get(JPAService.class);

            if (jpaService != null) {
                this.bundleJob = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
                this.bundleActions = jpaService.execute(new BundleActionsGetJPAExecutor(jobId));
                LogUtils.setLogInfo(bundleJob, logInfo);
                super.setJob(bundleJob);

            }
            else {
                throw new CommandException(ErrorCode.E0610);
            }
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (bundleJob.getStatus() == Job.Status.SUCCEEDED
                || bundleJob.getStatus() == Job.Status.FAILED
                || bundleJob.getStatus() == Job.Status.DONEWITHERROR
                || bundleJob.getStatus() == Job.Status.KILLED) {
            LOG.info("Bundle job cannot be killed - job already SUCCEEDED, FAILED, KILLED or DONEWITHERROR, job id = "
                    + jobId + ", status = " + bundleJob.getStatus());
            throw new PreconditionException(ErrorCode.E1020, jobId);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.KillTransitionXCommand#killChildren()
     */
    @Override
    public void killChildren() throws CommandException {
        if (bundleActions != null) {
            for (BundleActionBean action : bundleActions) {
                if (action.getCoordId() != null) {
                    queue(new CoordKillXCommand(action.getCoordId()));
                    updateBundleAction(action);
                    LOG.debug("Killed bundle action = [{0}], new status = [{1}], pending = [{2}] and queue CoordKillXCommand for [{3}]",
                            action.getBundleActionId(), action.getStatus(), action.getPending(), action.getCoordId());
                } else {
                    updateBundleAction(action);
                    LOG.debug("Killed bundle action = [{0}], current status = [{1}], pending = [{2}]", action.getBundleActionId(), action
                            .getStatus(), action.getPending());
                }

            }
        }
        LOG.debug("Killed coord jobs for the bundle=[{0}]", jobId);
    }

    /**
     * Update bundle action
     *
     * @param action
     * @throws CommandException
     */
    private void updateBundleAction(BundleActionBean action) {
        action.setLastModifiedTime(new Date());
        if (!action.isTerminalStatus()) {
            action.incrementAndGetPending();
            action.setStatus(Job.Status.KILLED);
        }
        updateList.add(action);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#notifyParent()
     */
    @Override
    public void notifyParent() {
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#getJob()
     */
    @Override
    public Job getJob() {
        return bundleJob;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#updateJob()
     */
    @Override
    public void updateJob() {
        updateList.add(bundleJob);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.KillTransitionXCommand#performWrites()
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

}
