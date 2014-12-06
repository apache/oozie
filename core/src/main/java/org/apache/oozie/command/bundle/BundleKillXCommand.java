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
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.Job;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.KillTransitionXCommand;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.coord.CoordKillXCommand;
import org.apache.oozie.executor.jpa.BatchQueryExecutor.UpdateEntry;
import org.apache.oozie.executor.jpa.BundleActionQueryExecutor.BundleActionQuery;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.BundleActionQueryExecutor;
import org.apache.oozie.executor.jpa.BundleJobQueryExecutor;
import org.apache.oozie.executor.jpa.BundleJobQueryExecutor.BundleJobQuery;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;

public class BundleKillXCommand extends KillTransitionXCommand {
    private final String jobId;
    private BundleJobBean bundleJob;
    private List<BundleActionBean> bundleActions;

    public BundleKillXCommand(String jobId) {
        super("bundle_kill", "bundle_kill", 1);
        this.jobId = ParamChecker.notEmpty(jobId, "jobId");
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
    public void loadState() throws CommandException {
        try {
            this.bundleJob = BundleJobQueryExecutor.getInstance().get(BundleJobQuery.GET_BUNDLE_JOB, jobId);
            this.bundleActions = BundleActionQueryExecutor.getInstance().getList(
                    BundleActionQuery.GET_BUNDLE_ACTIONS_STATUS_UNIGNORED_FOR_BUNDLE, jobId);
            LogUtils.setLogInfo(bundleJob);
            super.setJob(bundleJob);

        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

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
        else {
            // Due to race condition bundle action pending might be true
            // while coordinator is killed.
            if (action.isPending()) {
                if (action.getCoordId() == null) {
                    action.setPending(0);
                }
                else {
                    try {
                        CoordinatorJobBean coordJob = CoordJobQueryExecutor.getInstance().get(
                                CoordJobQuery.GET_COORD_JOB, action.getCoordId());
                        if (!coordJob.isPending() && coordJob.isTerminalStatus()) {
                            action.setPending(0);
                            action.setStatus(coordJob.getStatus());
                        }
                    }
                    catch (JPAExecutorException e) {
                        LOG.warn("Error in checking coord job status:" + action.getCoordId(), e);
                    }
                }
            }
        }
        updateList.add(new UpdateEntry<BundleActionQuery>(BundleActionQuery.UPDATE_BUNDLE_ACTION_STATUS_PENDING_MODTIME, action));
    }

    @Override
    public void notifyParent() {
    }

    @Override
    public Job getJob() {
        return bundleJob;
    }

    @Override
    public void updateJob() {
        updateList.add(new UpdateEntry<BundleJobQuery>(BundleJobQuery.UPDATE_BUNDLE_JOB_STATUS_PENDING_MODTIME, bundleJob));
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

}
