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
import java.util.HashMap;
import java.util.List;

import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.Job.Status;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.StatusTransitXCommand;
import org.apache.oozie.executor.jpa.BundleActionQueryExecutor;
import org.apache.oozie.executor.jpa.BundleJobQueryExecutor;
import org.apache.oozie.executor.jpa.BundleActionQueryExecutor.BundleActionQuery;
import org.apache.oozie.executor.jpa.BundleJobQueryExecutor.BundleJobQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.StatusUtils;

/**
 * BundleStatusTransitXCommand update job's status according to its child actions' status. If all child actions' pending
 * flag equals 0 (job done), we reset the job's pending flag to 0. If all child actions are succeeded, we set the job's
 * status to SUCCEEDED.
 */
public class BundleStatusTransitXCommand extends StatusTransitXCommand {

    private String jobId;
    private List<BundleActionBean> bundleActions;
    private BundleJobBean bundleJob;
    private boolean foundPending;
    private HashMap<Job.Status, Integer> bundleActionStatus = new HashMap<Job.Status, Integer>();

    public BundleStatusTransitXCommand(String id) {
        super("bundle_status_transit", "bundle_status_transit", 0);
        this.jobId = id;
    }

    @Override
    public String getEntityKey() {
        return jobId;
    }

    @Override
    protected void loadState() throws CommandException {
        try {
            bundleJob = BundleJobQueryExecutor.getInstance().get(
                    BundleJobQuery.GET_BUNDLE_JOB_ID_STATUS_PENDING_MODTIME, jobId);

            bundleActions = BundleActionQueryExecutor.getInstance().getList(
                    BundleActionQuery.GET_BUNDLE_UNIGNORED_ACTION_STATUS_PENDING_FOR_BUNDLE, jobId);
            for (BundleActionBean bAction : bundleActions) {
                int counter = 0;
                if (bundleActionStatus.containsKey(bAction.getStatus())) {
                    counter = bundleActionStatus.get(bAction.getStatus()) + 1;
                }
                else {
                    ++counter;
                }
                bundleActionStatus.put(bAction.getStatus(), counter);
                if (bAction.getCoordId() == null
                        && (bAction.getStatus() == Job.Status.FAILED || bAction.getStatus() == Job.Status.KILLED) ) {
                    new BundleKillXCommand(jobId).call();
                    bundleJob = BundleJobQueryExecutor.getInstance().get(
                            BundleJobQuery.GET_BUNDLE_JOB_ID_STATUS_PENDING_MODTIME, jobId);
                    bundleJob.setStatus(Job.Status.FAILED);
                    bundleJob.setLastModifiedTime(new Date());
                    BundleJobQueryExecutor.getInstance().executeUpdate(BundleJobQuery.UPDATE_BUNDLE_JOB_STATUS,
                            bundleJob);
                }

                if (bAction.isPending()) {
                    LOG.debug(bAction + " has pending flag set");
                    foundPending = true;
                }
            }
            LogUtils.setLogInfo(bundleJob);
        }
        catch (JPAExecutorException e) {
            throw new CommandException(ErrorCode.E1322, e);
        }
    }

    @Override
    protected Job.Status getJobStatus() throws CommandException {
        Job.Status jobStatus = super.getJobStatus();
        if (jobStatus == null) {
            if (isPrepRunningState()) {
                return getPrepRunningStatus();
            }
        }

        return jobStatus;
    }

    @Override
    protected boolean isTerminalState() {
        return !foundPending
                && bundleActions.size() == getActionStatusCount(Job.Status.SUCCEEDED)
                        + getActionStatusCount(Job.Status.FAILED) + getActionStatusCount(Job.Status.KILLED)
                        + getActionStatusCount(Job.Status.DONEWITHERROR);
    }

    @Override
    protected Job.Status getTerminalStatus() {

        // If all bundle action is done and bundle is killed, then don't change the status.
        if (bundleJob.getStatus().equals(Job.Status.KILLED)) {
            return Job.Status.KILLED;

        }
        // If all the bundle actions are succeeded then bundle job should be succeeded.
        if (bundleActions.size() == getActionStatusCount(Job.Status.SUCCEEDED)) {
            return Job.Status.SUCCEEDED;

        }
        else if (bundleActions.size() == getActionStatusCount(Job.Status.KILLED)) {
            // If all the bundle actions are KILLED then bundle job should be KILLED.
            return Job.Status.KILLED;
        }
        else if (bundleActions.size() == getActionStatusCount(Job.Status.FAILED)) {
            // If all the bundle actions are FAILED then bundle job should be FAILED.
            return Job.Status.FAILED;
        }
        else {
            return Job.Status.DONEWITHERROR;

        }
    }

    @Override
    protected boolean isPausedState() {

        if (bundleJob.getStatus() == Job.Status.PAUSED || bundleJob.getStatus() == Job.Status.PAUSEDWITHERROR) {
            return true;
        }
        else {
            return getBottomUpPauseStatus() != null;
        }
    }

    @Override
    protected Job.Status getPausedState() {
        if (bundleJob.getStatus() == Job.Status.PAUSED || bundleJob.getStatus() == Job.Status.PAUSEDWITHERROR) {
            if (hasTerminatedActions() || bundleActionStatus.containsKey(Job.Status.SUSPENDEDWITHERROR)
                    || bundleActionStatus.containsKey(Job.Status.RUNNINGWITHERROR)
                    || bundleActionStatus.containsKey(Job.Status.PAUSEDWITHERROR)) {
                return Job.Status.PAUSEDWITHERROR;
            }
            else {
                return Job.Status.PAUSED;
            }
        }
        return getBottomUpPauseStatus();

    }

    @Override
    protected boolean isSuspendedState() {
        if (bundleJob.getStatus() == Job.Status.SUSPENDED || bundleJob.getStatus() == Job.Status.SUSPENDEDWITHERROR) {
            return true;
        }

        return getBottomUpSuspendedState() != null;

    }

    @Override
    protected Job.Status getSuspendedStatus() {
        if (bundleJob.getStatus() == Job.Status.SUSPENDED || bundleJob.getStatus() == Job.Status.SUSPENDEDWITHERROR) {
            if (hasTerminatedActions() || bundleActionStatus.containsKey(Job.Status.SUSPENDEDWITHERROR)
                    || bundleActionStatus.containsKey(Job.Status.PAUSEDWITHERROR)) {
                return Job.Status.SUSPENDEDWITHERROR;
            }
            else {
                return Job.Status.SUSPENDED;
            }

        }
        return getBottomUpSuspendedState();

    }

    @Override
    protected boolean isRunningState() {
        return true;
    }

    @Override
    protected Status getRunningState() {
        if (bundleJob.getStatus() != Job.Status.PREP) {
            return getRunningStatus(bundleActionStatus);
        }
        else
            return null;
    }

    @Override
    protected void updateJobStatus(Job.Status bundleStatus) throws JPAExecutorException {
        LOG.info("Set bundle job [" + jobId + "] status to '" + bundleStatus + "' from '" + bundleJob.getStatus() + "'");

        String jobId = bundleJob.getId();
        // Update the Bundle Job
        // Check for backward support when RUNNINGWITHERROR, SUSPENDEDWITHERROR and
        // PAUSEDWITHERROR is not supported
        bundleJob.setStatus(StatusUtils.getStatusIfBackwardSupportTrue(bundleStatus));
        if (foundPending) {
            bundleJob.setPending();
            LOG.info("Bundle job [" + jobId + "] Pending set to TRUE");
        }
        else {
            bundleJob.resetPending();
            LOG.info("Bundle job [" + jobId + "] Pending set to FALSE");
        }
        BundleJobQueryExecutor.getInstance().executeUpdate(BundleJobQuery.UPDATE_BUNDLE_JOB_STATUS_PENDING_MODTIME,
                bundleJob);
    }

    /**
     * bottom up; check the status of parent through their children.
     *
     * @return the bottom up pause status
     */
    private Job.Status getBottomUpPauseStatus() {

        if (bundleActionStatus.containsKey(Job.Status.PAUSED)
                && bundleActions.size() == bundleActionStatus.get(Job.Status.PAUSED)) {
            return Job.Status.PAUSED;

        }
        else if (bundleActionStatus.containsKey(Job.Status.PAUSEDWITHERROR)
                && bundleActions.size() == getActionStatusCount(Job.Status.PAUSED)
                        + getActionStatusCount(Job.Status.PAUSEDWITHERROR)) {
            return Job.Status.PAUSEDWITHERROR;
        }

        return null;
    }

    /**
     * Bottom up update status of parent from the status of its children.
     *
     * @return the bottom up suspended state
     */
    private Job.Status getBottomUpSuspendedState() {

        if (!foundPending && bundleActionStatus.containsKey(Job.Status.SUSPENDED)
                || bundleActionStatus.containsKey(Job.Status.SUSPENDEDWITHERROR)) {

            if (bundleActions.size() == getActionStatusCount(Job.Status.SUSPENDED)
                    + getActionStatusCount(Job.Status.SUCCEEDED)) {
                return Job.Status.SUSPENDED;
            }
            else if (bundleActions.size() == getActionStatusCount(Job.Status.SUSPENDEDWITHERROR)
                    + bundleActionStatus.get(Job.Status.SUSPENDED) + getActionStatusCount(Job.Status.SUCCEEDED)
                    + getActionStatusCount(Job.Status.KILLED) + getActionStatusCount(Job.Status.FAILED)
                    + getActionStatusCount(Job.Status.DONEWITHERROR)) {
                return Job.Status.SUSPENDEDWITHERROR;

            }
        }
        return null;
    }

    private boolean hasTerminatedActions() {
        return bundleActionStatus.containsKey(Job.Status.KILLED) || bundleActionStatus.containsKey(Job.Status.FAILED)
                || bundleActionStatus.containsKey(Job.Status.DONEWITHERROR);

    }

    private boolean isPrepRunningState() {
        return !foundPending && bundleActionStatus.containsKey(Job.Status.PREP)
                && bundleActions.size() > bundleActionStatus.get(Job.Status.PREP);
    }

    private Status getPrepRunningStatus() {
        return getRunningStatus(bundleActionStatus);

    }

    private int getActionStatusCount(final Job.Status status) {

        if (bundleActionStatus.containsKey(status)) {
            return bundleActionStatus.get(status);
        }
        else {
            return 0;
        }
    }

    private Job.Status getRunningStatus(HashMap<Job.Status, Integer> actionStatus) {
        if (actionStatus.containsKey(Job.Status.FAILED) || actionStatus.containsKey(Job.Status.KILLED)
                || actionStatus.containsKey(Job.Status.DONEWITHERROR)
                || actionStatus.containsKey(Job.Status.RUNNINGWITHERROR)) {
            return Job.Status.RUNNINGWITHERROR;
        }
        else {
            return Job.Status.RUNNING;
        }
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }

}
