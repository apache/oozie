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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.Job.Status;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.StatusTransitXCommand;
import org.apache.oozie.command.bundle.BundleStatusUpdateXCommand;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.SchemaService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.StatusTransitService;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.StatusUtils;

/**
 * CoordStatusTransitXCommand update coord job's status according to its child actions' status. If all child actions'
 * pending flag equals 0 (job done), we reset the job's pending flag to 0. If all child actions are succeeded, we set
 * the job's status to SUCCEEDED.
 */
public class CoordStatusTransitXCommand extends StatusTransitXCommand {

    private final String jobId;
    private CoordinatorJobBean coordJob;
    int coordActionCount;
    private final Map<CoordinatorAction.Status, Integer> coordActionStatus = new HashMap<CoordinatorAction.Status, Integer>();
    boolean isPending = false;

    final boolean backwardSupportForCoordStatus = Services.get().getConf()
            .getBoolean(StatusTransitService.CONF_BACKWARD_SUPPORT_FOR_COORD_STATUS, false);

    public CoordStatusTransitXCommand(String jobId) {
        super("coord_status_transit", "coord_status_transit", 0);
        this.jobId = jobId;
    }

    @Override
    public String getEntityKey() {
        return jobId;
    }

    @Override
    protected void loadState() throws CommandException {
        try {
            coordJob = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB, jobId);
            List<CoordinatorActionBean> coordActionStatusList = CoordActionQueryExecutor.getInstance().getList(
                    CoordActionQuery.GET_COORD_ACTIONS_STATUS_UNIGNORED, jobId);

            long count = (Long) CoordActionQueryExecutor.getInstance().getSingleValue(
                    CoordActionQuery.GET_COORD_ACTIONS_PENDING_COUNT, jobId);
            if (count > 0) {
                isPending = true;
            }

            for (CoordinatorAction coordAction : coordActionStatusList) {
                int counter = 0;
                if (coordActionStatus.containsKey(coordAction.getStatus())) {
                    counter = coordActionStatus.get(coordAction.getStatus()) + 1;
                }
                else {
                    ++counter;
                }
                coordActionStatus.put(coordAction.getStatus(), counter);
            }
            coordActionCount = coordActionStatusList.size();
        }
        catch (JPAExecutorException jpae) {
            throw new CommandException(ErrorCode.E1025, jpae);
        }
        LogUtils.setLogInfo(this.coordJob);
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        // if namespace 0.1 is used and backward support is true, then ignore this coord job
        if (backwardSupportForCoordStatus == true && coordJob.getAppNamespace() != null
                && coordJob.getAppNamespace().equals(SchemaService.COORDINATOR_NAMESPACE_URI_1)) {
            throw new CommandException(ErrorCode.E1025,
                    " Coord namespace is 0.1 and backward.support.for.coord.status is set");
        }

    }

    @Override
    protected Job.Status getJobStatus() throws CommandException {
        Job.Status jobStatus = super.getJobStatus();
        if (jobStatus == null) {
            jobStatus = coordJob.getStatus();
        }

        return jobStatus;
    }

    @Override
    protected boolean isTerminalState() {
        return (coordJob.isDoneMaterialization() || coordJob.getStatus() == Job.Status.FAILED ||
                coordJob.getStatus() == Job.Status.KILLED) && isCoordTerminalStatus(coordActionCount);
    }

    @Override
    protected Status getTerminalStatus() {

        // If all coord action is done and coord is killed, then don't change the status.
        if (coordJob.getStatus().equals(Job.Status.KILLED)) {
            return Job.Status.KILLED;

        }
        // If all the coordinator actions are succeeded then coordinator job should be succeeded.
        if (coordActionCount == (getStatusCount(CoordinatorAction.Status.SUCCEEDED)
                + getStatusCount(CoordinatorAction.Status.SKIPPED)) && coordJob.isDoneMaterialization()) {
            return Job.Status.SUCCEEDED;

        }
        else if (coordActionCount == getStatusCount(CoordinatorAction.Status.KILLED)) {
            // If all the coordinator actions are KILLED then coordinator job should be KILLED.
            return Job.Status.KILLED;

        }
        else if (coordActionCount == getStatusCount(CoordinatorAction.Status.FAILED)) {
            // If all the coordinator actions are FAILED then coordinator job should be FAILED.
            return Job.Status.FAILED;

        }
        else {
            return Job.Status.DONEWITHERROR;
        }
    }

    @Override
    protected boolean isPausedState() {
        return coordJob.getStatus().equals(Job.Status.PAUSED)
                || coordJob.getStatus().equals(Job.Status.PAUSEDWITHERROR);
    }

    @Override
    protected Status getPausedState() {
        return hasTerminatedActions() ? Job.Status.PAUSEDWITHERROR : Job.Status.PAUSED;
    }

    @Override
    protected boolean isSuspendedState() {
        if (coordJob.getStatus() == Job.Status.SUSPENDED
                || coordJob.getStatus() == Job.Status.SUSPENDEDWITHERROR
                || coordJob.getStatus() == Job.Status.PREPSUSPENDED) {
            return true;
        }
        else {
            return getBottomUpSuspendedState() != null;
        }
    }

    @Override
    protected Status getSuspendedStatus() {
        if (coordJob.getStatus() == Job.Status.SUSPENDED || coordJob.getStatus() == Job.Status.SUSPENDEDWITHERROR) {
            return hasTerminatedActions() ? Job.Status.SUSPENDEDWITHERROR : Job.Status.SUSPENDED;
        } else if (coordJob.getStatus() == Job.Status.PREPSUSPENDED) {
            return Job.Status.PREPSUSPENDED;
        }
        else {
            return getBottomUpSuspendedState();
        }
    }

    @Override
    protected boolean isRunningState() {
        return coordJob.getStatus() != Job.Status.PREP;
    }

    @Override
    protected Status getRunningState() {
        return hasTerminatedActions() ? Job.Status.RUNNINGWITHERROR : Job.Status.RUNNING;
    }

    @Override
    protected void updateJobStatus(Status coordStatus) throws JPAExecutorException, CommandException {
        final Job.Status prevStatus = coordJob.getStatus();

        boolean prevPending = coordJob.isPending();
        if (isPending) {
            coordJob.setPending();
        }
        else {
            coordJob.resetPending();
        }
        boolean isPendingStateChanged = prevPending != coordJob.isPending();

        // Update the Coord Job
        if (coordJob.isTerminalStatus()
                && (coordStatus == Job.Status.SUSPENDED || coordStatus == Job.Status.SUSPENDEDWITHERROR)) {
            LOG.info("Coord Job [" + coordJob.getId() + "] status to " + coordStatus
                    + " can not be updated as its already in Terminal state");
            if (isPendingStateChanged) {
                LOG.info("Pending for job  [" + coordJob.getId() + "] is changed to to '" + coordJob.isPending()
                        + "' from '" + prevStatus + "'");
                coordJob.setLastModifiedTime(new Date());
                CoordJobQueryExecutor.getInstance().executeUpdate(
                        CoordJobQuery.UPDATE_COORD_JOB_STATUS_PENDING_MODTIME, coordJob);
            }
            return;

        }

        // Check for backward support when RUNNINGWITHERROR, SUSPENDEDWITHERROR and PAUSEDWITHERROR is
        // not supported
        coordJob.setStatus(StatusUtils.getStatusIfBackwardSupportTrue(coordStatus));
        // Backward support when coordinator namespace is 0.1
        coordJob.setStatus(StatusUtils.getStatus(coordJob));
        if (coordJob.getStatus() != prevStatus || isPendingStateChanged) {
            LOG.info("Set coordinator job [" + coordJob.getId() + "] status to '" + coordJob.getStatus() + "' from '"
                    + prevStatus + "'");
            coordJob.setLastModifiedTime(new Date());
            CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB_STATUS_PENDING_MODTIME,
                    coordJob);
        }
        // update bundle action only when status changes in coord job
        if (coordJob.getBundleId() != null) {
            if (!prevStatus.equals(coordJob.getStatus())) {
                new BundleStatusUpdateXCommand(coordJob, prevStatus).call();
            }
        }
    }

    /**
     * Bottom up look for children to check the parent's status only if materialization is done and all actions are
     * non-pending.
     *
     * @return the bottom up suspended state
     */
    protected Job.Status getBottomUpSuspendedState() {
        if (coordJob.isDoneMaterialization() && !isPending
                && coordActionStatus.containsKey(CoordinatorAction.Status.SUSPENDED)) {

            if (coordActionCount == coordActionStatus.get(CoordinatorAction.Status.SUSPENDED)
                    + getStatusCount(CoordinatorAction.Status.SUCCEEDED)) {
                return Job.Status.SUSPENDED;

            }
            else if (coordActionCount == coordActionStatus.get(CoordinatorAction.Status.SUSPENDED)
                    + getStatusCount(CoordinatorAction.Status.SUCCEEDED)
                    + getStatusCount(CoordinatorAction.Status.KILLED) + getStatusCount(CoordinatorAction.Status.FAILED)
                    + getStatusCount(CoordinatorAction.Status.TIMEDOUT)) {
                return Job.Status.SUSPENDEDWITHERROR;

            }
        }
        return null;
    }

    private boolean isCoordTerminalStatus(int coordActionsCount) {
        return coordActionsCount == getStatusCount(CoordinatorAction.Status.SUCCEEDED)
                + getStatusCount(CoordinatorAction.Status.FAILED) + getStatusCount(CoordinatorAction.Status.KILLED)
                + getStatusCount(CoordinatorAction.Status.TIMEDOUT) + getStatusCount(CoordinatorAction.Status.SKIPPED);

    }

    private int getStatusCount(CoordinatorAction.Status status) {
        int statusCount = 0;
        if (coordActionStatus.containsKey(status)) {
            statusCount = coordActionStatus.get(status);
        }
        return statusCount;
    }

    private boolean hasTerminatedActions() {
        return coordActionStatus.containsKey(CoordinatorAction.Status.KILLED)
                || coordActionStatus.containsKey(CoordinatorAction.Status.FAILED)
                || coordActionStatus.containsKey(CoordinatorAction.Status.TIMEDOUT);
    }

}
