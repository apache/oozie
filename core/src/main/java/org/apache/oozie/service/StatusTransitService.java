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
package org.apache.oozie.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.Job;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.bundle.BundleKillXCommand;
import org.apache.oozie.command.bundle.BundleStatusUpdateXCommand;
import org.apache.oozie.executor.jpa.BundleActionsGetByLastModifiedTimeJPAExecutor;
import org.apache.oozie.executor.jpa.BundleActionsGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleActionsGetStatusPendingJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobsGetRunningOrPendingJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionsGetByLastModifiedTimeJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetActionsStatusJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetPendingActionsCountJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobsGetPendingJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.MemoryLocks;
import org.apache.oozie.util.StatusUtils;
import org.apache.oozie.util.XLog;

/**
 * StateTransitService is scheduled to run at the configured interval.
 * <p/>
 * It is to update job's status according to its child actions' status. If all child actions' pending flag equals 0 (job
 * done), we reset the job's pending flag to 0. If all child actions are succeeded, we set the job's status to
 * SUCCEEDED.
 */
public class StatusTransitService implements Service {
    public static final String CONF_PREFIX = Service.CONF_PREFIX + "StatusTransitService.";
    public static final String CONF_STATUSTRANSIT_INTERVAL = CONF_PREFIX + "statusTransit.interval";
    public static final String CONF_BACKWARD_SUPPORT_FOR_COORD_STATUS = CONF_PREFIX + "backward.support.for.coord.status";
    public static final String CONF_BACKWARD_SUPPORT_FOR_STATES_WITHOUT_ERROR = CONF_PREFIX + "backward.support.for.states.without.error";
    private static int limit = -1;
    private static Date lastInstanceStartTime = null;
    private final static XLog LOG = XLog.getLog(StatusTransitRunnable.class);

    /**
     * StateTransitRunnable is the runnable which is scheduled to run at the configured interval.
     * <p/>
     * It is to update job's status according to its child actions' status. If all child actions' pending flag equals 0
     * (job done), we reset the job's pending flag to 0. If all child actions are succeeded, we set the job's status to
     * SUCCEEDED.
     */
    static class StatusTransitRunnable implements Runnable {
        private JPAService jpaService = null;
        private MemoryLocks.LockToken lock;

        public StatusTransitRunnable() {
            jpaService = Services.get().get(JPAService.class);
            if (jpaService == null) {
                LOG.error("Missing JPAService");
            }
        }

        @Override
        public void run() {
            try {
                Date curDate = new Date(); // records the start time of this service run;

                // first check if there is some other instance running;
                lock = Services.get().get(MemoryLocksService.class).getWriteLock(StatusTransitService.class.getName(),
                        lockTimeout);
                if (lock == null) {
                    LOG.info("This StatusTransitService instance"
                            + " will not run since there is already an instance running");
                }
                else {
                    LOG.info("Acquired lock for [{0}]", StatusTransitService.class.getName());
                    // running coord jobs transit service
                    coordTransit();
                    // running bundle jobs transit service
                    bundleTransit();

                    lastInstanceStartTime = curDate;
                }
            }
            catch (Exception ex) {
                LOG.warn("Exception happened during StatusTransitRunnable ", ex);
            }
            finally {
                // release lock;
                if (lock != null) {
                    lock.release();
                    LOG.info("Released lock for [{0}]", StatusTransitService.class.getName());
                }
            }
        }

        public List<BundleJobBean> removeDuplicates(List<BundleJobBean> pendingJobList) {
            Set<BundleJobBean> s = new TreeSet<BundleJobBean>(new Comparator<BundleJobBean>() {
                @Override
                public int compare(BundleJobBean b1, BundleJobBean b2) {
                    if (b1.getId().equals(b2.getId())) {
                        return 0;
                    }
                    else
                        return 1;
                }
            });
            s.addAll(pendingJobList);
            return new ArrayList<BundleJobBean>(s);
        }

        /**
         * Aggregate bundle actions' status to bundle jobs
         *
         * @throws JPAExecutorException thrown if failed in db updates or retrievals
         * @throws CommandException thrown if failed to run commands
         */
        private void bundleTransit() throws JPAExecutorException, CommandException {
            List<BundleJobBean> pendingJobCheckList = null;

            if (lastInstanceStartTime == null) {
                LOG.info("Running bundle status service first instance");
                // this is the first instance, we need to check for all pending or running jobs;
                pendingJobCheckList = jpaService.execute(new BundleJobsGetRunningOrPendingJPAExecutor(limit));
            }
            else {
                LOG.info("Running bundle status service from last instance time =  "
                        + DateUtils.formatDateOozieTZ(lastInstanceStartTime));
                // this is not the first instance, we should only check jobs that have actions been
                // updated >= start time of last service run;
                List<BundleActionBean> actionList = jpaService
                        .execute(new BundleActionsGetByLastModifiedTimeJPAExecutor(lastInstanceStartTime));
                Set<String> bundleIds = new HashSet<String>();
                for (BundleActionBean action : actionList) {
                    bundleIds.add(action.getBundleId());
                }
                pendingJobCheckList = new ArrayList<BundleJobBean>();
                for (String bundleId : bundleIds.toArray(new String[bundleIds.size()])) {
                    BundleJobBean bundle = jpaService.execute(new BundleJobGetJPAExecutor(bundleId));
                    // Running bundle job might have pending false
                    if (bundle.isPending() || bundle.getStatus().equals(Job.Status.RUNNING)
                            || bundle.getStatus().equals(Job.Status.RUNNINGWITHERROR)
                            || bundle.getStatus().equals(Job.Status.PAUSED)
                            || bundle.getStatus().equals(Job.Status.PAUSEDWITHERROR)) {
                        pendingJobCheckList.add(bundle);
                    }
                }
            }
            aggregateBundleJobsStatus(pendingJobCheckList);
        }

        private void aggregateBundleJobsStatus(List<BundleJobBean> bundleLists) throws JPAExecutorException,
                CommandException {
            if (bundleLists != null) {
                    for (BundleJobBean bundleJob : bundleLists) {
                        try {
                            String jobId = bundleJob.getId();
                            Job.Status[] bundleStatus = new Job.Status[1];
                            bundleStatus[0] = bundleJob.getStatus();
                            List<BundleActionBean> bundleActions = jpaService.execute(new BundleActionsGetStatusPendingJPAExecutor(
                                    jobId));
                            HashMap<Job.Status, Integer> bundleActionStatus = new HashMap<Job.Status, Integer>();
                            boolean foundPending = false;
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
                                            && (bAction.getStatus() == Job.Status.FAILED || bAction.getStatus() == Job.Status.KILLED)) {
                                        (new BundleKillXCommand(jobId)).call();
                                        LOG.info("Bundle job ["+ jobId
                                                        + "] has been killed since one of its coordinator job failed submission.");
                                    }

                                 if (bAction.isPending()) {
                                    foundPending = true;
                                }
                            }

                            if (!foundPending && checkTerminalStatus(bundleActionStatus, bundleActions, bundleStatus)) {
                                LOG.info("Set bundle job [" + jobId + "] status to '" + bundleStatus[0].toString()
                                        + "' from '" + bundleJob.getStatus() + "'");
                                updateBundleJob(foundPending, bundleJob, bundleStatus[0]);
                            }
                            else if (!foundPending && checkPrepStatus(bundleActionStatus, bundleActions, bundleStatus)) {
                                LOG.info("Set bundle job [" + jobId + "] status to '" + bundleStatus[0].toString()
                                        + "' from '" + bundleJob.getStatus() + "'");
                                updateBundleJob(foundPending, bundleJob, bundleStatus[0]);
                            }
                            else if (checkPausedStatus(bundleActionStatus, bundleActions, bundleStatus)) {
                                LOG.info("Set bundle job [" + jobId + "] status to '" + bundleStatus[0].toString()
                                        + "' from '" + bundleJob.getStatus() + "'");
                                updateBundleJob(foundPending, bundleJob, bundleStatus[0]);
                            }
                            else if (checkSuspendStatus(bundleActionStatus, bundleActions, bundleStatus, foundPending)) {
                                LOG.info("Set bundle job [" + jobId + "] status to '" + bundleStatus[0].toString()
                                        + "' from '" + bundleJob.getStatus() + "'");
                                updateBundleJob(foundPending, bundleJob, bundleStatus[0]);
                            }
                            else if (checkRunningStatus(bundleActionStatus, bundleActions, bundleStatus)) {
                                LOG.info("Set bundle job [" + jobId + "] status to '" + bundleStatus[0].toString()
                                        + "' from '" + bundleJob.getStatus() + "'");
                                updateBundleJob(foundPending, bundleJob, bundleStatus[0]);
                            }
                        }
                        catch (Exception ex) {
                            LOG.error("Exception happened during aggregate bundle job's status, job = "
                                    + bundleJob.getId(), ex);
                        }
                    }
                }

        }

        private void aggregateCoordJobsStatus(List<CoordinatorJobBean> CoordList) throws JPAExecutorException,
                CommandException {
            if (CoordList != null) {
                Configuration conf = Services.get().getConf();
                boolean backwardSupportForCoordStatus = conf.getBoolean(CONF_BACKWARD_SUPPORT_FOR_COORD_STATUS, false);

                for (CoordinatorJobBean coordJob : CoordList) {
                    try {
                        // if namespace 0.1 is used and backward support is true, then ignore this coord job
                        if (backwardSupportForCoordStatus == true && coordJob.getAppNamespace() != null
                                && coordJob.getAppNamespace().equals(SchemaService.COORDINATOR_NAMESPACE_URI_1)) {
                            continue;
                        }
                        String jobId = coordJob.getId();
                        Job.Status[] coordStatus = new Job.Status[1];
                        coordStatus[0] = coordJob.getStatus();
                        //Get count of Coordinator actions with pending true
                        boolean isPending = false;
                        int count = jpaService.execute(new CoordJobGetPendingActionsCountJPAExecutor(jobId));
                        if (count > 0) {
                             isPending = true;
                        }
                        // Get status of Coordinator actions
                        List<CoordinatorAction.Status> coordActionStatusList = jpaService
                                .execute(new CoordJobGetActionsStatusJPAExecutor(jobId));
                        HashMap<CoordinatorAction.Status, Integer> coordActionStatus = new HashMap<CoordinatorAction.Status, Integer>();

                        for (CoordinatorAction.Status status : coordActionStatusList) {
                            int counter = 0;
                            if (coordActionStatus.containsKey(status)) {
                                counter = coordActionStatus.get(status) + 1;
                            }
                            else {
                                ++counter;
                            }
                            coordActionStatus.put(status, counter);
                        }

                        int nonPendingCoordActionsCount = coordActionStatusList.size();
                        boolean isDoneMaterialization = coordJob.isDoneMaterialization();
                        if ((isDoneMaterialization || coordStatus[0] == Job.Status.FAILED || coordStatus[0] == Job.Status.KILLED)
                                && checkCoordTerminalStatus(coordActionStatus, nonPendingCoordActionsCount,
                                        coordStatus, isDoneMaterialization)) {
                            LOG.info("Set coordinator job [" + jobId + "] status to '" + coordStatus[0].toString()
                                    + "' from '" + coordJob.getStatus() + "'");
                            updateCoordJob(isPending, coordJob, coordStatus[0]);
                        }
                        else if (checkCoordPausedStatus(coordActionStatus, nonPendingCoordActionsCount, coordStatus)) {
                            LOG.info("Set coordinator job [" + jobId + "] status to " + coordStatus[0].toString()
                                    + "' from '" + coordJob.getStatus() + "'");
                            updateCoordJob(isPending, coordJob, coordStatus[0]);
                        }
                        else if(checkCoordSuspendStatus( coordActionStatus, nonPendingCoordActionsCount, coordStatus, coordJob.isDoneMaterialization(), isPending)) {
                            LOG.info("Set coordinator job [" + jobId + "] status to " + coordStatus[0].toString()
                                    + "' from '" + coordJob.getStatus() + "'");
                            updateCoordJob(isPending, coordJob, coordStatus[0]);
                        }
                        else if (checkCoordRunningStatus(coordActionStatus, nonPendingCoordActionsCount, coordStatus)) {
                            LOG.info("Set coordinator job [" + jobId + "] status to " + coordStatus[0].toString()
                                    + "' from '" + coordJob.getStatus() + "'");
                            updateCoordJob(isPending, coordJob, coordStatus[0]);
                        }
                        else {
                            checkCoordPending(isPending, coordJob, true);
                        }
                    }
                    catch (Exception ex) {
                        LOG.error("Exception happened during aggregate coordinator job's status, job = "
                                + coordJob.getId(), ex);
                    }
                }

            }
        }

        private boolean checkTerminalStatus(HashMap<Job.Status, Integer> bundleActionStatus,
                List<BundleActionBean> bundleActions, Job.Status[] bundleStatus) {
            boolean ret = false;
            int totalValuesSucceed = 0;
            if (bundleActionStatus.containsKey(Job.Status.SUCCEEDED)) {
                totalValuesSucceed = bundleActionStatus.get(Job.Status.SUCCEEDED);
            }
            int totalValuesFailed = 0;
            if (bundleActionStatus.containsKey(Job.Status.FAILED)) {
                totalValuesFailed = bundleActionStatus.get(Job.Status.FAILED);
            }
            int totalValuesKilled = 0;
            if (bundleActionStatus.containsKey(Job.Status.KILLED)) {
                totalValuesKilled = bundleActionStatus.get(Job.Status.KILLED);
            }

            int totalValuesDoneWithError = 0;
            if (bundleActionStatus.containsKey(Job.Status.DONEWITHERROR)) {
                totalValuesDoneWithError = bundleActionStatus.get(Job.Status.DONEWITHERROR);
            }

            if (bundleActions.size() == (totalValuesSucceed + totalValuesFailed + totalValuesKilled + totalValuesDoneWithError)) {
                // If all the bundle actions are succeeded then bundle job should be succeeded.
                if (bundleActions.size() == totalValuesSucceed) {
                    bundleStatus[0] = Job.Status.SUCCEEDED;
                    ret = true;
                }
                else if (bundleActions.size() == totalValuesKilled) {
                    // If all the bundle actions are KILLED then bundle job should be KILLED.
                    bundleStatus[0] = Job.Status.KILLED;
                    ret = true;
                }
                else if (bundleActions.size() == totalValuesFailed) {
                    // If all the bundle actions are FAILED then bundle job should be FAILED.
                    bundleStatus[0] = Job.Status.FAILED;
                    ret = true;
                }
                else {
                    bundleStatus[0] = Job.Status.DONEWITHERROR;
                    ret = true;
                }
            }
            return ret;
        }

        private boolean checkCoordTerminalStatus(HashMap<CoordinatorAction.Status, Integer> coordActionStatus,
                int coordActionsCount, Job.Status[] coordStatus, boolean isDoneMaterialization) {
            boolean ret = false;
            int totalValuesSucceed = 0;
            if (coordActionStatus.containsKey(CoordinatorAction.Status.SUCCEEDED)) {
                totalValuesSucceed = coordActionStatus.get(CoordinatorAction.Status.SUCCEEDED);
            }
            int totalValuesFailed = 0;
            if (coordActionStatus.containsKey(CoordinatorAction.Status.FAILED)) {
                totalValuesFailed = coordActionStatus.get(CoordinatorAction.Status.FAILED);
            }
            int totalValuesKilled = 0;
            if (coordActionStatus.containsKey(CoordinatorAction.Status.KILLED)) {
                totalValuesKilled = coordActionStatus.get(CoordinatorAction.Status.KILLED);
            }

            int totalValuesTimeOut = 0;
            if (coordActionStatus.containsKey(CoordinatorAction.Status.TIMEDOUT)) {
                totalValuesTimeOut = coordActionStatus.get(CoordinatorAction.Status.TIMEDOUT);
            }

            if (coordActionsCount == (totalValuesSucceed + totalValuesFailed + totalValuesKilled + totalValuesTimeOut)) {
                // If all the coordinator actions are succeeded then coordinator job should be succeeded.
                if (coordActionsCount == totalValuesSucceed && isDoneMaterialization) {
                    coordStatus[0] = Job.Status.SUCCEEDED;
                    ret = true;
                }
                else if (coordActionsCount == totalValuesKilled) {
                    // If all the coordinator actions are KILLED then coordinator job should be KILLED.
                    coordStatus[0] = Job.Status.KILLED;
                    ret = true;
                }
                else if (coordActionsCount == totalValuesFailed) {
                    // If all the coordinator actions are FAILED then coordinator job should be FAILED.
                    coordStatus[0] = Job.Status.FAILED;
                    ret = true;
                }
                else {
                    coordStatus[0] = Job.Status.DONEWITHERROR;
                    ret = true;
                }
            }
            return ret;
        }

        private boolean checkPrepStatus(HashMap<Job.Status, Integer> bundleActionStatus,
                List<BundleActionBean> bundleActions, Job.Status[] bundleStatus) {
            boolean ret = false;
            if (bundleActionStatus.containsKey(Job.Status.PREP)) {
                // If all the bundle actions are PREP then bundle job should be RUNNING.
                if (bundleActions.size() > bundleActionStatus.get(Job.Status.PREP)) {
                    bundleStatus[0] = Job.Status.RUNNING;
                    ret = true;
                }
            }
            return ret;
        }

        private boolean checkPausedStatus(HashMap<Job.Status, Integer> bundleActionStatus,
                List<BundleActionBean> bundleActions, Job.Status[] bundleJobStatus) {
            boolean ret = false;

            // TODO - When bottom up cmds are allowed to change the status of parent job,
            // if none of the bundle actions are in paused or pausedwitherror, the function should return
            // false

            // top down
            // If the bundle job is PAUSED or PAUSEDINERROR and no children are in error
            // state, then job should be PAUSED otherwise it should be pausedwitherror
            if (bundleJobStatus[0] == Job.Status.PAUSED || bundleJobStatus[0] == Job.Status.PAUSEDWITHERROR) {
                if (bundleActionStatus.containsKey(Job.Status.KILLED)
                        || bundleActionStatus.containsKey(Job.Status.FAILED)
                        || bundleActionStatus.containsKey(Job.Status.DONEWITHERROR)
                        || bundleActionStatus.containsKey(Job.Status.SUSPENDEDWITHERROR)
                        || bundleActionStatus.containsKey(Job.Status.RUNNINGWITHERROR)
                        || bundleActionStatus.containsKey(Job.Status.PAUSEDWITHERROR)) {
                    bundleJobStatus[0] = Job.Status.PAUSEDWITHERROR;
                }
                else {
                    bundleJobStatus[0] = Job.Status.PAUSED;
                }
                ret = true;
            }

            // bottom up; check the status of parent through their children
            else if (bundleActionStatus.containsKey(Job.Status.PAUSED)
                    && (bundleActions.size() == bundleActionStatus.get(Job.Status.PAUSED))) {
                bundleJobStatus[0] = Job.Status.PAUSED;
                ret = true;
            }
            else if (bundleActionStatus.containsKey(Job.Status.PAUSEDWITHERROR)) {
                int pausedActions = bundleActionStatus.containsKey(Job.Status.PAUSED) ? bundleActionStatus
                        .get(Job.Status.PAUSED) : 0;
                if (bundleActions.size() == pausedActions + bundleActionStatus.get(Job.Status.PAUSEDWITHERROR)) {
                    bundleJobStatus[0] = Job.Status.PAUSEDWITHERROR;
                    ret = true;
                }
            }
            else {
                ret = false;
            }
            return ret;
        }


        private boolean checkSuspendStatus(HashMap<Job.Status, Integer> bundleActionStatus,
                List<BundleActionBean> bundleActions, Job.Status[] bundleStatus, boolean isPending) {
            boolean ret = false;

            // TODO - When bottom up cmds are allowed to change the status of parent job,
            // if none of the bundle actions are in suspended or suspendedwitherror, the function should return
            // false

            // top down
            // if job is suspended
            if (bundleStatus[0] == Job.Status.SUSPENDED
                    || bundleStatus[0] == Job.Status.SUSPENDEDWITHERROR) {
                if (bundleActionStatus.containsKey(Job.Status.KILLED)
                        || bundleActionStatus.containsKey(Job.Status.FAILED)
                        || bundleActionStatus.containsKey(Job.Status.DONEWITHERROR)
                        || bundleActionStatus.containsKey(Job.Status.SUSPENDEDWITHERROR)
                        || bundleActionStatus.containsKey(Job.Status.PAUSEDWITHERROR)) {
                    bundleStatus[0] = Job.Status.SUSPENDEDWITHERROR;
                }
                else {
                    bundleStatus[0] = Job.Status.SUSPENDED;
                }
                ret =true;
            }

            // bottom up
            // Update status of parent from the status of its children
            else if (!isPending && bundleActionStatus.containsKey(Job.Status.SUSPENDED)
                    || bundleActionStatus.containsKey(Job.Status.SUSPENDEDWITHERROR)) {
                int succeededActions = bundleActionStatus.containsKey(Job.Status.SUCCEEDED) ? bundleActionStatus
                        .get(Job.Status.SUCCEEDED) : 0;
                int killedActions = bundleActionStatus.containsKey(Job.Status.KILLED) ? bundleActionStatus
                        .get(Job.Status.KILLED) : 0;
                int failedActions = bundleActionStatus.containsKey(Job.Status.FAILED) ? bundleActionStatus
                        .get(Job.Status.FAILED) : 0;
                int doneWithErrorActions = bundleActionStatus.containsKey(Job.Status.DONEWITHERROR) ? bundleActionStatus
                        .get(Job.Status.DONEWITHERROR) : 0;

                if (bundleActions.size() == bundleActionStatus.get(Job.Status.SUSPENDED) + succeededActions) {
                    bundleStatus[0] = Job.Status.SUSPENDED;
                    ret = true;
                }
                else if (bundleActions.size()  == bundleActionStatus.get(Job.Status.SUSPENDEDWITHERROR)
                        + bundleActionStatus.get(Job.Status.SUSPENDED) + succeededActions + killedActions + failedActions + doneWithErrorActions) {
                    bundleStatus[0] = Job.Status.SUSPENDEDWITHERROR;
                    ret = true;
                }
            }
            return ret;

        }

        private boolean checkCoordPausedStatus(HashMap<CoordinatorAction.Status, Integer> coordActionStatus,
                int coordActionsCount, Job.Status[] coordStatus){
            boolean ret = false;
            if (coordStatus[0].equals(Job.Status.PAUSED) || coordStatus[0].equals(Job.Status.PAUSEDWITHERROR)) {
                if (coordActionStatus.containsKey(CoordinatorAction.Status.KILLED)
                        || coordActionStatus.containsKey(CoordinatorAction.Status.FAILED)
                        || coordActionStatus.containsKey(CoordinatorAction.Status.TIMEDOUT)) {
                    coordStatus[0] = Job.Status.PAUSEDWITHERROR;
                }
                else {
                    coordStatus[0] = Job.Status.PAUSED;
                }
                ret = true;
            }
            return ret;
        }
        private boolean checkCoordSuspendStatus(HashMap<CoordinatorAction.Status, Integer> coordActionStatus,
                int coordActionsCount, Job.Status[] coordStatus, boolean isDoneMaterialization, boolean isPending) {
            boolean ret = false;

            // TODO - When bottom up cmds are allowed to change the status of parent job
            //if none of the coord actions are in suspended or suspendedwitherror and materialization done is false
            //,then the function should return
            // false

            // top down
            // check for children only when parent is suspended
            if (coordStatus[0] == Job.Status.SUSPENDED || coordStatus[0] == Job.Status.SUSPENDEDWITHERROR) {

                if (coordActionStatus.containsKey(CoordinatorAction.Status.KILLED)
                        || coordActionStatus.containsKey(CoordinatorAction.Status.FAILED)
                        || coordActionStatus.containsKey(CoordinatorAction.Status.TIMEDOUT)) {
                    coordStatus[0] = Job.Status.SUSPENDEDWITHERROR;
                }
                else {
                    coordStatus[0] = Job.Status.SUSPENDED;
                }
                ret = true;
            }
            // bottom up
            // look for children to check the parent's status only if materialization is
            // done and all actions are non-pending
            else if (isDoneMaterialization && !isPending && coordActionStatus.containsKey(CoordinatorAction.Status.SUSPENDED)) {
                int succeededActions = coordActionStatus.containsKey(CoordinatorAction.Status.SUCCEEDED) ? coordActionStatus
                       .get(CoordinatorAction.Status.SUCCEEDED) : 0;
                int killedActions = coordActionStatus.containsKey(CoordinatorAction.Status.KILLED) ? coordActionStatus
                        .get(CoordinatorAction.Status.KILLED) : 0;
                int failedActions = coordActionStatus.containsKey(CoordinatorAction.Status.FAILED) ? coordActionStatus
                        .get(CoordinatorAction.Status.FAILED) : 0;
                int timedoutActions = coordActionStatus.containsKey(CoordinatorAction.Status.TIMEDOUT) ? coordActionStatus
                        .get(CoordinatorAction.Status.TIMEDOUT) : 0;

                if (coordActionsCount == coordActionStatus.get(CoordinatorAction.Status.SUSPENDED) + succeededActions) {
                    coordStatus[0] = Job.Status.SUSPENDED;
                    ret = true;
                }
                else if (coordActionsCount == coordActionStatus.get(CoordinatorAction.Status.SUSPENDED)
                        + succeededActions + killedActions + failedActions + timedoutActions) {
                    coordStatus[0] = Job.Status.SUSPENDEDWITHERROR;
                    ret = true;
                }
            }
            return ret;
        }

        private boolean checkCoordRunningStatus(HashMap<CoordinatorAction.Status, Integer> coordActionStatus,
                int coordActionsCount, Job.Status[] coordStatus) {
            boolean ret = false;
            if (coordStatus[0] != Job.Status.PREP) {
                if (coordActionStatus.containsKey(CoordinatorAction.Status.KILLED)
                        || coordActionStatus.containsKey(CoordinatorAction.Status.FAILED)
                        || coordActionStatus.containsKey(CoordinatorAction.Status.TIMEDOUT)) {
                    coordStatus[0] = Job.Status.RUNNINGWITHERROR;
                }
                else {
                    coordStatus[0] = Job.Status.RUNNING;
                }
                ret = true;
            }
            return ret;
        }

        private boolean checkRunningStatus(HashMap<Job.Status, Integer> bundleActionStatus,
                List<BundleActionBean> bundleActions, Job.Status[] bundleStatus) {
            boolean ret = false;
            if (bundleStatus[0] != Job.Status.PREP) {
                if (bundleActionStatus.containsKey(Job.Status.FAILED)
                        || bundleActionStatus.containsKey(Job.Status.KILLED)
                        || bundleActionStatus.containsKey(Job.Status.DONEWITHERROR)
                        || bundleActionStatus.containsKey(Job.Status.RUNNINGWITHERROR)) {
                    bundleStatus[0] = Job.Status.RUNNINGWITHERROR;
                }
                else {
                    bundleStatus[0] = Job.Status.RUNNING;
                }
                ret = true;
            }
            return ret;

        }

        private void updateBundleJob(boolean isPending, BundleJobBean bundleJob, Job.Status bundleStatus)
                throws JPAExecutorException {
            String jobId = bundleJob.getId();
            // Update the Bundle Job
            // Check for backward support when RUNNINGWITHERROR, SUSPENDEDWITHERROR and
            // PAUSEDWITHERROR is not supported
            bundleJob.setStatus(StatusUtils.getStatusIfBackwardSupportTrue(bundleStatus));
            if (isPending) {
                bundleJob.setPending();
                LOG.info("Bundle job [" + jobId + "] Pending set to TRUE");
            }
            else {
                bundleJob.resetPending();
                LOG.info("Bundle job [" + jobId + "] Pending set to FALSE");
            }
            jpaService.execute(new BundleJobUpdateJPAExecutor(bundleJob));
        }

        private void updateCoordJob(boolean isPending, CoordinatorJobBean coordJob, Job.Status coordStatus)
                throws JPAExecutorException, CommandException {
            Job.Status prevStatus = coordJob.getStatus();
            // Update the Coord Job
            if (coordJob.getStatus() == Job.Status.SUCCEEDED || coordJob.getStatus() == Job.Status.FAILED
                    || coordJob.getStatus() == Job.Status.KILLED || coordJob.getStatus() == Job.Status.DONEWITHERROR) {
                if (coordStatus == Job.Status.SUSPENDED || coordStatus == Job.Status.SUSPENDEDWITHERROR) {
                    LOG.info("Coord Job [" + coordJob.getId()
                            + "] status to "+ coordStatus +" can not be updated as its already in Terminal state");
                    return;
                }
            }

            checkCoordPending(isPending, coordJob, false);
            // Check for backward support when RUNNINGWITHERROR, SUSPENDEDWITHERROR and PAUSEDWITHERROR is
            // not supported
            coordJob.setStatus(StatusUtils.getStatusIfBackwardSupportTrue(coordStatus));
            // Backward support when coordinator namespace is 0.1
            coordJob.setStatus(StatusUtils.getStatus(coordJob));
            coordJob.setLastModifiedTime(new Date());
            jpaService.execute(new CoordJobUpdateJPAExecutor(coordJob));
            // update bundle action only when status changes in coord job
            if (coordJob.getBundleId() != null) {
                if (!prevStatus.equals(coordJob.getStatus())) {
                    BundleStatusUpdateXCommand bundleStatusUpdate = new BundleStatusUpdateXCommand(coordJob, prevStatus);
                    bundleStatusUpdate.call();
                }
            }
        }

        private void checkCoordPending(boolean isPending, CoordinatorJobBean coordJob, boolean saveToDB) throws JPAExecutorException {
            // Checking the coordinator pending should be updated or not
            if (isPending) {
                coordJob.setPending();
                LOG.info("Coord job [" + coordJob.getId() + "] Pending set to TRUE");
            }
            else {
                coordJob.resetPending();
                LOG.info("Coord job [" + coordJob.getId() + "] Pending set to FALSE");
            }

            if (saveToDB) {
                jpaService.execute(new CoordJobUpdateJPAExecutor(coordJob));
            }
        }

        /**
         * Aggregate coordinator actions' status to coordinator jobs
         *
         * @throws JPAExecutorException thrown if failed in db updates or retrievals
         * @throws CommandException thrown if failed to run commands
         */
        private void coordTransit() throws JPAExecutorException, CommandException {
            List<CoordinatorJobBean> pendingJobCheckList = null;
            if (lastInstanceStartTime == null) {
                LOG.info("Running coordinator status service first instance");
                // this is the first instance, we need to check for all pending jobs;
                pendingJobCheckList = jpaService.execute(new CoordJobsGetPendingJPAExecutor(limit));
            }
            else {
                LOG.info("Running coordinator status service from last instance time =  "
                        + DateUtils.formatDateOozieTZ(lastInstanceStartTime));
                // this is not the first instance, we should only check jobs
                // that have actions been
                // updated >= start time of last service run;
                List<String> coordJobIdList = jpaService
                        .execute(new CoordActionsGetByLastModifiedTimeJPAExecutor(lastInstanceStartTime));
                Set<String> coordIds = new HashSet<String>();
                for (String coordJobId : coordJobIdList) {
                    coordIds.add(coordJobId);
                }
                pendingJobCheckList = new ArrayList<CoordinatorJobBean>();
                for (String coordId : coordIds.toArray(new String[coordIds.size()])) {
                    CoordinatorJobBean coordJob;
                    try{
                        coordJob = jpaService.execute(new CoordJobGetJPAExecutor(coordId));
                    }
                    catch (JPAExecutorException jpaee) {
                        if (jpaee.getErrorCode().equals(ErrorCode.E0604)) {
                            LOG.warn("Exception happened during StatusTransitRunnable; Coordinator Job doesn't exist", jpaee);
                            continue;
                        } else {
                            throw jpaee;
                        }
                    }
                    // Running coord job might have pending false
                    Job.Status coordJobStatus = coordJob.getStatus();
                    if (coordJob.isPending() || coordJobStatus.equals(Job.Status.PAUSED)
                            || coordJobStatus.equals(Job.Status.RUNNING)
                            || coordJobStatus.equals(Job.Status.RUNNINGWITHERROR)
                            || coordJobStatus.equals(Job.Status.PAUSEDWITHERROR)) {
                        pendingJobCheckList.add(coordJob);
                    }
                }
            }
            aggregateCoordJobsStatus(pendingJobCheckList);
        }
    }

    /**
     * Initializes the {@link StatusTransitService}.
     *
     * @param services services instance.
     */
    @Override
    public void init(Services services) {
        Configuration conf = services.getConf();
        Runnable stateTransitRunnable = new StatusTransitRunnable();
        services.get(SchedulerService.class).schedule(stateTransitRunnable, 10,
                conf.getInt(CONF_STATUSTRANSIT_INTERVAL, 60), SchedulerService.Unit.SEC);
    }

    /**
     * Destroy the StateTransit Jobs Service.
     */
    @Override
    public void destroy() {
    }

    /**
     * Return the public interface for the purge jobs service.
     *
     * @return {@link StatusTransitService}.
     */
    @Override
    public Class<? extends Service> getInterface() {
        return StatusTransitService.class;
    }
}

