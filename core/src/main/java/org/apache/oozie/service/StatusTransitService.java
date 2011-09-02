/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.Job;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.bundle.BundleKillXCommand;
import org.apache.oozie.command.bundle.BundleStatusUpdateXCommand;
import org.apache.oozie.executor.jpa.BundleActionsGetByLastModifiedTimeJPAExecutor;
import org.apache.oozie.executor.jpa.BundleActionsGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobsGetPendingJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobsGetRunningJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionsGetByLastModifiedTimeJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetActionsJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
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

        /**
         * Aggregate bundle actions' status to bundle jobs
         *
         * @throws JPAExecutorException thrown if failed in db updates or retrievals
         * @throws CommandException thrown if failed to run commands
         */
        private void bundleTransit() throws JPAExecutorException, CommandException {
            List<BundleJobBean> pendingJobCheckList = null;
            List<BundleJobBean> runningJobCheckList = null;
            List<List<BundleJobBean>> bundleLists = new ArrayList<List<BundleJobBean>>();
            if (lastInstanceStartTime == null) {
                LOG.info("Running bundle status service first instance");
                // this is the first instance, we need to check for all pending jobs;
                pendingJobCheckList = jpaService.execute(new BundleJobsGetPendingJPAExecutor(limit));
                runningJobCheckList = jpaService.execute(new BundleJobsGetRunningJPAExecutor(limit));
                bundleLists.add(pendingJobCheckList);
                bundleLists.add(runningJobCheckList);
            }
            else {
                LOG.info("Running bundle status service from last instance time =  "
                        + DateUtils.convertDateToString(lastInstanceStartTime));
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
                    if (bundle.isPending() || bundle.getStatus().equals(Job.Status.RUNNING)) {
                        pendingJobCheckList.add(bundle);
                    }
                }
                runningJobCheckList = pendingJobCheckList;
                bundleLists.add(pendingJobCheckList);
            }
            aggregateBundleJobsStatus(bundleLists);
        }

        private void aggregateBundleJobsStatus(List<List<BundleJobBean>> bundleLists) throws JPAExecutorException,
                CommandException {
            if (bundleLists != null) {
                for (List<BundleJobBean> listBundleBean : bundleLists) {
                    for (BundleJobBean bundleJob : listBundleBean) {
                        try {
                            String jobId = bundleJob.getId();
                            Job.Status[] bundleStatus = new Job.Status[1];
                            bundleStatus[0] = bundleJob.getStatus();
                            List<BundleActionBean> bundleActions = jpaService.execute(new BundleActionsGetJPAExecutor(
                                    jobId));
                            HashMap<Job.Status, Integer> bundleActionStatus = new HashMap<Job.Status, Integer>();
                            boolean foundPending = false;
                            for (BundleActionBean bAction : bundleActions) {
                                if (!bAction.isPending()) {
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
                                }
                                else {
                                    foundPending = true;
                                    break;
                                }
                            }

                            if (foundPending) {
                                continue;
                            }

                            if (checkTerminalStatus(bundleActionStatus, bundleActions, bundleStatus)) {
                                LOG.info("Set bundle job [" + jobId + "] status to '" + bundleStatus[0].toString()
                                        + "' from '" + bundleJob.getStatus() + "'");
                                updateBundleJob(bundleActionStatus, bundleActions, bundleJob, bundleStatus[0]);
                            }
                            else if (checkPrepStatus(bundleActionStatus, bundleActions, bundleStatus)) {
                                LOG.info("Set bundle job [" + jobId + "] status to '" + bundleStatus[0].toString()
                                        + "' from '" + bundleJob.getStatus() + "'");
                                updateBundleJob(bundleActionStatus, bundleActions, bundleJob, bundleStatus[0]);
                            }
                            else if (checkPausedStatus(bundleActionStatus, bundleActions, bundleStatus)) {
                                LOG.info("Set bundle job [" + jobId + "] status to '" + bundleStatus[0].toString()
                                        + "' from '" + bundleJob.getStatus() + "'");
                                updateBundleJob(bundleActionStatus, bundleActions, bundleJob, bundleStatus[0]);
                            }
                            else if (checkSuspendStatus(bundleActionStatus, bundleActions, bundleStatus)) {
                                LOG.info("Set bundle job [" + jobId + "] status to '" + bundleStatus[0].toString()
                                        + "' from '" + bundleJob.getStatus() + "'");
                                updateBundleJob(bundleActionStatus, bundleActions, bundleJob, bundleStatus[0]);
                            }
                            else if (checkRunningStatus(bundleActionStatus, bundleActions, bundleStatus)) {
                                LOG.info("Set bundle job [" + jobId + "] status to '" + bundleStatus[0].toString()
                                        + "' from '" + bundleJob.getStatus() + "'");
                                updateBundleJob(bundleActionStatus, bundleActions, bundleJob, bundleStatus[0]);
                            }
                        }
                        catch (Exception ex) {
                            LOG.error("Exception happened during aggregate bundle job's status, job = "
                                    + bundleJob.getId(), ex);
                        }
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
                        List<CoordinatorActionBean> coordActions = jpaService
                                .execute(new CoordJobGetActionsJPAExecutor(jobId));
                        HashMap<CoordinatorAction.Status, Integer> coordActionStatus = new HashMap<CoordinatorAction.Status, Integer>();
                        boolean foundPending = false;
                        for (CoordinatorActionBean cAction : coordActions) {
                            if (!cAction.isPending()) {
                                int counter = 0;
                                if (coordActionStatus.containsKey(cAction.getStatus())) {
                                    counter = coordActionStatus.get(cAction.getStatus()) + 1;
                                }
                                else {
                                    ++counter;
                                }
                                coordActionStatus.put(cAction.getStatus(), counter);
                            }
                            else {
                                foundPending = true;
                                break;
                            }
                        }

                        if (foundPending) {
                            continue;
                        }

                        if (coordJob.isDoneMaterialization()
                                && checkCoordTerminalStatus(coordActionStatus, coordActions, coordStatus)) {
                            LOG.info("Set coordinator job [" + jobId + "] status to '" + coordStatus[0].toString()
                                    + "' from '" + coordJob.getStatus() + "'");
                            updateCoordJob(coordActionStatus, coordActions, coordJob, coordStatus[0]);
                        }
                        else if (coordJob.isDoneMaterialization()
                                && checkCoordSuspendStatus(coordActionStatus, coordActions, coordStatus)) {
                            LOG.info("Set coordinator job [" + jobId + "] status to " + coordStatus[0].toString()
                                    + "' from '" + coordJob.getStatus() + "'");
                            updateCoordJob(coordActionStatus, coordActions, coordJob, coordStatus[0]);
                        }
                        else if (checkCoordRunningStatus(coordActionStatus, coordActions, coordStatus)) {
                            LOG.info("Set coordinator job [" + jobId + "] status to " + coordStatus[0].toString()
                                    + "' from '" + coordJob.getStatus() + "'");
                            updateCoordJob(coordActionStatus, coordActions, coordJob, coordStatus[0]);
                        }
                        // checking pending flag for job when user killed or suspended the job
                        else {
                            checkCoordPending(coordActionStatus, coordActions, coordJob, true);
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
                List<CoordinatorActionBean> coordActions, Job.Status[] coordStatus) {
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

            if (coordActions.size() == (totalValuesSucceed + totalValuesFailed + totalValuesKilled + totalValuesTimeOut)) {
                // If all the coordinator actions are succeeded then coordinator job should be succeeded.
                if (coordActions.size() == totalValuesSucceed) {
                    coordStatus[0] = Job.Status.SUCCEEDED;
                    ret = true;
                }
                else if (coordActions.size() == totalValuesKilled) {
                    // If all the coordinator actions are KILLED then coordinator job should be KILLED.
                    coordStatus[0] = Job.Status.KILLED;
                    ret = true;
                }
                else if (coordActions.size() == totalValuesFailed) {
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
                List<BundleActionBean> bundleActions, Job.Status[] bundleStatus) {
            boolean ret = false;
            if (bundleActionStatus.containsKey(Job.Status.PAUSED)) {
                if (bundleActions.size() == bundleActionStatus.get(Job.Status.PAUSED)) {
                    bundleStatus[0] = Job.Status.PAUSED;
                    ret = true;
                }
                else if (bundleActionStatus.containsKey(Job.Status.PAUSEDWITHERROR)
                        && (bundleActions.size() == bundleActionStatus.get(Job.Status.PAUSED)
                                + bundleActionStatus.get(Job.Status.PAUSEDWITHERROR))) {
                    // bundleStatus = Job.Status.PAUSEDWITHERROR;
                    // We need to change this to PAUSEDWITHERROR in future when we add this to coordinator
                    bundleStatus[0] = Job.Status.PAUSED;
                    ret = true;
                }
            }
            return ret;
        }

        private boolean checkSuspendStatus(HashMap<Job.Status, Integer> bundleActionStatus,
                List<BundleActionBean> bundleActions, Job.Status[] bundleStatus) {
            boolean ret = false;
            if (bundleActionStatus.containsKey(Job.Status.SUSPENDED)) {
                if (bundleActions.size() == bundleActionStatus.get(Job.Status.SUSPENDED)) {
                    bundleStatus[0] = Job.Status.SUSPENDED;
                    ret = true;
                }
                else if (bundleActionStatus.containsKey(Job.Status.SUSPENDEDWITHERROR)
                        && (bundleActions.size() == bundleActionStatus.get(Job.Status.SUSPENDED)
                                + bundleActionStatus.get(Job.Status.SUSPENDEDWITHERROR))) {
                    // bundleStatus = Job.Status.SUSPENDEDWITHERROR;
                    // We need to change this to SUSPENDEDWITHERROR in future when we add this to coordinator
                    bundleStatus[0] = Job.Status.SUSPENDED;
                    ret = true;
                }
            }
            return ret;
        }

        private boolean checkCoordSuspendStatus(HashMap<CoordinatorAction.Status, Integer> coordActionStatus,
                List<CoordinatorActionBean> coordActions, Job.Status[] coordStatus) {
            boolean ret = false;
            if (coordActionStatus.containsKey(CoordinatorAction.Status.SUSPENDED)) {
                if (coordActions.size() == coordActionStatus.get(CoordinatorAction.Status.SUSPENDED)) {
                    coordStatus[0] = Job.Status.SUSPENDED;
                    ret = true;
                }
            }
            return ret;
        }

        private boolean checkCoordRunningStatus(HashMap<CoordinatorAction.Status, Integer> coordActionStatus,
                List<CoordinatorActionBean> coordActions, Job.Status[] coordStatus) {
            boolean ret = false;
            if (coordActionStatus.containsKey(CoordinatorAction.Status.RUNNING)) {
                // If all the bundle actions are succeeded then bundle job should be succeeded.
                if (coordActions.size() == coordActionStatus.get(CoordinatorAction.Status.RUNNING)) {
                    coordStatus[0] = Job.Status.RUNNING;
                    ret = true;
                }
                else if (coordActionStatus.get(CoordinatorAction.Status.RUNNING) > 0) {
                    if ((coordActionStatus.containsKey(CoordinatorAction.Status.FAILED) && coordActionStatus.get(CoordinatorAction.Status.FAILED) > 0)
                            || (coordActionStatus.containsKey(CoordinatorAction.Status.KILLED) && coordActionStatus
                                    .get(CoordinatorAction.Status.KILLED) > 0)
                            || (coordActionStatus.containsKey(CoordinatorAction.Status.TIMEDOUT) && coordActionStatus
                                    .get(CoordinatorAction.Status.TIMEDOUT) > 0)) {
                        // coordStatus = Job.Status.RUNNINGWITHERROR;
                        // We need to change this to RUNNINGWIHERROR in future when we add this to coordinator
                        coordStatus[0] = Job.Status.RUNNING;
                        ret = true;
                    }
                }
            }
            return ret;
        }

        private boolean checkRunningStatus(HashMap<Job.Status, Integer> bundleActionStatus,
                List<BundleActionBean> bundleActions, Job.Status[] bundleStatus) {
            boolean ret = false;
            if (bundleActionStatus.containsKey(Job.Status.RUNNING)) {
                // If all the bundle actions are succeeded then bundle job should be succeeded.
                if (bundleActions.size() == bundleActionStatus.get(Job.Status.RUNNING)) {
                    bundleStatus[0] = Job.Status.RUNNING;
                    ret = true;
                }
                else if (bundleActionStatus.get(Job.Status.RUNNING) > 0) {
                    if ((bundleActionStatus.containsKey(Job.Status.FAILED) && bundleActionStatus.get(Job.Status.FAILED) > 0)
                            || (bundleActionStatus.containsKey(Job.Status.KILLED) && bundleActionStatus
                                    .get(Job.Status.KILLED) > 0)
                            || (bundleActionStatus.containsKey(Job.Status.DONEWITHERROR) && bundleActionStatus
                                    .get(Job.Status.DONEWITHERROR) > 0)
                            || (bundleActionStatus.containsKey(Job.Status.RUNNINGWITHERROR) && bundleActionStatus
                                    .get(Job.Status.RUNNINGWITHERROR) > 0)) {
                        // bundleStatus = Job.Status.RUNNINGWITHERROR;
                        // We need to change this to RUNNINGWIHERROR in future when we add this to coordinator
                        bundleStatus[0] = Job.Status.RUNNING;
                        ret = true;
                    }
                }
            }
            return ret;
        }

        private void updateBundleJob(HashMap<Job.Status, Integer> bundleActionStatus,
                List<BundleActionBean> bundleActions, BundleJobBean bundleJob, Job.Status bundleStatus)
                throws JPAExecutorException {
            String jobId = bundleJob.getId();
            boolean pendingBundleJob = bundleJob.isPending();
            // Checking the bundle pending should be updated or not
            int totalNonPendingActions = 0;
            for (Job.Status js : bundleActionStatus.keySet()) {
                totalNonPendingActions += bundleActionStatus.get(js);
            }

            if (totalNonPendingActions == bundleActions.size()) {
                pendingBundleJob = false;
            }

            // Update the Bundle Job
            bundleJob.setStatus(bundleStatus);
            if (pendingBundleJob) {
                bundleJob.setPending();
                LOG.info("Bundle job [" + jobId + "] Pending set to TRUE");
            }
            else {
                bundleJob.resetPending();
                LOG.info("Bundle job [" + jobId + "] Pending set to FALSE");
            }
            jpaService.execute(new BundleJobUpdateJPAExecutor(bundleJob));
        }

        private void updateCoordJob(HashMap<CoordinatorAction.Status, Integer> coordActionStatus,
                List<CoordinatorActionBean> coordActions, CoordinatorJobBean coordJob, Job.Status coordStatus)
                throws JPAExecutorException, CommandException {
            Job.Status prevStatus = coordJob.getStatus();
            // Update the Coord Job
            if (coordJob.getStatus() == Job.Status.SUCCEEDED || coordJob.getStatus() == Job.Status.FAILED
                    || coordJob.getStatus() == Job.Status.KILLED || coordJob.getStatus() == Job.Status.DONEWITHERROR) {
                if (coordStatus == Job.Status.SUSPENDED) {
                    LOG.info("Coord Job [" + coordJob.getId()
                            + "] status can not be updated as its already in Terminal state");
                    return;
                }
            }

            checkCoordPending(coordActionStatus, coordActions, coordJob, false);
            coordJob.setStatus(coordStatus);
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

        private void checkCoordPending(HashMap<CoordinatorAction.Status, Integer> coordActionStatus,
                List<CoordinatorActionBean> coordActions, CoordinatorJobBean coordJob, boolean saveToDB) throws JPAExecutorException {
            boolean pendingCoordJob = coordJob.isPending();
            // Checking the coordinator pending should be updated or not
            int totalNonPendingActions = 0;
            for (CoordinatorAction.Status js : coordActionStatus.keySet()) {
                totalNonPendingActions += coordActionStatus.get(js);
            }

            if (totalNonPendingActions == coordActions.size()) {
                pendingCoordJob = false;
            }

            if (pendingCoordJob) {
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
                        + DateUtils.convertDateToString(lastInstanceStartTime));
                // this is not the first instance, we should only check jobs that have actions been
                // updated >= start time of last service run;
                List<CoordinatorActionBean> actionList = jpaService
                        .execute(new CoordActionsGetByLastModifiedTimeJPAExecutor(lastInstanceStartTime));
                Set<String> coordIds = new HashSet<String>();
                for (CoordinatorActionBean action : actionList) {
                    coordIds.add(action.getJobId());
                }
                pendingJobCheckList = new ArrayList<CoordinatorJobBean>();
                for (String coordId : coordIds.toArray(new String[coordIds.size()])) {
                    CoordinatorJobBean coordJob = jpaService.execute(new CoordJobGetJPAExecutor(coordId));
                    // Running coord job might have pending false
                    if (coordJob.isPending() || coordJob.getStatus().equals(Job.Status.RUNNING)) {
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
