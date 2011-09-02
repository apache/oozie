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
import org.apache.oozie.executor.jpa.CoordActionsPendingFalseCountGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionsPendingFalseStatusCountGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobsGetPendingJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.SchedulerService;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.MemoryLocks;
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
                LOG.warn("Exception happened during bundle job status transition", ex);
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
            updateBundleJobStatus(bundleLists);
        }

        private void updateBundleJobStatus(List<List<BundleJobBean>> bundleLists) throws JPAExecutorException,
                CommandException {
            if (bundleLists != null) {
                for (List<BundleJobBean> listBundleBean : bundleLists) {
                    for (BundleJobBean bundleJob : listBundleBean) {
                        String jobId = bundleJob.getId();
                        Job.Status[] bundleStatus = new Job.Status[1];
                        bundleStatus[0] = bundleJob.getStatus();
                        bundleJob.isPending();
                        List<BundleActionBean> bundleActions = jpaService
                                .execute(new BundleActionsGetJPAExecutor(jobId));
                        HashMap<Job.Status, Integer> bundleActionStatus = new HashMap<Job.Status, Integer>();
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
                                    LOG.info("Bundle job [" + jobId
                                            + "] has been killed since one of its coordinator job failed submission.");
                                }
                            }
                            else {
                                break;
                            }
                        }

                        if (checkTerminalStatus(bundleActionStatus, bundleActions, bundleStatus)) {
                            LOG.info("Bundle job [" + jobId + "] Status set to " + bundleStatus[0].toString());
                            updateBundleJob(bundleActionStatus, bundleActions, bundleJob, bundleStatus[0]);
                            continue;
                        }
                        else if (checkPrepStatus(bundleActionStatus, bundleActions, bundleStatus)) {
                            LOG.info("Bundle job [" + jobId + "] Status set to " + bundleStatus[0].toString());
                            updateBundleJob(bundleActionStatus, bundleActions, bundleJob, bundleStatus[0]);
                            continue;
                        }
                        else if (checkPausedStatus(bundleActionStatus, bundleActions, bundleStatus)) {
                            LOG.info("Bundle job [" + jobId + "] Status set to " + bundleStatus[0].toString());
                            updateBundleJob(bundleActionStatus, bundleActions, bundleJob, bundleStatus[0]);
                            continue;
                        }
                        else if (checkSuspendStatus(bundleActionStatus, bundleActions, bundleStatus)) {
                            LOG.info("Bundle job [" + jobId + "] Status set to " + bundleStatus[0].toString());
                            updateBundleJob(bundleActionStatus, bundleActions, bundleJob, bundleStatus[0]);
                            continue;
                        }
                        else if (checkRunningStatus(bundleActionStatus, bundleActions, bundleStatus)) {
                            LOG.info("Bundle job [" + jobId + "] Status set to " + bundleStatus[0].toString());
                            updateBundleJob(bundleActionStatus, bundleActions, bundleJob, bundleStatus[0]);
                            continue;
                        }
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

            if (bundleActions.size() == (totalValuesSucceed + totalValuesFailed + totalValuesKilled)) {
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
                    if (coordJob.isPending()) {
                        pendingJobCheckList.add(coordJob);
                    }
                }
            }

            for (CoordinatorJobBean coordJob : pendingJobCheckList) {
                String jobId = coordJob.getId();
                Job.Status prevStatus = coordJob.getStatus();
                CoordActionsPendingFalseCountGetJPAExecutor actionsPendingFalseCmd = new CoordActionsPendingFalseCountGetJPAExecutor(
                        jobId);
                int totalPendingFalse = jpaService.execute(actionsPendingFalseCmd);
                if (coordJob.getLastActionNumber() == totalPendingFalse) {
                    coordJob.resetPending();
                    LOG.debug("All coordinator actions are pending=false so update coordinator job [" + jobId
                            + "] pending=false.");
                    if (coordJob.getStatus().equals(Job.Status.RUNNING)) {
                        // If all actions succeed, coordinator job succeeds, otherwise done with error
                        CoordActionsPendingFalseStatusCountGetJPAExecutor actionsSucceedCmd = new CoordActionsPendingFalseStatusCountGetJPAExecutor(
                                jobId, Job.Status.SUCCEEDED.toString());
                        int totalSucceeds = jpaService.execute(actionsSucceedCmd);
                        if (coordJob.getLastActionNumber() == totalSucceeds) {
                            coordJob.setStatus(Job.Status.SUCCEEDED);
                            LOG.info("Update coordinator job [" + jobId + "] to SUCCEEDED.");
                        }
                        else {
                            coordJob.setStatus(Job.Status.DONEWITHERROR);
                            LOG.info("Update coordinator job [" + jobId + "] to DONEWITHERROR.");
                        }
                    }
                    jpaService.execute(new CoordJobUpdateJPAExecutor(coordJob));
                    // update bundle action only when status changes in coord job
                    if (coordJob.getBundleId() != null) {
                        if (!prevStatus.equals(coordJob.getStatus())) {
                            BundleStatusUpdateXCommand bundleStatusUpdate = new BundleStatusUpdateXCommand(coordJob,
                                    prevStatus);
                            bundleStatusUpdate.call();
                        }
                    }
                }
            }
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