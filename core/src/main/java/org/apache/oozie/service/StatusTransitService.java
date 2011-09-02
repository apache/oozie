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
import org.apache.oozie.executor.jpa.BundleActionsFailedAndNullCoordCountGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleActionsGetByLastModifiedTimeJPAExecutor;
import org.apache.oozie.executor.jpa.BundleActionsNotEqualStatusCountGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleActionsNotTerminateStatusCountGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleActionsPendingTrueCountGetJPAExecutor;
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
                    LOG.info("This StatusTransitService instance will not run since there is already an instance running");
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
            if (lastInstanceStartTime == null) {
                LOG.info("Running bundle status service first instance");
                // this is the first instance, we need to check for all pending jobs;
                pendingJobCheckList = jpaService.execute(new BundleJobsGetPendingJPAExecutor(limit));
                runningJobCheckList = jpaService.execute(new BundleJobsGetRunningJPAExecutor(limit));
            }
            else {
                LOG.info("Running bundle status service from last instance time =  " + DateUtils.convertDateToString(lastInstanceStartTime));
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
                    //Running bundle job might have pending false
                    if (bundle.isPending() || bundle.getStatus().equals(Job.Status.RUNNING)) {
                        pendingJobCheckList.add(bundle);
                    }
                }

                runningJobCheckList = pendingJobCheckList;
            }

            for (BundleJobBean bundleJob : pendingJobCheckList) {
                String jobId = bundleJob.getId();
                // Check if if all bundle actions have pending = false.
                BundleActionsPendingTrueCountGetJPAExecutor actionsPendingTrueCmd = new BundleActionsPendingTrueCountGetJPAExecutor(
                        jobId);
                int totalPendingTrue = jpaService.execute(actionsPendingTrueCmd);
                if (totalPendingTrue == 0) {
                    bundleJob.resetPending();
                    jpaService.execute(new BundleJobUpdateJPAExecutor(bundleJob));
                    LOG.debug("All bundle actions are pending=false so update bundle job [" + jobId
                            + "] pending=false.");
                }
            }

            for (BundleJobBean bundleJob : runningJobCheckList) {
                String jobId = bundleJob.getId();
                // If all actions succeed, bundle succeeds;
                BundleActionsNotEqualStatusCountGetJPAExecutor actionsNotSucceedCmd = new BundleActionsNotEqualStatusCountGetJPAExecutor(
                        jobId, Job.Status.SUCCEEDED.toString());
                int totalNotSucceed = jpaService.execute(actionsNotSucceedCmd);
                if (totalNotSucceed == 0) {
                    bundleJob.setStatus(Job.Status.SUCCEEDED);
                    jpaService.execute(new BundleJobUpdateJPAExecutor(bundleJob));
                    LOG.info("Update bundle job [" + jobId + "] to SUCCEEDED.");
                    continue;
                }
                BundleActionsNotTerminateStatusCountGetJPAExecutor actionsNotTerminateCmd = new BundleActionsNotTerminateStatusCountGetJPAExecutor(
                        jobId);
                int totalNotTerminate = jpaService.execute(actionsNotTerminateCmd);
                if (totalNotSucceed > 0 && totalNotTerminate == 0) {
                    //check if all actions terminated
                    bundleJob.setStatus(Job.Status.DONEWITHERROR);
                    jpaService.execute(new BundleJobUpdateJPAExecutor(bundleJob));
                    LOG.info("Update bundle job [" + jobId + "] to DONEWITHERROR.");
                    continue;
                }

                BundleActionsPendingTrueCountGetJPAExecutor actionsPendingTrueCmd = new BundleActionsPendingTrueCountGetJPAExecutor(
                        jobId);
                int totalPendingTrue = jpaService.execute(actionsPendingTrueCmd);

                if (totalPendingTrue == 0) {
                    // If all actions finish submission and some submission failed, kill the whole bundle;
                    BundleActionsFailedAndNullCoordCountGetJPAExecutor actionsFailedAndNullCoordCmd = new BundleActionsFailedAndNullCoordCountGetJPAExecutor(
                            jobId);
                    int totalFailedAndNullCoord = jpaService.execute(actionsFailedAndNullCoordCmd);
                    if (totalFailedAndNullCoord > 0) {
                        (new BundleKillXCommand(jobId)).call();
                        LOG.info("Bundle job [" + jobId
                                + "] has been killed since one of its coordinator job failed submission.");
                    }
                }
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
                LOG.info("Running coordinator status service from last instance time =  " + DateUtils.convertDateToString(lastInstanceStartTime));
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