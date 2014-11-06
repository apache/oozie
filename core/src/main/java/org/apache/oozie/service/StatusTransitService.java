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

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.bundle.BundleStatusTransitXCommand;
import org.apache.oozie.command.coord.CoordStatusTransitXCommand;
import org.apache.oozie.executor.jpa.BundleJobQueryExecutor;
import org.apache.oozie.executor.jpa.BundleJobQueryExecutor.BundleJobQuery;
import org.apache.oozie.executor.jpa.BundleJobsGetRunningOrPendingJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.executor.jpa.CoordJobsGetPendingJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.lock.LockToken;
import org.apache.oozie.util.XLog;

/**
 * StateTransitService is scheduled to run at the configured interval.
 * <p/>
 * It is to update job's status according to its child actions' status. If all child actions' pending flag equals 0 (job
 * done), we reset the job's pending flag to 0. If all child actions are succeeded, we set the job's status to
 * SUCCEEDED.
 */
public class StatusTransitService implements Service {
    private static final String CONF_PREFIX = Service.CONF_PREFIX + "StatusTransitService.";
    private static final String CONF_STATUSTRANSIT_INTERVAL = CONF_PREFIX + "statusTransit.interval";
    public static final String CONF_BACKWARD_SUPPORT_FOR_COORD_STATUS = CONF_PREFIX
            + "backward.support.for.coord.status";
    public static final String CONF_BACKWARD_SUPPORT_FOR_STATES_WITHOUT_ERROR = CONF_PREFIX
            + "backward.support.for.states.without.error";
    public static int limit = -1;
    public static Date lastInstanceStartTime = null;
    public final static XLog LOG = XLog.getLog(StatusTransitRunnable.class);

    /**
     * StateTransitRunnable is the runnable which is scheduled to run at the configured interval.
     * <p/>
     * It is to update job's status according to its child actions' status. If all child actions' pending flag equals 0
     * (job done), we reset the job's pending flag to 0. If all child actions are succeeded, we set the job's status to
     * SUCCEEDED.
     */
    public static class StatusTransitRunnable implements Runnable {
        private JPAService jpaService = null;
        private LockToken lock;

        private Set<String> coordFailedIds = new HashSet<String>();
        private Set<String> bundleFailedIds = new HashSet<String>();

        public StatusTransitRunnable() {
            jpaService = Services.get().get(JPAService.class);
            if (jpaService == null) {
                LOG.error("Missing JPAService");
            }
        }

        @Override
        public void run() {
            try {
                final Date curDate = new Date(); // records the start time of this service run;

                // first check if there is some other instance running;
                lock = Services.get().get(MemoryLocksService.class)
                        .getWriteLock(StatusTransitService.class.getName(), lockTimeout);
                if (lock == null) {
                    LOG.info("This StatusTransitService instance"
                            + " will not run since there is already an instance running");
                }
                else {
                    LOG.info("Acquired lock for [{0}]", StatusTransitService.class.getName());
                    coordTransit();
                    bundleTransit();
                    lastInstanceStartTime = curDate;
                }
            }
            catch (Exception ex) {
                LOG.warn("Exception happened during StatusTransitRunnable ", ex);
            }
            finally {
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
            List<BundleJobBean> pendingJobCheckList;
            final Set<String> bundleIds = new HashSet<String>();

            if (lastInstanceStartTime == null) {
                LOG.info("Running bundle status service first instance");
                // This is the first instance, we need to check for all pending or running jobs;
                // TODO currently limit is = -1. Need to do actual batching
                pendingJobCheckList = jpaService.execute(new BundleJobsGetRunningOrPendingJPAExecutor(limit));
            }
            else {
                LOG.info("Running bundle status service from last instance time =  "
                        + DateUtils.formatDateOozieTZ(lastInstanceStartTime));
                // this is not the first instance, we should only check jobs that have actions been
                // updated >= start time of last service run;
                pendingJobCheckList = BundleJobQueryExecutor.getInstance().getList(
                        BundleJobQuery.GET_BUNDLE_IDS_FOR_STATUS_TRANSIT, lastInstanceStartTime);
            }
            for (BundleJobBean job : pendingJobCheckList) {
                bundleIds.add(job.getId());
            }
            bundleIds.addAll(bundleFailedIds);
            bundleFailedIds.clear();
            for (final String jobId : bundleIds) {
                try {
                    new BundleStatusTransitXCommand(jobId).call();
                }
                catch (CommandException e) {
                    // Unable to acquire lock. Will try next time
                    if (e.getErrorCode() == ErrorCode.E0606) {
                        bundleFailedIds.add(jobId);
                        LOG.info("Unable to acquire lock for " + jobId + ". Will try next time");
                    }
                    else {
                        LOG.error("Error running BundleStatusTransitXCommand for job " + jobId, e);
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
            final Set<String> coordIds = new HashSet<String>();
            if (lastInstanceStartTime == null) {
                LOG.info("Running coordinator status service first instance");
                // this is the first instance, we need to check for all pending jobs;
                pendingJobCheckList = jpaService.execute(new CoordJobsGetPendingJPAExecutor(limit));
            }
            else {
                LOG.info("Running coordinator status service from last instance time =  "
                        + DateUtils.formatDateOozieTZ(lastInstanceStartTime));
                // this is not the first instance, we should only check jobs.
                // that have actions or jobs been updated >= start time of last service run;
                pendingJobCheckList = CoordJobQueryExecutor.getInstance().getList(
                        CoordJobQuery.GET_COORD_IDS_FOR_STATUS_TRANSIT, lastInstanceStartTime);

                pendingJobCheckList.addAll(CoordJobQueryExecutor.getInstance().getList(
                        CoordJobQuery.GET_COORD_JOBS_CHANGED, lastInstanceStartTime));
            }
            for (final CoordinatorJobBean job : pendingJobCheckList) {
                coordIds.add(job.getId());
            }
            coordIds.addAll(coordFailedIds);
            coordFailedIds.clear();
            for (final String coordId : coordIds) {
                try {
                    new CoordStatusTransitXCommand(coordId).call();
                }
                catch (CommandException e) {
                    // Unable to acquire lock. Will try next time
                    if (e.getErrorCode() == ErrorCode.E0606) {
                        coordFailedIds.add(coordId);
                        LOG.info("Unable to acquire lock for " + coordId + ". Will try next time");

                    }
                    else {
                        LOG.error("Error running CoordStatusTransitXCommand for job " + coordId, e);
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
        final Configuration conf = services.getConf();
        Runnable stateTransitRunnable = new StatusTransitRunnable();
        services.get(SchedulerService.class).schedule(stateTransitRunnable, 10,
                ConfigurationService.getInt(conf, CONF_STATUSTRANSIT_INTERVAL), SchedulerService.Unit.SEC);
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
