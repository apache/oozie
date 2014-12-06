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
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.command.coord.CoordMaterializeTransitionXCommand;
import org.apache.oozie.executor.jpa.CoordActionsActiveCountJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobsToBeMaterializedJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.util.XCallable;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.DateUtils;

/**
 * The coordinator Materialization Lookup trigger service schedule lookup trigger command for every interval (default is
 * 5 minutes ). This interval could be configured through oozie configuration defined is either oozie-default.xml or
 * oozie-site.xml using the property name oozie.service.CoordMaterializeTriggerService.lookup.interval
 */
public class CoordMaterializeTriggerService implements Service {
    public static final String CONF_PREFIX = Service.CONF_PREFIX + "CoordMaterializeTriggerService.";
    /**
     * Time interval, in seconds, at which the Job materialization service will be scheduled to run.
     */
    public static final String CONF_LOOKUP_INTERVAL = CONF_PREFIX + "lookup.interval";
    /**
     * This configuration defined the duration for which job should be materialized in future
     */
    public static final String CONF_MATERIALIZATION_WINDOW = CONF_PREFIX + "materialization.window";
    /**
     * The number of callables to be queued in a batch.
     */
    public static final String CONF_CALLABLE_BATCH_SIZE = CONF_PREFIX + "callable.batch.size";
    /**
     * The number of coordinator jobs to be picked for materialization at a given time.
     */
    public static final String CONF_MATERIALIZATION_SYSTEM_LIMIT = CONF_PREFIX + "materialization.system.limit";

    private static final String INSTRUMENTATION_GROUP = "coord_job_mat";
    private static final String INSTR_MAT_JOBS_COUNTER = "jobs";
    private static final int CONF_LOOKUP_INTERVAL_DEFAULT = 300;
    private static final int CONF_MATERIALIZATION_WINDOW_DEFAULT = 3600;
    private static final int CONF_MATERIALIZATION_SYSTEM_LIMIT_DEFAULT = 50;

    /**
     * This runnable class will run in every "interval" to queue CoordMaterializeTransitionXCommand.
     */
    static class CoordMaterializeTriggerRunnable implements Runnable {
        private int materializationWindow;
        private int lookupInterval;
        private long delay = 0;
        private List<XCallable<Void>> callables;
        private List<XCallable<Void>> delayedCallables;

        public CoordMaterializeTriggerRunnable(int materializationWindow, int lookupInterval) {
            this.materializationWindow = materializationWindow;
            this.lookupInterval = lookupInterval;
        }

        @Override
        public void run() {
            runCoordJobMatLookup();

            if (null != callables) {
                boolean ret = Services.get().get(CallableQueueService.class).queueSerial(callables);
                if (ret == false) {
                    XLog.getLog(getClass()).warn(
                            "Unable to queue the callables commands for CoordMaterializeTriggerRunnable. "
                                    + "Most possibly command queue is full. Queue size is :"
                                    + Services.get().get(CallableQueueService.class).queueSize());
                }
                callables = null;
            }
            if (null != delayedCallables) {
                boolean ret = Services.get().get(CallableQueueService.class).queueSerial(delayedCallables, this.delay);
                if (ret == false) {
                    XLog.getLog(getClass()).warn(
                            "Unable to queue the delayedCallables commands for CoordMaterializeTriggerRunnable. "
                                    + "Most possibly Callable queue is full. Queue size is :"
                                    + Services.get().get(CallableQueueService.class).queueSize());
                }
                delayedCallables = null;
                this.delay = 0;
            }
        }

        /**
         * Recover coordinator jobs that should be materialized
         */
        private void runCoordJobMatLookup() {
            XLog.Info.get().clear();
            XLog LOG = XLog.getLog(getClass());
            try {
                // get current date
                Date currDate = new Date(new Date().getTime() + lookupInterval * 1000);
                // get list of all jobs that have actions that should be materialized.
                int materializationLimit = Services.get().getConf()
                        .getInt(CONF_MATERIALIZATION_SYSTEM_LIMIT, CONF_MATERIALIZATION_SYSTEM_LIMIT_DEFAULT);
                // account for under-utilization of limit due to jobs maxed out
                // against mat_throttle. hence repeat
                if (materializeCoordJobs(currDate, materializationLimit, LOG)) {
                    materializeCoordJobs(currDate, materializationLimit, LOG);
                }
            }

            catch (Exception ex) {
                LOG.error("Exception while attempting to materialize coordinator jobs, {0}", ex.getMessage(), ex);
            }
        }

        private boolean materializeCoordJobs(Date currDate, int limit, XLog LOG) {
            try {
                JPAService jpaService = Services.get().get(JPAService.class);
                CoordJobsToBeMaterializedJPAExecutor cmatcmd = new CoordJobsToBeMaterializedJPAExecutor(currDate, limit);
                List<CoordinatorJobBean> materializeJobs = jpaService.execute(cmatcmd);
                int rejected = 0;
                LOG.info("CoordMaterializeTriggerService - Curr Date= " + DateUtils.formatDateOozieTZ(currDate)  + ", Num jobs to materialize = "
                        + materializeJobs.size());
                for (CoordinatorJobBean coordJob : materializeJobs) {
                    if (Services.get().get(JobsConcurrencyService.class).isJobIdForThisServer(coordJob.getId())) {
                        Services.get().get(InstrumentationService.class).get()
                                .incr(INSTRUMENTATION_GROUP, INSTR_MAT_JOBS_COUNTER, 1);
                        int numWaitingActions = jpaService.execute(new CoordActionsActiveCountJPAExecutor(coordJob
                                .getId()));
                        LOG.info("Job :" + coordJob.getId() + "  numWaitingActions : " + numWaitingActions
                                + " MatThrottle : " + coordJob.getMatThrottling());
                        // update lastModifiedTime so next time others get picked up in LRU fashion
                        coordJob.setLastModifiedTime(new Date());
                        CoordJobQueryExecutor.getInstance().executeUpdate(
                                CoordJobQueryExecutor.CoordJobQuery.UPDATE_COORD_JOB_LAST_MODIFIED_TIME, coordJob);
                        if (numWaitingActions >= coordJob.getMatThrottling()) {
                            LOG.info("info for JobID [" + coordJob.getId() + "] " + numWaitingActions
                                    + " actions already waiting. MatThrottle is : " + coordJob.getMatThrottling());
                            rejected++;
                            continue;
                        }
                        queueCallable(new CoordMaterializeTransitionXCommand(coordJob.getId(), materializationWindow));
                    }
                }
                if (materializeJobs.size() == limit && rejected > 0) {
                    return true;
                }
            }
            catch (JPAExecutorException jex) {
                LOG.warn("JPAExecutorException while attempting to materialize coordinator jobs", jex);
            }
            return false;
        }

        /**
         * Adds callables to a list. If the number of callables in the list reaches {@link
         * CoordMaterializeTriggerService#CONF_CALLABLE_BATCH_SIZE}, the entire batch is queued and the callables list
         * is reset.
         *
         * @param callable the callable to queue.
         */
        private void queueCallable(XCallable<Void> callable) {
            if (callables == null) {
                callables = new ArrayList<XCallable<Void>>();
            }
            callables.add(callable);
            if (callables.size() == Services.get().getConf().getInt(CONF_CALLABLE_BATCH_SIZE, 10)) {
                boolean ret = Services.get().get(CallableQueueService.class).queueSerial(callables);
                if (ret == false) {
                    XLog.getLog(getClass()).warn(
                            "Unable to queue the callables commands for CoordMaterializeTriggerRunnable. "
                                    + "Most possibly command queue is full. Queue size is :"
                                    + Services.get().get(CallableQueueService.class).queueSize());
                }
                callables = new ArrayList<XCallable<Void>>();
            }
        }

    }

    @Override
    public void init(Services services) throws ServiceException {
        Configuration conf = services.getConf();
        // default is 3600sec (1hr)
        int materializationWindow = conf.getInt(CONF_MATERIALIZATION_WINDOW, CONF_MATERIALIZATION_WINDOW_DEFAULT);
        // default is 300sec (5min)
        int lookupInterval = Services.get().getConf().getInt(CONF_LOOKUP_INTERVAL, CONF_LOOKUP_INTERVAL_DEFAULT);

        Runnable lookupTriggerJobsRunnable = new CoordMaterializeTriggerRunnable(materializationWindow, lookupInterval);

        services.get(SchedulerService.class).schedule(lookupTriggerJobsRunnable, 10, lookupInterval,
                                                      SchedulerService.Unit.SEC);
    }

    @Override
    public void destroy() {
    }

    @Override
    public Class<? extends Service> getInterface() {
        return CoordMaterializeTriggerService.class;
    }

}
