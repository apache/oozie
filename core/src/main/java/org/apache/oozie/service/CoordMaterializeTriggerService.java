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
import org.apache.oozie.executor.jpa.CoordJobGetRunningActionsCountJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobsToBeMaterializedJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.util.XCallable;
import org.apache.oozie.util.XLog;

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
        private long delay = 0;
        private List<XCallable<Void>> callables;
        private List<XCallable<Void>> delayedCallables;

        public CoordMaterializeTriggerRunnable(int materializationWindow) {
            this.materializationWindow = materializationWindow;
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
            JPAService jpaService = Services.get().get(JPAService.class);
            try {

                // get current date
                Date currDate = new Date(new Date().getTime() + CONF_LOOKUP_INTERVAL_DEFAULT * 1000);
                // get list of all jobs that have actions that should be materialized.
                int materializationLimit = Services.get().getConf()
                        .getInt(CONF_MATERIALIZATION_SYSTEM_LIMIT, CONF_MATERIALIZATION_SYSTEM_LIMIT_DEFAULT);
                CoordJobsToBeMaterializedJPAExecutor cmatcmd = new CoordJobsToBeMaterializedJPAExecutor(currDate,
                        materializationLimit);
                List<CoordinatorJobBean> materializeJobs = jpaService.execute(cmatcmd);
                LOG.debug("CoordMaterializeTriggerService - Curr Date= " + currDate + ", Num jobs to materialize = "
                        + materializeJobs.size());
                for (CoordinatorJobBean coordJob : materializeJobs) {
                    Services.get().get(InstrumentationService.class).get()
                            .incr(INSTRUMENTATION_GROUP, INSTR_MAT_JOBS_COUNTER, 1);
                    int numWaitingActions = jpaService
                            .execute(new CoordActionsActiveCountJPAExecutor(coordJob.getId()));
                    LOG.debug("Job :" + coordJob.getId() + "  numWaitingActions : " + numWaitingActions
                            + " MatThrottle : " + coordJob.getMatThrottling());
                    // update lastModifiedTime so next time others might have higher chance to get pick up
                    coordJob.setLastModifiedTime(new Date());
                    jpaService.execute(new CoordJobUpdateJPAExecutor(coordJob));
                    if (numWaitingActions >= coordJob.getMatThrottling()) {
                        LOG.debug("Materialization skipped for JobID [" + coordJob.getId() + " already waiting "
                                + numWaitingActions + " actions. MatThrottle is : " + coordJob.getMatThrottling());
                        continue;
                    }
                    queueCallable(new CoordMaterializeTransitionXCommand(coordJob.getId(), materializationWindow));

                }

            }
            catch (JPAExecutorException jex) {
                LOG.warn("JPAExecutorException while attempting to materialize coordinator jobs", jex);
            }
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
        Runnable lookupTriggerJobsRunnable = new CoordMaterializeTriggerRunnable(conf.getInt(
                CONF_MATERIALIZATION_WINDOW, CONF_MATERIALIZATION_WINDOW_DEFAULT));// Default is 1 hour
        services.get(SchedulerService.class).schedule(lookupTriggerJobsRunnable, 10,
                                                      conf.getInt(CONF_LOOKUP_INTERVAL, CONF_LOOKUP_INTERVAL_DEFAULT),// Default is 5 minutes
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
