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

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.command.coord.CoordMaterializeTransitionXCommand;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.BatchQueryExecutor.UpdateEntry;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.lock.LockToken;
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

    public static final String CONF_SCHEDULING_INTERVAL = CONF_PREFIX + "scheduling.interval";
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

    /**
     * This runnable class will run in every "interval" to queue CoordMaterializeTransitionXCommand.
     */
    static class CoordMaterializeTriggerRunnable implements Runnable {
        private int materializationWindow;
        private int lookupInterval;
        private long delay = 0;
        private List<XCallable<Void>> callables;
        private List<XCallable<Void>> delayedCallables;
        private XLog LOG = XLog.getLog(getClass());


        public CoordMaterializeTriggerRunnable(int materializationWindow, int lookupInterval) {
            this.materializationWindow = materializationWindow;
            this.lookupInterval = lookupInterval;
        }

        @Override
        public void run() {
            LockToken lock = null;

            // first check if there is some other running instance from the same service;
            try {
                lock = Services.get().get(MemoryLocksService.class)
                        .getWriteLock(CoordMaterializeTriggerService.class.getName(), lockTimeout);

                if (lock != null) {
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
                        boolean ret = Services.get().get(CallableQueueService.class)
                                .queueSerial(delayedCallables, this.delay);
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

                else {
                    LOG.debug("Can't obtain lock, skipping");
                }
            }
            catch (Exception e) {
                LOG.error("Exception", e);
            }
            finally {
                if (lock != null) {
                    lock.release();
                    LOG.info("Released lock for [{0}]", CoordMaterializeTriggerService.class.getName());
                }

            }

        }

        /**
         * Recover coordinator jobs that should be materialized
         * @throws JPAExecutorException
         */
        private void runCoordJobMatLookup() throws JPAExecutorException {
            List<UpdateEntry> updateList = new ArrayList<UpdateEntry>();
            XLog.Info.get().clear();
            XLog LOG = XLog.getLog(getClass());
            try {
                // get current date
                Date currDate = new Date(new Date().getTime() + lookupInterval * 1000);
                // get list of all jobs that have actions that should be materialized.
                int materializationLimit = ConfigurationService.getInt(CONF_MATERIALIZATION_SYSTEM_LIMIT);
                materializeCoordJobs(currDate, materializationLimit, LOG, updateList);
            }

            catch (Exception ex) {
                LOG.error("Exception while attempting to materialize coordinator jobs, {0}", ex.getMessage(), ex);
            }
            finally {
                BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(null, updateList, null);
            }
        }

        private void materializeCoordJobs(Date currDate, int limit, XLog LOG, List<UpdateEntry> updateList)
                throws JPAExecutorException {
            try {
                List<CoordinatorJobBean> materializeJobs = CoordJobQueryExecutor.getInstance().getList(
                        CoordJobQuery.GET_COORD_JOBS_OLDER_FOR_MATERILZATION, currDate, limit);
                LOG.info("CoordMaterializeTriggerService - Curr Date= " + DateUtils.formatDateOozieTZ(currDate)
                        + ", Num jobs to materialize = " + materializeJobs.size());
                for (CoordinatorJobBean coordJob : materializeJobs) {
                    Services.get().get(InstrumentationService.class).get()
                            .incr(INSTRUMENTATION_GROUP, INSTR_MAT_JOBS_COUNTER, 1);
                    queueCallable(new CoordMaterializeTransitionXCommand(coordJob.getId(), materializationWindow));
                    coordJob.setLastModifiedTime(new Date());
                    updateList.add(new UpdateEntry<CoordJobQuery>(CoordJobQuery.UPDATE_COORD_JOB_LAST_MODIFIED_TIME,
                            coordJob));
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
            if (callables.size() == ConfigurationService.getInt(CONF_CALLABLE_BATCH_SIZE)) {
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
        // default is 3600sec (1hr)
        int materializationWindow = ConfigurationService.getInt(services.getConf(), CONF_MATERIALIZATION_WINDOW);
        // default is 300sec (5min)
        int lookupInterval = ConfigurationService.getInt(services.getConf(), CONF_LOOKUP_INTERVAL);
        // default is 300sec (5min)
        int schedulingInterval = Services.get().getConf().getInt(CONF_SCHEDULING_INTERVAL, lookupInterval);

        Runnable lookupTriggerJobsRunnable = new CoordMaterializeTriggerRunnable(materializationWindow, lookupInterval);

        services.get(SchedulerService.class).schedule(lookupTriggerJobsRunnable, 10, schedulingInterval,
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
