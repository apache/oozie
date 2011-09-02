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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.command.coord.CoordJobMatLookupCommand;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.util.XCallable;
import org.apache.oozie.util.XLog;

/**
 * The coordinator Materialization Lookup trigger service schedule lookup trigger command for every interval (default is
 * 5 minutes ). This interval could be configured through oozie configuration defined is either oozie-default.xml or
 * oozie-site.xml using the property name oozie.service.CoordJobMatLookupTriggerService.lookup.interval
 */
public class CoordJobMatLookupTriggerService implements Service {
    public static final String CONF_PREFIX = Service.CONF_PREFIX + "CoordJobMatLookupTriggerService.";
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

    private static final String INSTRUMENTATION_GROUP = "coord_job_mat_lookup";
    private static final String INSTR_MAT_JOBS_COUNTER = "jobs";
    private static final int CONF_LOOKUP_INTERVAL_DEFAULT = 300;
    private static final int CONF_MATERIALIZATION_WINDOW_DEFAULT = 3600;

    /**
     * This runnable class will run in every "interval" to queue CoordJobMatLookupTriggerCommand.
     */
    static class CoordJobMatLookupTriggerRunnable implements Runnable {
        private int materializationWindow;
        private long delay = 0;
        private List<XCallable<Void>> callables;
        private List<XCallable<Void>> delayedCallables;

        public CoordJobMatLookupTriggerRunnable(int materializationWindow) {
            this.materializationWindow = materializationWindow;
        }

        @Override
        public void run() {
            runCoordJobMatLookup();

            if (null != callables) {
                boolean ret = Services.get().get(CallableQueueService.class).queueSerial(callables);
                if (ret == false) {
                    XLog.getLog(getClass()).warn(
                            "Unable to queue the callables commands for CoordJobMatLookupTriggerRunnable. "
                                    + "Most possibly command queue is full. Queue size is :"
                                    + Services.get().get(CallableQueueService.class).queueSize());
                }
                callables = null;
            }
            if (null != delayedCallables) {
                boolean ret = Services.get().get(CallableQueueService.class).queueSerial(delayedCallables, this.delay);
                if (ret == false) {
                    XLog.getLog(getClass()).warn(
                            "Unable to queue the delayedCallables commands for CoordJobMatLookupTriggerRunnable. "
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
            XLog log = XLog.getLog(getClass());

            CoordinatorStore store = null;
            try {
                store = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
                store.beginTrx();

                // get current date
                Date currDate = new Date(new Date().getTime() - CONF_LOOKUP_INTERVAL_DEFAULT * 100);
                // get list of all jobs that have actions that should be
                // materialized.
                List<CoordinatorJobBean> materializeJobs = store.getCoordinatorJobsToBeMaterialized(currDate, 50);
                log.debug("CoordJobMatLookupTriggerService - Curr Date= " + currDate + ", Num jobs to materialize = "
                        + materializeJobs.size());
                for (CoordinatorJobBean coordJob : materializeJobs) {
                    Services.get().get(InstrumentationService.class).get().incr(INSTRUMENTATION_GROUP,
                                                                                INSTR_MAT_JOBS_COUNTER, 1);
                    queueCallable(new CoordJobMatLookupCommand(coordJob.getId(), materializationWindow));
                }

                store.commitTrx();
            }
            catch (StoreException ex) {
                if (store != null) {
                    store.rollbackTrx();
                }
                log.warn("Exception while accessing the store", ex);
            }
            catch (Exception ex) {
                log.error("Exception, {0}", ex.getMessage(), ex);
                if (store != null && store.isActive()) {
                    try {
                        store.rollbackTrx();
                    }
                    catch (RuntimeException rex) {
                        log.warn("openjpa error, {0}", rex.getMessage(), rex);
                    }
                }
            }
            finally {
                if (store != null) {
                    if (!store.isActive()) {
                        try {
                            store.closeTrx();
                        }
                        catch (RuntimeException rex) {
                            log.warn("Exception while attempting to close store", rex);
                        }
                    }
                    else {
                        log.warn("transaction is not committed or rolled back before closing entitymanager.");
                    }
                }
            }
        }

        /**
         * Adds callables to a list. If the number of callables in the list reaches {@link
         * CoordJobMatLookupTriggerService#CONF_CALLABLE_BATCH_SIZE}, the entire batch is queued and the callables list
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
                            "Unable to queue the callables commands for CoordJobMatLookupTriggerRunnable. "
                                    + "Most possibly command queue is full. Queue size is :"
                                    + Services.get().get(CallableQueueService.class).queueSize());
                }
                callables = new ArrayList<XCallable<Void>>();
            }
        }

        /**
         * Adds callables to a list. If the number of callables in the list reaches {@link
         * CoordJobMatLookupTriggerService#CONF_CALLABLE_BATCH_SIZE}, the entire batch is queued with the delay set to
         * the maximum delay of the callables in the list. The callables list and the delay is reset.
         *
         * @param callable the callable to queue.
         * @param delay the delay for the callable.
         */
        private void queueCallable(XCallable<Void> callable, long delay) {
            if (delayedCallables == null) {
                delayedCallables = new ArrayList<XCallable<Void>>();
            }
            this.delay = Math.max(this.delay, delay);
            delayedCallables.add(callable);
            if (delayedCallables.size() == Services.get().getConf().getInt(CONF_CALLABLE_BATCH_SIZE, 10)) {
                boolean ret = Services.get().get(CallableQueueService.class).queueSerial(delayedCallables, this.delay);
                if (ret == false) {
                    XLog.getLog(getClass()).warn(
                            "Unable to queue the delayedCallables commands for CoordJobMatLookupTriggerRunnable. "
                                    + "Most possibly Callable queue is full. Queue size is :"
                                    + Services.get().get(CallableQueueService.class).queueSize());
                }
                delayedCallables = new ArrayList<XCallable<Void>>();
                this.delay = 0;
            }
        }

    }

    @Override
    public void init(Services services) throws ServiceException {
        Configuration conf = services.getConf();
        Runnable lookupTriggerJobsRunnable = new CoordJobMatLookupTriggerRunnable(conf.getInt(
                CONF_MATERIALIZATION_WINDOW, CONF_MATERIALIZATION_WINDOW_DEFAULT));// Default is 1 hour
        services.get(SchedulerService.class).schedule(lookupTriggerJobsRunnable, 10,
                                                      conf.getInt(CONF_LOOKUP_INTERVAL, CONF_LOOKUP_INTERVAL_DEFAULT),// Default is 5 minutes
                                                      SchedulerService.Unit.SEC);
        return;
    }

    @Override
    public void destroy() {
        // TODO Auto-generated method stub

    }

    @Override
    public Class<? extends Service> getInterface() {
        return CoordJobMatLookupTriggerService.class;
    }

}
