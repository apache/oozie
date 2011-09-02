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
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.command.coord.CoordActionInputCheckCommand;
import org.apache.oozie.command.coord.CoordActionReadyCommand;
import org.apache.oozie.command.coord.CoordActionStartCommand;
import org.apache.oozie.command.coord.CoordRecoveryCommand;
import org.apache.oozie.command.wf.ActionEndCommand;
import org.apache.oozie.command.wf.ActionStartCommand;
import org.apache.oozie.command.wf.SignalCommand;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.Store;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.util.XCallable;
import org.apache.oozie.util.XLog;

/**
 * The Recovery Service checks for pending actions and premater coordinator jobs older than a configured age and then
 * queues them for execution.
 */
public class RecoveryService implements Service {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "RecoveryService.";
    public static final String CONF_PREFIX_WF_ACTIONS = Service.CONF_PREFIX + "wf.actions.";
    public static final String CONF_PREFIX_COORD = Service.CONF_PREFIX + "coord.";
    /**
     * Time interval, in seconds, at which the recovery service will be scheduled to run.
     */
    public static final String CONF_SERVICE_INTERVAL = CONF_PREFIX + "interval";
    /**
     * The number of callables to be queued in a batch.
     */
    public static final String CONF_CALLABLE_BATCH_SIZE = CONF_PREFIX + "callable.batch.size";
    /**
     * Age of actions to queue, in seconds.
     */
    public static final String CONF_WF_ACTIONS_OLDER_THAN = CONF_PREFIX_WF_ACTIONS + "older.than";
    /**
     * Age of coordinator jobs to recover, in seconds.
     */
    public static final String CONF_COORD_OLDER_THAN = CONF_PREFIX_COORD + "older.than";

    private static final String INSTRUMENTATION_GROUP = "recovery";
    private static final String INSTR_RECOVERED_ACTIONS_COUNTER = "actions";
    private static final String INSTR_RECOVERED_COORD_JOBS_COUNTER = "coord_jobs";
    private static final String INSTR_RECOVERED_COORD_ACTIONS_COUNTER = "coord_actions";

    /**
     * RecoveryRunnable is the Runnable which is scheduled to run with the configured interval, and takes care of the
     * queuing of commands.
     */
    static class RecoveryRunnable<S extends Store> implements Runnable {
        private long olderThan;
        private long coordOlderThan;
        private long delay = 0;
        private List<XCallable<Void>> callables;
        private List<XCallable<Void>> delayedCallables;
        private StringBuilder msg = null;

        public RecoveryRunnable(long olderThan, long coordOlderThan) {
            this.olderThan = olderThan;
            this.coordOlderThan = coordOlderThan;
        }

        public void run() {
            XLog.Info.get().clear();
            XLog log = XLog.getLog(getClass());
            msg = new StringBuilder();
            runWFRecovery();
            runCoordJobRecovery();
            runCoordActionRecovery();
            runCoordActionRecoveryForReady();
            log.debug("QUEUING [{0}] for potential recovery", msg.toString());
            boolean ret = false;
            if (null != callables) {
                ret = Services.get().get(CallableQueueService.class).queueSerial(callables);
                if (ret == false) {
                    log.warn("Unable to queue the callables commands for RecoveryService. "
                            + "Most possibly command queue is full. Queue size is :"
                            + Services.get().get(CallableQueueService.class).queueSize());
                }
                callables = null;
            }
            if (null != delayedCallables) {
                ret = Services.get().get(CallableQueueService.class).queueSerial(delayedCallables, this.delay);
                if (ret == false) {
                    log.warn("Unable to queue the delayedCallables commands for RecoveryService. "
                            + "Most possibly Callable queue is full. Queue size is :"
                            + Services.get().get(CallableQueueService.class).queueSize());
                }
                delayedCallables = null;
                this.delay = 0;
            }
        }

        /**
         * Recover coordinator jobs that are running and have lastModifiedTimestamp older than the specified interval
         */
        private void runCoordJobRecovery() {
            XLog.Info.get().clear();
            XLog log = XLog.getLog(getClass());

            CoordinatorStore store = null;
            try {
                store = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
                store.beginTrx();

                // get list of all jobs that have lastModifiedTimestamp older
                // than the specified interval
                List<CoordinatorJobBean> jobs = store.getCoordinatorJobsOlderThanStatus(coordOlderThan,
                                                                                        CoordinatorJob.Status.PREMATER.toString(), 50, false);
                //log.debug("QUEUING[{0}] PREMATER coord jobs for potential recovery", jobs.size());
                msg.append(", COORD_JOBS : " + jobs.size());
                for (CoordinatorJobBean coordJob : jobs) {
                    Services.get().get(InstrumentationService.class).get().incr(INSTRUMENTATION_GROUP,
                                                                                INSTR_RECOVERED_COORD_JOBS_COUNTER, 1);
                    queueCallable(new CoordRecoveryCommand(coordJob.getId()));
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
         * Recover coordinator actions that are staying in WAITING or SUBMITTED too long
         */
        private void runCoordActionRecovery() {
            XLog.Info.get().clear();
            XLog log = XLog.getLog(getClass());

            CoordinatorStore store = null;
            try {
                store = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
                store.beginTrx();

                List<CoordinatorActionBean> cactions = store.getRecoveryActionsOlderThan(coordOlderThan, false);
                //log.debug("QUEUING[{0}] WAITING and SUBMITTED coord actions for potential recovery", cactions.size());
                msg.append(", COORD_ACTIONS : " + cactions.size());
                for (CoordinatorActionBean caction : cactions) {
                    Services.get().get(InstrumentationService.class).get().incr(INSTRUMENTATION_GROUP,
                                                                                INSTR_RECOVERED_COORD_ACTIONS_COUNTER, 1);
                    if (caction.getStatus() == CoordinatorActionBean.Status.WAITING) {
                        queueCallable(new CoordActionInputCheckCommand(caction.getId()));
                        log.info("Recover a WAITTING coord action :" + caction.getId());
                    }
                    else {
                        if (caction.getStatus() == CoordinatorActionBean.Status.SUBMITTED) {
                            CoordinatorJobBean coordJob = store.getCoordinatorJob(caction.getJobId(), false);
                            queueCallable(new CoordActionStartCommand(caction.getId(), coordJob.getUser(), coordJob
                                    .getAuthToken()));
                            log.info("Recover a SUBMITTED coord action :" + caction.getId());
                        }
                    }
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
         * Recover coordinator actions that are staying in READY too long
         */
        private void runCoordActionRecoveryForReady() {
            XLog.Info.get().clear();
            XLog log = XLog.getLog(getClass());

            CoordinatorStore store = null;
            try {
                store = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
                store.beginTrx();
                List<String> jobids = store.getRecoveryActionsGroupByJobId(coordOlderThan);
                //log.debug("QUEUING[{0}] READY coord jobs for potential recovery", jobids.size());
                msg.append(", COORD_READY_JOBS : " + jobids.size());
                for (String jobid : jobids) {
                    queueCallable(new CoordActionReadyCommand(jobid));
                    log.info("Recover READY coord actions for jobid :" + jobid);
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
         * Recover wf actions
         */
        private void runWFRecovery() {
            XLog.Info.get().clear();
            XLog log = XLog.getLog(getClass());
            // queue command for action recovery
            WorkflowStore store = null;
            try {
                store = Services.get().get(StoreService.class).getStore(WorkflowStore.class);
                store.beginTrx();
                List<WorkflowActionBean> actions = null;
                try {
                    actions = store.getPendingActions(olderThan);
                }
                catch (StoreException ex) {
                    log.warn("Exception while reading pending actions from storage", ex);
                }
                //log.debug("QUEUING[{0}] pending wf actions for potential recovery", actions.size());
                msg.append(" WF_ACTIONS " + actions.size());

                for (WorkflowActionBean action : actions) {
                    Services.get().get(InstrumentationService.class).get().incr(INSTRUMENTATION_GROUP,
                                                                                INSTR_RECOVERED_ACTIONS_COUNTER, 1);
                    if (action.getStatus() == WorkflowActionBean.Status.PREP
                            || action.getStatus() == WorkflowActionBean.Status.START_MANUAL) {
                        queueCallable(new ActionStartCommand(action.getId(), action.getType()));
                    }
                    else {
                        if (action.getStatus() == WorkflowActionBean.Status.START_RETRY) {
                            Date nextRunTime = action.getPendingAge();
                            queueCallable(new ActionStartCommand(action.getId(), action.getType()), nextRunTime.getTime()
                                    - System.currentTimeMillis());
                        }
                        else {
                            if (action.getStatus() == WorkflowActionBean.Status.DONE
                                    || action.getStatus() == WorkflowActionBean.Status.END_MANUAL) {
                                queueCallable(new ActionEndCommand(action.getId(), action.getType()));
                            }
                            else {
                                if (action.getStatus() == WorkflowActionBean.Status.END_RETRY) {
                                    Date nextRunTime = action.getPendingAge();
                                    queueCallable(new ActionEndCommand(action.getId(), action.getType()), nextRunTime.getTime()
                                            - System.currentTimeMillis());
                                }
                                else {
                                    if (action.getStatus() == WorkflowActionBean.Status.OK
                                            || action.getStatus() == WorkflowActionBean.Status.ERROR) {
                                        queueCallable(new SignalCommand(action.getJobId(), action.getId()));
                                    }
                                }
                            }
                        }
                    }
                }
                store.commitTrx();
            }
            catch (StoreException ex) {
                if (store != null) {
                    store.rollbackTrx();
                }
                log.warn("Exception while getting store to get pending actions", ex);
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
         * RecoveryService#CONF_CALLABLE_BATCH_SIZE}, the entire batch is queued and the callables list is reset.
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
                            "Unable to queue the callables commands for RecoveryService. "
                                    + "Most possibly command queue is full. Queue size is :"
                                    + Services.get().get(CallableQueueService.class).queueSize());
                }
                callables = new ArrayList<XCallable<Void>>();
            }
        }

        /**
         * Adds callables to a list. If the number of callables in the list reaches {@link
         * RecoveryService#CONF_CALLABLE_BATCH_SIZE}, the entire batch is queued with the delay set to the maximum delay
         * of the callables in the list. The callables list and the delay is reset.
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
                    XLog.getLog(getClass()).warn("Unable to queue the delayedCallables commands for RecoveryService. "
                            + "Most possibly Callable queue is full. Queue size is :"
                            + Services.get().get(CallableQueueService.class).queueSize());
                }
                delayedCallables = new ArrayList<XCallable<Void>>();
                this.delay = 0;
            }
        }
    }

    /**
     * Initializes the RecoveryService.
     *
     * @param services services instance.
     */
    @Override
    public void init(Services services) {
        Configuration conf = services.getConf();
        Runnable recoveryRunnable = new RecoveryRunnable(conf.getInt(CONF_WF_ACTIONS_OLDER_THAN, 120), conf.getInt(
                CONF_COORD_OLDER_THAN, 600));
        services.get(SchedulerService.class).schedule(recoveryRunnable, 10, conf.getInt(CONF_SERVICE_INTERVAL, 600),
                                                      SchedulerService.Unit.SEC);
    }

    /**
     * Destroy the Recovery Service.
     */
    @Override
    public void destroy() {
    }

    /**
     * Return the public interface for the Recovery Service.
     *
     * @return {@link RecoveryService}.
     */
    @Override
    public Class<? extends Service> getInterface() {
        return RecoveryService.class;
    }
}
