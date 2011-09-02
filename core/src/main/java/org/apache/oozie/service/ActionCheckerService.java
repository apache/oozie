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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.command.coord.CoordActionCheckCommand;
import org.apache.oozie.command.wf.ActionCheckCommand;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.Store;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.util.XCallable;
import org.apache.oozie.util.XLog;

/**
 * The Action Checker Service queue ActionCheckCommands to check the status of running actions and
 * CoordActionCheckCommands to check the status of coordinator actions. The delay between checks on the same action can
 * be configured.
 */
public class ActionCheckerService implements Service {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "ActionCheckerService.";
    /**
     * The frequency at which the ActionCheckService will run.
     */
    public static final String CONF_ACTION_CHECK_INTERVAL = CONF_PREFIX + "action.check.interval";
    /**
     * The time, in seconds, between an ActionCheck for the same action.
     */
    public static final String CONF_ACTION_CHECK_DELAY = CONF_PREFIX + "action.check.delay";

    /**
     * The number of callables to be queued in a batch.
     */
    public static final String CONF_CALLABLE_BATCH_SIZE = CONF_PREFIX + "callable.batch.size";

    protected static final String INSTRUMENTATION_GROUP = "actionchecker";
    protected static final String INSTR_CHECK_ACTIONS_COUNTER = "checks_wf_actions";
    protected static final String INSTR_CHECK_COORD_ACTIONS_COUNTER = "checks_coord_actions";

    /**
     * {@link ActionCheckRunnable} is the runnable which is scheduled to run and queue Action checks.
     */
    static class ActionCheckRunnable<S extends Store> implements Runnable {
        private int actionCheckDelay;
        private List<XCallable<Void>> callables;
        private StringBuilder msg = null;

        public ActionCheckRunnable(int actionCheckDelay) {
            this.actionCheckDelay = actionCheckDelay;
        }

        public void run() {
            XLog.Info.get().clear();
            XLog log = XLog.getLog(getClass());
            msg = new StringBuilder();
            runWFActionCheck();
            runCoordActionCheck();
            log.debug("QUEUING [{0}] for potential checking", msg.toString());
            if (null != callables) {
                boolean ret = Services.get().get(CallableQueueService.class).queueSerial(callables);
                if (ret == false) {
                    log.warn("Unable to queue the callables commands for CheckerService. "
                            + "Most possibly command queue is full. Queue size is :"
                            + Services.get().get(CallableQueueService.class).queueSize());
                }
                callables = null;
            }
        }

        /**
         * check workflow actions
         */
        private void runWFActionCheck() {
            XLog.Info.get().clear();
            XLog log = XLog.getLog(getClass());

            WorkflowStore store = null;
            try {
                store = (WorkflowStore) Services.get().get(StoreService.class).getStore(WorkflowStore.class);
                store.beginTrx();
                List<WorkflowActionBean> actions = store.getRunningActions(actionCheckDelay);
                msg.append(" WF_ACTIONS : " + actions.size());
                for (WorkflowActionBean action : actions) {
                    Services.get().get(InstrumentationService.class).get().incr(INSTRUMENTATION_GROUP,
                                                                                INSTR_CHECK_ACTIONS_COUNTER, 1);
                    queueCallable(new ActionCheckCommand(action.getId()));
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
         * check coordinator actions
         */
        private void runCoordActionCheck() {
            XLog.Info.get().clear();
            XLog log = XLog.getLog(getClass());

            CoordinatorStore store = null;
            try {
                store = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
                store.beginTrx();
                List<CoordinatorActionBean> cactions = store.getRunningActionsOlderThan(actionCheckDelay, false);
                msg.append(" COORD_ACTIONS : " + cactions.size());
                for (CoordinatorActionBean caction : cactions) {
                    Services.get().get(InstrumentationService.class).get().incr(INSTRUMENTATION_GROUP,
                                                                                INSTR_CHECK_COORD_ACTIONS_COUNTER, 1);
                    queueCallable(new CoordActionCheckCommand(caction.getId(), actionCheckDelay));
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
         * ActionCheckerService#CONF_CALLABLE_BATCH_SIZE}, the entire batch is queued and the callables list is reset.
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
                            "Unable to queue the callables commands for CheckerService. "
                                    + "Most possibly command queue is full. Queue size is :"
                                    + Services.get().get(CallableQueueService.class).queueSize());
                }
                callables = new ArrayList<XCallable<Void>>();
            }
        }
    }

    /**
     * Initializes the Action Check service.
     *
     * @param services services instance.
     */
    @Override
    public void init(Services services) {
        Configuration conf = services.getConf();
        Runnable actionCheckRunnable = new ActionCheckRunnable(conf.getInt(CONF_ACTION_CHECK_DELAY, 600));
        services.get(SchedulerService.class).schedule(actionCheckRunnable, 10,
                                                      conf.getInt(CONF_ACTION_CHECK_INTERVAL, 60), SchedulerService.Unit.SEC);
    }

    /**
     * Destroy the Action Checker Services.
     */
    @Override
    public void destroy() {
    }

    /**
     * Return the public interface for the action checker service.
     *
     * @return {@link ActionCheckerService}.
     */
    @Override
    public Class<? extends Service> getInterface() {
        return ActionCheckerService.class;
    }
}
