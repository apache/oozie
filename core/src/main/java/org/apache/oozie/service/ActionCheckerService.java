/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.service;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.command.wf.ActionCheckCommand;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.service.CallableQueueService;
import org.apache.oozie.service.InstrumentationService;
import org.apache.oozie.service.SchedulerService;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XCallable;
import org.apache.oozie.util.XLog;

/**
 * The Action Checker Service queue ActionCheckCommands to check the status of
 * running actions. The delay between checks on the same action can be
 * configured.
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
    protected static final String INSTR_CHECK_ACTIONS_COUNTER = "checks";

    /**
     * {@link ActionCheckRunnable} is the runnable which is scheduled to run and
     * queue Action checks.
     */
    static class ActionCheckRunnable implements Runnable {
        private int actionCheckDelay;
        private List<XCallable<Void>> callables;

        public ActionCheckRunnable(int actionCheckDelay) {
            this.actionCheckDelay = actionCheckDelay;
        }

        public void run() {
            XLog.Info.get().clear();
            XLog log = XLog.getLog(getClass());

            WorkflowStore store = null;
            try {
                store = Services.get().get(WorkflowStoreService.class).create();
                List<WorkflowActionBean> actions =  store.getRunningActions(actionCheckDelay);
                for (WorkflowActionBean action : actions) {
                    if (action.isPending() && action.getStatus() == WorkflowActionBean.Status.RUNNING) {
                        Services.get().get(InstrumentationService.class).get().incr(INSTRUMENTATION_GROUP,
                                INSTR_CHECK_ACTIONS_COUNTER, 1);
                        queueCallable(new ActionCheckCommand(action.getId(), action.getType()));
                    }
                }
                if (null != callables) {
                    Services.get().get(CallableQueueService.class).queueSerial(callables);
                    callables = null;
                }
                log.info("Queuing [{0}] running actions for external status check", actions.size());
            }
            catch (StoreException ex) {
                log.warn(XLog.OPS, "Exception while accessing the store", ex);
            }
            finally {
                try {
                    if (store != null) {
                        store.close();
                    }
                }
                catch (StoreException ex) {
                    log.warn("Exception while attempting to close store", ex);
                }
            }
        }

        /**
         * Adds callables to a list. If the number of callables in the list
         * reaches {@link ActionCheckerService#CONF_CALLABLE_BATCH_SIZE}, the
         * entire batch is queued and the callables list is reset.
         *
         * @param callable the callable to queue.
         */
        private void queueCallable(XCallable<Void> callable) {
            if (callables == null) {
                callables = new ArrayList<XCallable<Void>>();
            }
            callables.add(callable);
            if (callables.size() == Services.get().getConf().getInt(CONF_CALLABLE_BATCH_SIZE, 10)) {
                Services.get().get(CallableQueueService.class).queueSerial(callables);
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
                                                      conf.getInt(CONF_ACTION_CHECK_INTERVAL, 60),
                                                      SchedulerService.Unit.SEC);
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
