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

import org.apache.oozie.command.wf.SignalCommand;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.command.wf.ActionEndCommand;
import org.apache.oozie.command.wf.ActionStartCommand;
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
 * The Recovery Service checks for pending actions older than a configured age
 * and queues them for execution.
 */
public class ActionRecoveryService implements Service {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "ActionRecoveryService.";
    /**
     * Age of actions to queue, in seconds. 
     */
    public static final String CONF_ACTIONS_OLDER_THAN = CONF_PREFIX + "actions.older.than";
    /**
     * Time interval, in seconds, at which the action recovery service will be scheduled to run.
     */
    public static final String CONF_PENDING_ACTIONS_INTERVAL = CONF_PREFIX + "pending.actions.interval";

    /**
     * The number of callables to be queued in a batch.
     */
    public static final String CONF_CALLABLE_BATCH_SIZE = CONF_PREFIX + "callable.batch.size";

    private static final String INSTRUMENTATION_GROUP = "actionrecovery";
    private static final String INSTR_RECOVERED_ACTIONS_COUNTER = "actions";

    /**
     * ActionRecoveryRunnable is the Runnable which is scheduled to run with the
     * configured interval, and takes care of the queuing of actions.
     */
    static class ActionRecoveryRunnable implements Runnable {
        private long olderThan;
        private long delay = 0;
        private List<XCallable<Void>> callables;
        private List<XCallable<Void>> delayedCallables;

        public ActionRecoveryRunnable(long olderThan) {
            this.olderThan = olderThan;
        }

        public void run() {
            XLog.Info.get().clear();
            XLog log = XLog.getLog(getClass());

            WorkflowStore store = null;
            try {
                store = Services.get().get(WorkflowStoreService.class).create();

                List<WorkflowActionBean> actions = null;
                try {
                    actions = store.getPendingActions(olderThan);
                }
                catch (StoreException ex) {
                    log.warn("Exception while reading pending actions from storage", ex);
                }

                log.info("Queuing[{0}] pending actions for potential recovery", actions.size());

                for (WorkflowActionBean action : actions) {
                    if (action.isPending()) {
                        Services.get().get(InstrumentationService.class).get().incr(INSTRUMENTATION_GROUP,
                                INSTR_RECOVERED_ACTIONS_COUNTER, 1);
                        if (action.getStatus() == WorkflowActionBean.Status.PREP
                                || action.getStatus() == WorkflowActionBean.Status.START_MANUAL) {
                            queueCallable(new ActionStartCommand(action.getId(), action.getType()));
                        }
                        else if (action.getStatus() == WorkflowActionBean.Status.START_RETRY) {
                            Date nextRunTime = action.getPendingAge();
                            queueCallable(new ActionStartCommand(action.getId(), action.getType()),
                                          nextRunTime.getTime() - System.currentTimeMillis());
                        }
                        else if (action.getStatus() == WorkflowActionBean.Status.DONE
                                || action.getStatus() == WorkflowActionBean.Status.END_MANUAL) {
                            queueCallable(new ActionEndCommand(action.getId(), action.getType()));
                        }
                        else if (action.getStatus() == WorkflowActionBean.Status.END_RETRY) {
                            Date nextRunTime = action.getPendingAge();
                            queueCallable(new ActionEndCommand(action.getId(), action.getType()),
                                          nextRunTime.getTime() - System.currentTimeMillis());
                        }
                        else if (action.getStatus() == WorkflowActionBean.Status.OK
                                || action.getStatus() == WorkflowActionBean.Status.ERROR) {
                            queueCallable(new SignalCommand(action.getJobId(), action.getId()));
                        }
                    }
                }
                if (null != callables) {
                    Services.get().get(CallableQueueService.class).queueSerial(callables);
                    callables = null;
                }
                if (null != delayedCallables) {
                    Services.get().get(CallableQueueService.class).queueSerial(delayedCallables, this.delay);
                    delayedCallables = null;
                    this.delay = 0;
                }
            }
            catch (StoreException ex) {
                log.warn("Exception while getting store to get pending actions", ex);
            }
            finally {
                try {
                    store.close();
                }
                catch (StoreException ex) {
                    log.warn("Exception while attemting to close store", ex);
                }
            }
        }
        
        /**
         * Adds callables to a list. If the number of callables in the list
         * reaches {@link ActionRecoveryService#CONF_CALLABLE_BATCH_SIZE}, the
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

        /**
         * Adds callables to a list. If the number of callables in the list
         * reaches {@link ActionRecoveryService#CONF_CALLABLE_BATCH_SIZE}, the
         * entire batch is queued with the delay set to the maximum delay of the
         * callables in the list. The callables list and the delay is reset.
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
                Services.get().get(CallableQueueService.class).queueSerial(delayedCallables, this.delay);
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
        Runnable actionRecoveryRunnable = new ActionRecoveryRunnable(conf.getInt(CONF_ACTIONS_OLDER_THAN, 120));
        services.get(SchedulerService.class).schedule(actionRecoveryRunnable, 10,
                                                      conf.getInt(CONF_PENDING_ACTIONS_INTERVAL, 60),
                                                      SchedulerService.Unit.SEC);
    }

    /**
     * Destroy the Action Recovery Service.
     */
    @Override
    public void destroy() {
    }

    /**
     * Return the public interface for the action recovery service.
     *
     * @return {@link ActionRecoveryService}.
     */
    @Override
    public Class<? extends Service> getInterface() {
        return ActionRecoveryService.class;
    }
}
