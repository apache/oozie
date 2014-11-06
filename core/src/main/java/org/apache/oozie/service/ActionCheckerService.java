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
import java.util.List;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.coord.CoordActionCheckXCommand;
import org.apache.oozie.command.wf.ActionCheckXCommand;
import org.apache.oozie.executor.jpa.CoordActionsRunningGetJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor.WorkflowActionQuery;
import org.apache.oozie.util.XCallable;
import org.apache.oozie.util.XLog;

/**
 * The Action Checker Service queue ActionCheckCommands to check the status of
 * running actions and CoordActionCheckCommands to check the status of
 * coordinator actions. The delay between checks on the same action can be
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
    protected static final String INSTR_CHECK_ACTIONS_COUNTER = "checks_wf_actions";
    protected static final String INSTR_CHECK_COORD_ACTIONS_COUNTER = "checks_coord_actions";


    /**
     * {@link ActionCheckRunnable} is the runnable which is scheduled to run and
     * queue Action checks.
     */
    static class ActionCheckRunnable implements Runnable {
        private int actionCheckDelay;
        private List<XCallable<Void>> callables;
        private StringBuilder msg = null;

        public ActionCheckRunnable(int actionCheckDelay) {
            this.actionCheckDelay = actionCheckDelay;
        }

        public void run() {
            XLog.Info.get().clear();
            XLog LOG = XLog.getLog(getClass());
            msg = new StringBuilder();
            try {
                runWFActionCheck();
                runCoordActionCheck();
            }
            catch (CommandException ce) {
                LOG.error("Unable to run action checks, ", ce);
            }

            LOG.debug("QUEUING [{0}] for potential checking", msg.toString());
            if (null != callables) {
                boolean ret = Services.get().get(CallableQueueService.class).queueSerial(callables);
                if (ret == false) {
                    LOG.warn("Unable to queue the callables commands for CheckerService. "
                            + "Most possibly command queue is full. Queue size is :"
                            + Services.get().get(CallableQueueService.class).queueSize());
                }
                callables = null;
            }
        }

        /**
         * check workflow actions
         *
         * @throws CommandException
         */
        private void runWFActionCheck() throws CommandException {
            JPAService jpaService = Services.get().get(JPAService.class);
            if (jpaService == null) {
                throw new CommandException(ErrorCode.E0610);
            }

            List<WorkflowActionBean> actions;
            try {
                actions = WorkflowActionQueryExecutor.getInstance().getList(WorkflowActionQuery.GET_RUNNING_ACTIONS,
                        actionCheckDelay);
            }
            catch (JPAExecutorException je) {
                throw new CommandException(je);
            }

            if (actions == null || actions.isEmpty()) {
                return;
            }

            List<String> actionIds = toIds(actions);
            try {
                actionIds = Services.get().get(JobsConcurrencyService.class).getJobIdsForThisServer(actionIds);
            }
            catch (Exception ex) {
                throw new CommandException(ErrorCode.E1700, ex.getMessage(), ex);
            }

            msg.append(" WF_ACTIONS : ").append(actionIds.size());

            for (String actionId : actionIds) {
                Services.get().get(InstrumentationService.class).get().incr(INSTRUMENTATION_GROUP,
                        INSTR_CHECK_ACTIONS_COUNTER, 1);
                    queueCallable(new ActionCheckXCommand(actionId));
            }

        }

        /**
         * check coordinator actions
         *
         * @throws CommandException
         */
        private void runCoordActionCheck() throws CommandException {
            JPAService jpaService = Services.get().get(JPAService.class);
            if (jpaService == null) {
                throw new CommandException(ErrorCode.E0610);
            }

            List<String> cactionIds;
            try {
                cactionIds = jpaService.execute(new CoordActionsRunningGetJPAExecutor(
                        actionCheckDelay));
            }
            catch (JPAExecutorException je) {
                throw new CommandException(je);
            }

            if (cactionIds == null || cactionIds.isEmpty()) {
                return;
            }

            try {
                cactionIds = Services.get().get(JobsConcurrencyService.class).getJobIdsForThisServer(cactionIds);
            }
            catch (Exception ex) {
                throw new CommandException(ErrorCode.E1700, ex.getMessage(), ex);
            }

            msg.append(" COORD_ACTIONS : ").append(cactionIds.size());

            for (String coordActionId : cactionIds) {
                Services.get().get(InstrumentationService.class).get().incr(INSTRUMENTATION_GROUP,
                        INSTR_CHECK_COORD_ACTIONS_COUNTER, 1);
                    queueCallable(new CoordActionCheckXCommand(coordActionId, actionCheckDelay));
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
            if (callables.size() == ConfigurationService.getInt(CONF_CALLABLE_BATCH_SIZE)) {
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

        private List<String> toIds(List<WorkflowActionBean> actions) {
            List<String> ids = new ArrayList<String>(actions.size());
            for (WorkflowActionBean action : actions) {
                ids.add(action.getId());
            }
            return ids;
        }
    }

    /**
     * Initializes the Action Check service.
     *
     * @param services services instance.
     */
    @Override
    public void init(Services services) {
        Runnable actionCheckRunnable = new ActionCheckRunnable(ConfigurationService.getInt
                (services.getConf(), CONF_ACTION_CHECK_DELAY));
        services.get(SchedulerService.class).schedule(actionCheckRunnable, 10,
                ConfigurationService.getInt(services.getConf(), CONF_ACTION_CHECK_INTERVAL),
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
