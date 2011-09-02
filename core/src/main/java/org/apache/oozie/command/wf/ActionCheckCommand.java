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
package org.apache.oozie.command.wf;

import java.sql.Timestamp;
import java.util.Date;

import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.WorkflowAction.Status;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.coord.CoordActionInputCheckCommand;
import org.apache.oozie.command.wf.ActionCommand.ActionExecutorContext;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.lite.LiteWorkflowInstance;

/**
 * Executes the check command for ActionHandlers. </p> Ensures the action is in RUNNING state before executing {@link
 * ActionExecutor#check(org.apache.oozie.action.ActionExecutor.Context, org.apache.oozie.client.WorkflowAction)}
 */
public class ActionCheckCommand extends ActionCommand<Void> {
    public static final String EXEC_DATA_MISSING = "EXEC_DATA_MISSING";
    private String id;
    private String jobId;
    private int actionCheckDelay;

    public ActionCheckCommand(String id) {
        this(id, -1);
    }

    public ActionCheckCommand(String id, int priority, int checkDelay) {
        super("action.check", "action.check", priority);
        this.id = id;
        this.actionCheckDelay = checkDelay;
    }

    public ActionCheckCommand(String id, int checkDelay) {
        this(id, -1, checkDelay);
    }

    @Override
    protected Void call(WorkflowStore store) throws StoreException, CommandException {

        // String jobId = Services.get().get(UUIDService.class).getId(id);
        WorkflowJobBean workflow = store.getWorkflow(jobId, false);
        setLogInfo(workflow);
        WorkflowActionBean action = store.getAction(id, false);
        setLogInfo(action);
        if (action.isPending() && action.getStatus() == WorkflowActionBean.Status.RUNNING) {
            setLogInfo(workflow);
            // if the action has been updated, quit this command
            if (actionCheckDelay > 0) {
                Timestamp actionCheckTs = new Timestamp(System.currentTimeMillis() - actionCheckDelay * 1000);
                Timestamp actionLmt = action.getLastCheckTimestamp();
                if (actionLmt.after(actionCheckTs)) {
                    XLog.getLog(getClass()).debug(
                            "The wf action :" + id + " has been udated recently. Ignoring ActionCheckCommand!");
                    return null;
                }
            }
            if (workflow.getStatus() == WorkflowJob.Status.RUNNING) {
                ActionExecutor executor = Services.get().get(ActionService.class).getExecutor(action.getType());
                if (executor != null) {
                    ActionExecutorContext context = null;
                    try {
                        boolean isRetry = false;
                        context = new ActionCommand.ActionExecutorContext(workflow, action, isRetry);
                        incrActionCounter(action.getType(), 1);

                        Instrumentation.Cron cron = new Instrumentation.Cron();
                        cron.start();
                        executor.check(context, action);
                        cron.stop();
                        addActionCron(action.getType(), cron);

                        if (action.isExecutionComplete()) {
                            if (!context.isExecuted()) {
                                XLog.getLog(getClass()).warn(XLog.OPS,
                                                             "Action Completed, ActionExecutor [{0}] must call setExecutionData()",
                                                             executor.getType());
                                action.setErrorInfo(EXEC_DATA_MISSING,
                                                    "Execution Complete, but Execution Data Missing from Action");
                                failJob(context);
                                action.setLastCheckTime(new Date());
                                store.updateAction(action);
                                store.updateWorkflow(workflow);
                                return null;
                            }
                            action.setPending();
                            queueCallable(new ActionEndCommand(action.getId(), action.getType()));
                        }
                        action.setLastCheckTime(new Date());
                        store.updateAction(action);
                        store.updateWorkflow(workflow);
                    }
                    catch (ActionExecutorException ex) {
                        XLog.getLog(getClass()).warn(
                                "Exception while executing check(). Error Code [{0}], Message[{1}]", ex.getErrorCode(),
                                ex.getMessage(), ex);

                        switch (ex.getErrorType()) {
                            case FAILED:
                                failAction(workflow, action);
                                break;
                        }
                        action.setLastCheckTime(new Date());
                        store.updateAction(action);
                        store.updateWorkflow(workflow);
                        return null;
                    }
                }
            }
        }
        return null;
    }

    private void failAction(WorkflowJobBean workflow, WorkflowActionBean action) throws CommandException {
        XLog.getLog(getClass()).warn("Failing Job [{0}] due to failed action [{1}]", workflow.getId(), action.getId());
        action.resetPending();
        action.setStatus(Status.FAILED);
        workflow.setStatus(WorkflowJob.Status.FAILED);
        incrJobCounter(INSTR_FAILED_JOBS_COUNTER, 1);
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        new Services().init();

        try {
            new ActionCheckCommand("0000001-100122154231282-oozie-dani-W@pig1").call();
            Thread.sleep(100000);
        }
        finally {
            new Services().destroy();
        }
    }

    @Override
    protected Void execute(WorkflowStore store) throws CommandException, StoreException {
        try {
            XLog.getLog(getClass()).debug("STARTED ActionCheckCommand for wf actionId=" + id + " priority =" + getPriority());
            jobId = Services.get().get(UUIDService.class).getId(id);
            if (lock(jobId)) {
                call(store);
            }
            else {
                queueCallable(new ActionCheckCommand(id, actionCheckDelay), LOCK_FAILURE_REQUEUE_INTERVAL);
                XLog.getLog(getClass()).warn("ActionCheckCommand lock was not acquired - failed {0}", id);
            }
        }
        catch (InterruptedException e) {
            queueCallable(new ActionCheckCommand(id, actionCheckDelay), LOCK_FAILURE_REQUEUE_INTERVAL);
            XLog.getLog(getClass()).warn("ActionCheckCommand lock was not acquired - interrupted exception failed {0}",
                                         id);
        }
        XLog.getLog(getClass()).debug("ENDED ActionCheckCommand for wf actionId=" + id + ", jobId=" + jobId);
        return null;
    }
}
