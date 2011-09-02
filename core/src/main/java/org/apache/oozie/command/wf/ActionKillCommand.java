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

import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.SLAEvent.SlaAppType;
import org.apache.oozie.client.SLAEvent.Status;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.db.SLADbOperations;

public class ActionKillCommand extends ActionCommand<Void> {
    private String id;
    private String jobId;

    public ActionKillCommand(String id, String type) {
        super("action.kill", type, 0);
        this.id = id;
    }

    protected Void call(WorkflowStore store) throws StoreException, CommandException {
        // String jobId = Services.get().get(UUIDService.class).getId(id);
        WorkflowJobBean workflow = store.getWorkflow(jobId, false);
        setLogInfo(workflow);
        WorkflowActionBean action = store.getAction(id, false);
        setLogInfo(action);
        if (action.isPending() && (action.getStatus() == WorkflowActionBean.Status.KILLED)) {
            ActionExecutor executor = Services.get().get(ActionService.class).getExecutor(action.getType());
            if (executor != null) {
                try {
                    boolean isRetry = false;
                    ActionExecutorContext context = new ActionCommand.ActionExecutorContext(workflow, action, isRetry);
                    incrActionCounter(action.getType(), 1);

                    Instrumentation.Cron cron = new Instrumentation.Cron();
                    cron.start();
                    executor.kill(context, action);
                    cron.stop();
                    addActionCron(action.getType(), cron);

                    action.resetPending();
                    action.setStatus(WorkflowActionBean.Status.KILLED);

                    store.updateAction(action);
                    store.updateWorkflow(workflow);
                    // Add SLA status event (KILLED) for WF_ACTION
                    SLADbOperations.writeStausEvent(action.getSlaXml(), action.getId(), store, Status.KILLED,
                                                    SlaAppType.WORKFLOW_ACTION);
                    queueCallable(new NotificationCommand(workflow, action));
                }
                catch (ActionExecutorException ex) {
                    action.resetPending();
                    action.setStatus(WorkflowActionBean.Status.FAILED);
                    action.setErrorInfo(ex.getErrorCode().toString(),
                                        "KILL COMMAND FAILED - exception while executing job kill");
                    workflow.setStatus(WorkflowJobBean.Status.KILLED);
                    store.updateAction(action);
                    store.updateWorkflow(workflow);
                    // What will happen to WF and COORD_ACTION, NOTIFICATION?
                    SLADbOperations.writeStausEvent(action.getSlaXml(), action.getId(), store, Status.FAILED,
                                                    SlaAppType.WORKFLOW_ACTION);
                    XLog.getLog(getClass()).warn("Exception while executing kill(). Error Code [{0}], Message[{1}]",
                                                 ex.getErrorCode(), ex.getMessage(), ex);
                }
            }
        }
        return null;
    }

    @Override
    protected Void execute(WorkflowStore store) throws CommandException, StoreException {
        XLog.getLog(getClass()).debug("STARTED ActionKillCommand for action " + id);
        try {
            jobId = Services.get().get(UUIDService.class).getId(id);
            if (lock(jobId)) {
                call(store);
            }
            else {
                queueCallable(new ActionKillCommand(id, type), LOCK_FAILURE_REQUEUE_INTERVAL);
                XLog.getLog(getClass()).warn("ActionKill lock was not acquired - failed {0}", id);
            }
        }
        catch (InterruptedException e) {
            queueCallable(new ActionKillCommand(id, type), LOCK_FAILURE_REQUEUE_INTERVAL);
            XLog.getLog(getClass()).warn("ActionKill lock was not acquired - interrupted exception failed {0}", id);
        }
        finally {
            XLog.getLog(getClass()).debug("ENDED ActionKillCommand for action " + id);
        }
        return null;
    }
}