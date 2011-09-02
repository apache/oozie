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

import java.util.Date;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.Instrumentation;

/**
 * Executes the check command for ActionHandlers.
 * </p>
 * Ensures the action is in RUNNING state before executing
 * {@link ActionExecutor#check(org.apache.oozie.action.ActionExecutor.Context, org.apache.oozie.client.WorkflowAction)}
 */
public class ActionCheckCommand extends ActionCommand<Void> {
    public static final String EXEC_DATA_MISSING = "EXEC_DATA_MISSING";
    private String id;

    public ActionCheckCommand(String id, String type) {
        super("action.check", type, -1);
        this.id = id;
    }

    @Override
    protected Void call(WorkflowStore store) throws StoreException, CommandException {
        String jobId = Services.get().get(UUIDService.class).getId(id);
        WorkflowJobBean workflow = store.getWorkflow(jobId, true);
        setLogInfo(workflow);
        WorkflowActionBean action = store.getAction(id, true);
        setLogInfo(action);
        if (action.isPending() && action.getStatus() == WorkflowActionBean.Status.RUNNING) {
            if (workflow.getStatus() == WorkflowJob.Status.RUNNING) {
                ActionExecutor executor = Services.get().get(ActionService.class).getExecutor(action.getType());
                if (executor != null) {
                    try {
                        boolean isRetry = false;
                        ActionExecutorContext context = new ActionCommand.ActionExecutorContext(workflow, action, isRetry);
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
                        return null;
                    }
                }
            }
        }
        return null;
    }
}
