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

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.DagELFunctions;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.Instrumentation;

import java.util.Date;

public class ActionEndCommand extends ActionCommand<Void> {
    public static final String COULD_NOT_END = "COULD_NOT_END";
    public static final String END_DATA_MISSING = "END_DATA_MISSING";

    private String id;

    public ActionEndCommand(String id, String type) {
        super("action.end", type, 0);
        this.id = id;
    }

    protected Void call(WorkflowStore store) throws StoreException, CommandException {
        String jobId = Services.get().get(UUIDService.class).getId(id);
        WorkflowJobBean workflow = store.getWorkflow(jobId, true);
        setLogInfo(workflow);
        WorkflowActionBean action = store.getAction(id, true);
        setLogInfo(action);
        if (action.isPending()
                && (action.getStatus() == WorkflowActionBean.Status.DONE || action.getStatus() == WorkflowActionBean.Status.END_RETRY || action
                        .getStatus() == WorkflowActionBean.Status.END_MANUAL)) {
            if (workflow.getStatus() == WorkflowJob.Status.RUNNING) {

                ActionExecutor executor = Services.get().get(ActionService.class).getExecutor(action.getType());
                Configuration conf = workflow.getWorkflowInstance().getConf();
                int maxRetries = conf.getInt(OozieClient.ACTION_MAX_RETRIES, executor.getMaxRetries());
                long retryInterval = conf.getLong(OozieClient.ACTION_RETRY_INTERVAL, executor.getRetryInterval());
                executor.setMaxRetries(maxRetries);
                executor.setRetryInterval(retryInterval);

                if (executor != null) {
                    boolean isRetry = false;
                    if (action.getStatus() == WorkflowActionBean.Status.END_RETRY
                            || action.getStatus() == WorkflowActionBean.Status.END_MANUAL) {
                        isRetry = true;
                    }
                    ActionExecutorContext context = new ActionCommand.ActionExecutorContext(workflow, action, isRetry);
                    try {

                        XLog.getLog(getClass()).debug(
                                "End, name [{0}] type [{1}] status[{2}] external status [{3}] signal value [{4}]",
                                action.getName(), action.getType(), action.getStatus(), action.getExternalStatus(),
                                action.getSignalValue());

                        DagELFunctions.setActionInfo(workflow.getWorkflowInstance(), action);
                        incrActionCounter(action.getType(), 1);

                        Instrumentation.Cron cron = new Instrumentation.Cron();
                        cron.start();
                        executor.end(context, action);
                        cron.stop();
                        addActionCron(action.getType(), cron);

                        if (!context.isEnded()) {
                            XLog.getLog(getClass()).warn(XLog.OPS,
                                "Action Ended, ActionExecutor [{0}] must call setEndData()", executor.getType());
                            action.setErrorInfo(END_DATA_MISSING, "Execution Ended, but End Data Missing from Action");
                            failJob(context);
                            store.updateAction(action);
                            store.updateWorkflow(workflow);
                            return null;
                        }
                        action.setRetries(0);
                        action.setEndTime(new Date());
                        store.updateAction(action);
                        store.updateWorkflow(workflow);
                        queueCallable(new NotificationCommand(workflow, action));
                        queueCallable(new SignalCommand(workflow.getId(), id));
                    }
                    catch (ActionExecutorException ex) {
                        XLog.getLog(getClass()).warn(
                                "Error ending action [{0}]. ErrorType [{1}], ErrorCode [{2}], Message [{3}]",
                                action.getName(), ex.getErrorType(), ex.getErrorCode(), ex.getMessage());
                        action.setErrorInfo(ex.getErrorCode(), ex.getMessage());
                        action.setEndTime(null);
                        switch (ex.getErrorType()) {
                            case TRANSIENT:
                                if (!handleTransient(context, executor, WorkflowAction.Status.END_RETRY)) {
                                    handleNonTransient(context, executor, WorkflowAction.Status.END_MANUAL);
                                    action.setPendingAge(new Date());
                                    action.setRetries(0);
                                }
                                action.setEndTime(null);
                                break;
                            case NON_TRANSIENT:
                                handleNonTransient(context, executor, WorkflowAction.Status.END_MANUAL);
                                action.setEndTime(null);
                                break;
                            case ERROR:
                                handleError(context, executor, COULD_NOT_END, false, WorkflowAction.Status.ERROR);
                                queueCallable(new SignalCommand(workflow.getId(), id));
                                break;
                            case FAILED:
                                failJob(context);
                                break;
                        }
                        store.updateAction(action);
                        store.updateWorkflow(workflow);
                    }
                }
                else {
                    throw new CommandException(ErrorCode.E0802, action.getType());
                }
            } else {
                XLog.getLog(getClass()).warn("Job state is not {0}. Skipping Action Execution",
                        WorkflowJob.Status.RUNNING.toString());
            }
        }
        return null;
    }
}