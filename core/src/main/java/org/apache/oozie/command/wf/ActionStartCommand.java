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
import java.io.StringReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.SLAEvent.SlaAppType;
import org.apache.oozie.client.SLAEvent.Status;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.FaultInjection;
import org.apache.oozie.XException;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.coord.CoordActionUpdateCommand;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.util.ELEvaluationException;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.util.db.SLADbOperations;
import org.apache.oozie.util.XConfiguration;

import javax.servlet.jsp.el.ELException;

public class ActionStartCommand extends ActionCommand<Void> {
    public static final String EL_ERROR = "EL_ERROR";
    public static final String EL_EVAL_ERROR = "EL_EVAL_ERROR";
    public static final String COULD_NOT_START = "COULD_NOT_START";
    public static final String START_DATA_MISSING = "START_DATA_MISSING";
    public static final String EXEC_DATA_MISSING = "EXEC_DATA_MISSING";

    private String id;
    private String jobId;

    public ActionStartCommand(String id, String type) {
        super("action.start", type, 0);
        this.id = id;
    }

    protected Void call(WorkflowStore store) throws StoreException, CommandException {
        WorkflowJobBean workflow = store.getWorkflow(jobId, false);
        setLogInfo(workflow);
        WorkflowActionBean action = store.getAction(id, false);
        XLog.getLog(getClass()).warn(XLog.STD,
                                     "[***" + action.getId() + "***]" + "In call()....status=" + action.getStatusStr());
        setLogInfo(action);
        if (action.isPending()
                && (action.getStatus() == WorkflowActionBean.Status.PREP
                || action.getStatus() == WorkflowActionBean.Status.START_RETRY || action.getStatus() == WorkflowActionBean.Status.START_MANUAL)) {
            if (workflow.getStatus() == WorkflowJob.Status.RUNNING) {

                ActionExecutor executor = Services.get().get(ActionService.class).getExecutor(action.getType());
                Configuration conf = workflow.getWorkflowInstance().getConf();

                int maxRetries = conf.getInt(OozieClient.ACTION_MAX_RETRIES, executor.getMaxRetries());
                long retryInterval = conf.getLong(OozieClient.ACTION_RETRY_INTERVAL, executor.getRetryInterval());
                executor.setMaxRetries(maxRetries);
                executor.setRetryInterval(retryInterval);

                if (executor != null) {
                    ActionExecutorContext context = null;
                    try {
                        boolean isRetry = false;
                        if (action.getStatus() == WorkflowActionBean.Status.START_RETRY
                                || action.getStatus() == WorkflowActionBean.Status.START_MANUAL) {
                            isRetry = true;
                        }
                        context = new ActionCommand.ActionExecutorContext(workflow, action, isRetry);
                        try {
                            String tmpActionConf = XmlUtils.removeComments(action.getConf());
                            String actionConf = context.getELEvaluator().evaluate(tmpActionConf, String.class);
                            action.setConf(actionConf);

                            XLog.getLog(getClass()).debug("Start, name [{0}] type [{1}] configuration{E}{E}{2}{E}",
                                                          action.getName(), action.getType(), actionConf);

                        }
                        catch (ELEvaluationException ex) {
                            throw new ActionExecutorException(ActionExecutorException.ErrorType.TRANSIENT,
                                                              EL_EVAL_ERROR, ex.getMessage(), ex);
                        }
                        catch (ELException ex) {
                            context.setErrorInfo(EL_ERROR, ex.getMessage());
                            XLog.getLog(getClass()).warn("ELException in ActionStartCommand ", ex.getMessage(), ex);
                            handleError(context, store, workflow, action);
                            return null;
                        }
                        catch (org.jdom.JDOMException je) {
                            context.setErrorInfo("ParsingError", je.getMessage());
                            XLog.getLog(getClass()).warn("JDOMException in ActionStartCommand ", je.getMessage(), je);
                            handleError(context, store, workflow, action);
                            return null;
                        }
                        catch (Exception ex) {
                            context.setErrorInfo(EL_ERROR, ex.getMessage());
                            XLog.getLog(getClass()).warn("Exception in ActionStartCommand ", ex.getMessage(), ex);
                            handleError(context, store, workflow, action);
                            return null;
                        }
                        action.setErrorInfo(null, null);
                        incrActionCounter(action.getType(), 1);

                        Instrumentation.Cron cron = new Instrumentation.Cron();
                        cron.start();
                        executor.start(context, action);
                        cron.stop();
                        FaultInjection.activate("org.apache.oozie.command.SkipCommitFaultInjection");
                        addActionCron(action.getType(), cron);

                        action.setRetries(0);
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
                        else {
                            if (!context.isStarted()) {
                                XLog.getLog(getClass()).warn(XLog.OPS,
                                                             "Action Started, ActionExecutor [{0}] must call setStartData()",
                                                             executor.getType());
                                action.setErrorInfo(START_DATA_MISSING,
                                                    "Execution Started, but Start Data Missing from Action");
                                failJob(context);
                                store.updateAction(action);
                                store.updateWorkflow(workflow);
                                return null;
                            }
                            queueCallable(new NotificationCommand(workflow, action));
                        }

                        XLog.getLog(getClass()).warn(XLog.STD,
                                                     "[***" + action.getId() + "***]" + "Action status=" + action.getStatusStr());

                        store.updateAction(action);
                        store.updateWorkflow(workflow);
                        // Add SLA status event (STARTED) for WF_ACTION
                        // SLADbOperations.writeSlaStatusEvent(eSla,
                        // action.getId(), Status.STARTED, store);
                        SLADbOperations.writeStausEvent(action.getSlaXml(), action.getId(), store, Status.STARTED,
                                                        SlaAppType.WORKFLOW_ACTION);
                        XLog.getLog(getClass()).warn(XLog.STD,
                                                     "[***" + action.getId() + "***]" + "Action updated in DB!");

                    }
                    catch (ActionExecutorException ex) {
                        XLog.getLog(getClass()).warn(
                                "Error starting action [{0}]. ErrorType [{1}], ErrorCode [{2}], Message [{3}]",
                                action.getName(), ex.getErrorType(), ex.getErrorCode(), ex.getMessage(), ex);
                        action.setErrorInfo(ex.getErrorCode(), ex.getMessage());
                        switch (ex.getErrorType()) {
                            case TRANSIENT:
                                if (!handleTransient(context, executor, WorkflowAction.Status.START_RETRY)) {
                                    handleNonTransient(context, executor, WorkflowAction.Status.START_MANUAL);
                                    action.setPendingAge(new Date());
                                    action.setRetries(0);
                                    action.setStartTime(null);
                                }
                                break;
                            case NON_TRANSIENT:
                                handleNonTransient(context, executor, WorkflowAction.Status.START_MANUAL);
                                break;
                            case ERROR:
                                handleError(context, executor, WorkflowAction.Status.ERROR.toString(), true,
                                            WorkflowAction.Status.DONE);
                                break;
                            case FAILED:
                                try {
                                    failJob(context);
                                    queueCallable(new CoordActionUpdateCommand(workflow));
                                    SLADbOperations.writeStausEvent(action.getSlaXml(), action.getId(), store,
                                                                    Status.FAILED, SlaAppType.WORKFLOW_ACTION);
                                    SLADbOperations.writeStausEvent(workflow.getSlaXml(), workflow.getId(), store,
                                                                    Status.FAILED, SlaAppType.WORKFLOW_JOB);
                                }
                                catch (XException x) {
                                    XLog.getLog(getClass()).warn("ActionStartCommand - case:FAILED ", x.getMessage());
                                }
                                break;
                        }
                        store.updateAction(action);
                        store.updateWorkflow(workflow);
                    }
                }
                else {
                    throw new CommandException(ErrorCode.E0802, action.getType());
                }

            }
            else {
                XLog.getLog(getClass()).warn("Job state is not {0}. Skipping Action Execution",
                                             WorkflowJob.Status.RUNNING.toString());
            }
        }
        return null;
    }

    private void handleError(ActionExecutorContext context, WorkflowStore store, WorkflowJobBean workflow,
                             WorkflowActionBean action) throws CommandException, StoreException {
        failJob(context);
        store.updateAction(action);
        store.updateWorkflow(workflow);
        SLADbOperations.writeStausEvent(action.getSlaXml(), action.getId(), store, Status.FAILED,
                                        SlaAppType.WORKFLOW_ACTION);
        SLADbOperations.writeStausEvent(workflow.getSlaXml(), workflow.getId(), store, Status.FAILED,
                                        SlaAppType.WORKFLOW_JOB);
        queueCallable(new CoordActionUpdateCommand(workflow));
        return;
    }

    @Override
    protected Void execute(WorkflowStore store) throws CommandException, StoreException {
        try {
            XLog.getLog(getClass()).debug("STARTED ActionStartCommand for wf actionId=" + id);
            jobId = Services.get().get(UUIDService.class).getId(id);
            if (lock(jobId)) {
                call(store);
            }
            else {
                queueCallable(new ActionStartCommand(id, type), LOCK_FAILURE_REQUEUE_INTERVAL);
                XLog.getLog(getClass()).warn("ActionStartCommand lock was not acquired - failed {0}", id);
            }
        }
        catch (InterruptedException e) {
            queueCallable(new ActionStartCommand(id, type), LOCK_FAILURE_REQUEUE_INTERVAL);
            XLog.getLog(getClass()).warn("ActionStartCommand lock was not acquired - interrupted exception failed {0}",
                                         id);
        }
        XLog.getLog(getClass()).debug("ENDED ActionStartCommand for wf actionId=" + id + ", jobId=" + jobId);
        return null;
    }

}
