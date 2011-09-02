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

import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.command.Command;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.service.WorkflowStoreService;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.ParamChecker;

import java.util.Date;

public class SignalCommand extends Command<Void> {

    protected static final String INSTR_SUCCEEDED_JOBS_COUNTER_NAME = "succeeded";

    private String jobId;
    private String actionId;

    protected SignalCommand(String name, int priority, String jobId) {
        super(name, name, priority, XLog.STD);
        this.jobId = ParamChecker.notEmpty(jobId, "jobId");
    }

    public SignalCommand(String jobId, String actionId) {
        super("signal", "signal", 0, XLog.STD);
        this.jobId = ParamChecker.notEmpty(jobId, "jobId");
        this.actionId = ParamChecker.notEmpty(actionId, "actionId");
    }

    @Override
    protected Void call(WorkflowStore store) throws CommandException, StoreException {
        WorkflowJobBean workflow = store.getWorkflow(jobId, true);
        setLogInfo(workflow);
        WorkflowActionBean action = null;
        boolean skipAction = false;
        if (actionId != null) {
            action = store.getAction(actionId, true);
            setLogInfo(action);
        }
        if ((action == null) || (action.isComplete() && action.isPending())) {
            try {
                if (workflow.getStatus() == WorkflowJob.Status.RUNNING
                        || workflow.getStatus() == WorkflowJob.Status.PREP) {
                    WorkflowInstance workflowInstance = workflow.getWorkflowInstance();
                    workflowInstance.setTransientVar(WorkflowStoreService.WORKFLOW_BEAN, workflow);
                    boolean completed;
                    if (action == null) {
                        if (workflow.getStatus() == WorkflowJob.Status.PREP) {
                            completed = workflowInstance.start();
                            workflow.setStatus(WorkflowJob.Status.RUNNING);
                            workflow.setStartTime(new Date());
                            queueCallable(new NotificationCommand(workflow));
                        }
                        else {
                            throw new CommandException(ErrorCode.E0801, workflow.getId());
                        }
                    }
                    else {
                        String skipVar = workflowInstance.getVar(action.getName() + WorkflowInstance.NODE_VAR_SEPARATOR
                                + ReRunCommand.TO_SKIP);
                        if (skipVar != null) {
                            skipAction = skipVar.equals("true");
                        }
                        completed = workflowInstance.signal(action.getExecutionPath(), action.getSignalValue());
                        action.resetPending();
                        if (!skipAction) {
                            action.setTransition(workflowInstance.getTransition(action.getName()));
                            store.updateAction(action);
                        }
                    }

                    if (completed) {
                        for (String actionToKillId : WorkflowStoreService.getActionsToKill(workflowInstance)) {
                            WorkflowActionBean actionToKill = store.getAction(actionToKillId, false);
                            actionToKill.setPending();
                            actionToKill.setStatus(WorkflowActionBean.Status.KILLED);
                            store.updateAction(actionToKill);
                            queueCallable(new ActionKillCommand(actionToKill.getId(), actionToKill.getType()));
                        }

                        for (String actionToFailId : WorkflowStoreService.getActionsToFail(workflowInstance)) {
                            WorkflowActionBean actionToFail = store.getAction(actionToFailId, false);
                            actionToFail.resetPending();
                            actionToFail.setStatus(WorkflowActionBean.Status.FAILED);
                            store.updateAction(actionToFail);
                        }

                        workflow.setStatus(WorkflowJob.Status.valueOf(workflowInstance.getStatus().toString()));
                        workflow.setEndTime(new Date());
                        queueCallable(new NotificationCommand(workflow));
                        if (workflow.getStatus() == WorkflowJob.Status.SUCCEEDED) {
                            incrJobCounter(INSTR_SUCCEEDED_JOBS_COUNTER_NAME, 1);
                        }
                    }
                    else {
                        for (WorkflowActionBean newAction : WorkflowStoreService.getStartedActions(workflowInstance)) {
                            String skipVar = workflowInstance.getVar(newAction.getName()
                                    + WorkflowInstance.NODE_VAR_SEPARATOR + ReRunCommand.TO_SKIP);
                            boolean skipNewAction = false;
                            if (skipVar != null) {
                                skipNewAction = skipVar.equals("true");
                            }
                            if (skipNewAction) {
                                WorkflowActionBean oldAction = store.getAction(newAction.getId(), false);
                                oldAction.setPending();
                                store.updateAction(oldAction);
                                queueCallable(new SignalCommand(jobId, oldAction.getId()));
                            }
                            else {
                                newAction.setPending();
                                store.insertAction(newAction);
                                queueCallable(new ActionStartCommand(newAction.getId(), newAction.getType()));
                            }
                        }
                    }
                    store.updateWorkflow(workflow);
                }
                else {
                    XLog.getLog(getClass()).warn("Workflow not RUNNING, current status [{0}]", workflow.getStatus());
                }
            }
            catch (WorkflowException ex) {
                throw new CommandException(ex);
            }
        }
        else {
            XLog.getLog(getClass()).warn(
                    "SignalCommand for action id :" + actionId + " is already processed. status=" + action.getStatus()
                            + ", Pending=" + action.isPending());
        }
        return null;
    }

}
