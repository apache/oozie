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
package org.apache.oozie.command.wf;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.SLAEvent.SlaAppType;
import org.apache.oozie.client.SLAEvent.Status;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.coord.CoordActionReadyCommand;
import org.apache.oozie.command.coord.CoordActionUpdateCommand;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.coord.CoordinatorJobException;
import org.apache.oozie.service.ELService;
import org.apache.oozie.service.SchemaService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.StoreService;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.service.WorkflowStoreService;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.util.db.SLADbOperations;
import org.apache.openjpa.lib.log.Log;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;

import java.io.StringReader;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class SignalCommand extends WorkflowCommand<Void> {

    protected static final String INSTR_SUCCEEDED_JOBS_COUNTER_NAME = "succeeded";

    private String jobId;
    private String actionId;

    protected SignalCommand(String name, int priority, String jobId) {
        super(name, name, priority, XLog.STD);
        this.jobId = ParamChecker.notEmpty(jobId, "jobId");
    }

    public SignalCommand(String jobId, String actionId) {
        super("signal", "signal", 1, XLog.STD);
        this.jobId = ParamChecker.notEmpty(jobId, "jobId");
        this.actionId = ParamChecker.notEmpty(actionId, "actionId");
    }

    @Override
    protected Void call(WorkflowStore store) throws CommandException, StoreException {

        WorkflowJobBean workflow = store.getWorkflow(jobId, false);
        setLogInfo(workflow);
        WorkflowActionBean action = null;
        boolean skipAction = false;
        if (actionId != null) {
            action = store.getAction(actionId, false);
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
                            workflow.setWorkflowInstance(workflowInstance);
                            // 1. Add SLA status event for WF-JOB with status
                            // STARTED
                            // 2. Add SLA registration events for all WF_ACTIONS
                            SLADbOperations.writeStausEvent(workflow.getSlaXml(), jobId, store, Status.STARTED,
                                                            SlaAppType.WORKFLOW_JOB);
                            writeSLARegistrationForAllActions(workflowInstance.getApp().getDefinition(), workflow
                                    .getUser(), workflow.getGroup(), workflow.getConf(), store);
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
                        workflow.setWorkflowInstance(workflowInstance);
                        action.resetPending();
                        if (!skipAction) {
                            action.setTransition(workflowInstance.getTransition(action.getName()));
                        }
                        store.updateAction(action);
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
                            SLADbOperations.writeStausEvent(action.getSlaXml(), action.getId(), store, Status.FAILED,
                                                            SlaAppType.WORKFLOW_ACTION);
                            store.updateAction(actionToFail);
                        }

                        workflow.setStatus(WorkflowJob.Status.valueOf(workflowInstance.getStatus().toString()));
                        workflow.setEndTime(new Date());
                        workflow.setWorkflowInstance(workflowInstance);
                        Status slaStatus = Status.SUCCEEDED;
                        switch (workflow.getStatus()) {
                            case SUCCEEDED:
                                slaStatus = Status.SUCCEEDED;
                                break;
                            case KILLED:
                                slaStatus = Status.KILLED;
                                break;
                            case FAILED:
                                slaStatus = Status.FAILED;
                                break;
                            default: // TODO about SUSPENDED

                        }
                        SLADbOperations.writeStausEvent(workflow.getSlaXml(), jobId, store, slaStatus,
                                                        SlaAppType.WORKFLOW_JOB);
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
                                String actionSlaXml = getActionSLAXml(newAction.getName(), workflowInstance.getApp()
                                        .getDefinition(), workflow.getConf());
                                // System.out.println("111111 actionXml " +
                                // actionSlaXml);
                                // newAction.setSlaXml(workflow.getSlaXml());
                                newAction.setSlaXml(actionSlaXml);
                                store.insertAction(newAction);
                                queueCallable(new ActionStartCommand(newAction.getId(), newAction.getType()));
                            }
                        }
                    }

                    store.updateWorkflow(workflow);
                    XLog.getLog(getClass()).debug(
                            "Updated the workflow status to " + workflow.getId() + "  status ="
                                    + workflow.getStatusStr());
                    if (workflow.getStatus() != WorkflowJob.Status.RUNNING
                            && workflow.getStatus() != WorkflowJob.Status.SUSPENDED) {
                        queueCallable(new CoordActionUpdateCommand(workflow));
                    }
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

    public static ELEvaluator createELEvaluatorForGroup(Configuration conf, String group) {
        ELEvaluator eval = Services.get().get(ELService.class).createEvaluator(group);
        for (Map.Entry<String, String> entry : conf) {
            eval.setVariable(entry.getKey(), entry.getValue());
        }
        return eval;
    }

    private String getActionSLAXml(String actionName, String wfXml, String wfConf) throws CommandException {
        String slaXml = null;
        // TODO need to fill-out the code
        // Get the appropriate action:slaXml and resolve that.
        try {
            // Configuration conf = new XConfiguration(new
            // StringReader(wfConf));
            Element eWfJob = XmlUtils.parseXml(wfXml);
            // String prefix = XmlUtils.getNamespacePrefix(eWfJob,
            // SchemaService.SLA_NAME_SPACE_URI);
            for (Element action : (List<Element>) eWfJob.getChildren("action", eWfJob.getNamespace())) {
                if (action.getAttributeValue("name").equals(actionName) == false) {
                    continue;
                }
                Element eSla = action.getChild("info", Namespace.getNamespace(SchemaService.SLA_NAME_SPACE_URI));
                if (eSla != null) {
                    // resolveSla(eSla, conf);
                    slaXml = XmlUtils.prettyPrint(eSla).toString();// Could use
                    // any
                    // non-null
                    // string
                    break;
                }
            }
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E1004, e.getMessage(), e);
        }
        return slaXml;
    }

    private String resolveSla(Element eSla, Configuration conf) throws CommandException {
        String slaXml = null;
        try {
            ELEvaluator evalSla = SubmitCommand.createELEvaluatorForGroup(conf, "wf-sla-submit");
            slaXml = SubmitCommand.resolveSla(eSla, evalSla);
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E1004, e.getMessage(), e);
        }
        return slaXml;
    }

    private void writeSLARegistrationForAllActions(String wfXml, String user, String group, String strConf,
                                                   WorkflowStore store) throws CommandException {
        try {
            Element eWfJob = XmlUtils.parseXml(wfXml);
            // String prefix = XmlUtils.getNamespacePrefix(eWfJob,
            // SchemaService.SLA_NAME_SPACE_URI);
            Configuration conf = new XConfiguration(new StringReader(strConf));
            for (Element action : (List<Element>) eWfJob.getChildren("action", eWfJob.getNamespace())) {
                Element eSla = action.getChild("info", Namespace.getNamespace(SchemaService.SLA_NAME_SPACE_URI));
                if (eSla != null) {
                    String slaXml = resolveSla(eSla, conf);
                    eSla = XmlUtils.parseXml(slaXml);
                    String actionId = Services.get().get(UUIDService.class).generateChildId(jobId,
                                                                                            action.getAttributeValue("name") + "");
                    SLADbOperations.writeSlaRegistrationEvent(eSla, store, actionId, SlaAppType.WORKFLOW_ACTION, user,
                                                              group);
                }
            }
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E1007, "workflow:Actions " + jobId, e);
        }

    }

    @Override
    protected Void execute(WorkflowStore store) throws CommandException, StoreException {
        XLog.getLog(getClass()).debug("STARTED SignalCommand for jobid=" + jobId + ", actionId=" + actionId);
        try {
            if (lock(jobId)) {
                call(store);
            }
            else {
                queueCallable(new SignalCommand(jobId, actionId), LOCK_FAILURE_REQUEUE_INTERVAL);
                XLog.getLog(getClass()).warn("SignalCommand lock was not acquired - failed {0}", jobId);
            }
        }
        catch (InterruptedException e) {
            queueCallable(new SignalCommand(jobId, actionId), LOCK_FAILURE_REQUEUE_INTERVAL);
            XLog.getLog(getClass()).warn("SignalCommand lock not acquired - interrupted exception failed {0}", jobId);
        }
        XLog.getLog(getClass()).debug("ENDED SignalCommand for jobid=" + jobId + ", actionId=" + actionId);
        return null;
    }
}
