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
package org.apache.oozie.command.wf;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.SLAEvent.SlaAppType;
import org.apache.oozie.client.SLAEvent.Status;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.coord.CoordActionUpdateXCommand;
import org.apache.oozie.command.wf.ActionXCommand.ActionExecutorContext;
import org.apache.oozie.executor.jpa.BulkUpdateInsertJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.apache.oozie.service.ELService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.SchemaService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.service.WorkflowStoreService;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.lite.KillNodeDef;
import org.apache.oozie.workflow.lite.NodeDef;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.InstrumentUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.util.db.SLADbXOperations;
import org.jdom.Element;
import org.jdom.Namespace;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class SignalXCommand extends WorkflowXCommand<Void> {

    protected static final String INSTR_SUCCEEDED_JOBS_COUNTER_NAME = "succeeded";

    private JPAService jpaService = null;
    private String jobId;
    private String actionId;
    private WorkflowJobBean wfJob;
    private WorkflowActionBean wfAction;
    private List<JsonBean> updateList = new ArrayList<JsonBean>();
    private List<JsonBean> insertList = new ArrayList<JsonBean>();


    public SignalXCommand(String name, int priority, String jobId) {
        super(name, name, priority);
        this.jobId = ParamChecker.notEmpty(jobId, "jobId");
    }

    public SignalXCommand(String jobId, String actionId) {
        this("signal", 1, jobId);
        this.actionId = ParamChecker.notEmpty(actionId, "actionId");
    }

    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    public String getEntityKey() {
        return this.jobId;
    }

    @Override
    protected void loadState() throws CommandException {
        try {
            jpaService = Services.get().get(JPAService.class);
            if (jpaService != null) {
                this.wfJob = jpaService.execute(new WorkflowJobGetJPAExecutor(jobId));
                LogUtils.setLogInfo(wfJob, logInfo);
                if (actionId != null) {
                    this.wfAction = jpaService.execute(new WorkflowActionGetJPAExecutor(actionId));
                    LogUtils.setLogInfo(wfAction, logInfo);
                }
            }
            else {
                throw new CommandException(ErrorCode.E0610);
            }
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if ((wfAction == null) || (wfAction.isComplete() && wfAction.isPending())) {
            if (wfJob.getStatus() != WorkflowJob.Status.RUNNING && wfJob.getStatus() != WorkflowJob.Status.PREP) {
                throw new PreconditionException(ErrorCode.E0813, wfJob.getStatusStr());
            }
        }
        else {
            throw new PreconditionException(ErrorCode.E0814, actionId, wfAction.getStatusStr(), wfAction.isPending());
        }
    }

    @Override
    protected Void execute() throws CommandException {
        LOG.debug("STARTED SignalCommand for jobid=" + jobId + ", actionId=" + actionId);
        WorkflowInstance workflowInstance = wfJob.getWorkflowInstance();
        workflowInstance.setTransientVar(WorkflowStoreService.WORKFLOW_BEAN, wfJob);
        boolean completed = false;
        boolean skipAction = false;
        if (wfAction == null) {
            if (wfJob.getStatus() == WorkflowJob.Status.PREP) {
                try {
                    completed = workflowInstance.start();
                }
                catch (WorkflowException e) {
                    throw new CommandException(e);
                }
                wfJob.setStatus(WorkflowJob.Status.RUNNING);
                wfJob.setStartTime(new Date());
                wfJob.setWorkflowInstance(workflowInstance);
                // 1. Add SLA status event for WF-JOB with status STARTED
                // 2. Add SLA registration events for all WF_ACTIONS
                SLADbXOperations.writeStausEvent(wfJob.getSlaXml(), jobId, Status.STARTED, SlaAppType.WORKFLOW_JOB);
                writeSLARegistrationForAllActions(workflowInstance.getApp().getDefinition(), wfJob.getUser(), wfJob
                        .getGroup(), wfJob.getConf());
                queue(new NotificationXCommand(wfJob));
            }
            else {
                throw new CommandException(ErrorCode.E0801, wfJob.getId());
            }
        }
        else {
            String skipVar = workflowInstance.getVar(wfAction.getName() + WorkflowInstance.NODE_VAR_SEPARATOR
                    + ReRunXCommand.TO_SKIP);
            if (skipVar != null) {
                skipAction = skipVar.equals("true");
            }
            try {
                completed = workflowInstance.signal(wfAction.getExecutionPath(), wfAction.getSignalValue());
            }
            catch (WorkflowException e) {
                throw new CommandException(e);
            }
            wfJob.setWorkflowInstance(workflowInstance);
            wfAction.resetPending();
            if (!skipAction) {
                wfAction.setTransition(workflowInstance.getTransition(wfAction.getName()));
                queue(new NotificationXCommand(wfJob, wfAction));
            }
            updateList.add(wfAction);
        }

        if (completed) {
            try {
                for (String actionToKillId : WorkflowStoreService.getActionsToKill(workflowInstance)) {
                    WorkflowActionBean actionToKill;

                    actionToKill = jpaService.execute(new WorkflowActionGetJPAExecutor(actionToKillId));

                    actionToKill.setPending();
                    actionToKill.setStatus(WorkflowActionBean.Status.KILLED);
                    updateList.add(actionToKill);
                    queue(new ActionKillXCommand(actionToKill.getId(), actionToKill.getType()));
                }

                for (String actionToFailId : WorkflowStoreService.getActionsToFail(workflowInstance)) {
                    WorkflowActionBean actionToFail = jpaService.execute(new WorkflowActionGetJPAExecutor(
                            actionToFailId));
                    actionToFail.resetPending();
                    actionToFail.setStatus(WorkflowActionBean.Status.FAILED);
                    queue(new NotificationXCommand(wfJob, actionToFail));
                    SLADbXOperations.writeStausEvent(wfAction.getSlaXml(), wfAction.getId(), Status.FAILED,
                            SlaAppType.WORKFLOW_ACTION);
                    updateList.add(actionToFail);
                }
            }
            catch (JPAExecutorException je) {
                throw new CommandException(je);
            }

            wfJob.setStatus(WorkflowJob.Status.valueOf(workflowInstance.getStatus().toString()));
            wfJob.setEndTime(new Date());
            wfJob.setWorkflowInstance(workflowInstance);
            Status slaStatus = Status.SUCCEEDED;
            switch (wfJob.getStatus()) {
                case SUCCEEDED:
                    slaStatus = Status.SUCCEEDED;
                    break;
                case KILLED:
                    slaStatus = Status.KILLED;
                    break;
                case FAILED:
                    slaStatus = Status.FAILED;
                    break;
                default: // TODO SUSPENDED
                    break;
            }
            SLADbXOperations.writeStausEvent(wfJob.getSlaXml(), jobId, slaStatus, SlaAppType.WORKFLOW_JOB);
            queue(new NotificationXCommand(wfJob));
            if (wfJob.getStatus() == WorkflowJob.Status.SUCCEEDED) {
                InstrumentUtils.incrJobCounter(INSTR_SUCCEEDED_JOBS_COUNTER_NAME, 1, getInstrumentation());
            }

            // output message for Kill node
            if (wfAction != null) { // wfAction could be a no-op job
                NodeDef nodeDef = workflowInstance.getNodeDef(wfAction.getExecutionPath());
                if (nodeDef != null && nodeDef instanceof KillNodeDef) {
                    boolean isRetry = false;
                    boolean isUserRetry = false;
                    ActionExecutorContext context = new ActionXCommand.ActionExecutorContext(wfJob, wfAction, isRetry,
                            isUserRetry);
                    try {
                        String tmpNodeConf = nodeDef.getConf();
                        String actionConf = context.getELEvaluator().evaluate(tmpNodeConf, String.class);
                        LOG.debug("Try to resolve KillNode message for jobid [{0}], actionId [{1}], before resolve [{2}], after resolve [{3}]",
                                        jobId, actionId, tmpNodeConf, actionConf);
                        if (wfAction.getErrorCode() != null) {
                            wfAction.setErrorInfo(wfAction.getErrorCode(), actionConf);
                        }
                        else {
                            wfAction.setErrorInfo(ErrorCode.E0729.toString(), actionConf);
                        }
                        updateList.add(wfAction);
                    }
                    catch (Exception ex) {
                        LOG.warn("Exception in SignalXCommand ", ex.getMessage(), ex);
                        throw new CommandException(ErrorCode.E0729, wfAction.getName(), ex);
                    }
                }
            }

        }
        else {
            for (WorkflowActionBean newAction : WorkflowStoreService.getStartedActions(workflowInstance)) {
                String skipVar = workflowInstance.getVar(newAction.getName() + WorkflowInstance.NODE_VAR_SEPARATOR
                        + ReRunXCommand.TO_SKIP);
                boolean skipNewAction = false;
                if (skipVar != null) {
                    skipNewAction = skipVar.equals("true");
                }
                try {
                    if (skipNewAction) {
                        WorkflowActionBean oldAction;

                        oldAction = jpaService.execute(new WorkflowActionGetJPAExecutor(newAction.getId()));

                        oldAction.setPending();
                        updateList.add(oldAction);

                        queue(new SignalXCommand(jobId, oldAction.getId()));
                    }
                    else {
                        newAction.setPending();
                        String actionSlaXml = getActionSLAXml(newAction.getName(), workflowInstance.getApp()
                                .getDefinition(), wfJob.getConf());
                        newAction.setSlaXml(actionSlaXml);
                        insertList.add(newAction);
                        LOG.debug("SignalXCommand: Name: "+ newAction.getName() + ", Id: " +newAction.getId() + ", Authcode:" + newAction.getCred());
                        queue(new ActionStartXCommand(newAction.getId(), newAction.getType()));
                    }
                }
                catch (JPAExecutorException je) {
                    throw new CommandException(je);
                }
            }
        }

        try {
            wfJob.setLastModifiedTime(new Date());
            updateList.add(wfJob);
            // call JPAExecutor to do the bulk writes
            jpaService.execute(new BulkUpdateInsertJPAExecutor(updateList, insertList));
        }
        catch (JPAExecutorException je) {
            throw new CommandException(je);
        }
        LOG.debug(
                "Updated the workflow status to " + wfJob.getId() + "  status =" + wfJob.getStatusStr());
        if (wfJob.getStatus() != WorkflowJob.Status.RUNNING && wfJob.getStatus() != WorkflowJob.Status.SUSPENDED) {
            // update coordinator action
            new CoordActionUpdateXCommand(wfJob).call();    //Note: Called even if wf is not necessarily instantiated by coordinator
            new WfEndXCommand(wfJob).call(); //To delete the WF temp dir
        }
        LOG.debug("ENDED SignalCommand for jobid=" + jobId + ", actionId=" + actionId);
        return null;
    }

    public static ELEvaluator createELEvaluatorForGroup(Configuration conf, String group) {
        ELEvaluator eval = Services.get().get(ELService.class).createEvaluator(group);
        for (Map.Entry<String, String> entry : conf) {
            eval.setVariable(entry.getKey(), entry.getValue());
        }
        return eval;
    }

    @SuppressWarnings("unchecked")
    private String getActionSLAXml(String actionName, String wfXml, String wfConf) throws CommandException {
        String slaXml = null;
        try {
            Element eWfJob = XmlUtils.parseXml(wfXml);
            for (Element action : (List<Element>) eWfJob.getChildren("action", eWfJob.getNamespace())) {
                if (action.getAttributeValue("name").equals(actionName) == false) {
                    continue;
                }
                Element eSla = action.getChild("info", Namespace.getNamespace(SchemaService.SLA_NAME_SPACE_URI));
                if (eSla != null) {
                    slaXml = XmlUtils.prettyPrint(eSla).toString();
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
            ELEvaluator evalSla = SubmitXCommand.createELEvaluatorForGroup(conf, "wf-sla-submit");
            slaXml = SubmitXCommand.resolveSla(eSla, evalSla);
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E1004, e.getMessage(), e);
        }
        return slaXml;
    }

    @SuppressWarnings("unchecked")
    private void writeSLARegistrationForAllActions(String wfXml, String user, String group, String strConf)
            throws CommandException {
        try {
            Element eWfJob = XmlUtils.parseXml(wfXml);
            Configuration conf = new XConfiguration(new StringReader(strConf));
            for (Element action : (List<Element>) eWfJob.getChildren("action", eWfJob.getNamespace())) {
                Element eSla = action.getChild("info", Namespace.getNamespace(SchemaService.SLA_NAME_SPACE_URI));
                if (eSla != null) {
                    String slaXml = resolveSla(eSla, conf);
                    eSla = XmlUtils.parseXml(slaXml);
                    String actionId = Services.get().get(UUIDService.class).generateChildId(jobId,
                            action.getAttributeValue("name") + "");
                    SLADbXOperations.writeSlaRegistrationEvent(eSla, actionId, SlaAppType.WORKFLOW_ACTION, user, group);
                }
            }
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E1007, "workflow:Actions " + jobId, e);
        }

    }

}
