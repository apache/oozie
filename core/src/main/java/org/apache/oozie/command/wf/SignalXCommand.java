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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.control.ForkActionExecutor;
import org.apache.oozie.action.control.StartActionExecutor;
import org.apache.oozie.action.oozie.SubWorkflowActionExecutor;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.SLAEvent.SlaAppType;
import org.apache.oozie.client.SLAEvent.Status;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.SLAEventBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.wf.ActionXCommand.ActionExecutorContext;
import org.apache.oozie.executor.jpa.BatchQueryExecutor.UpdateEntry;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor.WorkflowActionQuery;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor.WorkflowJobQuery;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.service.ELService;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.JPAService;
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

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.oozie.client.OozieClient;

@SuppressWarnings("deprecation")
public class SignalXCommand extends WorkflowXCommand<Void> {

    private JPAService jpaService = null;
    private String jobId;
    private String actionId;
    private WorkflowJobBean wfJob;
    private WorkflowActionBean wfAction;
    private List<UpdateEntry> updateList = new ArrayList<UpdateEntry>();
    private List<JsonBean> insertList = new ArrayList<JsonBean>();
    private boolean generateEvent = false;
    private String wfJobErrorCode;
    private String wfJobErrorMsg;

    public SignalXCommand(String name, int priority, String jobId) {
        super(name, name, priority);
        this.jobId = ParamChecker.notEmpty(jobId, "jobId");
    }

    public SignalXCommand(String jobId, String actionId) {
        this("signal", 1, jobId);
        this.actionId = ParamChecker.notEmpty(actionId, "actionId");
    }

    @Override
    protected void setLogInfo() {
        if (jobId != null) {
            LogUtils.setLogInfo(jobId);
        }
        else if (actionId !=null) {
            LogUtils.setLogInfo(actionId);
        }
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
    public String getKey() {
        return getName() + "_" + jobId + "_" + actionId;
    }

    @Override
    protected void loadState() throws CommandException {
        try {
            jpaService = Services.get().get(JPAService.class);
            if (jpaService != null) {
                this.wfJob = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW, jobId);
                LogUtils.setLogInfo(wfJob);
                if (actionId != null) {
                    this.wfAction = WorkflowActionQueryExecutor.getInstance().get(WorkflowActionQuery.GET_ACTION_SIGNAL, actionId);
                    LogUtils.setLogInfo(wfAction);
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
        WorkflowJob.Status prevStatus = wfJob.getStatus();
        boolean completed = false, skipAction = false;
        WorkflowActionBean syncAction = null;

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
                generateEvent = true;
                // 1. Add SLA status event for WF-JOB with status STARTED
                SLAEventBean slaEvent = SLADbXOperations.createStatusEvent(wfJob.getSlaXml(), jobId, Status.STARTED,
                        SlaAppType.WORKFLOW_JOB);
                if (slaEvent != null) {
                    insertList.add(slaEvent);
                }
                // 2. Add SLA registration events for all WF_ACTIONS
                createSLARegistrationForAllActions(workflowInstance.getApp().getDefinition(), wfJob.getUser(),
                        wfJob.getGroup(), wfJob.getConf());
                queue(new WorkflowNotificationXCommand(wfJob));
            }
            else {
                throw new CommandException(ErrorCode.E0801, wfJob.getId());
            }
        }
        else {
            WorkflowInstance.Status initialStatus = workflowInstance.getStatus();
            String skipVar = workflowInstance.getVar(wfAction.getName() + WorkflowInstance.NODE_VAR_SEPARATOR
                    + ReRunXCommand.TO_SKIP);
            if (skipVar != null) {
                skipAction = skipVar.equals("true");
            }
            try {
                completed = workflowInstance.signal(wfAction.getExecutionPath(), wfAction.getSignalValue());
            }
            catch (WorkflowException e) {
               LOG.error("Workflow action failed : " + e.getMessage(), e);
                wfJob.setStatus(WorkflowJob.Status.valueOf(workflowInstance.getStatus().toString()));
                completed = true;
            }
            wfJob.setWorkflowInstance(workflowInstance);
            wfAction.resetPending();
            if (!skipAction) {
                wfAction.setTransition(workflowInstance.getTransition(wfAction.getName()));
                queue(new WorkflowNotificationXCommand(wfJob, wfAction));
            }
            updateList.add(new UpdateEntry<WorkflowActionQuery>(WorkflowActionQuery.UPDATE_ACTION_PENDING_TRANS,
                    wfAction));
            WorkflowInstance.Status endStatus = workflowInstance.getStatus();
            if (endStatus != initialStatus) {
                generateEvent = true;
            }
        }

        if (completed) {
            try {
                for (String actionToKillId : WorkflowStoreService.getActionsToKill(workflowInstance)) {
                    WorkflowActionBean actionToKill;

                    actionToKill = WorkflowActionQueryExecutor.getInstance().get(
                            WorkflowActionQuery.GET_ACTION_ID_TYPE_LASTCHECK, actionToKillId);

                    actionToKill.setPending();
                    actionToKill.setStatus(WorkflowActionBean.Status.KILLED);
                    updateList.add(new UpdateEntry<WorkflowActionQuery>(
                            WorkflowActionQuery.UPDATE_ACTION_STATUS_PENDING, actionToKill));
                    queue(new ActionKillXCommand(actionToKill.getId(), actionToKill.getType()));
                }

                for (String actionToFailId : WorkflowStoreService.getActionsToFail(workflowInstance)) {
                    WorkflowActionBean actionToFail = WorkflowActionQueryExecutor.getInstance().get(
                            WorkflowActionQuery.GET_ACTION_FAIL, actionToFailId);
                    actionToFail.resetPending();
                    actionToFail.setStatus(WorkflowActionBean.Status.FAILED);
                    if (wfJobErrorCode != null) {
                        wfJobErrorCode = actionToFail.getErrorCode();
                        wfJobErrorMsg = actionToFail.getErrorMessage();
                    }
                    queue(new WorkflowNotificationXCommand(wfJob, actionToFail));
                    SLAEventBean slaEvent = SLADbXOperations.createStatusEvent(wfAction.getSlaXml(), wfAction.getId(),
                            Status.FAILED, SlaAppType.WORKFLOW_ACTION);
                    if (slaEvent != null) {
                        insertList.add(slaEvent);
                    }
                    updateList.add(new UpdateEntry<WorkflowActionQuery>(
                            WorkflowActionQuery.UPDATE_ACTION_STATUS_PENDING, actionToFail));
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
            SLAEventBean slaEvent = SLADbXOperations.createStatusEvent(wfJob.getSlaXml(), jobId, slaStatus,
                    SlaAppType.WORKFLOW_JOB);
            if (slaEvent != null) {
                insertList.add(slaEvent);
            }
            queue(new WorkflowNotificationXCommand(wfJob));
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
                    InstrumentUtils.incrJobCounter(INSTR_KILLED_JOBS_COUNTER_NAME, 1, getInstrumentation());
                    try {
                        String tmpNodeConf = nodeDef.getConf();
                        String actionConf = context.getELEvaluator().evaluate(tmpNodeConf, String.class);
                        LOG.debug(
                                "Try to resolve KillNode message for jobid [{0}], actionId [{1}], before resolve [{2}], "
                                        + "after resolve [{3}]", jobId, actionId, tmpNodeConf, actionConf);
                        if (wfAction.getErrorCode() != null) {
                            wfAction.setErrorInfo(wfAction.getErrorCode(), actionConf);
                        }
                        else {
                            wfAction.setErrorInfo(ErrorCode.E0729.toString(), actionConf);
                        }
                        updateList.add(new UpdateEntry<WorkflowActionQuery>(
                                WorkflowActionQuery.UPDATE_ACTION_PENDING_TRANS_ERROR, wfAction));
                    }
                    catch (Exception ex) {
                        LOG.warn("Exception in SignalXCommand ", ex.getMessage(), ex);
                        throw new CommandException(ErrorCode.E0729, wfAction.getName(), ex);
                    }
                }
            }

        }
        else {
            for (WorkflowActionBean newAction : WorkflowStoreService.getActionsToStart(workflowInstance)) {
                boolean isOldWFAction = false;

                // In case of subworkflow rerun when failed option have been provided, rerun command do not delete
                // old action. To avoid twice entry for same action, Checking in Db if the workflow action already exist.
                if(SubWorkflowActionExecutor.ACTION_TYPE.equals(newAction.getType())) {
                    try {
                        WorkflowActionBean oldAction = WorkflowActionQueryExecutor.getInstance()
                                .get(WorkflowActionQuery.GET_ACTION_CHECK,
                                        newAction.getId());
                        newAction.setExternalId(oldAction.getExternalId());
                        newAction.setCreatedTime(oldAction.getCreatedTime());
                        isOldWFAction = true;
                    } catch (JPAExecutorException e) {
                        if(e.getErrorCode() != ErrorCode.E0605) {
                            throw new CommandException(e);
                        }
                    }
                }

                String skipVar = workflowInstance.getVar(newAction.getName() + WorkflowInstance.NODE_VAR_SEPARATOR
                        + ReRunXCommand.TO_SKIP);
                boolean skipNewAction = false, suspendNewAction = false;
                if (skipVar != null) {
                    skipNewAction = skipVar.equals("true");
                }

                if (skipNewAction) {
                    WorkflowActionBean oldAction = new WorkflowActionBean();
                    oldAction.setId(newAction.getId());
                    oldAction.setPending();
                    oldAction.setExecutionPath(newAction.getExecutionPath());
                    updateList.add(new UpdateEntry<WorkflowActionQuery>(WorkflowActionQuery.UPDATE_ACTION_PENDING,
                            oldAction));
                    queue(new SignalXCommand(jobId, oldAction.getId()));
                }
                else {
                    if(!skipAction) {
                        try {
                            // Make sure that transition node for a forked action
                            // is inserted only once
                            WorkflowActionQueryExecutor.getInstance().get(WorkflowActionQuery.GET_ACTION_ID_TYPE_LASTCHECK,
                                    newAction.getId());

                            continue;
                        } catch (JPAExecutorException jee) {
                        }
                    }
                    suspendNewAction = checkForSuspendNode(newAction);
                    newAction.setPending();
                    String actionSlaXml = getActionSLAXml(newAction.getName(), workflowInstance.getApp()
                            .getDefinition(), wfJob.getConf());
                    newAction.setSlaXml(actionSlaXml);
                    if(!isOldWFAction) {
                        newAction.setCreatedTime(new Date());
                        insertList.add(newAction);
                    } else {
                        updateList.add(new UpdateEntry<WorkflowActionQuery>(WorkflowActionQuery.UPDATE_ACTION_START,
                                newAction));
                    }
                    LOG.debug("SignalXCommand: Name: " + newAction.getName() + ", Id: " + newAction.getId()
                            + ", Authcode:" + newAction.getCred());
                    if (wfAction != null) { // null during wf job submit
                        ActionService as = Services.get().get(ActionService.class);
                        ActionExecutor current = as.getExecutor(wfAction.getType());
                        LOG.trace("Current Action Type:" + current.getClass());
                        if (!suspendNewAction) {
                            if (!(current instanceof ForkActionExecutor) && !(current instanceof StartActionExecutor)) {
                                // Excluding :start: here from executing first action synchronously since it
                                // blocks the consumer thread till the action is submitted to Hadoop,
                                // in turn reducing the number of new submissions the threads can accept.
                                // Would also be susceptible to longer delays in case Hadoop cluster is busy.
                                syncAction = newAction;
                            }
                            else {
                                queue(new ActionStartXCommand(newAction.getId(), newAction.getType()));
                            }
                        }
                    }
                    else {
                        syncAction = newAction; // first action after wf submit should always be sync
                    }
                }
            }
        }

        try {
            wfJob.setLastModifiedTime(new Date());
            updateList.add(new UpdateEntry<WorkflowJobQuery>(
                    WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_INSTANCE_MOD_START_END, wfJob));
            // call JPAExecutor to do the bulk writes
            BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(insertList, updateList, null);
            if (prevStatus != wfJob.getStatus()) {
                LOG.debug("Updated the workflow status to " + wfJob.getId() + "  status =" + wfJob.getStatusStr());
            }
            if (generateEvent && EventHandlerService.isEnabled()) {
                generateEvent(wfJob, wfJobErrorCode, wfJobErrorMsg);
            }
        }
        catch (JPAExecutorException je) {
            throw new CommandException(je);
        }
        // Changing to synchronous call from asynchronous queuing to prevent
        // undue delay from between end of previous and start of next action
        if (wfJob.getStatus() != WorkflowJob.Status.RUNNING
                && wfJob.getStatus() != WorkflowJob.Status.SUSPENDED) {
            // only for asynchronous actions, parent coord action's external id will
            // persisted and following update will succeed.
            updateParentIfNecessary(wfJob);
            new WfEndXCommand(wfJob).call(); // To delete the WF temp dir
        }
        else if (syncAction != null) {
            new ActionStartXCommand(wfJob, syncAction.getId(), syncAction.getType()).call(getEntityKey());
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
                Element eSla = XmlUtils.getSLAElement(action);
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
    private void createSLARegistrationForAllActions(String wfXml, String user, String group, String strConf)
            throws CommandException {
        try {
            Element eWfJob = XmlUtils.parseXml(wfXml);
            Configuration conf = new XConfiguration(new StringReader(strConf));
            for (Element action : (List<Element>) eWfJob.getChildren("action", eWfJob.getNamespace())) {
                Element eSla = XmlUtils.getSLAElement(action);
                if (eSla != null) {
                    String slaXml = resolveSla(eSla, conf);
                    eSla = XmlUtils.parseXml(slaXml);
                    String actionId = Services.get().get(UUIDService.class)
                            .generateChildId(jobId, action.getAttributeValue("name") + "");
                    SLAEventBean slaEvent = SLADbXOperations.createSlaRegistrationEvent(eSla, actionId,
                            SlaAppType.WORKFLOW_ACTION, user, group);
                    if (slaEvent != null) {
                        insertList.add(slaEvent);
                    }
                }
            }
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E1007, "workflow:Actions " + jobId, e.getMessage(), e);
        }

    }

    private boolean checkForSuspendNode(WorkflowActionBean newAction) {
        boolean suspendNewAction = false;
        try {
            XConfiguration wfjobConf = new XConfiguration(new StringReader(wfJob.getConf()));
            String[] values = wfjobConf.getTrimmedStrings(OozieClient.OOZIE_SUSPEND_ON_NODES);
            if (values != null) {
                if (values.length == 1 && values[0].equals("*")) {
                    LOG.info("Reached suspend node at [{0}], suspending workflow [{1}]", newAction.getName(),
                            wfJob.getId());
                    queue(new SuspendXCommand(jobId));
                    suspendNewAction = true;
                }
                else {
                    for (String suspendPoint : values) {
                        if (suspendPoint.equals(newAction.getName())) {
                            LOG.info("Reached suspend node at [{0}], suspending workflow [{1}]", newAction.getName(),
                                    wfJob.getId());
                            queue(new SuspendXCommand(jobId));
                            suspendNewAction = true;
                            break;
                        }
                    }
                }
            }
        }
        catch (IOException ex) {
            LOG.warn("Error reading " + OozieClient.OOZIE_SUSPEND_ON_NODES + ", ignoring [{0}]", ex.getMessage());
        }
        return suspendNewAction;
    }

}
