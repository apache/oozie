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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.servlet.jsp.el.ELException;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.FaultInjection;
import org.apache.oozie.SLAEventBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.XException;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.action.control.ControlNodeActionExecutor;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.SLAEvent.SlaAppType;
import org.apache.oozie.client.SLAEvent.Status;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.XCommand;
import org.apache.oozie.executor.jpa.BatchQueryExecutor.UpdateEntry;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor.WorkflowActionQuery;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor.WorkflowJobQuery;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ELEvaluationException;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.JobUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.util.db.SLADbXOperations;

@SuppressWarnings("deprecation")
public class ActionStartXCommand extends ActionXCommand<org.apache.oozie.command.wf.ActionXCommand.ActionExecutorContext> {
    public static final String EL_ERROR = "EL_ERROR";
    public static final String EL_EVAL_ERROR = "EL_EVAL_ERROR";
    public static final String COULD_NOT_START = "COULD_NOT_START";
    public static final String START_DATA_MISSING = "START_DATA_MISSING";
    public static final String EXEC_DATA_MISSING = "EXEC_DATA_MISSING";

    private String jobId = null;
    protected String actionId = null;
    protected WorkflowJobBean wfJob = null;
    protected WorkflowActionBean wfAction = null;
    private JPAService jpaService = null;
    private ActionExecutor executor = null;
    private List<UpdateEntry> updateList = new ArrayList<UpdateEntry>();
    private List<JsonBean> insertList = new ArrayList<JsonBean>();
    protected ActionExecutorContext context = null;

    public ActionStartXCommand(String actionId, String type) {
        super("action.start", type, 0);
        this.actionId = actionId;
        this.jobId = Services.get().get(UUIDService.class).getId(actionId);
    }

    public ActionStartXCommand(WorkflowJobBean job, String actionId, String type) {
        super("action.start", type, 0);
        this.actionId = actionId;
        this.wfJob = job;
        this.jobId = wfJob.getId();
    }

    @Override
    protected void setLogInfo() {
        LogUtils.setLogInfo(actionId);
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
                if (wfJob == null) {
                    this.wfJob = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW, jobId);
                }
                this.wfAction = WorkflowActionQueryExecutor.getInstance().get(WorkflowActionQuery.GET_ACTION, actionId);
                LogUtils.setLogInfo( wfJob);
                LogUtils.setLogInfo(wfAction);
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
        if (wfJob == null) {
            throw new PreconditionException(ErrorCode.E0604, jobId);
        }
        if (wfAction == null) {
            throw new PreconditionException(ErrorCode.E0605, actionId);
        }
        if (wfAction.isPending()
                && (wfAction.getStatus() == WorkflowActionBean.Status.PREP
                        || wfAction.getStatus() == WorkflowActionBean.Status.START_RETRY
                        || wfAction.getStatus() == WorkflowActionBean.Status.START_MANUAL
                        || wfAction.getStatus() == WorkflowActionBean.Status.USER_RETRY
                        )) {
            if (wfJob.getStatus() != WorkflowJob.Status.RUNNING) {
                throw new PreconditionException(ErrorCode.E0810, WorkflowJob.Status.RUNNING.toString());
            }
        }
        else {
            throw new PreconditionException(ErrorCode.E0816, wfAction.isPending(), wfAction.getStatusStr());
        }

        executor = Services.get().get(ActionService.class).getExecutor(wfAction.getType());
        if (executor == null) {
            throw new CommandException(ErrorCode.E0802, wfAction.getType());
        }
    }

    @Override
    protected ActionExecutorContext execute() throws CommandException {
        LOG.debug("STARTED ActionStartXCommand for wf actionId=" + actionId);
        Configuration conf = wfJob.getWorkflowInstance().getConf();

        int maxRetries = 0;
        long retryInterval = 0;
        boolean execSynchronous = false;

        if (!(executor instanceof ControlNodeActionExecutor)) {
            maxRetries = conf.getInt(OozieClient.ACTION_MAX_RETRIES, executor.getMaxRetries());
            retryInterval = conf.getLong(OozieClient.ACTION_RETRY_INTERVAL, executor.getRetryInterval());
        }

        executor.setMaxRetries(maxRetries);
        executor.setRetryInterval(retryInterval);

        try {
            boolean isRetry = false;
            if (wfAction.getStatus() == WorkflowActionBean.Status.START_RETRY
                    || wfAction.getStatus() == WorkflowActionBean.Status.START_MANUAL) {
                isRetry = true;
                prepareForRetry(wfAction);
            }
            boolean isUserRetry = false;
            if (wfAction.getStatus() == WorkflowActionBean.Status.USER_RETRY) {
                isUserRetry = true;
                prepareForRetry(wfAction);
            }
            context = getContext(isRetry, isUserRetry);
            boolean caught = false;
            try {
                if (!(executor instanceof ControlNodeActionExecutor)) {
                    String tmpActionConf = XmlUtils.removeComments(wfAction.getConf());
                    String actionConf = context.getELEvaluator().evaluate(tmpActionConf, String.class);
                    wfAction.setConf(actionConf);
                    LOG.debug("Start, name [{0}] type [{1}] configuration{E}{E}{2}{E}", wfAction.getName(), wfAction
                            .getType(), actionConf);
                }
            }
            catch (ELEvaluationException ex) {
                caught = true;
                throw new ActionExecutorException(ActionExecutorException.ErrorType.TRANSIENT, EL_EVAL_ERROR, ex
                        .getMessage(), ex);
            }
            catch (ELException ex) {
                caught = true;
                context.setErrorInfo(EL_ERROR, ex.getMessage());
                LOG.warn("ELException in ActionStartXCommand ", ex.getMessage(), ex);
                handleError(context, wfJob, wfAction);
            }
            catch (org.jdom.JDOMException je) {
                caught = true;
                context.setErrorInfo("ParsingError", je.getMessage());
                LOG.warn("JDOMException in ActionStartXCommand ", je.getMessage(), je);
                handleError(context, wfJob, wfAction);
            }
            catch (Exception ex) {
                caught = true;
                context.setErrorInfo(EL_ERROR, ex.getMessage());
                LOG.warn("Exception in ActionStartXCommand ", ex.getMessage(), ex);
                handleError(context, wfJob, wfAction);
            }
            if(!caught) {
                wfAction.setErrorInfo(null, null);
                incrActionCounter(wfAction.getType(), 1);

                LOG.info("Start action [{0}] with user-retry state : userRetryCount [{1}], userRetryMax [{2}], userRetryInterval"
                        + " [{3}]",
                                wfAction.getId(), wfAction.getUserRetryCount(), wfAction.getUserRetryMax(), wfAction
                                        .getUserRetryInterval());

                Instrumentation.Cron cron = new Instrumentation.Cron();
                cron.start();
                // do not override starttime for retries
                if (wfAction.getStartTime() == null) {
                    context.setStartTime();
                }
                context.setVar(JobUtils.getRetryKey(wfAction, JsonTags.WORKFLOW_ACTION_START_TIME),
                        String.valueOf(new Date().getTime()));
                executor.start(context, wfAction);
                cron.stop();
                FaultInjection.activate("org.apache.oozie.command.SkipCommitFaultInjection");
                addActionCron(wfAction.getType(), cron);

                wfAction.setRetries(0);
                if (wfAction.isExecutionComplete()) {
                    if (!context.isExecuted()) {
                        LOG.warn(XLog.OPS, "Action Completed, ActionExecutor [{0}] must call setExecutionData()", executor
                                .getType());
                        wfAction.setErrorInfo(EXEC_DATA_MISSING,
                                "Execution Complete, but Execution Data Missing from Action");
                        failJob(context);
                    } else {
                        wfAction.setPending();
                        if (!(executor instanceof ControlNodeActionExecutor)) {
                            queue(new ActionEndXCommand(wfAction.getId(), wfAction.getType()));
                        }
                        else {
                            execSynchronous = true;
                        }
                    }
                }
                else {
                    if (!context.isStarted()) {
                        LOG.warn(XLog.OPS, "Action Started, ActionExecutor [{0}] must call setStartData()", executor
                                .getType());
                        wfAction.setErrorInfo(START_DATA_MISSING, "Execution Started, but Start Data Missing from Action");
                        failJob(context);
                    } else {
                        queue(new WorkflowNotificationXCommand(wfJob, wfAction));
                    }
                }

                LOG.info(XLog.STD, "[***" + wfAction.getId() + "***]" + "Action status=" + wfAction.getStatusStr());

                updateList.add(new UpdateEntry<WorkflowActionQuery>(WorkflowActionQuery.UPDATE_ACTION_START, wfAction));
                updateJobLastModified();
                // Add SLA status event (STARTED) for WF_ACTION
                SLAEventBean slaEvent = SLADbXOperations.createStatusEvent(wfAction.getSlaXml(), wfAction.getId(), Status.STARTED,
                        SlaAppType.WORKFLOW_ACTION);
                if(slaEvent != null) {
                    insertList.add(slaEvent);
                }
                LOG.info(XLog.STD, "[***" + wfAction.getId() + "***]" + "Action updated in DB!");
            }
        }
        catch (ActionExecutorException ex) {
            LOG.warn("Error starting action [{0}]. ErrorType [{1}], ErrorCode [{2}], Message [{3}]",
                    wfAction.getName(), ex.getErrorType(), ex.getErrorCode(), ex.getMessage(), ex);
            wfAction.setErrorInfo(ex.getErrorCode(), ex.getMessage());
            switch (ex.getErrorType()) {
                case TRANSIENT:
                    if (!handleTransient(context, executor, WorkflowAction.Status.START_RETRY)) {
                        handleNonTransient(context, executor, WorkflowAction.Status.START_MANUAL);
                        wfAction.setPendingAge(new Date());
                        wfAction.setRetries(0);
                        wfAction.setStartTime(null);
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
                        endWF();
                        SLAEventBean slaEvent1 = SLADbXOperations.createStatusEvent(wfAction.getSlaXml(), wfAction.getId(),
                                Status.FAILED,
                                SlaAppType.WORKFLOW_ACTION);
                        if(slaEvent1 != null) {
                            insertList.add(slaEvent1);
                        }
                    }
                    catch (XException x) {
                        LOG.warn("ActionStartXCommand - case:FAILED ", x.getMessage());
                    }
                    break;
            }
            updateList.add(new UpdateEntry<WorkflowActionQuery>(WorkflowActionQuery.UPDATE_ACTION_START, wfAction));
            updateJobLastModified();
        }
        finally {
            try {
                BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(insertList, updateList, null);
                if (!(executor instanceof ControlNodeActionExecutor) && EventHandlerService.isEnabled()) {
                    generateEvent(wfAction, wfJob.getUser());
                }
                if (execSynchronous) {
                    // Changing to synchronous call from asynchronous queuing to prevent
                    // undue delay from ::start:: to action due to queuing
                    callActionEnd();
                }
            }
            catch (JPAExecutorException e) {
                throw new CommandException(e);
            }
        }

        LOG.debug("ENDED ActionStartXCommand for wf actionId=" + actionId + ", jobId=" + jobId);

        return null;
    }

    protected void callActionEnd() throws CommandException {
        new ActionEndXCommand(wfAction.getId(), wfAction.getType()).call();
    }

    /**
     * Get action executor context
     * @param isRetry
     * @param isUserRetry
     * @return ActionExecutorContext returns action executor context
     */
    protected ActionExecutorContext getContext(boolean isRetry, boolean isUserRetry) {
        return new ActionXCommand.ActionExecutorContext(wfJob, wfAction, isRetry, isUserRetry);
    }

    protected void updateJobLastModified(){
        wfJob.setLastModifiedTime(new Date());
        updateList.add(new UpdateEntry<WorkflowJobQuery>(WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_INSTANCE_MODIFIED, wfJob));
    }

    protected void endWF() throws CommandException{
        updateParentIfNecessary(wfJob, 3);
        new WfEndXCommand(wfJob).call(); // To delete the WF temp dir
        SLAEventBean slaEvent2 = SLADbXOperations.createStatusEvent(wfJob.getSlaXml(), wfJob.getId(), Status.FAILED,
                SlaAppType.WORKFLOW_JOB);
        if(slaEvent2 != null) {
            insertList.add(slaEvent2);
        }
    }

    protected void handleError(ActionExecutorContext context, WorkflowJobBean workflow, WorkflowActionBean action)
            throws CommandException {
        failJob(context);
        updateList.add(new UpdateEntry<WorkflowActionQuery>(WorkflowActionQuery.UPDATE_ACTION_START, wfAction));
        updateJobLastModified();
        SLAEventBean slaEvent1 = SLADbXOperations.createStatusEvent(action.getSlaXml(), action.getId(),
                Status.FAILED, SlaAppType.WORKFLOW_ACTION);
        if(slaEvent1 != null) {
            insertList.add(slaEvent1);
        }
        endWF();
        return;
    }

    @Override
    public String getKey(){
        return getName() + "_" + actionId;
    }

    private void prepareForRetry(WorkflowActionBean wfAction) {
        if (wfAction.getType().equals("map-reduce")) {
            // need to delete child job id of original run
            wfAction.setExternalChildIDs("");
        }
    }

    @Override
    protected void queueCommandForTransientFailure(long retryDelayMillis){
        queue(new ActionStartXCommand(wfAction.getId(), wfAction.getType()), retryDelayMillis);
    }

    protected void queue(XCommand<?> command, long msDelay) {
        // ActionStartXCommand is synchronously called from SignalXCommand passing wfJob so that it doesn't have to
        //reload wfJob again. We need set wfJob to null, so that it get reloaded when the requeued command executes.
        if (command instanceof ActionStartXCommand) {
            ((ActionStartXCommand)command).wfJob = null;
        }
        super.queue(command, msDelay);
    }
}
