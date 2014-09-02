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

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.DagELFunctions;
import org.apache.oozie.ErrorCode;
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
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.BatchQueryExecutor.UpdateEntry;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor.WorkflowActionQuery;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor.WorkflowJobQuery;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.db.SLADbXOperations;
import org.apache.oozie.workflow.WorkflowInstance;

@SuppressWarnings("deprecation")
public class ActionEndXCommand extends ActionXCommand<Void> {
    public static final String COULD_NOT_END = "COULD_NOT_END";
    public static final String END_DATA_MISSING = "END_DATA_MISSING";

    private String jobId = null;
    private String actionId = null;
    private WorkflowJobBean wfJob = null;
    private WorkflowActionBean wfAction = null;
    private JPAService jpaService = null;
    private ActionExecutor executor = null;
    private List<UpdateEntry> updateList = new ArrayList<UpdateEntry>();
    private List<JsonBean> insertList = new ArrayList<JsonBean>();

    public ActionEndXCommand(String actionId, String type) {
        super("action.end", type, 0);
        this.actionId = actionId;
        this.jobId = Services.get().get(UUIDService.class).getId(actionId);
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
    public String getKey() {
        return getName() + "_" + actionId;
    }

    @Override
    protected void loadState() throws CommandException {
        try {
            jpaService = Services.get().get(JPAService.class);
            if (jpaService != null) {
                this.wfJob = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW_ACTION_OP,
                        jobId);
                this.wfAction = WorkflowActionQueryExecutor.getInstance().get(WorkflowActionQuery.GET_ACTION_END,
                        actionId);
                LogUtils.setLogInfo(wfJob);
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
                && (wfAction.getStatus() == WorkflowActionBean.Status.DONE
                        || wfAction.getStatus() == WorkflowActionBean.Status.END_RETRY || wfAction.getStatus() == WorkflowActionBean.Status.END_MANUAL)) {

            if (wfJob.getStatus() != WorkflowJob.Status.RUNNING) {
                throw new PreconditionException(ErrorCode.E0811,  WorkflowJob.Status.RUNNING.toString());
            }
        }
        else {
            throw new PreconditionException(ErrorCode.E0812, wfAction.isPending(), wfAction.getStatusStr());
        }

        executor = Services.get().get(ActionService.class).getExecutor(wfAction.getType());
        if (executor == null) {
            throw new CommandException(ErrorCode.E0802, wfAction.getType());
        }
    }

    @Override
    protected Void execute() throws CommandException {
        LOG.debug("STARTED ActionEndXCommand for action " + actionId);

        Configuration conf = wfJob.getWorkflowInstance().getConf();

        int maxRetries = 0;
        long retryInterval = 0;

        if (!(executor instanceof ControlNodeActionExecutor)) {
            maxRetries = conf.getInt(OozieClient.ACTION_MAX_RETRIES, executor.getMaxRetries());
            retryInterval = conf.getLong(OozieClient.ACTION_RETRY_INTERVAL, executor.getRetryInterval());
        }

        executor.setMaxRetries(maxRetries);
        executor.setRetryInterval(retryInterval);

        boolean isRetry = false;
        if (wfAction.getStatus() == WorkflowActionBean.Status.END_RETRY
                || wfAction.getStatus() == WorkflowActionBean.Status.END_MANUAL) {
            isRetry = true;
        }
        boolean isUserRetry = false;
        ActionExecutorContext context = new ActionXCommand.ActionExecutorContext(wfJob, wfAction, isRetry, isUserRetry);
        try {

            LOG.debug(
                    "End, name [{0}] type [{1}] status[{2}] external status [{3}] signal value [{4}]",
                    wfAction.getName(), wfAction.getType(), wfAction.getStatus(), wfAction.getExternalStatus(),
                    wfAction.getSignalValue());

            Instrumentation.Cron cron = new Instrumentation.Cron();
            cron.start();
            executor.end(context, wfAction);
            cron.stop();
            addActionCron(wfAction.getType(), cron);

            WorkflowInstance wfInstance = wfJob.getWorkflowInstance();
            DagELFunctions.setActionInfo(wfInstance, wfAction);
            wfJob.setWorkflowInstance(wfInstance);
            incrActionCounter(wfAction.getType(), 1);

            if (!context.isEnded()) {
                LOG.warn(XLog.OPS, "Action Ended, ActionExecutor [{0}] must call setEndData()",
                        executor.getType());
                wfAction.setErrorInfo(END_DATA_MISSING, "Execution Ended, but End Data Missing from Action");
                failJob(context);
            } else {
                wfAction.setRetries(0);
                wfAction.setEndTime(new Date());

                boolean shouldHandleUserRetry = false;
                Status slaStatus = null;
                switch (wfAction.getStatus()) {
                    case OK:
                        slaStatus = Status.SUCCEEDED;
                        break;
                    case KILLED:
                        slaStatus = Status.KILLED;
                        break;
                    case FAILED:
                        slaStatus = Status.FAILED;
                        shouldHandleUserRetry = true;
                        break;
                    case ERROR:
                        LOG.info("ERROR is considered as FAILED for SLA");
                        slaStatus = Status.KILLED;
                        shouldHandleUserRetry = true;
                        break;
                    default:
                        slaStatus = Status.FAILED;
                        shouldHandleUserRetry = true;
                        break;
                }
                if (!shouldHandleUserRetry || !handleUserRetry(wfAction)) {
                    SLAEventBean slaEvent = SLADbXOperations.createStatusEvent(wfAction.getSlaXml(), wfAction.getId(), slaStatus, SlaAppType.WORKFLOW_ACTION);
                    if(slaEvent != null) {
                        insertList.add(slaEvent);
                    }
                }
            }
            updateList.add(new UpdateEntry<WorkflowActionQuery>(WorkflowActionQuery.UPDATE_ACTION_END,wfAction));
            wfJob.setLastModifiedTime(new Date());
            updateList.add(new UpdateEntry<WorkflowJobQuery>(WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_INSTANCE_MODIFIED, wfJob));
        }
        catch (ActionExecutorException ex) {
            LOG.warn(
                    "Error ending action [{0}]. ErrorType [{1}], ErrorCode [{2}], Message [{3}]",
                    wfAction.getName(), ex.getErrorType(), ex.getErrorCode(), ex.getMessage());
            wfAction.setErrorInfo(ex.getErrorCode(), ex.getMessage());
            wfAction.setEndTime(null);

            switch (ex.getErrorType()) {
                case TRANSIENT:
                    if (!handleTransient(context, executor, WorkflowAction.Status.END_RETRY)) {
                        handleNonTransient(context, executor, WorkflowAction.Status.END_MANUAL);
                        wfAction.setPendingAge(new Date());
                        wfAction.setRetries(0);
                    }
                    wfAction.setEndTime(null);
                    break;
                case NON_TRANSIENT:
                    handleNonTransient(context, executor, WorkflowAction.Status.END_MANUAL);
                    wfAction.setEndTime(null);
                    break;
                case ERROR:
                    handleError(context, executor, COULD_NOT_END, false, WorkflowAction.Status.ERROR);
                    break;
                case FAILED:
                    failJob(context);
                    break;
            }

            WorkflowInstance wfInstance = wfJob.getWorkflowInstance();
            DagELFunctions.setActionInfo(wfInstance, wfAction);
            wfJob.setWorkflowInstance(wfInstance);

            updateList.add(new UpdateEntry<WorkflowActionQuery>(WorkflowActionQuery.UPDATE_ACTION_END,wfAction));
            wfJob.setLastModifiedTime(new Date());
            updateList.add(new UpdateEntry<WorkflowJobQuery>(WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_INSTANCE_MODIFIED, wfJob));
        }
        finally {
            try {
                BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(insertList, updateList, null);
            }
            catch (JPAExecutorException e) {
                throw new CommandException(e);
            }
            if (!(executor instanceof ControlNodeActionExecutor) && EventHandlerService.isEnabled()) {
                generateEvent(wfAction, wfJob.getUser());
            }
            new SignalXCommand(jobId, actionId).call(getEntityKey());
        }

        LOG.debug("ENDED ActionEndXCommand for action " + actionId);
        return null;
    }

}
