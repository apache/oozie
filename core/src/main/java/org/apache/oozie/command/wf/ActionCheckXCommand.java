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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor.WorkflowActionQuery;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.BatchQueryExecutor.UpdateEntry;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor.WorkflowJobQuery;
import org.apache.oozie.service.ActionCheckerService;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.XLog;

/**
 * Executes the check command for ActionHandlers. </p> Ensures the action is in
 * RUNNING state before executing
 * {@link ActionExecutor#check(org.apache.oozie.action.ActionExecutor.Context, org.apache.oozie.client.WorkflowAction)}
 */
public class ActionCheckXCommand extends ActionXCommand<Void> {
    public static final String EXEC_DATA_MISSING = "EXEC_DATA_MISSING";
    private String actionId;
    private String jobId;
    private int actionCheckDelay;
    private WorkflowJobBean wfJob = null;
    private WorkflowActionBean wfAction = null;
    private ActionExecutor executor = null;
    private List<UpdateEntry> updateList = new ArrayList<UpdateEntry>();
    private boolean generateEvent = false;

    public ActionCheckXCommand(String actionId) {
        this(actionId, -1);
    }

    public ActionCheckXCommand(String actionId, int priority, int checkDelay) {
        super("action.check", "action.check", priority);
        this.actionId = actionId;
        this.actionCheckDelay = checkDelay;
        this.jobId = Services.get().get(UUIDService.class).getId(actionId);
    }

    public ActionCheckXCommand(String actionId, int checkDelay) {
        this(actionId, 0, checkDelay);
    }

    @Override
    protected void setLogInfo() {
        LogUtils.setLogInfo(actionId);
    }

    @Override
    protected void eagerLoadState() throws CommandException {
        try {
            this.wfJob = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW_STATUS, jobId);
            this.wfAction = WorkflowActionQueryExecutor.getInstance().get(WorkflowActionQuery.GET_ACTION_ID_TYPE_LASTCHECK,
                    actionId);
            LogUtils.setLogInfo(wfJob);
            LogUtils.setLogInfo(wfAction);
        }
        catch (JPAExecutorException ex) {
            throw new CommandException(ex);
        }
    }

    @Override
    protected void eagerVerifyPrecondition() throws CommandException, PreconditionException {
        if (wfJob == null) {
            throw new PreconditionException(ErrorCode.E0604, jobId);
        }
        if (wfAction == null) {
            throw new PreconditionException(ErrorCode.E0605, actionId);
        }
        // if the action has been updated, quit this command
        if (actionCheckDelay > 0) {
            Timestamp actionCheckTs = new Timestamp(System.currentTimeMillis() - actionCheckDelay * 1000);
            Timestamp actionLmt = wfAction.getLastCheckTimestamp();
            if (actionLmt.after(actionCheckTs)) {
                throw new PreconditionException(ErrorCode.E0817, actionId);
            }
        }

        executor = Services.get().get(ActionService.class).getExecutor(wfAction.getType());
        if (executor == null) {
            throw new CommandException(ErrorCode.E0802, wfAction.getType());
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
    protected void loadState() throws CommandException {
        try {
            this.wfJob = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW_ACTION_OP, jobId);
            this.wfAction = WorkflowActionQueryExecutor.getInstance().get(WorkflowActionQuery.GET_ACTION_CHECK,
                    actionId);
        }
        catch (JPAExecutorException e) {
            throw new CommandException(e);
        }
        LogUtils.setLogInfo(wfJob);
        LogUtils.setLogInfo(wfAction);
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (!wfAction.isPending() || wfAction.getStatus() != WorkflowActionBean.Status.RUNNING) {
            throw new PreconditionException(ErrorCode.E0815, wfAction.isPending(), wfAction.getStatusStr());
        }
        if (wfJob.getStatus() != WorkflowJob.Status.RUNNING && wfJob.getStatus() != WorkflowJob.Status.SUSPENDED) {
            wfAction.setLastCheckTime(new Date());
            try {
                WorkflowActionQueryExecutor.getInstance().executeUpdate(
                        WorkflowActionQuery.UPDATE_ACTION_FOR_LAST_CHECKED_TIME, wfAction);
            }
            catch (JPAExecutorException e) {
                throw new CommandException(e);
            }
            throw new PreconditionException(ErrorCode.E0818, wfAction.getId(), wfJob.getId(), wfJob.getStatus());
        }
    }

    @Override
    protected Void execute() throws CommandException {
        LOG.debug("STARTED ActionCheckXCommand for wf actionId=" + actionId + " priority =" + getPriority());
        ActionExecutorContext context = null;
        boolean execSynchronous = false;
        try {
            boolean isRetry = false;
            if (wfAction.getRetries() > 0) {
                isRetry = true;
            }
            boolean isUserRetry = false;
            context = new ActionXCommand.ActionExecutorContext(wfJob, wfAction, isRetry, isUserRetry);
            incrActionCounter(wfAction.getType(), 1);

            Instrumentation.Cron cron = new Instrumentation.Cron();
            cron.start();
            executor.check(context, wfAction);
            cron.stop();
            addActionCron(wfAction.getType(), cron);

            if (wfAction.isExecutionComplete()) {
                if (!context.isExecuted()) {
                    LOG.warn(XLog.OPS, "Action Completed, ActionExecutor [{0}] must call setExecutionData()", executor
                            .getType());
                    wfAction.setErrorInfo(EXEC_DATA_MISSING,
                            "Execution Complete, but Execution Data Missing from Action");
                    failJob(context);
                    generateEvent = true;
                } else {
                    wfAction.setPending();
                    execSynchronous = true;
                }
            }
            wfAction.setLastCheckTime(new Date());
            updateList.add(new UpdateEntry<WorkflowActionQuery>(WorkflowActionQuery.UPDATE_ACTION_CHECK, wfAction));
            wfJob.setLastModifiedTime(new Date());
            updateList.add(new UpdateEntry<WorkflowJobQuery>(WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_INSTANCE_MODIFIED,
                    wfJob));
        }
        catch (ActionExecutorException ex) {
            LOG.warn("Exception while executing check(). Error Code [{0}], Message[{1}]", ex.getErrorCode(), ex
                    .getMessage(), ex);

            wfAction.setErrorInfo(ex.getErrorCode(), ex.getMessage());
            switch (ex.getErrorType()) {
                case ERROR:
                    // If allowed to retry, this will handle it; otherwise, we should fall through to FAILED
                    if (handleUserRetry(wfAction)) {
                        break;
                    }
                case FAILED:
                    failJob(context, wfAction);
                    generateEvent = true;
                    break;
                case TRANSIENT:                 // retry N times, then suspend workflow
                    if (!handleTransient(context, executor, WorkflowAction.Status.RUNNING)) {
                        handleNonTransient(context, executor, WorkflowAction.Status.START_MANUAL);
                        generateEvent = true;
                        wfAction.setPendingAge(new Date());
                        wfAction.setRetries(0);
                        wfAction.setStartTime(null);
                    }
                    break;
            }
            wfAction.setLastCheckTime(new Date());
            updateList = new ArrayList<UpdateEntry>();
            updateList.add(new UpdateEntry<WorkflowActionQuery>(WorkflowActionQuery.UPDATE_ACTION_CHECK, wfAction));
            wfJob.setLastModifiedTime(new Date());
            updateList.add(new UpdateEntry<WorkflowJobQuery>(WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_INSTANCE_MODIFIED,
                    wfJob));
        }
        finally {
            try {
                BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(null, updateList, null);
                if (generateEvent && EventHandlerService.isEnabled()) {
                    generateEvent(wfAction, wfJob.getUser());
                }
                if (execSynchronous) {
                    new ActionEndXCommand(wfAction.getId(), wfAction.getType()).call(getEntityKey());
                }
            }
            catch (JPAExecutorException e) {
                throw new CommandException(e);
            }
        }

        LOG.debug("ENDED ActionCheckXCommand for wf actionId=" + actionId + ", jobId=" + jobId);
        return null;
    }

    protected long getRetryInterval() {
        return (executor != null) ? executor.getRetryInterval() : ActionExecutor.RETRY_INTERVAL;
    }

    @Override
    public String getKey() {
        return getName() + "_" + actionId;
    }

}
