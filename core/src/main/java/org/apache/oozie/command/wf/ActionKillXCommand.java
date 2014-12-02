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

import org.apache.oozie.ErrorCode;
import org.apache.oozie.SLAEventBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.XException;
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
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.action.control.ControlNodeActionExecutor;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.db.SLADbXOperations;

/**
 * Kill workflow action and invoke action executor to kill the underlying context.
 *
 */
@SuppressWarnings("deprecation")
public class ActionKillXCommand extends ActionXCommand<Void> {
    private String actionId;
    private String jobId;
    private WorkflowJobBean wfJob;
    private WorkflowActionBean wfAction;
    private JPAService jpaService = null;
    private List<UpdateEntry> updateList = new ArrayList<UpdateEntry>();
    private List<JsonBean> insertList = new ArrayList<JsonBean>();

    public ActionKillXCommand(String actionId, String type) {
        super("action.kill", type, 0);
        this.actionId = actionId;
        this.jobId = Services.get().get(UUIDService.class).getId(actionId);
    }

    public ActionKillXCommand(String actionId) {
        this(actionId, "action.kill");
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
        return getName() + "_" + this.actionId;
    }

    @Override
    protected void loadState() throws CommandException {
        try {
            jpaService = Services.get().get(JPAService.class);

            if (jpaService != null) {
                this.wfJob = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW_ACTION_OP, jobId);
                this.wfAction = WorkflowActionQueryExecutor.getInstance().get(WorkflowActionQuery.GET_ACTION, actionId);
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
        if (wfAction.getStatus() != WorkflowActionBean.Status.KILLED) {
            throw new PreconditionException(ErrorCode.E0726, wfAction.getId());
        }
    }

    @Override
    protected Void execute() throws CommandException {
        LOG.debug("STARTED WorkflowActionKillXCommand for action " + actionId);

        if (wfAction.isPending()) {
            ActionExecutor executor = Services.get().get(ActionService.class).getExecutor(wfAction.getType());
            if (executor != null) {
                try {
                    boolean isRetry = false;
                    boolean isUserRetry = false;
                    ActionExecutorContext context = new ActionXCommand.ActionExecutorContext(wfJob, wfAction,
                            isRetry, isUserRetry);
                    incrActionCounter(wfAction.getType(), 1);

                    Instrumentation.Cron cron = new Instrumentation.Cron();
                    cron.start();
                    executor.kill(context, wfAction);
                    cron.stop();
                    addActionCron(wfAction.getType(), cron);

                    wfAction.resetPending();
                    wfAction.setStatus(WorkflowActionBean.Status.KILLED);
                    wfAction.setEndTime(new Date());

                    updateList.add(new UpdateEntry<WorkflowActionQuery>(WorkflowActionQuery.UPDATE_ACTION_END, wfAction));
                    wfJob.setLastModifiedTime(new Date());
                    updateList.add(new UpdateEntry<WorkflowJobQuery>(WorkflowJobQuery.UPDATE_WORKFLOW_MODTIME, wfJob));
                    // Add SLA status event (KILLED) for WF_ACTION
                    SLAEventBean slaEvent = SLADbXOperations.createStatusEvent(wfAction.getSlaXml(), wfAction.getId(), Status.KILLED,
                            SlaAppType.WORKFLOW_ACTION);
                    if(slaEvent != null) {
                        insertList.add(slaEvent);
                    }
                    queue(new WorkflowNotificationXCommand(wfJob, wfAction));
                }
                catch (ActionExecutorException ex) {
                    wfAction.resetPending();
                    wfAction.setStatus(WorkflowActionBean.Status.FAILED);
                    wfAction.setErrorInfo(ex.getErrorCode().toString(),
                            "KILL COMMAND FAILED - exception while executing job kill");
                    wfAction.setEndTime(new Date());

                    wfJob.setStatus(WorkflowJobBean.Status.KILLED);
                    updateList.add(new UpdateEntry<WorkflowActionQuery>(WorkflowActionQuery.UPDATE_ACTION_END, wfAction));
                    wfJob.setLastModifiedTime(new Date());
                    updateList.add(new UpdateEntry<WorkflowJobQuery>(WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_MODTIME, wfJob));
                    // What will happen to WF and COORD_ACTION, NOTIFICATION?
                    SLAEventBean slaEvent = SLADbXOperations.createStatusEvent(wfAction.getSlaXml(), wfAction.getId(), Status.FAILED,
                            SlaAppType.WORKFLOW_ACTION);
                    if(slaEvent != null) {
                        insertList.add(slaEvent);
                    }
                    LOG.warn("Exception while executing kill(). Error Code [{0}], Message[{1}]",
                            ex.getErrorCode(), ex.getMessage(), ex);
                }
                finally {
                    try {
                        BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(insertList, updateList, null);
                        if (!(executor instanceof ControlNodeActionExecutor) && EventHandlerService.isEnabled()) {
                            generateEvent(wfAction, wfJob.getUser());
                        }
                    }
                    catch (JPAExecutorException e) {
                        throw new CommandException(e);
                    }
                }
            }
        }
        LOG.debug("ENDED WorkflowActionKillXCommand for action " + actionId);
        return null;
    }

}
