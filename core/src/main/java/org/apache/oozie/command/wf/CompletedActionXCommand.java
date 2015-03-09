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

import java.util.Properties;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor.WorkflowActionQuery;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.service.CallbackService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;

/**
 * This command is executed once the Workflow command is finished.
 */
public class CompletedActionXCommand extends WorkflowXCommand<Void> {
    private final String actionId;
    private final String externalStatus;
    private WorkflowActionBean wfactionBean;
    private int earlyRequeueCount;

    public CompletedActionXCommand(String actionId, String externalStatus, Properties actionData, int priority,
                                   int earlyRequeueCount) {
        super("callback", "callback", priority);
        this.actionId = ParamChecker.notEmpty(actionId, "actionId");
        this.externalStatus = ParamChecker.notEmpty(externalStatus, "externalStatus");
        this.earlyRequeueCount = earlyRequeueCount;
    }

    public CompletedActionXCommand(String actionId, String externalStatus, Properties actionData, int priority) {
        this(actionId, externalStatus, actionData, 1, 0);
    }

    public CompletedActionXCommand(String actionId, String externalStatus, Properties actionData) {
        this(actionId, externalStatus, actionData, 1);
    }

    @Override
    protected void setLogInfo() {
        LogUtils.setLogInfo(actionId);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.command.XCommand#eagerLoadState()
     */
    @Override
    protected void eagerLoadState() throws CommandException {
        try {
            this.wfactionBean = WorkflowActionQueryExecutor.getInstance().get(WorkflowActionQuery.GET_ACTION_COMPLETED,
                    this.actionId);
        }
        catch (Exception ex) {
            throw new CommandException(ErrorCode.E0603, ex.getMessage(), ex);
        }
        LogUtils.setLogInfo(this.wfactionBean);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.command.XCommand#eagerVerifyPrecondition()
     */
    @Override
    protected void eagerVerifyPrecondition() throws CommandException, PreconditionException {
        if (this.wfactionBean.getStatus() != WorkflowActionBean.Status.RUNNING
                && this.wfactionBean.getStatus() != WorkflowActionBean.Status.PREP) {
            throw new CommandException(ErrorCode.E0800, actionId, this.wfactionBean.getStatus());
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.command.XCommand#execute()
     */
    @Override
    protected Void execute() throws CommandException {
        // If the action is still in PREP, we probably received a callback before Oozie was able to update from PREP to RUNNING;
        // we'll requeue this command a few times and hope that it switches to RUNNING before giving up
        if (this.wfactionBean.getStatus() == WorkflowActionBean.Status.PREP) {
            int maxEarlyRequeueCount = Services.get().get(CallbackService.class).getEarlyRequeueMaxRetries();
            if (this.earlyRequeueCount < maxEarlyRequeueCount) {
                long delay = getRequeueDelay();
                LOG.warn("Received early callback for action still in PREP state; will wait [{0}]ms and requeue up to [{1}] more"
                        + " times", delay, (maxEarlyRequeueCount - earlyRequeueCount));
                queue(new CompletedActionXCommand(this.actionId, this.externalStatus, null, this.getPriority(),
                        this.earlyRequeueCount + 1), delay);
            } else {
                throw new CommandException(ErrorCode.E0822, actionId);
            }
        } else {    // RUNNING
            ActionExecutor executor = Services.get().get(ActionService.class).getExecutor(this.wfactionBean.getType());
            // this is done because oozie notifications (of sub-wfs) is send
            // every status change, not only on completion.
            if (executor.isCompleted(externalStatus)) {
                queue(new ActionCheckXCommand(this.wfactionBean.getId(), getPriority(), -1));
            }
        }
        return null;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    public String getEntityKey() {
        return null;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.command.XCommand#isLockRequired()
     */
    @Override
    protected boolean isLockRequired() {
        return false;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.command.XCommand#loadState()
     */
    @Override
    protected void loadState() throws CommandException {
        eagerLoadState();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        eagerVerifyPrecondition();
    }
}
