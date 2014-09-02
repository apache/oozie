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

    public CompletedActionXCommand(String actionId, String externalStatus, Properties actionData, int priority) {
        super("callback", "callback", priority);
        this.actionId = ParamChecker.notEmpty(actionId, "actionId");
        this.externalStatus = ParamChecker.notEmpty(externalStatus, "externalStatus");
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
        if (this.wfactionBean.getStatus() != WorkflowActionBean.Status.RUNNING) {
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
        ActionExecutor executor = Services.get().get(ActionService.class).getExecutor(this.wfactionBean.getType());
        // this is done because oozie notifications (of sub-wfs) is send
        // every status change, not only on completion.
        if (executor.isCompleted(externalStatus)) {
            queue(new ActionCheckXCommand(this.wfactionBean.getId(), getPriority(), -1));
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
