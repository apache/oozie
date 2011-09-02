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

import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.command.Command;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.store.Store;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;
import org.apache.oozie.service.Services;

import java.util.Properties;

public class CompletedActionCommand extends WorkflowCommand<Void> {
    private String actionId;
    private String externalStatus;
    private Properties actionData;

    public CompletedActionCommand(String actionId, String externalStatus, Properties actionData, int priority) {
        super("callback", "callback", priority, XLog.STD);
        this.actionId = ParamChecker.notEmpty(actionId, "actionId");
        this.externalStatus = ParamChecker.notEmpty(externalStatus, "externalStatus");
        this.actionData = actionData;
    }

    public CompletedActionCommand(String actionId, String externalStatus, Properties actionData) {
        this(actionId, externalStatus, actionData, 0);
    }

    @Override
    protected Void call(WorkflowStore store) throws StoreException, CommandException {
        WorkflowActionBean action = store.getAction(actionId, false);
        setLogInfo(action);
        if (action.getStatus() == WorkflowActionBean.Status.RUNNING) {
            ActionExecutor executor = Services.get().get(ActionService.class).getExecutor(action.getType());
            // this is done because oozie notifications (of sub-wfs) is send
            // every status change, not only on completion.
            if (executor.isCompleted(externalStatus)) {
                queueCallable(new ActionCheckCommand(action.getId(), getPriority(), -1));
            }
        }
        else {
            throw new CommandException(ErrorCode.E0800, actionId, action.getStatus());
        }
        return null;
    }

}
