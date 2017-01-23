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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.JsonUtils;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.wf.ActionXCommand.ActionExecutorContext;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor.WorkflowActionQuery;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor.WorkflowJobQuery;
import org.apache.oozie.util.JobUtils;
import org.apache.oozie.util.LogUtils;

public class WorkflowActionRetryInfoXCommand extends WorkflowXCommand<List<Map<String, String>>> {
    private String actionId;
    private WorkflowJobBean wfJob;
    protected WorkflowActionBean wfAction = null;

    public WorkflowActionRetryInfoXCommand(String id) {
        super("action.retries.info", "action.retries.info", 1);
        this.actionId = id;
    }

    @Override
    protected List<Map<String, String>> execute() throws CommandException {
        List<Map<String, String>> retriesList = new ArrayList<Map<String, String>>();
        ActionExecutorContext context = new ActionXCommand.ActionExecutorContext(wfJob, wfAction);
        for (int i = 0; i < wfAction.getUserRetryCount(); i++) {
            Map<String, String> retries = new HashMap<String, String>();
            String value = context.getVar(JobUtils.getRetryKey(JsonTags.WORKFLOW_ACTION_EXTERNAL_CHILD_IDS, i));
            if (value != null) {
                retries.put(JsonTags.WORKFLOW_ACTION_EXTERNAL_CHILD_IDS, value);
            }
            value = context.getVar(JobUtils.getRetryKey(JsonTags.WORKFLOW_ACTION_CONSOLE_URL, i));
            if (value != null) {
                retries.put(JsonTags.WORKFLOW_ACTION_CONSOLE_URL, value);
            }
            value = context.getVar(JobUtils.getRetryKey(JsonTags.WORKFLOW_ACTION_START_TIME, i));
            if (value != null) {
                retries.put(JsonTags.WORKFLOW_ACTION_START_TIME,
                        JsonUtils.formatDateRfc822(new Date(Long.parseLong(value))));
            }
            value = context.getVar(JobUtils.getRetryKey(JsonTags.WORKFLOW_ACTION_END_TIME, i));
            if (value != null) {
                retries.put(JsonTags.WORKFLOW_ACTION_END_TIME,
                        JsonUtils.formatDateRfc822(new Date(Long.parseLong(value))));
            }
            retries.put(JsonTags.ACTION_ATTEMPT, String.valueOf(i + 1));
            retriesList.add(retries);
        }
        return retriesList;
    }

    @Override
    public String getEntityKey() {
        return null;
    }

    @Override
    protected void loadState() throws CommandException {
        try {
            this.wfAction = WorkflowActionQueryExecutor.getInstance().get(WorkflowActionQuery.GET_ACTION_CHECK,
                    actionId);
            this.wfJob = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW_DEFINITION,
                    actionId.substring(0, actionId.indexOf("@")));
            LogUtils.setLogInfo(wfAction);
        }
        catch (JPAExecutorException ex) {
            if (ex.getErrorCode() == ErrorCode.E0605) {
                throw new CommandException(ErrorCode.E0605, actionId);
            }
            else {
                throw new CommandException(ex);
            }
        }
    }

    @Override
    protected void verifyPrecondition() throws CommandException {
    }

    @Override
    protected boolean isLockRequired() {
        return false;
    }

}
