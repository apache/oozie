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

package org.apache.oozie.command.sla;

import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.XException;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor.WorkflowActionQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.util.LogUtils;

public class SLAWorkflowActionJobHistoryXCommand extends SLAJobHistoryXCommand {

    WorkflowActionBean wfAction = null;

    public SLAWorkflowActionJobHistoryXCommand(String jobId) {
        super(jobId);
    }

    protected void loadState() throws CommandException {

        try {
            wfAction = WorkflowActionQueryExecutor.getInstance().get(WorkflowActionQuery.GET_ACTION_COMPLETED, jobId);
        }
        catch (JPAExecutorException e) {
            throw new CommandException(e);
        }
        LogUtils.setLogInfo(wfAction);
    }

    protected void updateSLASummary() throws XException {
        updateSLASummary(wfAction.getId(), wfAction.getStartTime(), wfAction.getEndTime(), wfAction.getStatusStr());

    }

    @Override
    protected boolean isJobEnded() {
        return wfAction.isComplete() || wfAction.isTerminalWithFailure();
    }
}
