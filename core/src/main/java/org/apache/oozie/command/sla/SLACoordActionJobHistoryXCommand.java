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

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor.WorkflowJobQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.util.LogUtils;

public class SLACoordActionJobHistoryXCommand extends SLAJobHistoryXCommand {

    CoordinatorActionBean cAction = null;

    public SLACoordActionJobHistoryXCommand(String jobId) {
        super(jobId);
    }


    protected void loadState() throws CommandException {
        try {
            cAction = CoordActionQueryExecutor.getInstance().get(CoordActionQuery.GET_COORD_ACTION_FOR_SLA, jobId);
        }
        catch (JPAExecutorException e) {
            throw new CommandException(e);
        }
        LogUtils.setLogInfo(cAction);
    }

    protected void updateSLASummary() throws CommandException {
        try {
            updateSLASummaryForCoordAction(cAction);
        }
        catch (JPAExecutorException e) {
            throw new CommandException(e);
        }

    }

    protected void updateSLASummaryForCoordAction(CoordinatorActionBean bean) throws JPAExecutorException {
        String wrkflowId = bean.getExternalId();
        if (wrkflowId != null) {
            WorkflowJobBean wrkflow = WorkflowJobQueryExecutor.getInstance().get(
                    WorkflowJobQuery.GET_WORKFLOW_START_END_TIME, wrkflowId);
            if (wrkflow != null) {
                updateSLASummary(bean.getId(), wrkflow.getStartTime(), wrkflow.getEndTime(), bean.getStatusStr());
            }
        }
        else{
            updateSLASummary(bean.getId(), null, bean.getLastModifiedTime(), bean.getStatusStr());
        }
    }

    @Override
    protected boolean isJobEnded() {
        return cAction.isTerminalStatus();
    }
}
