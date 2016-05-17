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
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor.WorkflowActionQuery;
import org.apache.oozie.sla.SLACalcStatus;
import org.apache.oozie.util.LogUtils;

public class SLAWorkflowActionJobEventXCommand extends SLAJobEventXCommand {
    WorkflowActionBean wa;

    public SLAWorkflowActionJobEventXCommand(SLACalcStatus slaCalc, long lockTimeOut) {
        super(slaCalc, lockTimeOut);
    }

    @Override
    protected void loadState() throws CommandException {
        try {
            wa = WorkflowActionQueryExecutor.getInstance().get(WorkflowActionQuery.GET_ACTION_FOR_SLA, slaCalc.getId());
        }
        catch (JPAExecutorException e) {
            throw new CommandException(e);
        }
        LogUtils.setLogInfo(wa);

    }


    @Override
    protected void updateJobInfo() {
        if (wa.getEndTime() != null) {
            setEnded(true);
            if (wa.isTerminalWithFailure() || wa.getEndTime().getTime() > slaCalc.getExpectedEnd().getTime()) {
                setEndMiss(true);
            }
        }
        slaCalc.setActualStart(wa.getStartTime());
        slaCalc.setActualEnd(wa.getEndTime());
        slaCalc.setJobStatus(wa.getStatusStr());
    }

}
