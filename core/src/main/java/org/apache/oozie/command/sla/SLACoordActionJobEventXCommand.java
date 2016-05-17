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
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor.WorkflowJobQuery;
import org.apache.oozie.sla.SLACalcStatus;
import org.apache.oozie.util.LogUtils;

public class SLACoordActionJobEventXCommand extends SLAJobEventXCommand {
    CoordinatorActionBean ca;
    WorkflowJobBean wf;

    public SLACoordActionJobEventXCommand(SLACalcStatus slaCalc, long lockTimeOut) {
        super(slaCalc, lockTimeOut);
    }

    @Override
    protected void loadState() throws CommandException {
        try {
            ca = CoordActionQueryExecutor.getInstance().get(CoordActionQuery.GET_COORD_ACTION_FOR_SLA, slaCalc.getId());
            if (ca.getExternalId() != null) {
                wf = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW_FOR_SLA, ca.getExternalId());
            }
            LogUtils.setLogInfo(ca);
        }
        catch (JPAExecutorException e) {
            throw new CommandException(e);
        }
    }


    protected void updateJobInfo() {
        if (ca.isTerminalStatus()) {
            setEnded(true);
            setEndMiss(ca.isTerminalWithFailure());
            slaCalc.setActualEnd(ca.getLastModifiedTime());
            if (wf != null) {
                if (wf.getEndTime() != null) {
                    if (slaCalc.getExpectedEnd() != null
                            && wf.getEndTime().getTime() > slaCalc.getExpectedEnd().getTime()) {
                        setEndMiss(true);
                    }
                    slaCalc.setActualEnd(wf.getEndTime());
                }
                slaCalc.setActualStart(wf.getStartTime());
            }
        }
        else {
            if (wf != null) {
                slaCalc.setActualStart(wf.getStartTime());
            }
        }
        slaCalc.setJobStatus(ca.getStatusStr());
    }


}
