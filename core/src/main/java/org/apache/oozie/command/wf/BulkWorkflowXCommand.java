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

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.WorkflowsInfo;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.OperationType;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.WorkflowsJobGetJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;

import java.util.List;
import java.util.Map;

public class BulkWorkflowXCommand extends WorkflowXCommand<WorkflowsInfo> {
    private final Map<String, List<String>> filter;
    private final int start;
    private final int len;
    private WorkflowsInfo workflowsInfo;
    private OperationType operation;

    /**
     * constructor taking the filter information.
     *
     * @param filter Can be name, status, user, group and combination of these
     * @param start starting from this index in the list of workflows matching the filter are killed
     * @param length number of workflows to be killed from the list of workflows matching the filter and starting from
     *        index "start".
     */
    public BulkWorkflowXCommand(Map<String, List<String>> filter, int start, int length, OperationType operation) {
        super("bulkkill", "bulkkill", 1, true);
        this.filter = filter;
        this.start = start;
        this.len = length;
        this.operation = operation;
    }

    /* (non-Javadoc)
    * @see org.apache.oozie.command.XCommand#execute()
    */
    @Override
    protected WorkflowsInfo execute() throws CommandException {
        try {
            List<WorkflowJobBean> workflows = this.workflowsInfo.getWorkflows();
            for (WorkflowJobBean job : workflows) {
                switch (operation) {
                    case Kill:
                        if (job.getStatus() == WorkflowJob.Status.PREP
                                || job.getStatus() == WorkflowJob.Status.RUNNING
                                || job.getStatus() == WorkflowJob.Status.SUSPENDED
                                || job.getStatus() == WorkflowJob.Status.FAILED) {
                            new KillXCommand(job.getId()).call();
                        }
                        break;
                    case Suspend:
                        if (job.getStatus() == WorkflowJob.Status.RUNNING) {
                            new SuspendXCommand(job.getId()).call();
                        }
                        break;
                    case Resume:
                        if (job.getStatus() == WorkflowJob.Status.SUSPENDED) {
                            new ResumeXCommand(job.getId()).call();
                        }
                        break;
                    default:
                        throw new CommandException(ErrorCode.E1102, operation);
                }
            }
            loadJobs();
            return this.workflowsInfo;
        }
        catch (Exception ex) {
            throw new CommandException(ErrorCode.E0725, ex.getMessage(), ex);
        }
    }

    /* (non-Javadoc)
    * @see org.apache.oozie.command.XCommand#getEntityKey()
    */
    @Override
    public String getEntityKey() {
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#isLockRequired()
     */
    @Override
    protected boolean isLockRequired() {
        return false;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#loadState()
     */
    @Override
    protected void loadState() throws CommandException {
        loadJobs();
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }

    private void loadJobs() throws CommandException {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            if (jpaService != null) {
                this.workflowsInfo = jpaService.execute(
                        new WorkflowsJobGetJPAExecutor(this.filter, this.start, this.len));
            }
            else {
                throw new CommandException(ErrorCode.E0610);
            }
        }
        catch (Exception ex) {
            throw new CommandException(ErrorCode.E0603, ex.getMessage(), ex);
        }
    }
}
