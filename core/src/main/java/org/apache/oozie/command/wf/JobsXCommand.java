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

import java.util.List;
import java.util.Map;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.WorkflowsInfo;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.WorkflowsJobGetJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;

public class JobsXCommand extends WorkflowXCommand<WorkflowsInfo> {
    private final Map<String, List<String>> filter;
    private final int start;
    private final int len;
    private WorkflowsInfo workflows;

    /**
     * Constructor taking the filter information
     *
     * @param filter Can be name, status, user, group and combination of these
     * @param start starting from this index in the list of workflows matching the filter are returned
     * @param length number of workflows to be returned from the list of workflows matching the filter and starting from
     *        index "start".
     */
    public JobsXCommand(Map<String, List<String>> filter, int start, int length) {
        super("job.info", "job.info", 1, true);
        this.filter = filter;
        this.start = start;
        this.len = length;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#execute()
     */
    @Override
    protected WorkflowsInfo execute() throws CommandException {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            if (jpaService != null) {
                this.workflows = jpaService.execute(new WorkflowsJobGetJPAExecutor(this.filter, this.start, this.len));
            }
            else {
                throw new CommandException(ErrorCode.E0610);
            }
            for (WorkflowJobBean workflow : this.workflows.getWorkflows()) {
                workflow.setConsoleUrl(JobXCommand.getJobConsoleUrl(workflow.getId()));
            }
            return this.workflows;
        }
        catch (Exception ex) {
            throw new CommandException(ErrorCode.E0603, ex.getMessage(), ex);
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
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }
}
