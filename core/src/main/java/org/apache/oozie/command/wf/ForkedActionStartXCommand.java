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

import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.XCommand;
import org.apache.oozie.command.wf.ActionXCommand.ActionExecutorContext;

public class ForkedActionStartXCommand extends ActionStartXCommand {

    public ForkedActionStartXCommand(String actionId, String type) {
        super(actionId, type);
    }

    public ForkedActionStartXCommand(WorkflowJobBean wfJob, String id, String type) {
        super(wfJob, id, type);
    }

    protected ActionExecutorContext execute() throws CommandException {
        super.execute();
        return context;
    }

    @Override
    public String getEntityKey() {
        return actionId;
    }

    // In case of requeue follow the old approach.
    @Override
    protected void queue(XCommand<?> command, long msDelay) {

        if (command instanceof ForkedActionStartXCommand) {
            LOG.debug("Queueing ActionStartXCommand command");
            super.queue(new ActionStartXCommand(wfAction.getId(), wfAction.getType()), msDelay);
        }
        else {
            LOG.debug("Queueing " + command);
            super.queue(command, msDelay);
        }
    }

    // Job will be failed by SignalXcommand, because ForkedActionStartXCommand doesn't have lock on jobId.
    @Override
    public void failJob(ActionExecutor.Context context, WorkflowActionBean action) throws CommandException {
        this.context.setJobStatus(Job.Status.FAILED);
    }

    @Override
    protected void updateParentIfNecessary(WorkflowJobBean wfjob, int maxRetries) throws CommandException {
    }

    @Override
    protected void handleNonTransient(ActionExecutor.Context context, ActionExecutor executor,
            WorkflowAction.Status status) throws CommandException {
        this.context.setJobStatus(Job.Status.SUSPENDED);
    }

    @Override
    protected void handleError(ActionExecutorContext context, WorkflowJobBean workflow, WorkflowActionBean action)
            throws CommandException {
        this.context.setJobStatus(Job.Status.FAILED);
    }

    @Override
    protected void updateJobLastModified() {
    }

    // Not killing job, setting flag so that signalX can kill the job
    @Override
    protected void endWF() {
        context.setShouldEndWF(true);
    }

    @Override
    protected void callActionEnd() {
        queue(new ActionEndXCommand(wfAction.getId(), wfAction.getType()));
    }

    @Override
    protected ActionExecutorContext  getContext(boolean isRetry, boolean isUserRetry){
        return  new ActionXCommand.ForkedActionExecutorContext(wfJob, wfAction, isRetry, isUserRetry);
    }

}
