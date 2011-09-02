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

import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.command.Command;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;

import java.util.Date;

public class KillCommand extends Command<Void> {

    private String id;

    public KillCommand(String id) {
        super("kill", "kill", 0, XLog.STD);
        this.id = ParamChecker.notEmpty(id, "id");
    }

    @Override
    protected Void call(WorkflowStore store) throws StoreException, CommandException {
        try {
            WorkflowJobBean workflow = store.getWorkflow(id, true);
            setLogInfo(workflow);

            if (workflow.getStatus() == WorkflowJob.Status.PREP || workflow.getStatus() == WorkflowJob.Status.RUNNING ||
                workflow.getStatus() == WorkflowJob.Status.SUSPENDED || workflow.getStatus() == WorkflowJob.Status.FAILED) {
                workflow.setEndTime(new Date());
                if (workflow.getStatus() != WorkflowJob.Status.FAILED) {
                    incrJobCounter(1);
                    workflow.setStatus(WorkflowJob.Status.KILLED);
                    workflow.getWorkflowInstance().kill();
                }
                for (WorkflowActionBean action : store.getActionsForWorkflow(id, true)) {
                    if (action.getStatus() == WorkflowActionBean.Status.RUNNING
                            || action.getStatus() == WorkflowActionBean.Status.DONE) {
                        action.setPending();
                        action.setStatus(WorkflowActionBean.Status.KILLED);
                        store.updateAction(action);
                        queueCallable(new ActionKillCommand(action.getId(), action.getType()));
                    }
                    if (action.getStatus() == WorkflowActionBean.Status.PREP
                            || action.getStatus() == WorkflowActionBean.Status.START_RETRY
                            || action.getStatus() == WorkflowActionBean.Status.START_MANUAL
                            || action.getStatus() == WorkflowActionBean.Status.END_RETRY
                            || action.getStatus() == WorkflowActionBean.Status.END_MANUAL) {
                        action.setStatus(WorkflowActionBean.Status.KILLED);
                        action.resetPending();
                        store.updateAction(action);
                    }
                }
                store.updateWorkflow(workflow);
                queueCallable(new NotificationCommand(workflow));
            }
            return null;
        }
        catch (WorkflowException ex) {
            throw new CommandException(ex);
        }
    }
}