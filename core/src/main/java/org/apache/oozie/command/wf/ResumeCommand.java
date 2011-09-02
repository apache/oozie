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

public class ResumeCommand extends Command<Void> {

    private String id;

    public ResumeCommand(String id) {
        super("resume", "resume", 0, XLog.STD);
        this.id = ParamChecker.notEmpty(id, "id");
    }

    @Override
    protected Void call(WorkflowStore store) throws StoreException, CommandException {
        try {
            WorkflowJobBean workflow = store.getWorkflow(id, true);
            setLogInfo(workflow);
            if (workflow.getStatus() == WorkflowJob.Status.SUSPENDED) {
                incrJobCounter(1);
                workflow.getWorkflowInstance().resume();
                workflow.setStatus(WorkflowJob.Status.RUNNING);
                for (WorkflowActionBean action : store.getActionsForWorkflow(id, true)) {
                    if (action.isPending()) {
                        if (action.getStatus() == WorkflowActionBean.Status.PREP
                                || action.getStatus() == WorkflowActionBean.Status.START_MANUAL) {
                            queueCallable(new ActionStartCommand(action.getId(), action.getType()));
                        }
                        else if (action.getStatus() == WorkflowActionBean.Status.START_RETRY) {
                            Date nextRunTime = action.getPendingAge();
                            queueCallable(new ActionStartCommand(action.getId(), action.getType()),
                                          nextRunTime.getTime() - System.currentTimeMillis());
                        }
                        else if (action.getStatus() == WorkflowActionBean.Status.DONE
                                || action.getStatus() == WorkflowActionBean.Status.END_MANUAL) {
                            queueCallable(new ActionEndCommand(action.getId(), action.getType()));
                        }
                        else if (action.getStatus() == WorkflowActionBean.Status.END_RETRY) {
                            Date nextRunTime = action.getPendingAge();
                            queueCallable(new ActionEndCommand(action.getId(), action.getType()),
                                          nextRunTime.getTime() - System.currentTimeMillis());
                        }

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