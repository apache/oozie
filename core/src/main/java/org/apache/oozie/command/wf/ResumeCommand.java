/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.command.wf;

import java.util.Date;

import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.lite.LiteWorkflowInstance;

public class ResumeCommand extends WorkflowCommand<Void> {

    private String id;

    public ResumeCommand(String id) {
        super("resume", "resume", 1, XLog.STD);
        this.id = ParamChecker.notEmpty(id, "id");
    }

    @Override
    protected Void call(WorkflowStore store) throws StoreException, CommandException {
        try {
            WorkflowJobBean workflow = store.getWorkflow(id, false);
            setLogInfo(workflow);
            if (workflow.getStatus() == WorkflowJob.Status.SUSPENDED) {
                incrJobCounter(1);
                workflow.getWorkflowInstance().resume();
                WorkflowInstance wfInstance = workflow.getWorkflowInstance();
                ((LiteWorkflowInstance) wfInstance).setStatus(WorkflowInstance.Status.RUNNING);
                workflow.setWorkflowInstance(wfInstance);
                workflow.setStatus(WorkflowJob.Status.RUNNING);

                for (WorkflowActionBean action : store.getActionsForWorkflow(id, false)) {

                    // Set pending flag to true for the actions that are START_RETRY or
                    // START_MANUAL or END_RETRY or END_MANUAL
                    if (action.isRetryOrManual()) {
                        action.setPendingOnly();
                        store.updateAction(action);
                    }

                    if (action.isPending()) {
                        if (action.getStatus() == WorkflowActionBean.Status.PREP
                                || action.getStatus() == WorkflowActionBean.Status.START_MANUAL) {
                            queueCallable(new ActionStartCommand(action.getId(), action.getType()));
                        }
                        else {
                            if (action.getStatus() == WorkflowActionBean.Status.START_RETRY) {
                                Date nextRunTime = action.getPendingAge();
                                queueCallable(new ActionStartCommand(action.getId(), action.getType()),
                                              nextRunTime.getTime() - System.currentTimeMillis());
                            }
                            else {
                                if (action.getStatus() == WorkflowActionBean.Status.DONE
                                        || action.getStatus() == WorkflowActionBean.Status.END_MANUAL) {
                                    queueCallable(new ActionEndCommand(action.getId(), action.getType()));
                                }
                                else {
                                    if (action.getStatus() == WorkflowActionBean.Status.END_RETRY) {
                                        Date nextRunTime = action.getPendingAge();
                                        queueCallable(new ActionEndCommand(action.getId(), action.getType()),
                                                      nextRunTime.getTime() - System.currentTimeMillis());
                                    }
                                }
                            }
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

    @Override
    protected Void execute(WorkflowStore store) throws CommandException, StoreException {
        XLog.getLog(getClass()).debug("STARTED ResumeCommand for action " + id);
        try {
            if (lock(id)) {
                call(store);
            }
            else {
                queueCallable(new KillCommand(id), LOCK_FAILURE_REQUEUE_INTERVAL);
                XLog.getLog(getClass()).warn("Resume lock was not acquired - failed {0}", id);
            }
        }
        catch (InterruptedException e) {
            queueCallable(new KillCommand(id), LOCK_FAILURE_REQUEUE_INTERVAL);
            XLog.getLog(getClass()).warn("ResumeCommand lock was not acquired - interrupted exception failed {0}", id);
        }
        finally {
            XLog.getLog(getClass()).debug("ENDED ResumeCommand for action " + id);
        }
        return null;
    }
}
