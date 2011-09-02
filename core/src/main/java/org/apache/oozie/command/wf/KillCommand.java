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
import org.apache.oozie.client.SLAEvent.SlaAppType;
import org.apache.oozie.client.SLAEvent.Status;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.command.Command;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.store.Store;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.lite.LiteWorkflowInstance;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.db.SLADbOperations;

import java.util.Date;

public class KillCommand extends WorkflowCommand<Void> {

    private String id;
    private final XLog log = XLog.getLog(getClass());

    public KillCommand(String id) {
        super("kill", "kill", 0, XLog.STD);
        this.id = ParamChecker.notEmpty(id, "id");
    }

    @Override
    protected Void call(WorkflowStore store) throws StoreException, CommandException {
        try {
            log.info("In Workflow KillCommand.call() for jobId=" + id);
            WorkflowJobBean workflow = store.getWorkflow(id, false);
            setLogInfo(workflow);

            if (workflow.getStatus() == WorkflowJob.Status.PREP || workflow.getStatus() == WorkflowJob.Status.RUNNING
                    || workflow.getStatus() == WorkflowJob.Status.SUSPENDED
                    || workflow.getStatus() == WorkflowJob.Status.FAILED) {
                workflow.setEndTime(new Date());

                if (workflow.getStatus() != WorkflowJob.Status.FAILED) {
                    incrJobCounter(1);
                    workflow.setStatus(WorkflowJob.Status.KILLED);
                    SLADbOperations.writeStausEvent(workflow.getSlaXml(), workflow.getId(), store, Status.KILLED,
                                                    SlaAppType.WORKFLOW_JOB);
                    workflow.getWorkflowInstance().kill();
                    WorkflowInstance wfInstance = workflow.getWorkflowInstance();
                    ((LiteWorkflowInstance) wfInstance).setStatus(WorkflowInstance.Status.KILLED);
                    workflow.setWorkflowInstance(wfInstance);
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
                        SLADbOperations.writeStausEvent(action.getSlaXml(), action.getId(), store, Status.KILLED,
                                                        SlaAppType.WORKFLOW_ACTION);
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

    @Override
    protected Void execute(WorkflowStore store) throws CommandException, StoreException {
        try {
            XLog.getLog(getClass()).debug("STARTED KillCommand for job " + id);
            if (lock(id)) {
                call(store);
            }
            else {
                queueCallable(new KillCommand(id), LOCK_FAILURE_REQUEUE_INTERVAL);
                XLog.getLog(getClass()).warn("KillCommand lock was not acquired - failed {0}", id);
            }
        }
        catch (InterruptedException e) {
            queueCallable(new KillCommand(id), LOCK_FAILURE_REQUEUE_INTERVAL);
            XLog.getLog(getClass()).warn("KillCommand lock was not acquired - interrupted exception failed {0}", id);
        }
        XLog.getLog(getClass()).debug("ENDED KillCommand for job " + id);
        return null;
    }
}
