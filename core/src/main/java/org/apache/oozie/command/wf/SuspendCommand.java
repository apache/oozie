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

import java.util.Date;

public class SuspendCommand extends WorkflowCommand<Void> {

    private String id;

    public SuspendCommand(String id) {
        super("suspend", "suspend", 0, XLog.STD);
        this.id = ParamChecker.notEmpty(id, "id");
    }

    @Override
    protected Void call(WorkflowStore store) throws StoreException, CommandException {
        try {
            WorkflowJobBean workflow = store.getWorkflow(id, false);
            setLogInfo(workflow);
            if (workflow.getStatus() == WorkflowJob.Status.RUNNING) {
                incrJobCounter(1);
                suspendJob(workflow, id);
                store.updateWorkflow(workflow);
                queueCallable(new NotificationCommand(workflow));
            }
            return null;
        }
        catch (WorkflowException ex) {
            throw new CommandException(ex);
        }
    }

    public static void suspendJob(WorkflowJobBean workflow, String id) throws WorkflowException {
        if (workflow.getStatus() == WorkflowJob.Status.RUNNING) {
            workflow.getWorkflowInstance().suspend();
            WorkflowInstance wfInstance = workflow.getWorkflowInstance();
            ((LiteWorkflowInstance) wfInstance).setStatus(WorkflowInstance.Status.SUSPENDED);
            workflow.setStatus(WorkflowJob.Status.SUSPENDED);
            workflow.setWorkflowInstance(wfInstance);
        }
    }

    @Override
    protected Void execute(WorkflowStore store) throws CommandException, StoreException {
        XLog.getLog(getClass()).debug("STARTED SuspendCommand for action " + id);
        try {
            if (lock(id)) {
                call(store);
            }
            else {
                queueCallable(new SuspendCommand(id), LOCK_FAILURE_REQUEUE_INTERVAL);
                XLog.getLog(getClass()).warn("Suspend lock was not acquired - failed {0}", id);
            }
        }
        catch (InterruptedException e) {
            queueCallable(new SuspendCommand(id), LOCK_FAILURE_REQUEUE_INTERVAL);
            XLog.getLog(getClass()).warn("SuspendCommand lock was not acquired - interrupted exception failed {0}", id);
        }
        finally {
            XLog.getLog(getClass()).debug("ENDED SuspendCommand for action " + id);
        }
        return null;
    }
}
