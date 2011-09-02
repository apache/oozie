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

import java.util.List;

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

public class SuspendCommand extends WorkflowCommand<Void> {

    private String id;

    public SuspendCommand(String id) {
        super("suspend", "suspend", 1, XLog.STD);
        this.id = ParamChecker.notEmpty(id, "id");
    }

    @Override
    protected Void call(WorkflowStore store) throws StoreException, CommandException {
        try {
            WorkflowJobBean workflow = store.getWorkflow(id, false);
            setLogInfo(workflow);
            if (workflow.getStatus() == WorkflowJob.Status.RUNNING) {
                incrJobCounter(1);
                suspendJob(store, workflow, id, null);
                store.updateWorkflow(workflow);
                queueCallable(new NotificationCommand(workflow));
            }
            return null;
        }
        catch (WorkflowException ex) {
            throw new CommandException(ex);
        }
    }

    /**
     * Suspend the workflow job and pending flag to false for the actions that
     * are START_RETRY or START_MANUAL or END_RETRY or END_MANUAL
     *
     * @param store WorkflowStore
     * @param workflow WorkflowJobBean
     * @param id String
     * @param actionId String
     * @throws WorkflowException
     * @throws StoreException
     */
    public static void suspendJob(WorkflowStore store, WorkflowJobBean workflow, String id, String actionId)
            throws WorkflowException, StoreException {
        if (workflow.getStatus() == WorkflowJob.Status.RUNNING) {
            workflow.getWorkflowInstance().suspend();
            WorkflowInstance wfInstance = workflow.getWorkflowInstance();
            ((LiteWorkflowInstance) wfInstance).setStatus(WorkflowInstance.Status.SUSPENDED);
            workflow.setStatus(WorkflowJob.Status.SUSPENDED);
            workflow.setWorkflowInstance(wfInstance);

            setPendingFalseForActions(store, id, actionId);
        }
    }

    /**
     * Set pending flag to false for the actions that are START_RETRY or
     * START_MANUAL or END_RETRY or END_MANUAL
     * <p/>
     *
     * @param store WorkflowStore
     * @param id workflow id
     * @param actionId workflow action id
     * @throws StoreException
     */
    public static void setPendingFalseForActions(WorkflowStore store, String id, String actionId) throws StoreException {
        List<WorkflowActionBean> actions = store.getRetryAndManualActions(id);
        for (WorkflowActionBean action : actions) {
            if (actionId != null && actionId.equals(action.getId())) {
                // this action has been changed in handleNonTransient()
                continue;
            }
            else {
                action.resetPendingOnly();
            }
            store.updateAction(action);
        }
    }

    @Override
    protected Void execute(WorkflowStore store) throws CommandException, StoreException {
        XLog.getLog(getClass()).debug("STARTED SuspendCommand for wf id=" + id);
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
            XLog.getLog(getClass()).debug("ENDED SuspendCommand for wf id=" + id);
        }
        return null;
    }
}
