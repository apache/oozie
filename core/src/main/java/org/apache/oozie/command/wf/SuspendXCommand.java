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
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.BatchQueryExecutor.UpdateEntry;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor.WorkflowActionQuery;
import org.apache.oozie.executor.jpa.WorkflowActionRetryManualGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor.WorkflowJobQuery;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.InstrumentUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.lite.LiteWorkflowInstance;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class SuspendXCommand extends WorkflowXCommand<Void> {
    private final String wfid;
    private WorkflowJobBean wfJobBean;
    private JPAService jpaService;
    private List<UpdateEntry> updateList = new ArrayList<UpdateEntry>();

    public SuspendXCommand(String id) {
        super("suspend", "suspend", 1);
        this.wfid = ParamChecker.notEmpty(id, "wfid");
    }

    @Override
    protected void setLogInfo() {
        LogUtils.setLogInfo(wfid);
    }

    @Override
    protected Void execute() throws CommandException {
        InstrumentUtils.incrJobCounter(getName(), 1, getInstrumentation());
        try {
            suspendJob(this.jpaService, this.wfJobBean, this.wfid, null, updateList);
            this.wfJobBean.setLastModifiedTime(new Date());
            updateList.add(new UpdateEntry<WorkflowJobQuery>(WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_INSTANCE_MODIFIED,
                    this.wfJobBean));
            BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(null, updateList, null);
            queue(new WorkflowNotificationXCommand(this.wfJobBean));
            //Calling suspend recursively to handle parent workflow
            suspendParentWorkFlow();
        }
        catch (WorkflowException e) {
            throw new CommandException(e);
        }
        catch (JPAExecutorException je) {
            throw new CommandException(je);
        }
        finally {
            updateParentIfNecessary(wfJobBean);
        }
        return null;
    }

    /**
     * It will suspend the parent workflow
     * @throws CommandException
     */
    private void suspendParentWorkFlow() throws CommandException {
        if (this.wfJobBean.getParentId() != null && this.wfJobBean.getParentId().contains("-W")) {
            new SuspendXCommand(this.wfJobBean.getParentId()).call();
        } else {
            // update the action of the parent workflow if it is launched by coordinator
            updateParentIfNecessary(wfJobBean);
        }
    }

    /**
     * Suspend the workflow job and pending flag to false for the actions that are START_RETRY or START_MANUAL or
     * END_RETRY or END_MANUAL
     *
     * @param jpaService jpa service
     * @param workflow workflow job
     * @param id workflow job id
     * @param actionId workflow action id
     * @throws WorkflowException thrown if failed to suspend workflow instance
     * @throws CommandException thrown if unable set pending false for actions
     */
    public static void suspendJob(JPAService jpaService, WorkflowJobBean workflow, String id,
            String actionId, List<UpdateEntry> updateList) throws WorkflowException, CommandException {
        if (workflow.getStatus() == WorkflowJob.Status.RUNNING) {
            workflow.getWorkflowInstance().suspend();
            WorkflowInstance wfInstance = workflow.getWorkflowInstance();
            ((LiteWorkflowInstance) wfInstance).setStatus(WorkflowInstance.Status.SUSPENDED);
            workflow.setStatus(WorkflowJob.Status.SUSPENDED);
            workflow.setWorkflowInstance(wfInstance);

            setPendingFalseForActions(jpaService, id, actionId, updateList);
            if (EventHandlerService.isEnabled()) {
                generateEvent(workflow);
            }
        }
    }

    /**
     * Set pending flag to false for the actions that are START_RETRY or START_MANUAL or END_RETRY or END_MANUAL
     * <p/>
     *
     * @param jpaService jpa service
     * @param id workflow job id
     * @param actionId workflow action id
     * @throws CommandException thrown if failed to update workflow action
     */
    private static void setPendingFalseForActions(JPAService jpaService, String id, String actionId,
            List<UpdateEntry> updateList) throws CommandException {
        List<WorkflowActionBean> actions;
        try {
            actions = jpaService.execute(new WorkflowActionRetryManualGetJPAExecutor(id));

            for (WorkflowActionBean action : actions) {
                if (actionId != null && actionId.equals(action.getId())) {
                    // this action has been changed in handleNonTransient()
                    continue;
                }
                else {
                    action.resetPendingOnly();
                }
                if (updateList != null) { // will be null when suspendJob
                                          // invoked statically via
                                          // handleNonTransient()
                    updateList.add(new UpdateEntry<WorkflowActionQuery>(
                            WorkflowActionQuery.UPDATE_ACTION_STATUS_PENDING, action));
                }
            }
        }
        catch (JPAExecutorException je) {
            throw new CommandException(je);
        }
    }

    @Override
    protected void eagerLoadState() throws CommandException {
        try {
            jpaService = Services.get().get(JPAService.class);
            this.wfJobBean = WorkflowJobQueryExecutor.getInstance()
                    .get(WorkflowJobQuery.GET_WORKFLOW_STATUS, this.wfid);
        }
        catch (Exception ex) {
            throw new CommandException(ErrorCode.E0603, ex.getMessage(), ex);
        }
        LogUtils.setLogInfo(this.wfJobBean);
    }

    @Override
    protected void eagerVerifyPrecondition() throws CommandException, PreconditionException {
        if (this.wfJobBean.getStatus() != WorkflowJob.Status.RUNNING) {
            throw new PreconditionException(ErrorCode.E0727, this.wfJobBean.getId(), this.wfJobBean.getStatus());
        }
    }

    @Override
    public String getEntityKey() {
        return this.wfid;
    }

    @Override
    public String getKey() {
        return getName() + "_" + this.wfid;
    }

    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    protected void loadState() throws CommandException {
        try {
            this.wfJobBean = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW_SUSPEND,
                    this.wfid);
        }
        catch (Exception ex) {
            throw new CommandException(ErrorCode.E0603, ex.getMessage(), ex);
        }
        LogUtils.setLogInfo(wfJobBean);
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        eagerVerifyPrecondition();
    }
}
