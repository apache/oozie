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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.BulkUpdateInsertJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionRetryManualGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.InstrumentUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.lite.LiteWorkflowInstance;

public class SuspendXCommand extends WorkflowXCommand<Void> {
    private final String wfid;
    private WorkflowJobBean wfJobBean;
    private JPAService jpaService;
    private List<JsonBean> updateList = new ArrayList<JsonBean>();

    public SuspendXCommand(String id) {
        super("suspend", "suspend", 1);
        this.wfid = ParamChecker.notEmpty(id, "wfid");
    }

    @Override
    protected Void execute() throws CommandException {
        InstrumentUtils.incrJobCounter(getName(), 1, getInstrumentation());
        try {
            suspendJob(this.jpaService, this.wfJobBean, this.wfid, null, updateList);
            this.wfJobBean.setLastModifiedTime(new Date());
            updateList.add(this.wfJobBean);
            jpaService.execute(new BulkUpdateInsertJPAExecutor(updateList, null));
            queue(new NotificationXCommand(this.wfJobBean));
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
            String actionId, List<JsonBean> updateList) throws WorkflowException, CommandException {
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
            List<JsonBean> updateList) throws CommandException {
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
                    updateList.add(action);
                }
            }
        }
        catch (JPAExecutorException je) {
            throw new CommandException(je);
        }
    }

    @Override
    protected void eagerLoadState() throws CommandException {
        super.eagerLoadState();
        try {
            jpaService = Services.get().get(JPAService.class);
            if (jpaService != null) {
                this.wfJobBean = jpaService.execute(new WorkflowJobGetJPAExecutor(this.wfid));
            }
            else {
                throw new CommandException(ErrorCode.E0610);
            }
        }
        catch (Exception ex) {
            throw new CommandException(ErrorCode.E0603, ex.getMessage(), ex);
        }
        LogUtils.setLogInfo(this.wfJobBean, logInfo);
    }

    @Override
    protected void eagerVerifyPrecondition() throws CommandException, PreconditionException {
        super.eagerVerifyPrecondition();
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
        eagerLoadState();
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        eagerVerifyPrecondition();
    }
}
