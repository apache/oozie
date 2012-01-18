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

import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.SLAEvent.SlaAppType;
import org.apache.oozie.client.SLAEvent.Status;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.XException;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.coord.CoordActionUpdateXCommand;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionsGetForJobJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobUpdateJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.lite.LiteWorkflowInstance;
import org.apache.oozie.util.InstrumentUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.db.SLADbXOperations;

import java.util.Date;
import java.util.List;

/**
 * Kill workflow job and its workflow instance and queue a {@link WorkflowActionKillXCommand} to kill the workflow
 * actions.
 */
public class KillXCommand extends WorkflowXCommand<Void> {

    private String wfId;
    private WorkflowJobBean wfJob;
    private List<WorkflowActionBean> actionList;
    private JPAService jpaService = null;

    public KillXCommand(String wfId) {
        super("kill", "kill", 1);
        this.wfId = ParamChecker.notEmpty(wfId, "wfId");
    }

    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    public String getEntityKey() {
        return this.wfId;
    }

    @Override
    protected void loadState() throws CommandException {
        try {
            jpaService = Services.get().get(JPAService.class);
            if (jpaService != null) {
                this.wfJob = jpaService.execute(new WorkflowJobGetJPAExecutor(wfId));
                this.actionList = jpaService.execute(new WorkflowActionsGetForJobJPAExecutor(wfId));
                LogUtils.setLogInfo(wfJob, logInfo);
            }
            else {
                throw new CommandException(ErrorCode.E0610);
            }
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (wfJob.getStatus() != WorkflowJob.Status.PREP && wfJob.getStatus() != WorkflowJob.Status.RUNNING
                && wfJob.getStatus() != WorkflowJob.Status.SUSPENDED && wfJob.getStatus() != WorkflowJob.Status.FAILED) {
            throw new PreconditionException(ErrorCode.E0725, wfJob.getId());
        }
    }

    @Override
    protected Void execute() throws CommandException {
        LOG.info("STARTED WorkflowKillXCommand for jobId=" + wfId);

        wfJob.setEndTime(new Date());

        if (wfJob.getStatus() != WorkflowJob.Status.FAILED) {
            InstrumentUtils.incrJobCounter(getName(), 1, getInstrumentation());
            wfJob.setStatus(WorkflowJob.Status.KILLED);
            SLADbXOperations.writeStausEvent(wfJob.getSlaXml(), wfJob.getId(), Status.KILLED, SlaAppType.WORKFLOW_JOB);
            try {
                wfJob.getWorkflowInstance().kill();
            }
            catch (WorkflowException e) {
                throw new CommandException(ErrorCode.E0725, e.getMessage(), e);
            }
            WorkflowInstance wfInstance = wfJob.getWorkflowInstance();
            ((LiteWorkflowInstance) wfInstance).setStatus(WorkflowInstance.Status.KILLED);
            wfJob.setWorkflowInstance(wfInstance);
        }
        try {
            for (WorkflowActionBean action : actionList) {
                if (action.getStatus() == WorkflowActionBean.Status.RUNNING
                        || action.getStatus() == WorkflowActionBean.Status.DONE) {
                    action.setPending();
                    action.setStatus(WorkflowActionBean.Status.KILLED);

                    jpaService.execute(new WorkflowActionUpdateJPAExecutor(action));

                    queue(new ActionKillXCommand(action.getId(), action.getType()));
                }
                if (action.getStatus() == WorkflowActionBean.Status.PREP
                        || action.getStatus() == WorkflowActionBean.Status.START_RETRY
                        || action.getStatus() == WorkflowActionBean.Status.START_MANUAL
                        || action.getStatus() == WorkflowActionBean.Status.END_RETRY
                        || action.getStatus() == WorkflowActionBean.Status.END_MANUAL) {

                    action.setStatus(WorkflowActionBean.Status.KILLED);
                    action.resetPending();
                    SLADbXOperations.writeStausEvent(action.getSlaXml(), action.getId(), Status.KILLED,
                            SlaAppType.WORKFLOW_ACTION);
                    jpaService.execute(new WorkflowActionUpdateJPAExecutor(action));
                }
            }
            jpaService.execute(new WorkflowJobUpdateJPAExecutor(wfJob));
            queue(new NotificationXCommand(wfJob));
        }
        catch (JPAExecutorException je) {
            throw new CommandException(je);
        }
        finally {
            if(wfJob.getStatus() == WorkflowJob.Status.KILLED) {
                 new WfEndXCommand(wfJob).call(); //To delete the WF temp dir
            }
            // update coordinator action
            new CoordActionUpdateXCommand(wfJob).call();
        }

        LOG.info("ENDED WorkflowKillXCommand for jobId=" + wfId);
        return null;
    }

}
