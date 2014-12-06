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
package org.apache.oozie.command.coord;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.SLAEventBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.XException;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.db.SLADbOperations;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.SLAEvent.SlaAppType;
import org.apache.oozie.client.SLAEvent.Status;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.CoordActionGetForExternalIdJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor;
import org.apache.oozie.executor.jpa.CoordinatorJobGetForUserAppnameJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.BatchQueryExecutor.UpdateEntry;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;

@SuppressWarnings("deprecation")
public class CoordActionUpdateXCommand extends CoordinatorXCommand<Void> {
    private WorkflowJobBean workflow;
    private CoordinatorActionBean coordAction = null;
    private CoordinatorJobBean coordJob;
    private JPAService jpaService = null;
    private int maxRetries = 1;
    private List<UpdateEntry> updateList = new ArrayList<UpdateEntry>();
    private List<JsonBean> insertList = new ArrayList<JsonBean>();

    public CoordActionUpdateXCommand(WorkflowJobBean workflow) {
        super("coord-action-update", "coord-action-update", 1);
        this.workflow = workflow;
    }

    public CoordActionUpdateXCommand(WorkflowJobBean workflow, int maxRetries) {
        super("coord-action-update", "coord-action-update", 1);
        this.workflow = workflow;
        this.maxRetries = maxRetries;
    }

    @Override
    protected Void execute() throws CommandException {
        try {
            LOG.debug("STARTED CoordActionUpdateXCommand for wfId=" + workflow.getId());
            Status slaStatus = null;
            if (workflow.getStatus() == WorkflowJob.Status.SUCCEEDED) {
                coordAction.setStatus(CoordinatorAction.Status.SUCCEEDED);
                coordAction.setPending(0);
                slaStatus = Status.SUCCEEDED;
            }
            else if (workflow.getStatus() == WorkflowJob.Status.FAILED) {
                coordAction.setStatus(CoordinatorAction.Status.FAILED);
                coordAction.setPending(0);
                slaStatus = Status.FAILED;
            }
            else if (workflow.getStatus() == WorkflowJob.Status.KILLED) {
                coordAction.setStatus(CoordinatorAction.Status.KILLED);
                coordAction.setPending(0);
                slaStatus = Status.KILLED;
            }
            else if (workflow.getStatus() == WorkflowJob.Status.SUSPENDED) {
                coordAction.setStatus(CoordinatorAction.Status.SUSPENDED);
                coordAction.decrementAndGetPending();
            }
            else if (workflow.getStatus() == WorkflowJob.Status.RUNNING) {
                // resume workflow job and update coord action accordingly
                coordAction.setStatus(CoordinatorAction.Status.RUNNING);
                coordAction.decrementAndGetPending();
            }
            else {
                LOG.warn("Unexpected workflow " + workflow.getId() + " STATUS " + workflow.getStatus());
                // update lastModifiedTime
                coordAction.setLastModifiedTime(new Date());
                CoordActionQueryExecutor.getInstance().executeUpdate(
                        CoordActionQueryExecutor.CoordActionQuery.UPDATE_COORD_ACTION_FOR_MODIFIED_DATE, coordAction);
                // TODO - Uncomment this when bottom up rerun can change terminal state
                /* CoordinatorJobBean coordJob = jpaService.execute(new CoordJobGetJPAExecutor(coordAction.getJobId()));
                if (!coordJob.isPending()) {
                    coordJob.setPending();
                    jpaService.execute(new CoordJobUpdateJPAExecutor(coordJob));
                }*/
                return null;
            }

            LOG.info("Updating Coordintaor action id :" + coordAction.getId() + " status "
                    + " to " + coordAction.getStatus() + ", pending = " + coordAction.getPending());

            coordAction.setLastModifiedTime(new Date());
            updateList.add(new UpdateEntry<CoordActionQuery>(CoordActionQuery.UPDATE_COORD_ACTION_STATUS_PENDING_TIME,
                    coordAction));
            // TODO - Uncomment this when bottom up rerun can change terminal state
            /*CoordinatorJobBean coordJob = jpaService.execute(new CoordJobGetJPAExecutor(coordAction.getJobId()));
            if (!coordJob.isPending()) {
                coordJob.setPending();
                jpaService.execute(new CoordJobUpdateJPAExecutor(coordJob));
                LOG.info("Updating Coordinator job "+ coordJob.getId() + "pending to true");
            }*/
            if (slaStatus != null) {
                SLAEventBean slaEvent = SLADbOperations.createStatusEvent(coordAction.getSlaXml(), coordAction.getId(), slaStatus,
                        SlaAppType.COORDINATOR_ACTION, LOG);
                if(slaEvent != null) {
                    insertList.add(slaEvent);
                }
            }
            if (workflow.getStatus() != WorkflowJob.Status.SUSPENDED
                    && workflow.getStatus() != WorkflowJob.Status.RUNNING) {
                queue(new CoordActionReadyXCommand(coordAction.getJobId()));
            }

            BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(insertList, updateList, null);
            if (EventHandlerService.isEnabled()) {
                generateEvent(coordAction, coordJob.getUser(), coordJob.getAppName(), workflow.getStartTime());
            }

            LOG.debug("ENDED CoordActionUpdateXCommand for wfId=" + workflow.getId());
        }
        catch (XException ex) {
            LOG.warn("CoordActionUpdate Failed ", ex.getMessage());
            throw new CommandException(ex);
        }
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    public String getEntityKey() {
        return workflow.getParentId().substring(0, workflow.getParentId().indexOf("@"));
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#isLockRequired()
     */
    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    protected void loadState() throws CommandException {
        jpaService = Services.get().get(JPAService.class);
        int retries = 0;
        while (retries++ < maxRetries) {
            try {
                coordAction = jpaService.execute(new CoordActionGetForExternalIdJPAExecutor(workflow.getId()));
                if (coordAction != null) {
                    coordJob = jpaService
                            .execute(new CoordinatorJobGetForUserAppnameJPAExecutor(coordAction.getJobId()));
                    LogUtils.setLogInfo(coordAction, logInfo);
                    break;
                }
                if (retries < maxRetries) {
                    Thread.sleep(500);
                }
            }
            catch (JPAExecutorException je) {
                LOG.warn("Could not load coord action {0}", je.getMessage(), je);
            }
            catch (InterruptedException ex) {
                LOG.warn("Retry to load coord action is interrupted {0}", ex.getMessage(), ex);
            }
        }
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {

        // if coord action is RUNNING and pending false and workflow is RUNNING, this doesn't need to be updated.
        if (workflow.getStatus() == WorkflowJob.Status.RUNNING
                && coordAction.getStatus() == CoordinatorAction.Status.RUNNING && !coordAction.isPending()) {
            try {
                CoordActionQueryExecutor.getInstance().executeUpdate(
                        CoordActionQueryExecutor.CoordActionQuery.UPDATE_COORD_ACTION_STATUS_PENDING_TIME, coordAction);
            }
            catch (JPAExecutorException je) {
                throw new CommandException(je);
            }
            throw new PreconditionException(ErrorCode.E1100, ", workflow is RUNNING and coordinator action is RUNNING and pending false");
        }
    }

}
