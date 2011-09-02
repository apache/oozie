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
package org.apache.oozie.command.coord;

import java.util.Date;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.XException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.db.SLADbOperations;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.SLAEvent.SlaAppType;
import org.apache.oozie.client.SLAEvent.Status;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.CoordActionGetForExternalIdJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;

public class CoordActionUpdateXCommand extends CoordinatorXCommand<Void> {
    private final static XLog LOG = XLog.getLog(CoordActionUpdateXCommand.class);
    private WorkflowJobBean workflow;
    private CoordinatorActionBean coordAction = null;
    private JPAService jpaService = null;

    public CoordActionUpdateXCommand(WorkflowJobBean workflow) {
        super("coord-action-update", "coord-action-update", 1);
        this.workflow = workflow;
    }

    @Override
    protected Void execute() throws CommandException {
        try {
            LOG.debug("STARTED CoordActionUpdateXCommand for wfId=" + workflow.getId());

            Status slaStatus = null;

            if (workflow.getStatus() == WorkflowJob.Status.SUCCEEDED) {
                coordAction.setStatus(CoordinatorAction.Status.SUCCEEDED);
                slaStatus = Status.SUCCEEDED;
            }
            else if (workflow.getStatus() == WorkflowJob.Status.FAILED) {
                coordAction.setStatus(CoordinatorAction.Status.FAILED);
                slaStatus = Status.FAILED;
            }
            else if (workflow.getStatus() == WorkflowJob.Status.KILLED) {
                coordAction.setStatus(CoordinatorAction.Status.KILLED);
                slaStatus = Status.KILLED;
            }
            else {
                LOG.warn("Unexpected workflow " + workflow.getId() + " STATUS " + workflow.getStatus());
                // update lastModifiedTime
                coordAction.setLastModifiedTime(new Date());
                jpaService.execute(new org.apache.oozie.executor.jpa.CoordActionUpdateJPAExecutor(coordAction));

                return null;
            }

            LOG.info("Updating Coordintaor id :" + coordAction.getId() + "status to =" + coordAction.getStatus());
            coordAction.setLastModifiedTime(new Date());
            jpaService.execute(new org.apache.oozie.executor.jpa.CoordActionUpdateJPAExecutor(coordAction));
            if (slaStatus != null) {
                SLADbOperations.writeStausEvent(coordAction.getSlaXml(), coordAction.getId(), slaStatus,
                        SlaAppType.COORDINATOR_ACTION, LOG);
            }
            queue(new CoordActionReadyXCommand(coordAction.getJobId()));
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
    protected String getEntityKey() {
        return coordAction.getJobId();
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#isLockRequired()
     */
    @Override
    protected boolean isLockRequired() {
        return true;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#eagerLoadState()
     */
    @Override
    protected void eagerLoadState() throws CommandException {
        jpaService = Services.get().get(JPAService.class);
        if (jpaService == null) {
            throw new CommandException(ErrorCode.E0610);
        }

        try {
            coordAction = jpaService.execute(new CoordActionGetForExternalIdJPAExecutor(workflow.getId()));
        }
        catch (JPAExecutorException je) {
            throw new CommandException(je);
        }
        if (coordAction != null) {
            LogUtils.setLogInfo(coordAction, logInfo);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#eagerVerifyPrecondition()
     */
    @Override
    protected void eagerVerifyPrecondition() throws CommandException, PreconditionException {
        if (coordAction == null) {
            throw new PreconditionException(ErrorCode.E1100, ", coord action is null");
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#loadState()
     */
    @Override
    protected void loadState() throws CommandException {
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (workflow.getStatus() == WorkflowJob.Status.RUNNING || workflow.getStatus() == WorkflowJob.Status.SUSPENDED) {
            // update lastModifiedTime
            coordAction.setLastModifiedTime(new Date());
            try {
                jpaService.execute(new org.apache.oozie.executor.jpa.CoordActionUpdateJPAExecutor(coordAction));
            }
            catch (JPAExecutorException je) {
                throw new CommandException(je);
            }
            throw new PreconditionException(ErrorCode.E1100, ", workflow is RUNNING or SUSPENDED");
        }
    }

}
