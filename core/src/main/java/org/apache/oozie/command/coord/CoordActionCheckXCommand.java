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

import java.sql.Timestamp;
import java.util.Date;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.XException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.InstrumentUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.db.SLADbOperations;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.SLAEvent.SlaAppType;
import org.apache.oozie.client.SLAEvent.Status;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.CoordActionGetForCheckJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;

/**
 * The command checks workflow status for coordinator action.
 */
public class CoordActionCheckXCommand extends CoordinatorXCommand<Void> {
    private String actionId;
    private int actionCheckDelay;
    private CoordinatorActionBean coordAction = null;
    private JPAService jpaService = null;

    public CoordActionCheckXCommand(String actionId, int actionCheckDelay) {
        super("coord_action_check", "coord_action_check", 0);
        this.actionId = ParamChecker.notEmpty(actionId, "actionId");
        this.actionCheckDelay = actionCheckDelay;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#execute()
     */
    @Override
    protected Void execute() throws CommandException {
        try {
            InstrumentUtils.incrJobCounter(getName(), 1, getInstrumentation());
            WorkflowJobBean wf = jpaService.execute(new WorkflowJobGetJPAExecutor(coordAction.getExternalId()));

            Status slaStatus = null;

            if (wf.getStatus() == WorkflowJob.Status.SUCCEEDED) {
                coordAction.setStatus(CoordinatorAction.Status.SUCCEEDED);
                // set pending to false as the status is SUCCEEDED
                coordAction.setPending(0);
                slaStatus = Status.SUCCEEDED;
            }
            else {
                if (wf.getStatus() == WorkflowJob.Status.FAILED) {
                    coordAction.setStatus(CoordinatorAction.Status.FAILED);
                    slaStatus = Status.FAILED;
                    // set pending to false as the status is FAILED
                    coordAction.setPending(0);
                }
                else {
                    if (wf.getStatus() == WorkflowJob.Status.KILLED) {
                        coordAction.setStatus(CoordinatorAction.Status.KILLED);
                        slaStatus = Status.KILLED;
                        // set pending to false as the status is KILLED
                        coordAction.setPending(0);
                    }
                    else {
                        LOG.warn("Unexpected workflow " + wf.getId() + " STATUS " + wf.getStatus());
                        coordAction.setLastModifiedTime(new Date());
                        jpaService.execute(new org.apache.oozie.executor.jpa.CoordActionUpdateStatusJPAExecutor(coordAction));
                        return null;
                    }
                }
            }

            LOG.debug("Updating Coordintaor actionId :" + coordAction.getId() + "status to ="
                            + coordAction.getStatus());
            coordAction.setLastModifiedTime(new Date());
            jpaService.execute(new org.apache.oozie.executor.jpa.CoordActionUpdateStatusJPAExecutor(coordAction));

            if (slaStatus != null) {
                SLADbOperations.writeStausEvent(coordAction.getSlaXml(), coordAction.getId(), slaStatus,
                        SlaAppType.COORDINATOR_ACTION, LOG);
            }
        }
        catch (XException ex) {
            LOG.warn("CoordActionCheckCommand Failed ", ex);
            throw new CommandException(ex);
        }
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    public String getEntityKey() {
        return actionId;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#isLockRequired()
     */
    @Override
    protected boolean isLockRequired() {
        return true;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#loadState()
     */
    @Override
    protected void loadState() throws CommandException {
        try {
            jpaService = Services.get().get(JPAService.class);

            if (jpaService != null) {
                coordAction = jpaService.execute(new CoordActionGetForCheckJPAExecutor(actionId));
                LogUtils.setLogInfo(coordAction, logInfo);
            }
            else {
                throw new CommandException(ErrorCode.E0610);
            }
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        // if the action has been updated, quit this command
        Timestamp actionCheckTs = new Timestamp(System.currentTimeMillis() - actionCheckDelay * 1000);
        Timestamp cactionLmt = coordAction.getLastModifiedTimestamp();
        if (cactionLmt.after(actionCheckTs)) {
            throw new PreconditionException(ErrorCode.E1100, "The coord action :" + actionId
                    + " has been udated. Ignore CoordActionCheckCommand!");
        }
        if (coordAction.getStatus().equals(CoordinatorAction.Status.SUCCEEDED)
                || coordAction.getStatus().equals(CoordinatorAction.Status.FAILED)
                || coordAction.getStatus().equals(CoordinatorAction.Status.KILLED)) {
            throw new PreconditionException(ErrorCode.E1100);
        }
    }
}
