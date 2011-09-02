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

import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.command.bundle.BundleStatusUpdateXCommand;
import org.apache.oozie.command.wf.KillXCommand;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.KillTransitionXCommand;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.CoordActionUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetActionsJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;

import java.util.Date;
import java.util.List;

public class CoordKillXCommand extends KillTransitionXCommand {

    private String jobId;
    private final XLog LOG = XLog.getLog(CoordKillXCommand.class);
    private CoordinatorJobBean coordJob;
    private List<CoordinatorActionBean> actionList;
    private JPAService jpaService = null;
    private CoordinatorJob.Status prevStatus = null;

    public CoordKillXCommand(String id) {
        super("coord_kill", "coord_kill", 1);
        this.jobId = ParamChecker.notEmpty(id, "id");
    }

    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    protected String getEntityKey() {
        return this.jobId;
    }

    @Override
    protected void loadState() throws CommandException {
        try {
            jpaService = Services.get().get(JPAService.class);

            if (jpaService != null) {
                this.coordJob = jpaService.execute(new CoordJobGetJPAExecutor(jobId));
                this.actionList = jpaService.execute(new CoordJobGetActionsJPAExecutor(jobId));
                prevStatus = coordJob.getStatus();
                LogUtils.setLogInfo(coordJob, logInfo);
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
    }

    private void updateCoordAction(CoordinatorActionBean action) throws CommandException {
        action.setStatus(CoordinatorActionBean.Status.KILLED);
        action.incrementAndGetPending();
        action.setLastModifiedTime(new Date());
        try {
            jpaService.execute(new CoordActionUpdateJPAExecutor(action));
        }
        catch (JPAExecutorException e) {
            throw new CommandException(e);
        }
    }

    @Override
    public void killChildren() throws CommandException {
        try {
            if (actionList != null) {
                for (CoordinatorActionBean action : actionList) {
                    if (action.getStatus() != CoordinatorActionBean.Status.FAILED
                            && action.getStatus() != CoordinatorActionBean.Status.TIMEDOUT
                            && action.getStatus() != CoordinatorActionBean.Status.SUCCEEDED
                            && action.getStatus() != CoordinatorActionBean.Status.KILLED) {
                        // queue a WorkflowKillXCommand to delete the workflow job and actions
                        if (action.getExternalId() != null) {
                            queue(new KillXCommand(action.getExternalId()));
                            updateCoordAction(action);
                            LOG.debug("Killed coord action = [{0}], new status = [{1}], pending = [{2}] and queue KillXCommand for [{3}]",
                                            action.getId(), action.getStatus(), action.getPending(), action.getExternalId());
                        }
                        else {
                            updateCoordAction(action);
                            LOG.debug("Killed coord action = [{0}], current status = [{1}], pending = [{2}]", action.getId(), action
                                    .getStatus(), action.getPending());
                        }
                    }
                }
            }
            jpaService.execute(new CoordJobUpdateJPAExecutor(coordJob));

            LOG.debug("Killed coord actions for the coordinator=[{0}]", jobId);
        }
        catch (JPAExecutorException ex) {
            throw new CommandException(ex);
        }
    }

    @Override
    public void notifyParent() throws CommandException {
        // update bundle action
        if (coordJob.getBundleId() != null) {
            BundleStatusUpdateXCommand bundleStatusUpdate = new BundleStatusUpdateXCommand(coordJob, prevStatus);
            bundleStatusUpdate.call();
        }
    }

    @Override
    public void updateJob() throws CommandException {
        try {
            coordJob.setEndTime(new Date());
            jpaService.execute(new CoordJobUpdateJPAExecutor(coordJob));
        }
        catch (JPAExecutorException ex) {
            throw new CommandException(ex);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#getJob()
     */
    @Override
    public Job getJob() {
        return coordJob;
    }

}
