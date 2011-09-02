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
import java.util.List;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.SuspendTransitionXCommand;
import org.apache.oozie.command.bundle.BundleStatusUpdateXCommand;
import org.apache.oozie.command.wf.SuspendXCommand;
import org.apache.oozie.executor.jpa.CoordJobGetActionsJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.InstrumentUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;

public class CoordSuspendXCommand extends SuspendTransitionXCommand {
    private final String jobId;
    private final XLog LOG = XLog.getLog(getClass());
    private CoordinatorJobBean coordJob;
    private JPAService jpaService;
    private boolean exceptionOccured = false;
    CoordinatorJob.Status prevStatus;

    public CoordSuspendXCommand(String id) {
        super("coord_suspend", "coord_suspend", 1);
        this.jobId = ParamChecker.notEmpty(id, "id");
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    protected String getEntityKey() {
        return jobId;
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
        super.eagerLoadState();
        try {
            jpaService = Services.get().get(JPAService.class);
            if (jpaService != null) {
                this.coordJob = jpaService.execute(new CoordJobGetJPAExecutor(this.jobId));
                prevStatus = coordJob.getStatus();
            }
            else {
                throw new CommandException(ErrorCode.E0610);
            }
        }
        catch (Exception ex) {
            throw new CommandException(ErrorCode.E0603, ex);
        }
        LogUtils.setLogInfo(this.coordJob, logInfo);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        super.eagerVerifyPrecondition();
        if (coordJob.getStatus() == CoordinatorJob.Status.SUCCEEDED
                || coordJob.getStatus() == CoordinatorJob.Status.FAILED) {
            LOG.info("CoordSuspendCommand not suspended - " + "job finished or does not exist " + jobId);
            throw new PreconditionException(ErrorCode.E0728, coordJob.getStatus().toString());
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.SuspendTransitionXCommand#suspendChildren()
     */
    @Override
    public void suspendChildren() throws CommandException {
        try {
            List<CoordinatorActionBean> actionList = jpaService.execute(new CoordJobGetActionsJPAExecutor(jobId));
            for (CoordinatorActionBean action : actionList) {
                if (action.getStatus() == CoordinatorActionBean.Status.RUNNING) {
                    // queue a SuspendXCommand
                    if (action.getExternalId() != null) {
                        queue(new SuspendXCommand(action.getExternalId()));
                    }
                }
            }
        }
        catch (XException ex) {
            exceptionOccured = true;
            throw new CommandException(ex);
        }
        finally {
            if (exceptionOccured) {
                coordJob.setStatus(CoordinatorJob.Status.FAILED);
                try {
                    jpaService.execute(new CoordJobUpdateJPAExecutor(coordJob));
                }
                catch (JPAExecutorException ex) {
                    throw new CommandException(ex);
                }
            }

            //update bundle action
            if (this.coordJob.getBundleId() != null) {
                BundleStatusUpdateXCommand bundleStatusUpdate = new BundleStatusUpdateXCommand(coordJob, prevStatus);
                bundleStatusUpdate.call();
            }
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#notifyParent()
     */
    @Override
    public void notifyParent() throws CommandException {
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#updateJob()
     */
    @Override
    public void updateJob() throws CommandException {
        InstrumentUtils.incrJobCounter(getName(), 1, getInstrumentation());
        coordJob.setStatus(CoordinatorJob.Status.SUSPENDED);
        coordJob.setSuspendedTime(new Date());

        try {
            jpaService.execute(new CoordJobUpdateJPAExecutor(coordJob));
        }
        catch (JPAExecutorException e) {
            throw new CommandException(e);
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
