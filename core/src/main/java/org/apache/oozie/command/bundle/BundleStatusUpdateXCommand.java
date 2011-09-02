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
package org.apache.oozie.command.bundle;

import java.util.Date;

import org.apache.oozie.BundleActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.StatusUpdateXCommand;
import org.apache.oozie.executor.jpa.BundleActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleActionUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XLog;

/**
 * The command to update Bundle status
 */
public class BundleStatusUpdateXCommand extends StatusUpdateXCommand {
    private final XLog LOG = XLog.getLog(BundleStatusUpdateXCommand.class);
    private final CoordinatorJobBean coordjob;
    private JPAService jpaService = null;
    private BundleActionBean bundleaction;
    private final Job.Status prevStatus;

    /**
     * The constructor for class {@link BundleStatusUpdateXCommand}
     *
     * @param coordjob coordinator job bean
     * @param prevStatus coordinator job old status
     */
    public BundleStatusUpdateXCommand(CoordinatorJobBean coordjob, CoordinatorJob.Status prevStatus) {
        super("BundleStatusUpdate", "BundleStatusUpdate", 1);
        this.coordjob = coordjob;
        this.prevStatus = convertCoordStatustoJob(prevStatus);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#getJob()
     */
    @Override
    public Job getJob() {
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#notifyParent()
     */
    @Override
    public void notifyParent() throws CommandException {
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#setJob(org.apache.oozie.client.Job)
     */
    @Override
    public void setJob(Job job) {
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#transitToNext()
     */
    @Override
    public void transitToNext() throws CommandException {
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#updateJob()
     */
    @Override
    public void updateJob() throws CommandException {
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#execute()
     */
    @Override
    protected Void execute() throws CommandException {
        try {
            LOG.debug("STARTED BundleStatusUpdateXCommand with bubdle id : " + coordjob.getBundleId()
                    + " coord job ID: " + coordjob.getId() + " coord Status " + coordjob.getStatus());
            Job.Status coordCurrentStatus = convertCoordStatustoJob(coordjob.getStatus());
            LOG.info("Update bundle action [{0}] from prev status [{1}] to current coord status [{2}]", bundleaction
                    .getBundleActionId(), bundleaction.getStatusStr(), coordCurrentStatus);
            bundleaction.setStatus(coordCurrentStatus);

            if (bundleaction.isPending()) {
                bundleaction.decrementAndGetPending();
            }
            bundleaction.setLastModifiedTime(new Date());
            bundleaction.setCoordId(coordjob.getId());
            jpaService.execute(new BundleActionUpdateJPAExecutor(bundleaction));
            LOG.debug("ENDED BundleStatusUpdateXCommand with bubdle id : " + coordjob.getBundleId() + " coord job ID: "
                    + coordjob.getId() + " coord Status " + coordjob.getStatus());
        }
        catch (Exception ex) {
            throw new CommandException(ErrorCode.E1309, bundleaction.getBundleId(), bundleaction.getCoordName());
        }
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    protected String getEntityKey() {
        return this.bundleaction.getBundleActionId();
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
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (bundleaction.getStatusStr().compareToIgnoreCase(prevStatus.toString()) != 0) {
            // Previous status are not matched with bundle action status
            // So that's the error and we should not be updating the Bundle Action status
            // however we need to decrement the pending flag.
            if (bundleaction.isPending()) {
                bundleaction.decrementAndGetPending();
            }
            try {
                jpaService.execute(new BundleActionUpdateJPAExecutor(bundleaction));
            }
            catch (JPAExecutorException je) {
                throw new CommandException(je);
            }
            throw new PreconditionException(ErrorCode.E1308, bundleaction.getStatusStr(), prevStatus.toString());
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#eagerLoadState()
     */
    @Override
    protected void eagerLoadState() throws CommandException {
        try {
            super.eagerLoadState();
            jpaService = Services.get().get(JPAService.class);

            if (jpaService != null) {
                this.bundleaction = jpaService.execute(new BundleActionGetJPAExecutor(coordjob.getBundleId(), coordjob
                        .getAppName()));
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
     * @see org.apache.oozie.command.XCommand#eagerVerifyPrecondition()
     */
    @Override
    protected void eagerVerifyPrecondition() throws CommandException, PreconditionException {
    }

    /**
     * Convert coordinator job status to job status.
     *
     * @param coordStatus coordinator job status
     * @return job status
     */
    public static Job.Status convertCoordStatustoJob(CoordinatorJob.Status coordStatus) {
        if (coordStatus == CoordinatorJob.Status.PREMATER) {
            coordStatus = CoordinatorJob.Status.RUNNING;
        }

        for (Job.Status js : Job.Status.values()) {
            if (coordStatus.toString().compareToIgnoreCase(js.toString()) == 0) {
                return js;
            }
        }
        return null;
    }

}
