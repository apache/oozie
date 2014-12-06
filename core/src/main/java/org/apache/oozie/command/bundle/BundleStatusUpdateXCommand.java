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
import org.apache.oozie.executor.jpa.BundleActionQueryExecutor;
import org.apache.oozie.executor.jpa.BundleActionQueryExecutor.BundleActionQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;

/**
 * The command to update Bundle status
 */
public class BundleStatusUpdateXCommand extends StatusUpdateXCommand {
    private final CoordinatorJobBean coordjob;
    private JPAService jpaService = null;
    private BundleActionBean bundleaction;
    private final Job.Status prevStatus;
    private final boolean ignorePending;

    /**
     * The constructor for class {@link BundleStatusUpdateXCommand}
     *
     * @param coordjob coordinator job bean
     * @param prevStatus coordinator job old status
     */
    public BundleStatusUpdateXCommand(CoordinatorJobBean coordjob, CoordinatorJob.Status prevStatus) {
        this(coordjob, prevStatus, false);
    }

    public BundleStatusUpdateXCommand(CoordinatorJobBean coordjob, CoordinatorJob.Status prevStatus, boolean ignorePending) {
        super("BundleStatusUpdate", "BundleStatusUpdate", 1);
        this.coordjob = coordjob;
        this.prevStatus = prevStatus;
        this.ignorePending = ignorePending;
    }

    @Override
    protected Void execute() throws CommandException {
        try {
            LOG.debug("STARTED BundleStatusUpdateXCommand with bundle id : " + coordjob.getBundleId()
                    + " coord job ID: " + coordjob.getId() + " coord Status " + coordjob.getStatus());
            Job.Status coordCurrentStatus = coordjob.getStatus();
            // The status of bundle action should not be updated if the bundle action is in terminal state
            // and coord Id is null. For e.g if Bundleaction is killed and coord Id is null, then the status of bundle
            // should not be changed.
            if (bundleaction.getCoordId() != null
                    || !bundleaction.isTerminalStatus()
                    || (bundleaction.getCoordId() != null && bundleaction.isTerminalStatus() && coordjob
                            .isTerminalStatus())) {
                bundleaction.setStatus(coordCurrentStatus);
            }
            if (bundleaction.isPending() && !ignorePending) {
                bundleaction.decrementAndGetPending();
            }
            // TODO - Uncomment this when bottom up rerun can change terminal state
            /*BundleJobBean bundleJob = jpaService.execute(new BundleJobGetJPAExecutor(bundleaction.getBundleId()));
            if (!bundleJob.isPending()) {
                bundleJob.setPending();
                jpaService.execute(new BundleJobUpdateJPAExecutor(bundleJob));
                LOG.info("Updated bundle job [{0}] pending to true", bundleaction.getBundleId());
            }*/

            bundleaction.setLastModifiedTime(new Date());
            bundleaction.setCoordId(coordjob.getId());
            BundleActionQueryExecutor.getInstance().executeUpdate(
                    BundleActionQuery.UPDATE_BUNDLE_ACTION_STATUS_PENDING_MODTIME_COORDID, bundleaction);
            if (bundleaction.getCoordId() != null) {
                LOG.info(
                        "Updated bundle action [{0}] from prev status [{1}] to current coord status [{2}], " +
                        "and new bundle action pending [{3}]",
                        bundleaction.getBundleActionId(), bundleaction.getStatus(), coordCurrentStatus,
                        bundleaction.getPending());
            }
            else {
                LOG.info("Updated Bundle action [{0}], status = [{1}], pending = [{2}]", bundleaction.getBundleActionId(),
                        bundleaction.getStatus(), bundleaction.getPending());
            }
            LOG.debug("ENDED BundleStatusUpdateXCommand with bundle id : " + coordjob.getBundleId() + " coord job ID: "
                    + coordjob.getId() + " coord Status " + coordjob.getStatus());
        }
        catch (Exception ex) {
            throw new CommandException(ErrorCode.E1309, bundleaction.getBundleId(), bundleaction.getCoordName());
        }
        return null;
    }

    @Override
    public String getEntityKey() {
        return coordjob.getBundleId();
    }

    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    protected void loadState() throws CommandException {
        try {
            if (jpaService == null) {
                jpaService = Services.get().get(JPAService.class);
            }

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

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (bundleaction.getStatusStr().compareToIgnoreCase(prevStatus.toString()) != 0 && bundleaction.getCoordId()!=null) {
            // pending should be decremented only if status of coord job and bundle action is same
            // e.g if bundle is killed and coord job is running, then pending should not be false
            // to allow recovery service to pick and kill the coord job
            if (bundleaction.isTerminalStatus() && coordjob.isTerminalStatus()) {
                LOG.info("Bundle action [{0}] status [{1}] is different from prev coord status [{2}], "
                        + "but coord job is currently in terminal state = [{3}]",
                        bundleaction.getBundleActionId(), bundleaction.getStatusStr(), prevStatus.toString(),
                        coordjob.getStatus());
                return;
            }
            if (bundleaction.isPending() && coordjob.getStatus().equals(bundleaction.getStatus()) && !ignorePending) {
                bundleaction.decrementAndGetPending();
            }
            bundleaction.setLastModifiedTime(new Date());
            try {
                BundleActionQueryExecutor.getInstance().executeUpdate(
                        BundleActionQuery.UPDATE_BUNDLE_ACTION_PENDING_MODTIME, bundleaction);
            }
            catch (JPAExecutorException je) {
                throw new CommandException(je);
            }
            LOG.info("Bundle action [{0}] status [{1}] is different from prev coord status [{2}] and current coord"
                    + " status [{3}], decrement pending so new pending = [{4}]", bundleaction.getBundleActionId(),
                    bundleaction.getStatusStr(), prevStatus.toString(), coordjob.getStatusStr(),
                    bundleaction.getPending());
            throw new PreconditionException(ErrorCode.E1308, bundleaction.getStatusStr(), prevStatus.toString());
        }
        else if (bundleaction.getStatusStr().compareToIgnoreCase(prevStatus.toString()) != 0) {
            LOG.info(
                    "Bundle action [{0}] status [{1}] is different from prev coord status [{2}], " +
                    "pending = [{3}] and bundle not yet updated with coord-id",
                    bundleaction.getBundleActionId(), bundleaction.getStatusStr(), prevStatus.toString(),
                    bundleaction.getPending());
        }
    }

}
