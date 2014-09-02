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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.bundle.BundleStatusUpdateXCommand;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetActionByActionNumberJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.SLARegistrationQueryExecutor;
import org.apache.oozie.executor.jpa.SLARegistrationQueryExecutor.SLARegQuery;
import org.apache.oozie.executor.jpa.SLASummaryQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.executor.jpa.SLASummaryQueryExecutor.SLASummaryQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.BatchQueryExecutor.UpdateEntry;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.SLARegistrationBean;
import org.apache.oozie.sla.SLASummaryBean;
import org.apache.oozie.sla.service.SLAService;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.JobUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.StatusUtils;

public class CoordChangeXCommand extends CoordinatorXCommand<Void> {
    private final String jobId;
    private Date newEndTime = null;
    private Integer newConcurrency = null;
    private Date newPauseTime = null;
    private Date oldPauseTime = null;
    private boolean resetPauseTime = false;
    private CoordinatorJob.Status jobStatus = null;
    private CoordinatorJobBean coordJob;
    private JPAService jpaService = null;
    private Job.Status prevStatus;
    private List<UpdateEntry> updateList = new ArrayList<UpdateEntry>();
    private List<JsonBean> deleteList = new ArrayList<JsonBean>();

    private static final Set<String> ALLOWED_CHANGE_OPTIONS = new HashSet<String>();
    static {
        ALLOWED_CHANGE_OPTIONS.add("endtime");
        ALLOWED_CHANGE_OPTIONS.add("concurrency");
        ALLOWED_CHANGE_OPTIONS.add("pausetime");
        ALLOWED_CHANGE_OPTIONS.add(OozieClient.CHANGE_VALUE_STATUS);

    }

    /**
     * This command is used to update the Coordinator job with the new values Update the coordinator job bean and update
     * that to database.
     *
     * @param id Coordinator job id.
     * @param changeValue This the changed value in the form key=value.
     * @throws CommandException thrown if changeValue cannot be parsed properly.
     */
    public CoordChangeXCommand(String id, String changeValue) throws CommandException {
        super("coord_change", "coord_change", 0);
        this.jobId = ParamChecker.notEmpty(id, "id");
        ParamChecker.notEmpty(changeValue, "value");

        validateChangeValue(changeValue);
    }

    @Override
    protected void setLogInfo() {
        LogUtils.setLogInfo(jobId);
    }

    /**
     * @param changeValue change value.
     * @throws CommandException thrown if changeValue cannot be parsed properly.
     */
    private void validateChangeValue(String changeValue) throws CommandException {
        Map<String, String> map = JobUtils.parseChangeValue(changeValue);

        if (map.size() > ALLOWED_CHANGE_OPTIONS.size()) {
            throw new CommandException(ErrorCode.E1015, changeValue, "must change endtime|concurrency|pausetime|status");
        }

        java.util.Iterator<Entry<String, String>> iter = map.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<String, String> entry = iter.next();
            String key = entry.getKey();
            String value = entry.getValue();

            if (!ALLOWED_CHANGE_OPTIONS.contains(key)) {
                throw new CommandException(ErrorCode.E1015, changeValue, "must change endtime|concurrency|pausetime|status");
            }

            if (!key.equals(OozieClient.CHANGE_VALUE_PAUSETIME) && value.equalsIgnoreCase("")) {
                throw new CommandException(ErrorCode.E1015, changeValue, "value on " + key + " can not be empty");
            }
        }

        if (map.containsKey(OozieClient.CHANGE_VALUE_ENDTIME)) {
            String value = map.get(OozieClient.CHANGE_VALUE_ENDTIME);
            try {
                newEndTime = DateUtils.parseDateOozieTZ(value);
            }
            catch (Exception ex) {
                throw new CommandException(ErrorCode.E1015, value, "must be a valid date");
            }
        }

        if (map.containsKey(OozieClient.CHANGE_VALUE_CONCURRENCY)) {
            String value = map.get(OozieClient.CHANGE_VALUE_CONCURRENCY);
            try {
                newConcurrency = Integer.parseInt(value);
            }
            catch (NumberFormatException ex) {
                throw new CommandException(ErrorCode.E1015, value, "must be a valid integer");
            }
        }

        if (map.containsKey(OozieClient.CHANGE_VALUE_PAUSETIME)) {
            String value = map.get(OozieClient.CHANGE_VALUE_PAUSETIME);
            if (value.equals("")) { // this is to reset pause time to null;
                resetPauseTime = true;
            }
            else {
                try {
                    newPauseTime = DateUtils.parseDateOozieTZ(value);
                }
                catch (Exception ex) {
                    throw new CommandException(ErrorCode.E1015, value, "must be a valid date");
                }
            }
        }

        if (map.containsKey(OozieClient.CHANGE_VALUE_STATUS)) {
            String value = map.get(OozieClient.CHANGE_VALUE_STATUS);
            if (!StringUtils.isEmpty(value)) {
                jobStatus = CoordinatorJob.Status.valueOf(value);
            }
        }
    }

    /**
     * Check if new end time is valid.
     *
     * @param coordJob coordinator job id.
     * @param newEndTime new end time.
     * @throws CommandException thrown if new end time is not valid.
     */
    private void checkEndTime(CoordinatorJobBean coordJob, Date newEndTime) throws CommandException {
        //It's ok to set end date before start date.
    }

    /**
     * Check if new pause time is valid.
     *
     * @param coordJob coordinator job id.
     * @param newPauseTime new pause time.
     * @param newEndTime new end time, can be null meaning no change on end time.
     * @throws CommandException thrown if new pause time is not valid.
     */
    private void checkPauseTime(CoordinatorJobBean coordJob, Date newPauseTime)
            throws CommandException {
        //no check.
    }

    /**
     * Check if status change is valid.
     *
     * @param coordJob the coord job
     * @param jobStatus the job status
     * @throws CommandException the command exception
     */
    private void checkStatusChange(CoordinatorJobBean coordJob, CoordinatorJob.Status jobStatus)
            throws CommandException {
        if (!jobStatus.equals(CoordinatorJob.Status.RUNNING) && !jobStatus.equals(CoordinatorJob.Status.IGNORED)) {
            throw new CommandException(ErrorCode.E1015, jobStatus, " must be RUNNING or IGNORED");
        }

        if (jobStatus.equals(CoordinatorJob.Status.RUNNING)) {
            if (!(coordJob.getStatus().equals(CoordinatorJob.Status.FAILED) || coordJob.getStatus().equals(
                    CoordinatorJob.Status.KILLED) || coordJob.getStatus().equals(CoordinatorJob.Status.IGNORED))) {
                throw new CommandException(ErrorCode.E1015, jobStatus,
                        " Only FAILED, KILLED, IGNORED job can be changed to RUNNING. Current job status is "
                                + coordJob.getStatus());
            }
        }
        else {
            if (!(coordJob.getStatus().equals(CoordinatorJob.Status.FAILED) || coordJob.getStatus().equals(
                    CoordinatorJob.Status.KILLED))
                    || coordJob.isPending()) {
                throw new CommandException(ErrorCode.E1015, jobStatus,
                        " Only FAILED or KILLED non-pending job can be changed to IGNORED. Current job status is "
                                + coordJob.getStatus() + " and pending status is " + coordJob.isPending());
            }
        }
    }

    /**
     * Process lookahead created actions that become invalid because of the new pause time,
     * These actions will be deleted from DB, also the coordinator job will be updated accordingly
     *
     * @param coordJob coordinator job
     * @param newPauseTime new pause time
     * @throws JPAExecutorException, CommandException
     */
    private void processLookaheadActions(CoordinatorJobBean coordJob, Date newTime) throws CommandException,
            JPAExecutorException {
        int lastActionNumber = coordJob.getLastActionNumber();
        Date  lastActionTime = null;
        Date  tempDate = null;

        while ((tempDate = deleteAction(lastActionNumber, newTime)) != null) {
            lastActionNumber--;
            lastActionTime = tempDate;
        }
        if (lastActionTime != null) {
            LOG.debug("New pause/end date is : " + newTime + " and last action number is : " + lastActionNumber);
            coordJob.setLastActionNumber(lastActionNumber);
            coordJob.setLastActionTime(lastActionTime);
            coordJob.setNextMaterializedTime(lastActionTime);
            coordJob.resetDoneMaterialization();
        }
    }

    /**
     * Delete coordinator action
     *
     * @param actionNum coordinator action number
     */
    private Date deleteAction(int actionNum, Date afterDate) throws CommandException {
        try {
            if (actionNum <= 0) {
                return null;
            }

            String actionId = jpaService.execute(new CoordJobGetActionByActionNumberJPAExecutor(jobId, actionNum));
            CoordinatorActionBean bean = jpaService.execute(new CoordActionGetJPAExecutor(actionId));
            if (afterDate.compareTo(bean.getNominalTime()) <= 0) {
                // delete SLA registration entry (if any) for action
                if (SLAService.isEnabled()) {
                    Services.get().get(SLAService.class).removeRegistration(actionId);
                }
                SLARegistrationBean slaReg = SLARegistrationQueryExecutor.getInstance().get(SLARegQuery.GET_SLA_REG_ALL, actionId);
                if (slaReg != null) {
                    LOG.debug("Deleting registration bean corresponding to action " + slaReg.getId());
                    deleteList.add(slaReg);
                }
                SLASummaryBean slaSummaryBean = SLASummaryQueryExecutor.getInstance().get(
                        SLASummaryQuery.GET_SLA_SUMMARY, actionId);
                if (slaSummaryBean != null) {
                    LOG.debug("Deleting summary bean corresponding to action " + slaSummaryBean.getId());
                    deleteList.add(slaSummaryBean);
                }
                if (bean.getStatus() == CoordinatorAction.Status.WAITING
                        || bean.getStatus() == CoordinatorAction.Status.READY) {
                    deleteList.add(bean);
                }
                else {
                    throw new CommandException(ErrorCode.E1022, bean.getId());
                }
                return bean.getNominalTime();
            }
            else {
                return null;
            }

        }
        catch (JPAExecutorException e) {
            throw new CommandException(e);
        }
    }

    /**
     * Check if new end time, new concurrency, new pause time are valid.
     *
     * @param coordJob coordinator job id.
     * @param newEndTime new end time.
     * @param newConcurrency new concurrency.
     * @param newPauseTime new pause time.
     * @throws CommandException thrown if new values are not valid.
     */
    private void check(CoordinatorJobBean coordJob, Date newEndTime, Integer newConcurrency, Date newPauseTime,
            CoordinatorJob.Status jobStatus) throws CommandException {

        if (coordJob.getStatus() == CoordinatorJob.Status.KILLED
                || coordJob.getStatus() == CoordinatorJob.Status.IGNORED) {
            if (jobStatus == null || (newEndTime != null || newConcurrency != null || newPauseTime != null)) {
                throw new CommandException(ErrorCode.E1016);
            }
        }

        if (newEndTime != null) {
            checkEndTime(coordJob, newEndTime);
        }

        if (newPauseTime != null) {
            checkPauseTime(coordJob, newPauseTime);
        }
        if (jobStatus != null) {
            checkStatusChange(coordJob, jobStatus);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#execute()
     */
    @Override
    protected Void execute() throws CommandException {
        LOG.info("STARTED CoordChangeXCommand for jobId=" + jobId);

        try {
            if (newEndTime != null) {
                // during coord materialization, nextMaterializedTime is set to
                // startTime + n(actions materialized) * frequency and this can be AFTER endTime,
                // while doneMaterialization is true. Hence the following checks
                // for newEndTime being in the middle of endTime and nextMatdTime.
                // Since job is already done materialization so no need to change
                boolean dontChange = coordJob.getEndTime().before(newEndTime)
                        && coordJob.getNextMaterializedTime() != null
                        && coordJob.getNextMaterializedTime().after(newEndTime);
                if (!dontChange) {
                    coordJob.setEndTime(newEndTime);
                    // OOZIE-1703, we should SUCCEEDED the coord, if it's in PREP and new endtime is before start time
                    if (coordJob.getStartTime().compareTo(newEndTime) >= 0) {
                        if (coordJob.getStatus() != CoordinatorJob.Status.PREP) {
                            processLookaheadActions(coordJob, newEndTime);
                        }
                        if (coordJob.getStatus() == CoordinatorJob.Status.PREP
                                || coordJob.getStatus() == CoordinatorJob.Status.RUNNING) {
                            LOG.info("Changing coord status to SUCCEEDED, because it's in " + coordJob.getStatus()
                                    + " and new end time is before start time. Startime is " + coordJob.getStartTime()
                                    + " and new end time is " + newEndTime);

                            coordJob.setStatus(CoordinatorJob.Status.SUCCEEDED);
                            coordJob.resetPending();
                        }
                        coordJob.setDoneMaterialization();
                    }
                    else {
                        // move it to running iff new end time is after starttime.
                        if (coordJob.getStatus() == CoordinatorJob.Status.SUCCEEDED) {
                            coordJob.setStatus(CoordinatorJob.Status.RUNNING);
                        }
                        if (coordJob.getStatus() == CoordinatorJob.Status.DONEWITHERROR
                                || coordJob.getStatus() == CoordinatorJob.Status.FAILED) {
                            // Check for backward compatibility for Oozie versions (3.2 and before)
                            // when RUNNINGWITHERROR, SUSPENDEDWITHERROR and
                            // PAUSEDWITHERROR is not supported
                            coordJob.setStatus(StatusUtils
                                    .getStatusIfBackwardSupportTrue(CoordinatorJob.Status.RUNNINGWITHERROR));
                        }
                        coordJob.setPending();
                        coordJob.resetDoneMaterialization();
                        processLookaheadActions(coordJob, newEndTime);
                    }
                }

                else {
                    LOG.info("Didn't change endtime. Endtime is in between coord end time and next materialization time."
                            + "Coord endTime = " + DateUtils.formatDateOozieTZ(newEndTime)
                            + " next materialization time ="
                            + DateUtils.formatDateOozieTZ(coordJob.getNextMaterializedTime()));
                }
            }

            if (newConcurrency != null) {
                this.coordJob.setConcurrency(newConcurrency);
            }

            if (newPauseTime != null || resetPauseTime == true) {
                this.coordJob.setPauseTime(newPauseTime);
                if (oldPauseTime != null && newPauseTime != null) {
                    if (oldPauseTime.before(newPauseTime)) {
                        if (this.coordJob.getStatus() == Job.Status.PAUSED) {
                            this.coordJob.setStatus(Job.Status.RUNNING);
                        }
                        else if (this.coordJob.getStatus() == Job.Status.PAUSEDWITHERROR) {
                            this.coordJob.setStatus(Job.Status.RUNNINGWITHERROR);
                        }
                    }
                }
                else if (oldPauseTime != null && newPauseTime == null) {
                    if (this.coordJob.getStatus() == Job.Status.PAUSED) {
                        this.coordJob.setStatus(Job.Status.RUNNING);
                    }
                    else if (this.coordJob.getStatus() == Job.Status.PAUSEDWITHERROR) {
                        this.coordJob.setStatus(Job.Status.RUNNINGWITHERROR);
                    }
                }
                if (!resetPauseTime) {
                    processLookaheadActions(coordJob, newPauseTime);
                }
            }
            if (jobStatus != null) {
                coordJob.setStatus(jobStatus);
                LOG.info("Coord status is changed to " + jobStatus + " from " + prevStatus);
                if (jobStatus.equals(CoordinatorJob.Status.RUNNING)) {
                    coordJob.setPending();
                    if (coordJob.getNextMaterializedTime() != null
                            && coordJob.getEndTime().after(coordJob.getNextMaterializedTime())) {
                        coordJob.resetDoneMaterialization();
                    }
                } else if (jobStatus.equals(CoordinatorJob.Status.IGNORED)) {
                    coordJob.resetPending();
                    coordJob.setDoneMaterialization();
                }
            }

            if (coordJob.getNextMaterializedTime() != null && coordJob.getEndTime().compareTo(coordJob.getNextMaterializedTime()) <= 0) {
                LOG.info("[" + coordJob.getId() + "]: all actions have been materialized, job status = " + coordJob.getStatus()
                        + ", set pending to true");
                // set doneMaterialization to true when materialization is done
                coordJob.setDoneMaterialization();
            }

            coordJob.setLastModifiedTime(new Date());
            updateList.add(new UpdateEntry<CoordJobQuery>(CoordJobQuery.UPDATE_COORD_JOB_CHANGE, coordJob));
            BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(null, updateList, deleteList);

            return null;
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
        finally {
            LOG.info("ENDED CoordChangeXCommand for jobId=" + jobId);
            // update bundle action
            if (coordJob.getBundleId() != null) {
                //ignore pending as it'sync command
                BundleStatusUpdateXCommand bundleStatusUpdate = new BundleStatusUpdateXCommand(coordJob, prevStatus, true);
                bundleStatusUpdate.call();
            }
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    public String getEntityKey() {
        return this.jobId;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#loadState()
     */
    @Override
    protected void loadState() throws CommandException{
        jpaService = Services.get().get(JPAService.class);

        if (jpaService == null) {
            throw new CommandException(ErrorCode.E0610);
        }

        try {
            this.coordJob = jpaService.execute(new CoordJobGetJPAExecutor(jobId));
            oldPauseTime = coordJob.getPauseTime();
            prevStatus = coordJob.getStatus();
        }
        catch (JPAExecutorException e) {
            throw new CommandException(e);
        }

        LogUtils.setLogInfo(this.coordJob);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException,PreconditionException {
        check(this.coordJob, newEndTime, newConcurrency, newPauseTime, jobStatus);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#isLockRequired()
     */
    @Override
    protected boolean isLockRequired() {
        return true;
    }
}
