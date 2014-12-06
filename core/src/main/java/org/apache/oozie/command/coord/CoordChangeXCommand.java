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
import java.util.Calendar;
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
import org.apache.oozie.client.Job.Status;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.bundle.BundleStatusUpdateXCommand;
import org.apache.oozie.coord.TimeUnit;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetActionByActionNumberJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.sla.SLARegistrationGetJPAExecutor;
import org.apache.oozie.executor.jpa.sla.SLASummaryGetJPAExecutor;
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
     * Returns the actual last action time(one instance before coordJob.lastActionTime)
     * @return Date - last action time if coordJob.getLastActionTime() is not null, null otherwise
     */
    private Date getLastActionTime() {
        if(coordJob.getLastActionTime() == null)
            return null;

        Calendar d = Calendar.getInstance(DateUtils.getTimeZone(coordJob.getTimeZone()));
        d.setTime(coordJob.getLastActionTime());
        TimeUnit timeUnit = TimeUnit.valueOf(coordJob.getTimeUnitStr());
        d.add(timeUnit.getCalendarUnit(), -Integer.valueOf(coordJob.getFrequency()));
        return d.getTime();
    }

    /**
     * Check if new end time is valid.
     *
     * @param coordJob coordinator job id.
     * @param newEndTime new end time.
     * @throws CommandException thrown if new end time is not valid.
     */
    private void checkEndTime(CoordinatorJobBean coordJob, Date newEndTime) throws CommandException {
        // New endTime cannot be before coordinator job's start time.
        Date startTime = coordJob.getStartTime();
        if (newEndTime.before(startTime)) {
            throw new CommandException(ErrorCode.E1015, newEndTime, "cannot be before coordinator job's start time ["
                    + startTime + "]");
        }

        // New endTime cannot be before coordinator job's last action time.
        Date lastActionTime = getLastActionTime();
        if (lastActionTime != null) {
            if (!newEndTime.after(lastActionTime)) {
                throw new CommandException(ErrorCode.E1015, newEndTime,
                        "must be after coordinator job's last action time [" + lastActionTime + "]");
            }
        }
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
        // New pauseTime has to be a non-past time.
        Date d = new Date();
        if (newPauseTime.before(d)) {
            throw new CommandException(ErrorCode.E1015, newPauseTime, "must be a non-past time");
        }
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
        if (!jobStatus.equals(CoordinatorJob.Status.RUNNING)) {
            throw new CommandException(ErrorCode.E1015, jobStatus, " must be RUNNING");
        }

        if (!(coordJob.getStatus().equals(CoordinatorJob.Status.FAILED) || coordJob.getStatus().equals(
                CoordinatorJob.Status.KILLED))) {
            throw new CommandException(ErrorCode.E1015, jobStatus,
                    " Only FAILED or KILLED job can be changed to RUNNING. Current job status is "
                            + coordJob.getStatus());
        }
    }

    /**
     * Process lookahead created actions that become invalid because of the new pause time,
     * These actions will be deleted from DB, also the coordinator job will be updated accordingly
     *
     * @param coordJob coordinator job
     * @param newPauseTime new pause time
     */
    private void processLookaheadActions(CoordinatorJobBean coordJob, Date newPauseTime) throws CommandException {
        Date lastActionTime = coordJob.getLastActionTime();
        if (lastActionTime != null) {
            // d is the real last action time.
            Calendar d = Calendar.getInstance(DateUtils.getTimeZone(coordJob.getTimeZone()));
            d.setTime(getLastActionTime());
            TimeUnit timeUnit = TimeUnit.valueOf(coordJob.getTimeUnitStr());

            int lastActionNumber = coordJob.getLastActionNumber();

            boolean hasChanged = false;
            while (true) {
                if (!newPauseTime.after(d.getTime())) {
                    deleteAction(lastActionNumber);
                    d.add(timeUnit.getCalendarUnit(), -Integer.valueOf(coordJob.getFrequency()));
                    lastActionNumber = lastActionNumber - 1;

                    hasChanged = true;
                }
                else {
                    break;
                }
            }

            if (hasChanged == true) {
                coordJob.setLastActionNumber(lastActionNumber);
                d.add(timeUnit.getCalendarUnit(), Integer.valueOf(coordJob.getFrequency()));
                Date d1 = d.getTime();
                coordJob.setLastActionTime(d1);
                coordJob.setNextMaterializedTime(d1);
                coordJob.resetDoneMaterialization();
            }
        }
    }

    /**
     * Delete coordinator action
     *
     * @param actionNum coordinator action number
     */
    private void deleteAction(int actionNum) throws CommandException {
        try {
            String actionId = jpaService.execute(new CoordJobGetActionByActionNumberJPAExecutor(jobId, actionNum));
            CoordinatorActionBean bean = jpaService.execute(new CoordActionGetJPAExecutor(actionId));
            // delete SLA registration entry (if any) for action
            if (SLAService.isEnabled()) {
                Services.get().get(SLAService.class).removeRegistration(actionId);
            }
            SLARegistrationBean slaReg = jpaService.execute(new SLARegistrationGetJPAExecutor(actionId));
            if (slaReg != null) {
                LOG.debug("Deleting registration bean corresponding to action " + slaReg.getId());
                deleteList.add(slaReg);
            }
            SLASummaryBean slaSummaryBean = jpaService.execute(new SLASummaryGetJPAExecutor(actionId));
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

        if (coordJob.getStatus() == CoordinatorJob.Status.KILLED) {
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
                LOG.info("Coord status is changed to RUNNING from " + prevStatus);
                coordJob.setPending();
                if (jobStatus.equals(CoordinatorJob.Status.RUNNING)) {
                    if (coordJob.getNextMaterializedTime() != null
                            && coordJob.getEndTime().after(coordJob.getNextMaterializedTime())) {
                        coordJob.resetDoneMaterialization();
                    }
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
                BundleStatusUpdateXCommand bundleStatusUpdate = new BundleStatusUpdateXCommand(coordJob, prevStatus);
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

        LogUtils.setLogInfo(this.coordJob, logInfo);
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
