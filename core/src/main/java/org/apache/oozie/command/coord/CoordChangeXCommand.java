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

import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.bundle.BundleStatusUpdateXCommand;
import org.apache.oozie.executor.jpa.CoordActionRemoveJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetActionByActionNumberJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.JobUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;

public class CoordChangeXCommand extends CoordinatorXCommand<Void> {
    private final String jobId;
    private Date newEndTime = null;
    private Integer newConcurrency = null;
    private Date newPauseTime = null;
    private Date oldPauseTime = null;
    private boolean resetPauseTime = false;
    private CoordinatorJobBean coordJob;
    private JPAService jpaService = null;
    private Job.Status prevStatus;

    private static final Set<String> ALLOWED_CHANGE_OPTIONS = new HashSet<String>();
    static {
        ALLOWED_CHANGE_OPTIONS.add("endtime");
        ALLOWED_CHANGE_OPTIONS.add("concurrency");
        ALLOWED_CHANGE_OPTIONS.add("pausetime");
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
            throw new CommandException(ErrorCode.E1015, changeValue, "must change endtime|concurrency|pausetime");
        }

        java.util.Iterator<Entry<String, String>> iter = map.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<String, String> entry = iter.next();
            String key = entry.getKey();
            String value = entry.getValue();

            if (!ALLOWED_CHANGE_OPTIONS.contains(key)) {
                throw new CommandException(ErrorCode.E1015, changeValue, "must change endtime|concurrency|pausetime");
            }

            if (!key.equals(OozieClient.CHANGE_VALUE_PAUSETIME) && value.equalsIgnoreCase("")) {
                throw new CommandException(ErrorCode.E1015, changeValue, "value on " + key + " can not be empty");
            }
        }

        if (map.containsKey(OozieClient.CHANGE_VALUE_ENDTIME)) {
            String value = map.get(OozieClient.CHANGE_VALUE_ENDTIME);
            try {
                newEndTime = DateUtils.parseDateUTC(value);
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
                    newPauseTime = DateUtils.parseDateUTC(value);
                }
                catch (Exception ex) {
                    throw new CommandException(ErrorCode.E1015, value, "must be a valid date");
                }
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
        // New endTime cannot be before coordinator job's start time.
        Date startTime = coordJob.getStartTime();
        if (newEndTime.before(startTime)) {
            throw new CommandException(ErrorCode.E1015, newEndTime, "cannot be before coordinator job's start time ["
                    + startTime + "]");
        }

        // New endTime cannot be before coordinator job's last action time.
        Date lastActionTime = coordJob.getLastActionTime();
        if (lastActionTime != null) {
            Date d = new Date(lastActionTime.getTime() - coordJob.getFrequency() * 60 * 1000);
            if (!newEndTime.after(d)) {
                throw new CommandException(ErrorCode.E1015, newEndTime,
                        "must be after coordinator job's last action time [" + d + "]");
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
            Date d = new Date(lastActionTime.getTime() - coordJob.getFrequency() * 60 * 1000);
            int lastActionNumber = coordJob.getLastActionNumber();

            boolean hasChanged = false;
            while (true) {
                if (!newPauseTime.after(d)) {
                    deleteAction(coordJob.getId(), lastActionNumber);
                    d = new Date(d.getTime() - coordJob.getFrequency() * 60 * 1000);
                    lastActionNumber = lastActionNumber - 1;

                    hasChanged = true;
                }
                else {
                    break;
                }
            }

            if (hasChanged == true) {
                coordJob.setLastActionNumber(lastActionNumber);
                Date d1 = new Date(d.getTime() + coordJob.getFrequency() * 60 * 1000);
                coordJob.setLastActionTime(d1);
                coordJob.setNextMaterializedTime(d1);
                coordJob.resetDoneMaterialization();
            }
        }
    }

    /**
     * Delete last action for a coordinator job
     *
     * @param coordJob coordinator job
     * @param lastActionNum last action number of the coordinator job
     */
    private void deleteAction(String jobId, int lastActionNum) throws CommandException {
        try {
            String coordActionId = jpaService.execute(new CoordJobGetActionByActionNumberJPAExecutor(jobId, lastActionNum));
            jpaService.execute(new CoordActionRemoveJPAExecutor(coordActionId));
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
    private void check(CoordinatorJobBean coordJob, Date newEndTime, Integer newConcurrency, Date newPauseTime)
            throws CommandException {
        if (coordJob.getStatus() == CoordinatorJob.Status.KILLED) {
            throw new CommandException(ErrorCode.E1016);
        }

        if (newEndTime != null) {
            checkEndTime(coordJob, newEndTime);
        }

        if (newPauseTime != null) {
            checkPauseTime(coordJob, newPauseTime);
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
                coordJob.setEndTime(newEndTime);
                if (coordJob.getStatus() == CoordinatorJob.Status.SUCCEEDED
                        || coordJob.getStatus() == CoordinatorJob.Status.RUNNING
                        || coordJob.getStatus() == CoordinatorJob.Status.DONEWITHERROR
                        || coordJob.getStatus() == CoordinatorJob.Status.FAILED) {
                    coordJob.setStatus(CoordinatorJob.Status.RUNNING);
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
                    if (oldPauseTime.before(newPauseTime) && this.coordJob.getStatus() == Job.Status.PAUSED) {
                        this.coordJob.setStatus(Job.Status.RUNNING);
                    }
                }
                else if (oldPauseTime != null && newPauseTime == null && this.coordJob.getStatus() == Job.Status.PAUSED) {
                    this.coordJob.setStatus(Job.Status.RUNNING);
                }

                if (!resetPauseTime) {
                    processLookaheadActions(coordJob, newPauseTime);
                }
            }

            jpaService.execute(new CoordJobUpdateJPAExecutor(this.coordJob));

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
        check(this.coordJob, newEndTime, newConcurrency, newPauseTime);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#isLockRequired()
     */
    @Override
    protected boolean isLockRequired() {
        return true;
    }
}
