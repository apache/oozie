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

import org.apache.oozie.util.DateUtils;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.CoordActionRemoveJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetActionByActionNumberJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.util.JobUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;

import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

public class CoordChangeCommand extends CoordinatorCommand<Void> {
    private String jobId;
    private Date newEndTime = null;
    private Integer newConcurrency = null;
    private Date newPauseTime = null;
    private boolean resetPauseTime = false;
    private static final XLog LOG = XLog.getLog(CoordChangeCommand.class);    
    
    private static final Set<String> ALLOWED_CHANGE_OPTIONS = new HashSet<String>();
    static {
        ALLOWED_CHANGE_OPTIONS.add("endtime");
        ALLOWED_CHANGE_OPTIONS.add("concurrency");
        ALLOWED_CHANGE_OPTIONS.add("pausetime");
    }

    public CoordChangeCommand(String id, String changeValue) throws CommandException {
        super("coord_change", "coord_change", 0, XLog.STD);
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
     * @param coordJob coordinator job id.
     * @param newEndTime new end time.
     * @throws CommandException thrown if new end time is not valid.
     */
    private void checkEndTime(CoordinatorJobBean coordJob, Date newEndTime) throws CommandException {
        // New endTime cannot be before coordinator job's start time.
        Date startTime = coordJob.getStartTime();
        if (newEndTime.before(startTime)) {
            throw new CommandException(ErrorCode.E1015, newEndTime, "cannot be before coordinator job's start time [" + startTime + "]");
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
                
                if (coordJob.getStatus() == CoordinatorJob.Status.SUCCEEDED) {
                    coordJob.setStatus(CoordinatorJob.Status.RUNNING);
                }
            }
        }
    }

    /**
     * delete last action for a coordinator job
     * @param coordJob coordinator job
     * @param lastActionNum last action number of the coordinator job
     */
    private void deleteAction(String jobId, int lastActionNum) throws CommandException {
        JPAService jpaService = Services.get().get(JPAService.class);
        if (jpaService == null) {
            throw new CommandException(ErrorCode.E0610);
        }
        
        try {
            CoordinatorActionBean actionBean = jpaService.execute(new CoordJobGetActionByActionNumberJPAExecutor(jobId, lastActionNum));
            jpaService.execute(new CoordActionRemoveJPAExecutor(actionBean.getId()));
        }
        catch (JPAExecutorException e) {
            throw new CommandException(e);
        }
    }

    /**
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

    @Override
    protected Void call(CoordinatorStore store) throws StoreException, CommandException {
        try {
            CoordinatorJobBean coordJob = store.getCoordinatorJob(jobId, false);
            setLogInfo(coordJob);

            check(coordJob, newEndTime, newConcurrency, newPauseTime);

            if (newEndTime != null) {
                coordJob.setEndTime(newEndTime);
                if (coordJob.getStatus() == CoordinatorJob.Status.SUCCEEDED) {
                    coordJob.setStatus(CoordinatorJob.Status.RUNNING);
                }
            }

            if (newConcurrency != null) {
                coordJob.setConcurrency(newConcurrency);
            }

            if (newPauseTime != null || resetPauseTime == true) {
                coordJob.setPauseTime(newPauseTime);
                if (!resetPauseTime) {
                    processLookaheadActions(coordJob, newPauseTime);
                }
            }

            store.updateCoordinatorJob(coordJob);

            return null;
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

    @Override
    protected Void execute(CoordinatorStore store) throws StoreException, CommandException {
        LOG.info("STARTED CoordChangeCommand for jobId=" + jobId);
        try {
            if (lock(jobId)) {
                call(store);
            }
            else {
                throw new CommandException(ErrorCode.E0606, "job " + jobId
                        + " has been locked and cannot change value, please retry later");
            }
        }
        catch (InterruptedException e) {
            throw new CommandException(ErrorCode.E0606, "acquiring lock for job " + jobId + " failed "
                    + " with exception " + e.getMessage());
        }
        finally {
            LOG.info("ENDED CoordChangeCommand for jobId=" + jobId);
        }
        return null;
    }
}
