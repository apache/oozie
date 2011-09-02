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
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class CoordChangeCommand extends CoordinatorCommand<Void> {
    private String jobId;
    private Date newEndTime = null;
    private Integer newConcurrency = null;
    private Date newPauseTime = null;
    private boolean resetPauseTime = false;
    private final XLog log = XLog.getLog(getClass());

    public CoordChangeCommand(String id, String changeValue) throws CommandException {
        super("coord_change", "coord_change", 0, XLog.STD);
        this.jobId = ParamChecker.notEmpty(id, "id");
        ParamChecker.notEmpty(changeValue, "value");

        parseChangeValue(changeValue);
    }

    /**
     * @param changeValue change value.
     * @throws CommandException thrown if changeValue cannot be parsed properly.
     */
    private void parseChangeValue(String changeValue) throws CommandException {
        Map<String, String> map = new HashMap<String, String>();
        String[] tokens = changeValue.split(";");
        int size = tokens.length;

        if (size < 0 || size > 3) {
            throw new CommandException(ErrorCode.E1015, changeValue, "must change endtime|concurrency|pausetime");
        }

        for (String token : tokens) {
            String[] pair = token.split("=");
            String key = pair[0];

            if (!key.equals(OozieClient.CHANGE_VALUE_ENDTIME) && !key.equals(OozieClient.CHANGE_VALUE_CONCURRENCY)
                    && !key.equals(OozieClient.CHANGE_VALUE_PAUSETIME)) {
                throw new CommandException(ErrorCode.E1015, changeValue, "must change endtime|concurrency|pausetime");
            }

            if (!key.equals(OozieClient.CHANGE_VALUE_PAUSETIME) && pair.length != 2) {
                throw new CommandException(ErrorCode.E1015, changeValue, "elements on " + key + " must be name=value pair");
            }

            if (key.equals(OozieClient.CHANGE_VALUE_PAUSETIME) && pair.length != 2 && pair.length != 1) {
                throw new CommandException(ErrorCode.E1015, changeValue, "elements on " + key + " must be name=value pair or name=(empty string to reset pause time to null)");
            }

            if (map.containsKey(key)) {
                throw new CommandException(ErrorCode.E1015, changeValue, "can not specify repeated change values on "
                        + key);
            }

            if (pair.length == 2) {
                map.put(key, pair[1]);
            }
            else {
                map.put(key, "");
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
     * @param newEndTime new end time, can be null meaning no change on end
     *        time.
     * @throws CommandException thrown if new pause time is not valid.
     */
    private void checkPauseTime(CoordinatorJobBean coordJob, Date newPauseTime, Date newEndTime)
            throws CommandException {
        // New pauseTime cannot be before coordinator job's start time.
        Date startTime = coordJob.getStartTime();
        if (newPauseTime.before(startTime)) {
            throw new CommandException(ErrorCode.E1015, newPauseTime, "cannot be before coordinator job's start time ["
                    + startTime + "]");
        }

        // New pauseTime cannot be before coordinator job's last action time.
        Date lastActionTime = coordJob.getLastActionTime();
        if (lastActionTime != null) {
            Date d = new Date(lastActionTime.getTime() - coordJob.getFrequency() * 60 * 1000);
            if (!newPauseTime.after(d)) {
                throw new CommandException(ErrorCode.E1015, newPauseTime,
                        "must be after coordinator job's last action time [" + d + "]");
            }
        }

        // New pauseTime must be before coordinator job's end time.
        Date endTime = (newEndTime != null) ? newEndTime : coordJob.getEndTime();
        if (!newPauseTime.before(endTime)) {
            throw new CommandException(ErrorCode.E1015, newPauseTime, "must be before coordinator job's end time ["
                    + endTime + "]");
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
            checkPauseTime(coordJob, newPauseTime, newEndTime);
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
            }

            incrJobCounter(1);
            store.updateCoordinatorJob(coordJob);

            return null;
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

    @Override
    protected Void execute(CoordinatorStore store) throws StoreException, CommandException {
        log.info("STARTED CoordChangeCommand for jobId=" + jobId);
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
        return null;
    }
}
