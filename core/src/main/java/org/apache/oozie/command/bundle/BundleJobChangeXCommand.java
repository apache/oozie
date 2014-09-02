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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.XCommand;
import org.apache.oozie.command.coord.CoordChangeXCommand;
import org.apache.oozie.executor.jpa.BundleActionQueryExecutor;
import org.apache.oozie.executor.jpa.BundleActionQueryExecutor.BundleActionQuery;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.BatchQueryExecutor.UpdateEntry;
import org.apache.oozie.executor.jpa.BundleJobQueryExecutor;
import org.apache.oozie.executor.jpa.BundleJobQueryExecutor.BundleJobQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.JobUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.StatusUtils;

public class BundleJobChangeXCommand extends XCommand<Void> {
    private String jobId;
    private String changeValue;
    private List<BundleActionBean> bundleActions;
    private BundleJobBean bundleJob;
    private Date newPauseTime = null;
    private Date newEndTime = null;
    boolean isChangePauseTime = false;
    boolean isChangeEndTime = false;
    private List<UpdateEntry> updateList = new ArrayList<UpdateEntry>();

    private static final Set<String> ALLOWED_CHANGE_OPTIONS = new HashSet<String>();
    static {
        ALLOWED_CHANGE_OPTIONS.add("pausetime");
        ALLOWED_CHANGE_OPTIONS.add("endtime");
    }

    /**
     * @param id bundle job id
     * @param changeValue change value
     *
     * @throws CommandException thrown if failed to change bundle
     */
    public BundleJobChangeXCommand(String id, String changeValue) throws CommandException {
        super("bundle_change", "bundle_change", 1);
        this.jobId = ParamChecker.notEmpty(id, "id");
        this.changeValue = ParamChecker.notEmpty(changeValue, "changeValue");
    }

    /**
     * Check if new pause time is future time.
     *
     * @param newPauseTime new pause time.
     * @throws CommandException thrown if new pause time is not valid.
     */
    private void checkPauseTime(Date newPauseTime) throws CommandException {
        // New pauseTime has to be a non-past time.
        Date d = new Date();
        if (newPauseTime.before(d)) {
            throw new CommandException(ErrorCode.E1317, newPauseTime, "must be a non-past time");
        }
    }

    /**
     * Check if new pause time is future time.
     *
     * @param newEndTime new end time, can be null meaning no change on end time.
     * @throws CommandException thrown if new end time is not valid.
     */
    private void checkEndTime(Date newEndTime) throws CommandException {
        // New endTime has to be a non-past start time.
        Date startTime = bundleJob.getKickoffTime();
        if (startTime != null && newEndTime.before(startTime)) {
            throw new CommandException(ErrorCode.E1317, newEndTime, "must be greater then kickoff time");
        }
    }

    /**
     * validate if change value is valid.
     *
     * @param changeValue change value.
     * @throws CommandException thrown if changeValue cannot be parsed properly.
     */
    private void validateChangeValue(String changeValue) throws CommandException {
        Map<String, String> map = JobUtils.parseChangeValue(changeValue);

        if (map.size() > ALLOWED_CHANGE_OPTIONS.size()
                || !(map.containsKey(OozieClient.CHANGE_VALUE_PAUSETIME) || map
                        .containsKey(OozieClient.CHANGE_VALUE_ENDTIME))) {
            throw new CommandException(ErrorCode.E1317, changeValue, "can only change pausetime or end time");
        }

        if (map.containsKey(OozieClient.CHANGE_VALUE_PAUSETIME)) {
            isChangePauseTime = true;
        }
        else if(map.containsKey(OozieClient.CHANGE_VALUE_ENDTIME)){
            isChangeEndTime = true;
        }
        else {
            throw new CommandException(ErrorCode.E1317, changeValue, "should change pausetime or endtime");
        }

        if(isChangePauseTime){
            String value = map.get(OozieClient.CHANGE_VALUE_PAUSETIME);
            if (!value.equals(""))   {
                try {
                    newPauseTime = DateUtils.parseDateOozieTZ(value);
                }
                catch (Exception ex) {
                    throw new CommandException(ErrorCode.E1317, value, "is not a valid date");
                }

                checkPauseTime(newPauseTime);
            }
        }
        else if (isChangeEndTime){
            String value = map.get(OozieClient.CHANGE_VALUE_ENDTIME);
            if (!value.equals(""))   {
                try {
                    newEndTime = DateUtils.parseDateOozieTZ(value);
                }
                catch (Exception ex) {
                    throw new CommandException(ErrorCode.E1317, value, "is not a valid date");
                }

                checkEndTime(newEndTime);
            }
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#execute()
     */
    @Override
    protected Void execute() throws CommandException {
        StringBuffer changeReport = new StringBuffer();
        try {
            if (isChangePauseTime || isChangeEndTime) {
                if (isChangePauseTime) {
                    bundleJob.setPauseTime(newPauseTime);
                }
                else if (isChangeEndTime) {
                    bundleJob.setEndTime(newEndTime);
                    if (bundleJob.getStatus() == Job.Status.SUCCEEDED) {
                        bundleJob.setStatus(Job.Status.RUNNING);
                    }
                    if (bundleJob.getStatus() == Job.Status.DONEWITHERROR || bundleJob.getStatus() == Job.Status.FAILED) {
                        bundleJob.setStatus(StatusUtils.getStatusIfBackwardSupportTrue(Job.Status.RUNNINGWITHERROR));
                    }
                }
                for (BundleActionBean action : this.bundleActions) {
                    // queue coord change commands;
                    if (action.getStatus() != Job.Status.KILLED && action.getCoordId() != null) {
                        try {
                            new CoordChangeXCommand(action.getCoordId(), changeValue).call();
                        }
                        catch (Exception e) {
                            String errorMsg = action.getCoordId() + " : " + e.getMessage();
                            LOG.info("Change command failed " + errorMsg);
                            changeReport.append("[ ").append(errorMsg).append(" ]");
                        }
                    }
                    else {
                        String errorMsg = action.getCoordId() + " : Coord is in killed state";
                        LOG.info("Change command failed " + errorMsg);
                        changeReport.append("[ ").append(errorMsg).append(" ]");
                    }
                }
                updateList.add(new UpdateEntry<BundleJobQuery>(BundleJobQuery.UPDATE_BUNDLE_JOB_STATUS_PAUSE_ENDTIME,
                        bundleJob));
                BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(null, updateList, null);
            }
            if(!changeReport.toString().isEmpty()){
                throw new CommandException(ErrorCode.E1320, changeReport.toString());
            }
            return null;
        }
        catch (XException ex) {
            throw new CommandException(ex);
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
     * @see org.apache.oozie.command.XCommand#isLockRequired()
     */
    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    protected void loadState() throws CommandException {
        try {
            this.bundleJob = BundleJobQueryExecutor.getInstance().get(BundleJobQuery.GET_BUNDLE_JOB, bundleJob.getId());
            this.bundleActions = BundleActionQueryExecutor.getInstance().getList(
                    BundleActionQuery.GET_BUNDLE_ACTIONS_STATUS_UNIGNORED_FOR_BUNDLE, bundleJob.getId());
        }
        catch (JPAExecutorException Ex) {
            throw new CommandException(ErrorCode.E1311, this.jobId);
        }
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }

    @Override
    protected void eagerLoadState() throws CommandException {
        try {
            this.bundleJob = BundleJobQueryExecutor.getInstance().get(BundleJobQuery.GET_BUNDLE_JOB_STATUS, jobId);
            LogUtils.setLogInfo(bundleJob);
        }
        catch (JPAExecutorException ex) {
            throw new CommandException(ex);
        }
    }

    @Override
    protected void eagerVerifyPrecondition() throws CommandException, PreconditionException {
        validateChangeValue(changeValue);

        if (bundleJob == null) {
            LOG.info("BundleChangeCommand not succeeded - " + "job " + jobId + " does not exist");
            throw new PreconditionException(ErrorCode.E1314, jobId);
        }
        if (isChangePauseTime) {
            if (bundleJob.getStatus() == Job.Status.SUCCEEDED || bundleJob.getStatus() == Job.Status.FAILED
                    || bundleJob.getStatus() == Job.Status.KILLED || bundleJob.getStatus() == Job.Status.DONEWITHERROR) {
                LOG.info("BundleChangeCommand not succeeded for changing pausetime- " + "job " + jobId + " finished, status is "
                        + bundleJob.getStatusStr());
                throw new PreconditionException(ErrorCode.E1312, jobId, bundleJob.getStatus().toString());
            }
        }
        else if(isChangeEndTime){
            if (bundleJob.getStatus() == Job.Status.KILLED) {
                LOG.info("BundleChangeCommand not succeeded for changing endtime- " + "job " + jobId + " finished, status is "
                        + bundleJob.getStatusStr());
                throw new PreconditionException(ErrorCode.E1312, jobId, bundleJob.getStatus().toString());
            }
        }
    }
}
