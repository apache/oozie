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

import java.io.IOException;
import java.io.StringReader;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.SLAEvent.SlaAppType;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.CoordActionInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.coord.TimeUnit;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.Services;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.util.db.SLADbOperations;
import org.jdom.Element;
import org.jdom.JDOMException;

public class CoordActionMaterializeXCommand extends CoordinatorXCommand<Void> {
    private String jobId;
    private Date startTime;
    private Date endTime;
    private int lastActionNumber = 1; // over-ride by DB value
    private final static XLog log = XLog.getLog(CoordActionMaterializeXCommand.class);
    private String user;
    private String group;
    private JPAService jpaService = null;
    CoordinatorJobBean job = null;
    
    /**
     * Default timeout for catchup jobs, in minutes, after which coordinator input check will timeout
     */
    public static final String CONF_DEFAULT_TIMEOUT_CATCHUP = Service.CONF_PREFIX + "coord.catchup.default.timeout";

    public CoordActionMaterializeXCommand(String jobId, Date startTime, Date endTime) {
        super("coord_action_mater", "coord_action_mater", 1);
        this.jobId = ParamChecker.notEmpty(jobId, "jobId");
        this.startTime = startTime;
        this.endTime = endTime;
    }

    @Override
    protected Void execute() throws CommandException {

        this.user = job.getUser();
        this.group = job.getGroup();
        if (job.getStatus().equals(CoordinatorJobBean.Status.PREMATER)) {
            Configuration jobConf = null;
            log.debug("start job :" + jobId + " Materialization ");
            try {
                jobConf = new XConfiguration(new StringReader(job.getConf()));
            }
            catch (IOException ioe) {
                log.warn("Configuration parse error. read from DB :" + job.getConf(), ioe);
                throw new CommandException(ErrorCode.E1005, ioe);
            }

            Instrumentation.Cron cron = new Instrumentation.Cron();
            cron.start();
            try {
                materializeJobs(false, job, jobConf, log);
                updateJobTable(job);
            }
            catch (CommandException ex) {
                log.warn("Exception occurs:" + ex + " Making the job failed ");
                job.setStatus(CoordinatorJobBean.Status.FAILED);
                try {
                    jpaService.execute(new CoordJobUpdateJPAExecutor(job));
                }
                catch (JPAExecutorException jex) {
                    throw new CommandException(jex);
                }
            }
            catch (Exception e) {
                log.error("Excepion thrown :", e);
                throw new CommandException(ErrorCode.E1001, e.getMessage(), e);
            }
            cron.stop();
        }
        else {
            log.info("WARN: action is not in PREMATER state!  It's in state=" + job.getStatus());
        }
        return null;
    }

    /**
     * Create action instances starting from "start-time" to end-time" and store them into Action table.
     *
     * @param dryrun if this is a dry run
     * @param jobBean coordinator job bean
     * @param conf configuration object
     * @throws Exception
     */
    protected String materializeJobs(boolean dryrun, CoordinatorJobBean jobBean, Configuration conf,
                                     XLog log) throws Exception {
        String jobXml = jobBean.getJobXml();
        Element eJob = XmlUtils.parseXml(jobXml);
        TimeZone appTz = DateUtils.getTimeZone(jobBean.getTimeZone());
        int frequency = jobBean.getFrequency();
        TimeUnit freqTU = TimeUnit.valueOf(eJob.getAttributeValue("freq_timeunit"));
        TimeUnit endOfFlag = TimeUnit.valueOf(eJob.getAttributeValue("end_of_duration"));
        Calendar start = Calendar.getInstance(appTz);
        start.setTime(startTime);
        DateUtils.moveToEnd(start, endOfFlag);
        Calendar end = Calendar.getInstance(appTz);
        end.setTime(endTime);
        lastActionNumber = jobBean.getLastActionNumber();
        log.info("   *** materialize Actions for tz=" + appTz.getDisplayName() + ",\n start=" + start.getTime()
                + ", end=" + end.getTime() + "\n TimeUNIT " + freqTU.getCalendarUnit() + " Frequency :" + frequency
                + ":" + freqTU + " lastActionNumber " + lastActionNumber);
        // Keep the actual start time
        Calendar origStart = Calendar.getInstance(appTz);
        origStart.setTime(jobBean.getStartTimestamp());
        // Move to the End of duration, if needed.
        DateUtils.moveToEnd(origStart, endOfFlag);
        // Cloning the start time to be used in loop iteration
        Calendar effStart = (Calendar) origStart.clone();
        // Move the time when the previous action finished
        effStart.add(freqTU.getCalendarUnit(), lastActionNumber * frequency);

        String action = null;
        StringBuilder actionStrings = new StringBuilder();
        Date jobPauseTime = jobBean.getPauseTime();
        Calendar pause = null;
        if (jobPauseTime != null) {
            pause = Calendar.getInstance(appTz);
            pause.setTime(DateUtils.convertDateToTimestamp(jobPauseTime));
        }

        while (effStart.compareTo(end) < 0) {
            if (pause != null && effStart.compareTo(pause) >= 0) {
                break;
            }
            CoordinatorActionBean actionBean = new CoordinatorActionBean();
            lastActionNumber++;

            int timeout = jobBean.getTimeout();
            log.debug(origStart.getTime() + " Materializing action for time=" + effStart.getTime()
                    + ", lastactionnumber=" + lastActionNumber);
            action = CoordCommandUtils.materializeOneInstance(jobId, dryrun, (Element) eJob.clone(),
                    effStart.getTime(), lastActionNumber, conf, actionBean);
            int catchUpTOMultiplier = 1; // This value might be could be changed in future
            if (actionBean.getNominalTimestamp().before(jobBean.getCreatedTimestamp())) {
                // Catchup action
                timeout = catchUpTOMultiplier * timeout;
                // actionBean.setTimeOut(Services.get().getConf().getInt(CONF_DEFAULT_TIMEOUT_CATCHUP,
                // -1));
                log.info("Catchup timeout is :" + actionBean.getTimeOut());
            }
            actionBean.setTimeOut(timeout);
            if (!dryrun) {
                storeToDB(actionBean, action); // Storing to table
            }
            else {
                actionStrings.append("action for new instance");
                actionStrings.append(action);
            }
            // Restore the original start time
            effStart = (Calendar) origStart.clone();
            effStart.add(freqTU.getCalendarUnit(), lastActionNumber * frequency);
        }

        endTime = new Date(effStart.getTimeInMillis());
        if (!dryrun) {
            return action;
        }
        else {
            return actionStrings.toString();
        }
    }

    private void storeToDB(CoordinatorActionBean actionBean, String actionXml) throws Exception {
        log.debug("In storeToDB() action Id " + actionBean.getId() + " Size of actionXml " + actionXml.length());
        actionBean.setActionXml(actionXml);

        jpaService.execute(new CoordActionInsertJPAExecutor(actionBean));
        writeActionRegistration(actionXml, actionBean);

        // TODO: time 100s should be configurable
        queue(new CoordActionNotificationXCommand(actionBean), 100);
        queue(new CoordActionInputCheckXCommand(actionBean.getId()), 100);
    }

    private void writeActionRegistration(String actionXml, CoordinatorActionBean actionBean)
            throws Exception {
        Element eAction = XmlUtils.parseXml(actionXml);
        Element eSla = eAction.getChild("action", eAction.getNamespace()).getChild("info", eAction.getNamespace("sla"));
        SLADbOperations.writeSlaRegistrationEvent(eSla, actionBean.getId(), SlaAppType.COORDINATOR_ACTION, user, group, log);
    }

    private void updateJobTable(CoordinatorJobBean job) throws CommandException {
        job.setLastActionTime(endTime);
        job.setLastActionNumber(lastActionNumber);
        // if the job endtime == action endtime, then set status of job to
        // succeeded
        // we dont need to materialize this job anymore
        Date jobEndTime = job.getEndTime();
        if (jobEndTime.compareTo(endTime) <= 0) {
            job.setStatus(CoordinatorJob.Status.SUCCEEDED);
            log.info("[" + job.getId() + "]: Update status from PREMATER to SUCCEEDED");
        }
        else {
            job.setStatus(CoordinatorJob.Status.RUNNING);
            log.info("[" + job.getId() + "]: Update status from PREMATER to RUNNING");
        }
        job.setNextMaterializedTime(endTime);
        try {
            jpaService.execute(new CoordJobUpdateJPAExecutor(job));
        }
        catch (JPAExecutorException ex) {
            throw new CommandException(ex);
        }
    }

    @Override
    protected String getEntityKey() {
        return jobId;
    }

    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    protected void loadState() throws CommandException {
        jpaService = Services.get().get(JPAService.class);
        if (jpaService == null) {
            log.error(ErrorCode.E0610);
        }
        
        try {
            job = jpaService.execute(new CoordJobGetJPAExecutor(jobId));
        }
        catch (JPAExecutorException jex) {
            throw new CommandException(jex);
        }
        LogUtils.setLogInfo(job, logInfo);
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (job.getLastActionTime() != null && job.getLastActionTime().compareTo(endTime) >= 0) {
            throw new PreconditionException(ErrorCode.E1100, "ENDED Coordinator materialization for jobId = " + jobId
                    + " Action is *already* materialized for Materialization start time = " + startTime + " : Materialization end time = " + endTime + " Job status = " + job.getStatusStr());
        }

        if (endTime.after(job.getEndTime())) {
            throw new PreconditionException(ErrorCode.E1100, "ENDED Coordinator materialization for jobId = " + jobId + " Materialization end time = " + endTime
                    + " surpasses coordinator job's end time = " + job.getEndTime() + " Job status = " + job.getStatusStr());
        }

        if (job.getPauseTime() != null && !startTime.before(job.getPauseTime())) {
            // pausetime blocks real materialization - we change job's status back to RUNNING;
            if (job.getStatus() == CoordinatorJob.Status.PREMATER) {
                job.setStatus(CoordinatorJob.Status.RUNNING);
            }
            
            job.setLastModifiedTime(new Date());
            
            try {
                jpaService.execute(new CoordJobUpdateJPAExecutor(job));
            }
            catch (JPAExecutorException ex) {
                throw new CommandException(ex);
            }

            throw new PreconditionException(ErrorCode.E1100, "ENDED Coordinator materialization for jobId = " + jobId + " Materialization start time = " + startTime
                    + " is after or equal to coordinator job's pause time = " + job.getPauseTime() + " Job status = " + job.getStatusStr());
        }
    }

}
