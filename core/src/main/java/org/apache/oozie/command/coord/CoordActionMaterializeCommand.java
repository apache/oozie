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
import org.apache.oozie.coord.TimeUnit;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.Services;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.util.db.SLADbOperations;
import org.jdom.Element;
import org.jdom.JDOMException;

public class CoordActionMaterializeCommand extends CoordinatorCommand<Void> {
    private String jobId;
    private Date startTime;
    private Date endTime;
    private int lastActionNumber = 1; // over-ride by DB value
    private final XLog log = XLog.getLog(getClass());
    private String user;
    private String group;
    /**
     * Default timeout for catchup jobs, in minutes, after which coordinator input check will timeout
     */
    public static final String CONF_DEFAULT_TIMEOUT_CATCHUP = Service.CONF_PREFIX + "coord.catchup.default.timeout";

    public CoordActionMaterializeCommand(String jobId, Date startTime, Date endTime) {
        super("coord_action_mater", "coord_action_mater", 1, XLog.STD);
        this.jobId = jobId;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    @Override
    protected Void call(CoordinatorStore store) throws StoreException, CommandException {
        // CoordinatorJobBean job = store.getCoordinatorJob(jobId, true);
        CoordinatorJobBean job = store.getEntityManager().find(CoordinatorJobBean.class, jobId);
        setLogInfo(job);
        if (job.getLastActionTime() != null && job.getLastActionTime().compareTo(endTime) >= 0) {
            log.info("ENDED Coordinator materialization for jobId = " + jobId
                    + " Action is *already* materialized for Materialization start time = " + startTime + " : Materialization end time = " + endTime + " Job status = " + job.getStatusStr());
            return null;
        }

        if (endTime.after(job.getEndTime())) {
            log.info("ENDED Coordinator materialization for jobId = " + jobId + " Materialization end time = " + endTime
                    + " surpasses coordinator job's end time = " + job.getEndTime() + " Job status = " + job.getStatusStr());
            return null;
        }

        if (job.getPauseTime() != null && !startTime.before(job.getPauseTime())) {
            log.info("ENDED Coordinator materialization for jobId = " + jobId + " Materialization start time = " + startTime
                    + " is after or equal to coordinator job's pause time = " + job.getPauseTime() + " Job status = " + job.getStatusStr());
            // pausetime blocks real materialization - we change job's status back to RUNNING;
            if (job.getStatus() == CoordinatorJob.Status.PREMATER) {
                job.setStatus(CoordinatorJob.Status.RUNNING);
            }
            store.updateCoordinatorJob(job);
            return null;
        }

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
                materializeJobs(false, job, jobConf, store);
                updateJobTable(job, store);
            }
            catch (CommandException ex) {
                log.warn("Exception occurs:" + ex + " Making the job failed ");
                job.setStatus(CoordinatorJobBean.Status.FAILED);
                store.updateCoordinatorJob(job);
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
     * @param dryrun
     * @param jobBean
     * @param conf
     * @param store
     * @throws Exception
     */
    protected String materializeJobs(boolean dryrun, CoordinatorJobBean jobBean, Configuration conf,
                                     CoordinatorStore store) throws Exception {
        String jobXml = jobBean.getJobXml();
        Element eJob = XmlUtils.parseXml(jobXml);
        // TODO: always UTC?
        TimeZone appTz = DateUtils.getTimeZone(jobBean.getTimeZone());
        // TimeZone appTz = DateUtils.getTimeZone("UTC");
        int frequency = jobBean.getFrequency();
        TimeUnit freqTU = TimeUnit.valueOf(eJob.getAttributeValue("freq_timeunit"));
        TimeUnit endOfFlag = TimeUnit.valueOf(eJob.getAttributeValue("end_of_duration"));
        Calendar start = Calendar.getInstance(appTz);
        start.setTime(startTime);
        DateUtils.moveToEnd(start, endOfFlag);
        Calendar end = Calendar.getInstance(appTz);
        end.setTime(endTime);
        lastActionNumber = jobBean.getLastActionNumber();
        // DateUtils.moveToEnd(end, endOfFlag);
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
            Date actualTime = new Date();
            action = CoordCommandUtils.materializeOneInstance(jobId, dryrun, (Element) eJob.clone(),
                    effStart.getTime(), actualTime, lastActionNumber, conf, actionBean);
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
                storeToDB(actionBean, action, store); // Storing to table
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

    /**
     * Store an Action into database table.
     *
     * @param actionBean
     * @param actionXml
     * @param store
     * @param wantSla
     * @throws StoreException
     * @throws JDOMException
     */
    private void storeToDB(CoordinatorActionBean actionBean, String actionXml, CoordinatorStore store) throws Exception {
        log.debug("In storeToDB() action Id " + actionBean.getId() + " Size of actionXml " + actionXml.length());
        actionBean.setActionXml(actionXml);
        store.insertCoordinatorAction(actionBean);
        writeActionRegistration(actionXml, actionBean, store);

        // TODO: time 100s should be configurable
        queueCallable(new CoordActionNotificationXCommand(actionBean), 100);
        queueCallable(new CoordActionInputCheckXCommand(actionBean.getId(), actionBean.getJobId()), 100);
    }

    /**
     * @param actionXml
     * @param actionBean
     * @param store
     * @throws Exception
     */
    private void writeActionRegistration(String actionXml, CoordinatorActionBean actionBean, CoordinatorStore store)
            throws Exception {
        Element eAction = XmlUtils.parseXml(actionXml);
        Element eSla = eAction.getChild("action", eAction.getNamespace()).getChild("info", eAction.getNamespace("sla"));
        SLADbOperations.writeSlaRegistrationEvent(eSla, store, actionBean.getId(), SlaAppType.COORDINATOR_ACTION, user,
                                                  group);
    }

    /**
     * @param job
     * @param store
     * @throws StoreException
     */
    private void updateJobTable(CoordinatorJobBean job, CoordinatorStore store) throws StoreException {
        // TODO: why do we need this? Isn't lastMatTime enough???
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
        store.updateCoordinatorJob(job);
    }

    @Override
    protected Void execute(CoordinatorStore store) throws StoreException, CommandException {
        log.info("STARTED CoordActionMaterializeCommand for jobId=" + jobId + ", startTime=" + startTime + ", endTime="
                + endTime);
        try {
            if (lock(jobId)) {
                call(store);
            }
            else {
                queueCallable(new CoordActionMaterializeCommand(jobId, startTime, endTime),
                        LOCK_FAILURE_REQUEUE_INTERVAL);
                log.warn("CoordActionMaterializeCommand lock was not acquired - failed jobId=" + jobId
                        + ". Requeing the same.");
            }
        }
        catch (InterruptedException e) {
            queueCallable(new CoordActionMaterializeCommand(jobId, startTime, endTime), LOCK_FAILURE_REQUEUE_INTERVAL);
            log.warn("CoordActionMaterializeCommand lock acquiring failed with exception " + e.getMessage()
                    + " for jobId=" + jobId + " Requeing the same.");
        }
        finally {
            log.info(" ENDED CoordActionMaterializeCommand for jobId=" + jobId + ", startTime=" + startTime
                    + ", endTime=" + endTime);
        }
        return null;
    }



    /**
     * For preliminery testing. Should be removed soon
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        new Services().init();
        try {
            Date startTime = DateUtils.parseDateUTC("2009-02-01T01:00Z");
            Date endTime = DateUtils.parseDateUTC("2009-02-02T01:00Z");
            String jobId = "0000000-091207151850551-oozie-dani-C";
            CoordActionMaterializeCommand matCmd = new CoordActionMaterializeCommand(jobId, startTime, endTime);
            matCmd.call();
        }
        finally {
            try {
                Thread.sleep(60000);
            }
            catch (Exception ex) {
            }
            new Services().destroy();
        }
    }

}
