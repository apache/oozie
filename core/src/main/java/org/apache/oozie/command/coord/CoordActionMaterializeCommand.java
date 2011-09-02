/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.List;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.SLAEvent.SlaAppType;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.coord.CoordELEvaluator;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.coord.CoordUtils;
import org.apache.oozie.coord.CoordinatorJobException;
import org.apache.oozie.coord.SyncCoordAction;
import org.apache.oozie.coord.TimeUnit;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.util.db.SLADbOperations;
import org.jdom.Element;
import org.jdom.JDOMException;

public class CoordActionMaterializeCommand extends CoordinatorCommand<Void> {
    public static final String RESOLVED_UNRESOLVED_SEPARATOR = ";";
    private String jobId;
    private Date startTime;
    private Date endTime;
    private int lastActionNumber = 1; // over-ride by DB value
    private final XLog log = XLog.getLog(getClass());
    private String user;
    private String group;

    public CoordActionMaterializeCommand(String jobId, Date startTime, Date endTime) {
        super("coord_action_mater", "coord_action_mater", 0, XLog.STD);
        this.jobId = jobId;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    @Override
    protected Void call(CoordinatorStore store) throws StoreException, CommandException {
        //CoordinatorJobBean job = store.getCoordinatorJob(jobId, true);
        CoordinatorJobBean job = store.getEntityManager().find(CoordinatorJobBean.class, jobId);
        setLogInfo(job);
        if (job.getLastActionTime() != null && job.getLastActionTime().compareTo(endTime) >= 0) {
            log.info("ENDED Coordinator materialization for jobId=" + jobId
                    + " Action is *already* materialized for time " + startTime + " : " + endTime);
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
            catch (IOException e1) {
                log.warn("Configuration parse error. read from DB :" + job.getConf(), e1);
                throw new CommandException(ErrorCode.E1005, e1.getMessage(), e1);
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
        while (effStart.compareTo(end) < 0) {
            CoordinatorActionBean actionBean = new CoordinatorActionBean();
            lastActionNumber++;

            actionBean.setTimeOut(jobBean.getTimeout());

            log.info(origStart.getTime() + " Materializing action for time=" + effStart.getTime()
                    + ", lastactionnumber=" + lastActionNumber);
            action = materializeOneInstance(dryrun, (Element) eJob.clone(), effStart.getTime(), lastActionNumber, conf,
                                            actionBean);
            if (actionBean.getNominalTimestamp().before(jobBean.getCreatedTimestamp())) {
                actionBean.setTimeOut(-1);
            }

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
     * materialize one instance for specific nominal time. It includes: 1. Materialize data events (i.e. <data-in> and
     * <data-out>) 2. Materialize data properties (i.e dataIn(<DS>) and dataOut(<DS>) 3. remove 'start' and 'end' tag 4.
     * Add 'instance_number' and 'nominal-time' tag
     *
     * @param eAction : frequency unexploded-job
     * @param nominalTime : materialization time
     * @param instanceCount : instance numbers
     * @param conf
     * @return return one materialized job for specific nominal time
     * @throws Exception
     */
    private String materializeOneInstance(boolean dryrun, Element eAction, Date nominalTime, int instanceCount,
                                          Configuration conf, CoordinatorActionBean actionBean) throws Exception {
        String actionId = Services.get().get(UUIDService.class).generateChildId(jobId, instanceCount + "");
        SyncCoordAction appInst = new SyncCoordAction();
        appInst.setActionId(actionId);
        appInst.setName(eAction.getAttributeValue("name"));
        appInst.setNominalTime(nominalTime);
        int frequency = Integer.parseInt(eAction.getAttributeValue("frequency"));
        appInst.setFrequency(frequency);
        appInst.setTimeUnit(TimeUnit.valueOf(eAction.getAttributeValue("freq_timeunit"))); // TODO:
        appInst.setTimeZone(DateUtils.getTimeZone(eAction.getAttributeValue("timezone")));
        appInst.setEndOfDuration(TimeUnit.valueOf(eAction.getAttributeValue("end_of_duration")));

        StringBuffer dependencyList = new StringBuffer();

        Element inputList = eAction.getChild("input-events", eAction.getNamespace());
        List<Element> dataInList = null;
        if (inputList != null) {
            dataInList = (List<Element>) inputList.getChildren("data-in", eAction.getNamespace());
            materializeDataEvents(dataInList, appInst, conf, dependencyList);
        }

        Element outputList = eAction.getChild("output-events", eAction.getNamespace());
        List<Element> dataOutList = null;
        if (outputList != null) {
            dataOutList = (List<Element>) outputList.getChildren("data-out", eAction.getNamespace());
            StringBuffer tmp = new StringBuffer();
            materializeDataEvents(dataOutList, appInst, conf, tmp);// no dependency checks
        }

        eAction.removeAttribute("start");
        eAction.removeAttribute("end");
        eAction.setAttribute("instance-number", Integer.toString(instanceCount));
        eAction.setAttribute("action-nominal-time", DateUtils.formatDateUTC(nominalTime));

        boolean isSla = materializeSLA(eAction.getChild("action", eAction.getNamespace()).getChild("info",
                                                                                                   eAction.getNamespace("sla")), nominalTime, conf);

        // Setting up action bean
        actionBean.setCreatedConf(XmlUtils.prettyPrint(conf).toString());
        actionBean.setRunConf(XmlUtils.prettyPrint(conf).toString()); // TODO:
        actionBean.setCreatedTime(new Date());
        actionBean.setJobId(jobId);
        // actionBean.setId(jobId + "_" + instanceCount);
        actionBean.setId(actionId);
        actionBean.setLastModifiedTime(new Date());
        actionBean.setStatus(CoordinatorAction.Status.WAITING);
        actionBean.setActionNumber(instanceCount);
        actionBean.setMissingDependencies(dependencyList.toString());
        actionBean.setNominalTime(nominalTime);
        if (isSla == true) {
            actionBean.setSlaXml(XmlUtils.prettyPrint(
                    eAction.getChild("action", eAction.getNamespace()).getChild("info", eAction.getNamespace("sla")))
                    .toString());
        }

        // actionBean.setTrackerUri(trackerUri);//TOOD:
        // actionBean.setConsoleUrl(consoleUrl); //TODO:
        // actionBean.setType(type);//TODO:
        // actionBean.setErrorInfo(errorCode, errorMessage); //TODO:
        // actionBean.setExternalStatus(externalStatus);//TODO
        if (!dryrun) {
            return XmlUtils.prettyPrint(eAction).toString();
        }
        else {
            String action = XmlUtils.prettyPrint(eAction).toString();
            CoordActionInputCheckCommand coordActionInput = new CoordActionInputCheckCommand(actionBean.getId());
            StringBuilder actionXml = new StringBuilder(action);
            StringBuilder existList = new StringBuilder();
            StringBuilder nonExistList = new StringBuilder();
            StringBuilder nonResolvedList = new StringBuilder();
            getResolvedList(actionBean.getMissingDependencies(), nonExistList, nonResolvedList);
            Date actualTime = new Date();
            Configuration actionConf = new XConfiguration(new StringReader(actionBean.getRunConf()));
            coordActionInput.checkInput(actionXml, existList, nonExistList, actionConf, actualTime);
            return actionXml.toString();
        }

        // return XmlUtils.prettyPrint(eAction).toString();
    }

    /**
     * Materialize all <input-events>/<data-in> or <output-events>/<data-out> tags Create uris for resolved instances.
     * Create unresolved instance for latest().
     *
     * @param events
     * @param appInst
     * @param conf
     * @throws Exception
     */
    private void materializeDataEvents(List<Element> events, SyncCoordAction appInst, Configuration conf,
                                       StringBuffer dependencyList) throws Exception {

        if (events == null) {
            return;
        }
        StringBuffer unresolvedList = new StringBuffer();
        for (Element event : events) {
            StringBuilder instances = new StringBuilder();
            ELEvaluator eval = CoordELEvaluator.createInstancesELEvaluator(event, appInst, conf);
            // Handle list of instance tag
            resolveInstances(event, instances, appInst, conf, eval);
            // Handle start-instance and end-instance
            resolveInstanceRange(event, instances, appInst, conf, eval);
            // Separate out the unresolved instances
            separateResolvedAndUnresolved(event, instances, dependencyList);
            String tmpUnresolved = event.getChildTextTrim("unresolved-instances", event.getNamespace());
            if (tmpUnresolved != null) {
                if (unresolvedList.length() > 0) {
                    unresolvedList.append(CoordELFunctions.INSTANCE_SEPARATOR);
                }
                unresolvedList.append(tmpUnresolved);
            }
        }
        if (unresolvedList.length() > 0) {
            dependencyList.append(RESOLVED_UNRESOLVED_SEPARATOR);
            dependencyList.append(unresolvedList);
        }
        return;
    }

    /**
     * Resolve list of <instance> </instance> tags.
     *
     * @param event
     * @param instances
     * @param actionInst
     * @param conf
     * @throws Exception
     */
    private void resolveInstances(Element event, StringBuilder instances, SyncCoordAction actionInst,
                                  Configuration conf, ELEvaluator eval) throws Exception {
        for (Element eInstance : (List<Element>) event.getChildren("instance", event.getNamespace())) {
            if (instances.length() > 0) {
                instances.append(CoordELFunctions.INSTANCE_SEPARATOR);
            }
            instances.append(materializeInstance(event, eInstance.getTextTrim(), actionInst, conf, eval));
        }
        event.removeChildren("instance", event.getNamespace());
    }

    /**
     * Resolve <start-instance> <end-insatnce> tag. Don't resolve any latest()
     *
     * @param event
     * @param instances
     * @param appInst
     * @param conf
     * @throws Exception
     */
    private void resolveInstanceRange(Element event, StringBuilder instances, SyncCoordAction appInst,
                                      Configuration conf, ELEvaluator eval) throws Exception {
        Element eStartInst = event.getChild("start-instance", event.getNamespace());
        Element eEndInst = event.getChild("end-instance", event.getNamespace());
        if (eStartInst != null && eEndInst != null) {
            String strStart = eStartInst.getTextTrim();
            String strEnd = eEndInst.getTextTrim();
            checkIfBothSameType(strStart, strEnd);
            int startIndex = getInstanceNumber(strStart, event, appInst, conf);
            int endIndex = getInstanceNumber(strEnd, event, appInst, conf);
            if (startIndex > endIndex) {
                throw new CommandException(ErrorCode.E1010,
                                           " start-instance should be equal or earlier than the end-instance \n"
                                                   + XmlUtils.prettyPrint(event));
            }
            if (strStart.indexOf("latest") < 0 && strEnd.indexOf("latest") < 0) {
                // Everything could be resolved NOW. no latest() ELs
                for (int i = endIndex; i >= startIndex; i--) {
                    String matInstance = materializeInstance(event, "${coord:current(" + i + ")}", appInst, conf, eval);
                    if (matInstance == null || matInstance.length() == 0) {
                        // Earlier than dataset's initial instance
                        break;
                    }
                    if (instances.length() > 0) {
                        instances.append(CoordELFunctions.INSTANCE_SEPARATOR);
                    }
                    instances.append(matInstance);
                }
            }
            else { // latest(n) EL is present
                for (; startIndex <= endIndex; startIndex++) {
                    if (instances.length() > 0) {
                        instances.append(CoordELFunctions.INSTANCE_SEPARATOR);
                    }
                    instances.append("${coord:latest(" + startIndex + ")}");
                }
            }
            // Remove start-instance and end-instances
            event.removeChild("start-instance", event.getNamespace());
            event.removeChild("end-instance", event.getNamespace());
        }
    }

    private void checkIfBothSameType(String startInst, String endInst) throws CommandException {
        if ((startInst.indexOf("current") >= 0 && endInst.indexOf("latest") >= 0)
                || (startInst.indexOf("latest") >= 0 && endInst.indexOf("current") >= 0)) {
            throw new CommandException(ErrorCode.E1010,
                                       " start-instance and end-instance both should be either latest or current\n" + " start "
                                               + startInst + " and end " + endInst);
        }
    }

    /**
     * Create two new tags with <uris> and <unresolved-instances>.
     *
     * @param event
     * @param instances
     * @param dependencyList
     * @throws Exception
     */
    private void separateResolvedAndUnresolved(Element event, StringBuilder instances, StringBuffer dependencyList)
            throws Exception {
        StringBuilder unresolvedInstances = new StringBuilder();
        StringBuilder urisWithDoneFlag = new StringBuilder();
        String uris = createURIs(event, instances.toString(), unresolvedInstances, urisWithDoneFlag);
        if (uris.length() > 0) {
            Element uriInstance = new Element("uris", event.getNamespace());
            uriInstance.addContent(uris);
            event.getContent().add(1, uriInstance);
            if (dependencyList.length() > 0) {
                dependencyList.append(CoordELFunctions.INSTANCE_SEPARATOR);
            }
            dependencyList.append(urisWithDoneFlag);
        }
        if (unresolvedInstances.length() > 0) {
            Element elemInstance = new Element("unresolved-instances", event.getNamespace());
            elemInstance.addContent(unresolvedInstances.toString());
            event.getContent().add(1, elemInstance);
        }
    }

    /**
     * The function create a list of URIs separated by "," using the instances time stamp and URI-template
     *
     * @param event : <data-in> event
     * @param instances : List of time stanmp seprated by ","
     * @param unresolvedInstances : list of instance with latest function
     * @param urisWithDoneFlag : list of URIs with the done flag appended
     * @return : list of URIs separated by ",".
     * @throws Exception
     */
    private String createURIs(Element event, String instances, StringBuilder unresolvedInstances,
                              StringBuilder urisWithDoneFlag) throws Exception {
        if (instances == null || instances.length() == 0) {
            return "";
        }
        String[] instanceList = instances.split(CoordELFunctions.INSTANCE_SEPARATOR);
        StringBuilder uris = new StringBuilder();

        Element doneFlagElement = event.getChild("dataset", event.getNamespace()).getChild("done-flag",
                                                                                           event.getNamespace());
        String doneFlag = CoordUtils.getDoneFlag(doneFlagElement);

        for (int i = 0; i < instanceList.length; i++) {
            if (instanceList[i].indexOf("latest") >= 0) {
                if (unresolvedInstances.length() > 0) {
                    unresolvedInstances.append(CoordELFunctions.INSTANCE_SEPARATOR);
                }
                unresolvedInstances.append(instanceList[i]);
                continue;
            }
            ELEvaluator eval = CoordELEvaluator.createURIELEvaluator(instanceList[i]);
            if (uris.length() > 0) {
                uris.append(CoordELFunctions.INSTANCE_SEPARATOR);
                urisWithDoneFlag.append(CoordELFunctions.INSTANCE_SEPARATOR);
            }

            String uriPath = CoordELFunctions.evalAndWrap(eval, event.getChild("dataset", event.getNamespace())
                    .getChild("uri-template", event.getNamespace()).getTextTrim());
            uris.append(uriPath);
            if (doneFlag.length() > 0) {
                uriPath += "/" + doneFlag;
            }
            urisWithDoneFlag.append(uriPath);
        }
        return uris.toString();
    }

    /**
     * Materialize one instance like current(-2)
     *
     * @param event : <data-in>
     * @param expr : instance like current(-1)
     * @param appInst : application specific info
     * @param conf
     * @return materialized date string
     * @throws Exception
     */
    private String materializeInstance(Element event, String expr, SyncCoordAction appInst, Configuration conf,
                                       ELEvaluator evalInst) throws Exception {
        if (event == null) {
            return null;
        }
        // ELEvaluator eval = CoordELEvaluator.createInstancesELEvaluator(event,
        // appInst, conf);
        return CoordELFunctions.evalAndWrap(evalInst, expr);
    }

    /**
     * parse a function like coord:latest(n) and return the 'n'.
     *
     * @param function
     * @return parameter of the function
     * @throws Exception
     */
    private int getInstanceNumber(String function, Element event, SyncCoordAction appInst, Configuration conf)
            throws Exception {
        ELEvaluator eval = CoordELEvaluator
                .createInstancesELEvaluator("coord-action-create-inst", event, appInst, conf);
        String newFunc = CoordELFunctions.evalAndWrap(eval, function);
        int firstPos = newFunc.indexOf("(");
        int lastPos = newFunc.lastIndexOf(")");
        if (firstPos >= 0 && lastPos > firstPos) {
            String tmp = newFunc.substring(firstPos + 1, lastPos).trim();
            if (tmp.length() > 0) {
                return Integer.parseInt(tmp);
            }
        }
        // Error handling
        throw new RuntimeException("Unformatted function :" + newFunc);
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
        queueCallable(new CoordActionNotification(actionBean), 100);
        queueCallable(new CoordActionInputCheckCommand(actionBean.getId()), 100);
    }

    private void writeActionRegistration(String actionXml, CoordinatorActionBean actionBean, CoordinatorStore store)
            throws Exception {
        Element eAction = XmlUtils.parseXml(actionXml);
        Element eSla = eAction.getChild("action", eAction.getNamespace()).getChild("info", eAction.getNamespace("sla"));
        SLADbOperations.writeSlaRegistrationEvent(eSla, store, actionBean.getId(), SlaAppType.COORDINATOR_ACTION, user,
                                                  group);
    }

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

    private boolean materializeSLA(Element eSla, Date nominalTime, Configuration conf) throws CoordinatorJobException {
        if (eSla == null) {
            // System.out.println("NO SLA presents " +
            // eAppXml.getNamespace("sla"));
            return false;
        }
        try {
            ELEvaluator evalSla = CoordELEvaluator.createSLAEvaluator(nominalTime, conf);
            // System.out.println("SLA presents");
            List<Element> elemList = eSla.getChildren();
            for (Element elem : elemList) {
                String updated;
                try {
                    updated = CoordELFunctions.evalAndWrap(evalSla, elem.getText().trim());
                }
                catch (Exception e) {
                    throw new CoordinatorJobException(ErrorCode.E1004, e.getMessage(), e);
                }
                elem.removeContent();
                elem.addContent(updated);
            }
        }
        catch (Exception e) {
            throw new CoordinatorJobException(ErrorCode.E1004, e.getMessage(), e);
        }
        return true;
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
                queueCallable(new CoordActionMaterializeCommand(jobId, startTime, endTime), LOCK_FAILURE_REQUEUE_INTERVAL);
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

    public static String getResolvedList(String missDepList, StringBuilder resolved, StringBuilder unresolved) {
        if (missDepList != null) {
            int index = missDepList.indexOf(RESOLVED_UNRESOLVED_SEPARATOR);
            if (index < 0) {
                resolved.append(missDepList);
            }
            else {
                resolved.append(missDepList.substring(0, index));
                unresolved.append(missDepList.substring(index + 1));
            }
        }
        return resolved.toString();
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
