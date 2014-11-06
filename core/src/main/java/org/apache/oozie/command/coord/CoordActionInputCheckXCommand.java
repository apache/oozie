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
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.coord.CoordELEvaluator;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.coord.TimeUnit;
import org.apache.oozie.dependency.URIHandler;
import org.apache.oozie.dependency.URIHandlerException;
import org.apache.oozie.executor.jpa.CoordActionGetForInputCheckJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.CallableQueueService;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.URIHandlerService;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.StatusUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;

/**
 * The command to check if an action's data input paths exist in the file system.
 */
public class CoordActionInputCheckXCommand extends CoordinatorXCommand<Void> {

    public static final String COORD_EXECUTION_NONE_TOLERANCE = "oozie.coord.execution.none.tolerance";

    private final String actionId;
    /**
     * Property name of command re-queue interval for coordinator action input check in
     * milliseconds.
     */
    public static final String CONF_COORD_INPUT_CHECK_REQUEUE_INTERVAL = Service.CONF_PREFIX
            + "coord.input.check.requeue.interval";
    private CoordinatorActionBean coordAction = null;
    private CoordinatorJobBean coordJob = null;
    private JPAService jpaService = null;
    private String jobId = null;

    public CoordActionInputCheckXCommand(String actionId, String jobId) {
        super("coord_action_input", "coord_action_input", 1);
        this.actionId = ParamChecker.notEmpty(actionId, "actionId");
        this.jobId = jobId;
    }

    @Override
    protected void setLogInfo() {
        LogUtils.setLogInfo(actionId);
    }

    /**
     * Computes the nominal time of the next action.
     * Based on CoordMaterializeTransitionXCommand#materializeActions
     *
     * @return the nominal time of the next action
     * @throws ParseException
     */
    private Date computeNextNominalTime() throws ParseException {
        Date nextNominalTime;
        boolean isCronFrequency = false;
        int freq = -1;
        try {
            freq = Integer.parseInt(coordJob.getFrequency());
        } catch (NumberFormatException e) {
            isCronFrequency = true;
        }

        if (isCronFrequency) {
            nextNominalTime = CoordCommandUtils.getNextValidActionTimeForCronFrequency(coordAction.getNominalTime(), coordJob);
        } else {
            Calendar nextNominalTimeCal = Calendar.getInstance(DateUtils.getTimeZone(coordJob.getTimeZone()));
            nextNominalTimeCal.setTime(coordAction.getNominalTime());
            TimeUnit freqTU = TimeUnit.valueOf(coordJob.getTimeUnitStr());
            nextNominalTimeCal.add(freqTU.getCalendarUnit(), freq);
            nextNominalTime = nextNominalTimeCal.getTime();
        }

        // If the next nominal time is after the job's end time, then this is the last action, so return null
        if (nextNominalTime.after(coordJob.getEndTime())) {
            nextNominalTime = null;
        }
        return nextNominalTime;
    }

    @Override
    protected Void execute() throws CommandException {
        LOG.debug("[" + actionId + "]::ActionInputCheck:: Action is in WAITING state.");

        // this action should only get processed if current time > nominal time;
        // otherwise, requeue this action for delay execution;
        Date nominalTime = coordAction.getNominalTime();
        Date currentTime = new Date();
        if (nominalTime.compareTo(currentTime) > 0) {
            queue(new CoordActionInputCheckXCommand(coordAction.getId(), coordAction.getJobId()), Math.max((nominalTime.getTime() - currentTime
                    .getTime()), getCoordInputCheckRequeueInterval()));
            updateCoordAction(coordAction, false);
            LOG.info("[" + actionId
                    + "]::ActionInputCheck:: nominal Time is newer than current time, so requeue and wait. Current="
                    + DateUtils.formatDateOozieTZ(currentTime) + ", nominal=" + DateUtils.formatDateOozieTZ(nominalTime));

            return null;
        }

        StringBuilder actionXml = new StringBuilder(coordAction.getActionXml());
        boolean isChangeInDependency = false;
        try {
            Configuration actionConf = new XConfiguration(new StringReader(coordAction.getRunConf()));
            Date now = new Date();
            if (coordJob.getExecutionOrder().equals(CoordinatorJobBean.Execution.LAST_ONLY)) {
                Date nextNominalTime = computeNextNominalTime();
                if (nextNominalTime != null) {
                    // If the current time is after the next action's nominal time, then we've passed the window where this action
                    // should be started; so set it to SKIPPED
                    if (now.after(nextNominalTime)) {
                        LOG.info("LAST_ONLY execution: Preparing to skip action [{0}] because the current time [{1}] is later than "
                                + "the nominal time [{2}] of the next action]", coordAction.getId(),
                                DateUtils.formatDateOozieTZ(now), DateUtils.formatDateOozieTZ(nextNominalTime));
                        queue(new CoordActionSkipXCommand(coordAction, coordJob.getUser(), coordJob.getAppName()));
                        return null;
                    } else {
                        LOG.debug("LAST_ONLY execution: Not skipping action [{0}] because the current time [{1}] is earlier than "
                                + "the nominal time [{2}] of the next action]", coordAction.getId(),
                                DateUtils.formatDateOozieTZ(now), DateUtils.formatDateOozieTZ(nextNominalTime));
                    }
                }
            }
            else if (coordJob.getExecutionOrder().equals(CoordinatorJobBean.Execution.NONE)) {
                // If the current time is after the nominal time of this action plus some tolerance,
                // then we've passed the window where this action
                // should be started; so set it to SKIPPED
                Calendar cal = Calendar.getInstance(DateUtils.getTimeZone(coordJob.getTimeZone()));
                cal.setTime(nominalTime);
                cal.add(Calendar.MINUTE, ConfigurationService.getInt(COORD_EXECUTION_NONE_TOLERANCE));
                nominalTime = cal.getTime();
                if (now.after(nominalTime)) {
                    LOG.info("NONE execution: Preparing to skip action [{0}] because the current time [{1}] is later than "
                            + "the nominal time [{2}] of the current action]", coordAction.getId(),
                            DateUtils.formatDateOozieTZ(now), DateUtils.formatDateOozieTZ(nominalTime));
                    queue(new CoordActionSkipXCommand(coordAction, coordJob.getUser(), coordJob.getAppName()));
                    return null;
                } else {
                    LOG.debug("NONE execution: Not skipping action [{0}] because the current time [{1}] is earlier than "
                            + "the nominal time [{2}] of the current action]", coordAction.getId(),
                            DateUtils.formatDateOozieTZ(now), DateUtils.formatDateOozieTZ(coordAction.getNominalTime()));
                }
            }

            StringBuilder existList = new StringBuilder();
            StringBuilder nonExistList = new StringBuilder();
            StringBuilder nonResolvedList = new StringBuilder();
            String firstMissingDependency = "";
            String missingDeps = coordAction.getMissingDependencies();
            CoordCommandUtils.getResolvedList(missingDeps, nonExistList, nonResolvedList);

            // For clarity regarding which is the missing dependency in synchronous order
            // instead of printing entire list, some of which, may be available
            if(nonExistList.length() > 0) {
                firstMissingDependency = nonExistList.toString().split(CoordELFunctions.INSTANCE_SEPARATOR)[0];
            }
            LOG.info("[" + actionId + "]::CoordActionInputCheck:: Missing deps:" + firstMissingDependency + " "
                    + nonResolvedList.toString());
            // Updating the list of data dependencies that are available and those that are yet not
            boolean status = checkInput(actionXml, existList, nonExistList, actionConf);
            String pushDeps = coordAction.getPushMissingDependencies();
            // Resolve latest/future only when all current missingDependencies and
            // pushMissingDependencies are met
            if (status && nonResolvedList.length() > 0) {
                status = (pushDeps == null || pushDeps.length() == 0) ? checkUnResolvedInput(actionXml, actionConf)
                        : false;
            }
            coordAction.setLastModifiedTime(currentTime);
            coordAction.setActionXml(actionXml.toString());
            if (nonResolvedList.length() > 0 && status == false) {
                nonExistList.append(CoordCommandUtils.RESOLVED_UNRESOLVED_SEPARATOR).append(nonResolvedList);
            }
            String nonExistListStr = nonExistList.toString();
            if (!nonExistListStr.equals(missingDeps) || missingDeps.isEmpty()) {
                // missingDeps null or empty means action should become READY
                isChangeInDependency = true;
                coordAction.setMissingDependencies(nonExistListStr);
            }
            if (status && (pushDeps == null || pushDeps.length() == 0)) {
                String newActionXml = resolveCoordConfiguration(actionXml, actionConf, actionId);
                actionXml.replace(0, actionXml.length(), newActionXml);
                coordAction.setActionXml(actionXml.toString());
                coordAction.setStatus(CoordinatorAction.Status.READY);
                updateCoordAction(coordAction, true);
                new CoordActionReadyXCommand(coordAction.getJobId()).call(getEntityKey());
            }
            else if (!isTimeout(currentTime)) {
                if (status == false) {
                    queue(new CoordActionInputCheckXCommand(coordAction.getId(), coordAction.getJobId()),
                            getCoordInputCheckRequeueInterval());
                }
                updateCoordAction(coordAction, isChangeInDependency);
            }
            else {
                if (!nonExistListStr.isEmpty() && pushDeps == null || pushDeps.length() == 0) {
                    queue(new CoordActionTimeOutXCommand(coordAction, coordJob.getUser(), coordJob.getAppName()));
                }
                else {
                    // Let CoordPushDependencyCheckXCommand queue the timeout
                    queue(new CoordPushDependencyCheckXCommand(coordAction.getId()));
                }
                updateCoordAction(coordAction, isChangeInDependency);
            }
        }
        catch (AccessControlException e) {
            LOG.error("Permission error in ActionInputCheck", e);
            if (isTimeout(currentTime)) {
                LOG.debug("Queueing timeout command");
                Services.get().get(CallableQueueService.class)
                        .queue(new CoordActionTimeOutXCommand(coordAction, coordJob.getUser(), coordJob.getAppName()));
            }
            else {
                // Requeue InputCheckCommand for permission denied error with longer interval
                Services.get()
                        .get(CallableQueueService.class)
                        .queue(new CoordActionInputCheckXCommand(coordAction.getId(), coordAction.getJobId()),
                                2 * getCoordInputCheckRequeueInterval());
            }
            updateCoordAction(coordAction, isChangeInDependency);
        }
        catch (Exception e) {
            if (isTimeout(currentTime)) {
                LOG.debug("Queueing timeout command");
                // XCommand.queue() will not work when there is a Exception
                Services.get().get(CallableQueueService.class)
                        .queue(new CoordActionTimeOutXCommand(coordAction, coordJob.getUser(), coordJob.getAppName()));
            }
            updateCoordAction(coordAction, isChangeInDependency);
            throw new CommandException(ErrorCode.E1021, e.getMessage(), e);
        }
        return null;
    }


    static String resolveCoordConfiguration(StringBuilder actionXml, Configuration actionConf, String actionId) throws Exception {
        Element eAction = XmlUtils.parseXml(actionXml.toString());
        ELEvaluator eval = CoordELEvaluator.createDataEvaluator(eAction, actionConf, actionId);
        materializeDataProperties(eAction, actionConf, eval);
        return XmlUtils.prettyPrint(eAction).toString();
    }

    private boolean isTimeout(Date currentTime) {
        long waitingTime = (currentTime.getTime() - Math.max(coordAction.getNominalTime().getTime(), coordAction
                .getCreatedTime().getTime()))
                / (60 * 1000);
        int timeOut = coordAction.getTimeOut();
        return (timeOut >= 0) && (waitingTime > timeOut);
    }

    private void updateCoordAction(CoordinatorActionBean coordAction, boolean isChangeInDependency)
            throws CommandException {
        coordAction.setLastModifiedTime(new Date());
        if (jpaService != null) {
            try {
                if (isChangeInDependency) {
                    CoordActionQueryExecutor.getInstance().executeUpdate(
                            CoordActionQuery.UPDATE_COORD_ACTION_FOR_INPUTCHECK, coordAction);
                    if (EventHandlerService.isEnabled() && coordAction.getStatus() != CoordinatorAction.Status.READY) {
                        // since event is not to be generated unless action
                        // RUNNING via StartX
                        generateEvent(coordAction, coordJob.getUser(), coordJob.getAppName(), null);
                    }
                }
                else {
                    CoordActionQueryExecutor.getInstance().executeUpdate(
                            CoordActionQuery.UPDATE_COORD_ACTION_FOR_MODIFIED_DATE, coordAction);
                }
            }
            catch (JPAExecutorException jex) {
                throw new CommandException(ErrorCode.E1021, jex.getMessage(), jex);
            }
        }
    }

    /**
     * This function reads the value of re-queue interval for coordinator input
     * check command from the Oozie configuration provided by Configuration
     * Service. If nothing defined in the configuration, it uses the code
     * specified default value.
     *
     * @return re-queue interval in ms
     */
    public long getCoordInputCheckRequeueInterval() {
        long requeueInterval = ConfigurationService.getLong(CONF_COORD_INPUT_CHECK_REQUEUE_INTERVAL);
        return requeueInterval;
    }

    /**
     * To check the list of input paths if all of them exist
     *
     * @param actionXml action xml
     * @param existList the list of existed paths
     * @param nonExistList the list of non existed paths
     * @param conf action configuration
     * @return true if all input paths are existed
     * @throws Exception thrown of unable to check input path
     */
    protected boolean checkInput(StringBuilder actionXml, StringBuilder existList, StringBuilder nonExistList,
            Configuration conf) throws Exception {
        Element eAction = XmlUtils.parseXml(actionXml.toString());
        return checkResolvedUris(eAction, existList, nonExistList, conf);
    }

    protected boolean checkUnResolvedInput(StringBuilder actionXml, Configuration conf) throws Exception {
        Element eAction = XmlUtils.parseXml(actionXml.toString());
        LOG.debug("[" + actionId + "]::ActionInputCheck:: Checking Latest/future");
        boolean allExist = checkUnresolvedInstances(eAction, conf);
        if (allExist) {
            actionXml.replace(0, actionXml.length(), XmlUtils.prettyPrint(eAction).toString());
        }
        return allExist;
    }

    /**
     * Materialize data properties defined in <action> tag. it includes dataIn(<DS>) and dataOut(<DS>) it creates a list
     * of files that will be needed.
     *
     * @param eAction action element
     * @param conf action configuration
     * @throws Exception thrown if failed to resolve data properties
     * @update modify 'Action' element with appropriate list of files.
     */
    @SuppressWarnings("unchecked")
    static void materializeDataProperties(Element eAction, Configuration conf, ELEvaluator eval) throws Exception {
        Element configElem = eAction.getChild("action", eAction.getNamespace()).getChild("workflow",
                eAction.getNamespace()).getChild("configuration", eAction.getNamespace());
        if (configElem != null) {
            for (Element propElem : (List<Element>) configElem.getChildren("property", configElem.getNamespace())) {
                resolveTagContents("value", propElem, eval);
            }
        }
    }

    /**
     * To resolve property value which contains el functions
     *
     * @param tagName tag name
     * @param elem the child element of "property" element
     * @param eval el functions evaluator
     * @throws Exception thrown if unable to resolve tag value
     */
    private static void resolveTagContents(String tagName, Element elem, ELEvaluator eval) throws Exception {
        if (elem == null) {
            return;
        }
        Element tagElem = elem.getChild(tagName, elem.getNamespace());
        if (tagElem != null) {
            String updated = CoordELFunctions.evalAndWrap(eval, tagElem.getText());
            tagElem.removeContent();
            tagElem.addContent(updated);
        }
        else {
            XLog.getLog(CoordActionInputCheckXCommand.class).warn(" Value NOT FOUND " + tagName);
        }
    }

    /**
     * Check if any unsolved paths under data output. Resolve the unresolved data input paths.
     *
     * @param eAction action element
     * @param actionConf action configuration
     * @return true if successful to resolve input and output paths
     * @throws Exception thrown if failed to resolve data input and output paths
     */
    @SuppressWarnings("unchecked")
    private boolean checkUnresolvedInstances(Element eAction, Configuration actionConf) throws Exception {
        String strAction = XmlUtils.prettyPrint(eAction).toString();
        Date nominalTime = DateUtils.parseDateOozieTZ(eAction.getAttributeValue("action-nominal-time"));
        String actualTimeStr = eAction.getAttributeValue("action-actual-time");
        Date actualTime = null;
        if (actualTimeStr == null) {
            LOG.debug("Unable to get action-actual-time from action xml, this job is submitted " +
            "from previous version. Assign current date to actual time, action = " + actionId);
            actualTime = new Date();
        } else {
            actualTime = DateUtils.parseDateOozieTZ(actualTimeStr);
        }

        StringBuffer resultedXml = new StringBuffer();

        boolean ret;
        Element inputList = eAction.getChild("input-events", eAction.getNamespace());
        if (inputList != null) {
            ret = materializeUnresolvedEvent(inputList.getChildren("data-in", eAction.getNamespace()), nominalTime,
                    actualTime, actionConf);
            if (ret == false) {
                resultedXml.append(strAction);
                return false;
            }
        }

        // Using latest() or future() in output-event is not intuitive.
        // We need to make sure, this assumption is correct.
        Element outputList = eAction.getChild("output-events", eAction.getNamespace());
        if (outputList != null) {
            for (Element dEvent : (List<Element>) outputList.getChildren("data-out", eAction.getNamespace())) {
                if (dEvent.getChild(CoordCommandUtils.UNRESOLVED_INST_TAG, dEvent.getNamespace()) != null) {
                    throw new CommandException(ErrorCode.E1006, "coord:latest()/future()",
                            " not permitted in output-event ");
                }
            }
        }
        return true;
    }

    /**
     * Resolve the list of data input paths
     *
     * @param eDataEvents the list of data input elements
     * @param nominalTime action nominal time
     * @param actualTime current time
     * @param conf action configuration
     * @return true if all unresolved URIs can be resolved
     * @throws Exception thrown if failed to resolve data input paths
     */
    @SuppressWarnings("unchecked")
    private boolean materializeUnresolvedEvent(List<Element> eDataEvents, Date nominalTime, Date actualTime,
            Configuration conf) throws Exception {
        for (Element dEvent : eDataEvents) {
            if (dEvent.getChild(CoordCommandUtils.UNRESOLVED_INST_TAG, dEvent.getNamespace()) == null) {
                continue;
            }
            ELEvaluator eval = CoordELEvaluator.createLazyEvaluator(actualTime, nominalTime, dEvent, conf);
            String uresolvedInstance = dEvent.getChild(CoordCommandUtils.UNRESOLVED_INST_TAG, dEvent.getNamespace()).getTextTrim();
            String unresolvedList[] = uresolvedInstance.split(CoordELFunctions.INSTANCE_SEPARATOR);
            StringBuffer resolvedTmp = new StringBuffer();
            for (int i = 0; i < unresolvedList.length; i++) {
                String ret = CoordELFunctions.evalAndWrap(eval, unresolvedList[i]);
                Boolean isResolved = (Boolean) eval.getVariable("is_resolved");
                if (isResolved == false) {
                    LOG.info("[" + actionId + "]::Cannot resolve: " + ret);
                    return false;
                }
                if (resolvedTmp.length() > 0) {
                    resolvedTmp.append(CoordELFunctions.INSTANCE_SEPARATOR);
                }
                resolvedTmp.append((String) eval.getVariable("resolved_path"));
            }
            if (resolvedTmp.length() > 0) {
                if (dEvent.getChild("uris", dEvent.getNamespace()) != null) {
                    resolvedTmp.append(CoordELFunctions.INSTANCE_SEPARATOR).append(
                            dEvent.getChild("uris", dEvent.getNamespace()).getTextTrim());
                    dEvent.removeChild("uris", dEvent.getNamespace());
                }
                Element uriInstance = new Element("uris", dEvent.getNamespace());
                uriInstance.addContent(resolvedTmp.toString());
                dEvent.getContent().add(1, uriInstance);
            }
            dEvent.removeChild(CoordCommandUtils.UNRESOLVED_INST_TAG, dEvent.getNamespace());
        }

        return true;
    }

    /**
     * Check all resolved URIs existence
     *
     * @param eAction action element
     * @param existList the list of existed paths
     * @param nonExistList the list of paths to check existence
     * @param conf action configuration
     * @return true if all nonExistList paths exist
     * @throws IOException thrown if unable to access the path
     */
    private boolean checkResolvedUris(Element eAction, StringBuilder existList, StringBuilder nonExistList,
            Configuration conf) throws IOException {
        Element inputList = eAction.getChild("input-events", eAction.getNamespace());
        if (inputList != null) {
            if (nonExistList.length() > 0) {
                checkListOfPaths(existList, nonExistList, conf);
            }
            return nonExistList.length() == 0;
        }
        return true;
    }

    /**
     * Check a list of non existed paths and add to exist list if it exists
     *
     * @param existList the list of existed paths
     * @param nonExistList the list of paths to check existence
     * @param conf action configuration
     * @return true if all nonExistList paths exist
     * @throws IOException thrown if unable to access the path
     */
    private boolean checkListOfPaths(StringBuilder existList, StringBuilder nonExistList, Configuration conf)
            throws IOException {

        String[] uriList = nonExistList.toString().split(CoordELFunctions.INSTANCE_SEPARATOR);
        if (uriList[0] != null) {
            LOG.info("[" + actionId + "]::ActionInputCheck:: In checkListOfPaths: " + uriList[0] + " is Missing.");
        }

        nonExistList.delete(0, nonExistList.length());
        boolean allExists = true;
        String existSeparator = "", nonExistSeparator = "";
        String user = ParamChecker.notEmpty(conf.get(OozieClient.USER_NAME), OozieClient.USER_NAME);
        for (int i = 0; i < uriList.length; i++) {
            if (allExists) {
                allExists = pathExists(uriList[i], conf, user);
                LOG.info("[" + actionId + "]::ActionInputCheck:: File:" + uriList[i] + ", Exists? :" + allExists);
            }
            if (allExists) {
                existList.append(existSeparator).append(uriList[i]);
                existSeparator = CoordELFunctions.INSTANCE_SEPARATOR;
            }
            else {
                nonExistList.append(nonExistSeparator).append(uriList[i]);
                nonExistSeparator = CoordELFunctions.INSTANCE_SEPARATOR;
            }
        }
        return allExists;
    }

    /**
     * Check if given path exists
     *
     * @param sPath uri path
     * @param actionConf action configuration
     * @return true if path exists
     * @throws IOException thrown if unable to access the path
     */
    protected boolean pathExists(String sPath, Configuration actionConf, String user) throws IOException {
        LOG.debug("checking for the file " + sPath);
        try {
            URI uri = new URI(sPath);
            URIHandlerService service = Services.get().get(URIHandlerService.class);
            URIHandler handler = service.getURIHandler(uri);
            return handler.exists(uri, actionConf, user);
        }
        catch (URIHandlerException e) {
            coordAction.setErrorCode(e.getErrorCode().toString());
            coordAction.setErrorMessage(e.getMessage());
            if (e.getCause() != null && e.getCause() instanceof AccessControlException) {
                throw (AccessControlException) e.getCause();
            }
            else {
                throw new IOException(e);
            }
        }
        catch (URISyntaxException e) {
            coordAction.setErrorCode(ErrorCode.E0906.toString());
            coordAction.setErrorMessage(e.getMessage());
            throw new IOException(e);
        }
    }

    /**
     * The function create a list of URIs separated by "," using the instances time stamp and URI-template
     *
     * @param event : <data-in> event
     * @param instances : List of time stamp seprated by ","
     * @param unresolvedInstances : list of instance with latest/future function
     * @return : list of URIs separated by ",".
     * @throws Exception thrown if failed to create URIs from unresolvedInstances
     */
    @SuppressWarnings("unused")
    private String createURIs(Element event, String instances, StringBuilder unresolvedInstances) throws Exception {
        if (instances == null || instances.length() == 0) {
            return "";
        }
        String[] instanceList = instances.split(CoordELFunctions.INSTANCE_SEPARATOR);
        StringBuilder uris = new StringBuilder();

        for (int i = 0; i < instanceList.length; i++) {
            int funcType = CoordCommandUtils.getFuncType(instanceList[i]);
            if (funcType == CoordCommandUtils.LATEST || funcType == CoordCommandUtils.FUTURE) {
                if (unresolvedInstances.length() > 0) {
                    unresolvedInstances.append(CoordELFunctions.INSTANCE_SEPARATOR);
                }
                unresolvedInstances.append(instanceList[i]);
                continue;
            }
            ELEvaluator eval = CoordELEvaluator.createURIELEvaluator(instanceList[i]);
            if (uris.length() > 0) {
                uris.append(CoordELFunctions.INSTANCE_SEPARATOR);
            }
            uris.append(CoordELFunctions.evalAndWrap(eval, event.getChild("dataset", event.getNamespace()).getChild(
                    "uri-template", event.getNamespace()).getTextTrim()));
        }
        return uris.toString();
    }

    /**
     * getting the error code of the coord action. (used mainly for unit testing)
     */
    protected String getCoordActionErrorCode() {
        if (coordAction != null) {
            return coordAction.getErrorCode();
        }
        return null;
    }

    /**
     * getting the error message of the coord action. (used mainly for unit testing)
     */
    protected String getCoordActionErrorMsg() {
        if (coordAction != null) {
            return coordAction.getErrorMessage();
        }
        return null;
    }

    @Override
    public String getEntityKey() {
        return this.jobId;
    }

    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    protected void loadState() throws CommandException {
        if (jpaService == null) {
            jpaService = Services.get().get(JPAService.class);
        }
        try {
            coordAction = jpaService.execute(new CoordActionGetForInputCheckJPAExecutor(actionId));
            coordJob = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB_INPUT_CHECK,
                    coordAction.getJobId());
        }
        catch (JPAExecutorException je) {
            throw new CommandException(je);
        }
        LogUtils.setLogInfo(coordAction);
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (coordAction.getStatus() != CoordinatorActionBean.Status.WAITING) {
            throw new PreconditionException(ErrorCode.E1100, "[" + actionId
                    + "]::CoordActionInputCheck:: Ignoring action. Should be in WAITING state, but state="
                    + coordAction.getStatus());
        }

        // if eligible to do action input check when running with backward support is true
        if (StatusUtils.getStatusForCoordActionInputCheck(coordJob)) {
            return;
        }

        if (coordJob.getStatus() != Job.Status.RUNNING && coordJob.getStatus() != Job.Status.RUNNINGWITHERROR && coordJob.getStatus() != Job.Status.PAUSED
                && coordJob.getStatus() != Job.Status.PAUSEDWITHERROR) {
            throw new PreconditionException(
                    ErrorCode.E1100, "["+ actionId + "]::CoordActionInputCheck:: Ignoring action." +
                    		" Coordinator job is not in RUNNING/RUNNINGWITHERROR/PAUSED/PAUSEDWITHERROR state, but state="
                            + coordJob.getStatus());
        }
    }

    @Override
    public String getKey(){
        return getName() + "_" + actionId;
    }

}
