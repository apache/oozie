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
import java.util.ArrayList;
import java.util.TimeZone;
import java.util.List;
import java.util.Date;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.coord.CoordELEvaluator;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.coord.CoordUtils;
import org.apache.oozie.coord.CoordinatorJobException;
import org.apache.oozie.coord.SyncCoordAction;
import org.apache.oozie.coord.TimeUnit;
import org.apache.oozie.coord.input.logic.CoordInputLogicEvaluatorUtil;
import org.apache.oozie.coord.input.dependency.CoordInputDependency;
import org.apache.oozie.coord.input.logic.CoordInputLogicEvaluator;
import org.apache.oozie.coord.input.dependency.CoordInputDependencyFactory;
import org.apache.oozie.coord.input.dependency.CoordInputInstance;
import org.apache.oozie.dependency.ActionDependency;
import org.apache.oozie.dependency.DependencyChecker;
import org.apache.oozie.dependency.URIHandler;
import org.apache.oozie.dependency.URIHandler.DependencyType;
import org.apache.oozie.dependency.URIHandlerException;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.URIHandlerService;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.Pair;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Attribute;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.quartz.CronExpression;
import org.apache.commons.lang.StringUtils;
import org.apache.oozie.CoordinatorJobBean;

public class CoordCommandUtils {
    public static final int CURRENT = 0;
    public static final int LATEST = 1;
    public static final int FUTURE = 2;
    public static final int OFFSET = 3;
    public static final int ABSOLUTE = 4;
    public static final int ENDOFMONTHS = 5;
    public static final int ENDOFWEEKS = 6;
    public static final int ENDOFDAYS = 7;
    public static final int UNEXPECTED = -1;

    public static final String RESOLVED_UNRESOLVED_SEPARATOR = "!!";
    public static final String UNRESOLVED_INSTANCES_TAG = "unresolved-instances";

    /**
     * parse a function like coord:latest(n)/future() and return the 'n'.
     * <p>
     *
     * @param function
     * @param restArg
     * @return int instanceNumber
     * @throws Exception
     */
    public static int getInstanceNumber(String function, StringBuilder restArg) throws Exception {
        int funcType = getFuncType(function);
        if (funcType == ABSOLUTE) {
            return funcType;
        }
        if (funcType == CURRENT || funcType == LATEST || funcType == ENDOFMONTHS || funcType == ENDOFWEEKS
                || funcType == ENDOFDAYS) {
            return parseOneArg(function);
        }
        else {
            return parseMoreArgs(function, restArg);
        }
    }

    /**
     * Evaluates function for coord-action-create-inst tag
     * @param event
     * @param appInst
     * @param conf
     * @param function
     * @return evaluation result
     * @throws Exception
     */
    private static String evaluateInstanceFunction(Element event, SyncCoordAction appInst, Configuration conf,
            String function) throws Exception {
        ELEvaluator eval = CoordELEvaluator.createInstancesELEvaluator("coord-action-create-inst", event, appInst, conf);
        return CoordELFunctions.evalAndWrap(eval, function);
    }

    public static int parseOneArg(String funcName) throws Exception {
        int firstPos = funcName.indexOf("(");
        int lastPos = funcName.lastIndexOf(")");
        if (firstPos >= 0 && lastPos > firstPos) {
            String tmp = funcName.substring(firstPos + 1, lastPos).trim();
            if (tmp.length() > 0) {
                return (int) Double.parseDouble(tmp);
            }
        }
        throw new RuntimeException("Unformatted function :" + funcName);
    }

    public static String parseOneStringArg(String funcName) throws Exception {
        int firstPos = funcName.indexOf("(");
        int lastPos = funcName.lastIndexOf(")");
        if (firstPos >= 0 && lastPos > firstPos) {
            return funcName.substring(firstPos + 1, lastPos).trim();
        }
        throw new RuntimeException("Unformatted function :" + funcName);
    }

    private static int parseMoreArgs(String funcName, StringBuilder restArg) throws Exception {
        int firstPos = funcName.indexOf("(");
        int secondPos = funcName.lastIndexOf(",");
        int lastPos = funcName.lastIndexOf(")");
        if (firstPos >= 0 && secondPos > firstPos) {
            String tmp = funcName.substring(firstPos + 1, secondPos).trim();
            if (tmp.length() > 0) {
                restArg.append(funcName.substring(secondPos + 1, lastPos).trim());
                return (int) Double.parseDouble(tmp);
            }
        }
        throw new RuntimeException("Unformatted function :" + funcName);
    }

    /**
     * @param function EL function name
     * @return type of EL function
     */
    public static int getFuncType(String function) {
        if (function.indexOf("current") >= 0) {
            return CURRENT;
        }
        else if (function.indexOf("latest") >= 0) {
            return LATEST;
        }
        else if (function.indexOf("future") >= 0) {
            return FUTURE;
        }
        else if (function.indexOf("offset") >= 0) {
            return OFFSET;
        }
        else if (function.indexOf("absolute") >= 0) {
            return ABSOLUTE;
        }
        else if (function.indexOf("endOfMonths") >= 0) {
            return ENDOFMONTHS;
        }
        else if (function.indexOf("endOfWeeks") >= 0) {
            return ENDOFWEEKS;
        }
        else if (function.indexOf("endOfDays") >= 0) {
            return ENDOFDAYS;
        }
        return UNEXPECTED;
        // throw new RuntimeException("Unexpected instance name "+ function);
    }

    /**
     * @param startInst EL function name
     * @param endInst EL function name
     * @throws CommandException if both are not the same function
     */
    public static void checkIfBothSameType(String startInst, String endInst) throws CommandException {
        if (getFuncType(startInst) != getFuncType(endInst)) {
            if (getFuncType(startInst) == ABSOLUTE || getFuncType(startInst) == ENDOFMONTHS
                    || getFuncType(startInst) == ENDOFWEEKS || getFuncType(startInst) == ENDOFDAYS) {
                if (getFuncType(endInst) != CURRENT) {
                    throw new CommandException(ErrorCode.E1010,
                            "Only start-instance as absolute/endOfMonths/endOfWeeks/endOfDays and end-instance as current is"
                            + " supported."
                                    + " start = " + startInst + "  end = " + endInst);
                }
            }
            else {
                throw new CommandException(ErrorCode.E1010,
                        " start-instance and end-instance both should be either latest or current or future or offset\n"
                                + " start " + startInst + " and end " + endInst);
            }
        }
    }


    /**
     * Resolve list of &lt;instance&gt; &lt;/instance&gt; tags.
     *
     * @param event
     * @param instances
     * @param actionInst
     * @param conf
     * @param eval ELEvalautor
     * @throws Exception
     */
    public static void resolveInstances(Element event, StringBuilder instances, SyncCoordAction actionInst,
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
     * Resolve &lt;start-instance&gt; &lt;end-insatnce&gt; tag. Don't resolve any
     * latest()/future()
     *
     * @param event
     * @param instances
     * @param appInst
     * @param conf
     * @param eval ELEvalautor
     * @throws Exception
     */
    public static void resolveInstanceRange(Element event, StringBuilder instances, SyncCoordAction appInst,
            Configuration conf, ELEvaluator eval) throws Exception {
        Element eStartInst = event.getChild("start-instance", event.getNamespace());
        Element eEndInst = event.getChild("end-instance", event.getNamespace());
        if (eStartInst != null && eEndInst != null) {
            String strStart = evaluateInstanceFunction(event, appInst, conf, eStartInst.getTextTrim());
            String strEnd = evaluateInstanceFunction(event, appInst, conf, eEndInst.getTextTrim());
            checkIfBothSameType(strStart, strEnd);
            StringBuilder restArg = new StringBuilder(); // To store rest
                                                         // arguments for
                                                         // future
                                                         // function

            int startIndex = getInstanceNumber(strStart, restArg);
            String startRestArg = restArg.toString();
            restArg.delete(0, restArg.length());
            int endIndex = getInstanceNumber(strEnd, restArg);
            String endRestArg = restArg.toString();
            int funcType = getFuncType(strStart);

            if (funcType == ABSOLUTE) {
                resolveAbsoluteRange(event, instances, appInst, conf, eval, strStart, endIndex,
                        parseOneStringArg(strStart));
            }
            else if (funcType == ENDOFMONTHS) {
                resolveInstanceRangeEndOfDuration(TimeUnit.MONTH, event, instances, appInst, conf, eval, strStart,
                        startIndex, endIndex);
            }
            else if (funcType == ENDOFWEEKS) {
                resolveInstanceRangeEndOfDuration(TimeUnit.WEEK, event, instances, appInst, conf, eval, strStart,
                        startIndex, endIndex);
            }
            else if (funcType == ENDOFDAYS) {
                resolveInstanceRangeEndOfDuration(TimeUnit.DAY, event, instances, appInst, conf, eval, strStart,
                        startIndex, endIndex);
            }
            else {
                if (funcType == OFFSET) {
                    TimeUnit startU = TimeUnit.valueOf(startRestArg);
                    TimeUnit endU = TimeUnit.valueOf(endRestArg);
                    if (startU.getCalendarUnit() * startIndex > endU.getCalendarUnit() * endIndex) {
                        throw new CommandException(ErrorCode.E1010,
                                " start-instance should be equal or earlier than the end-instance \n"
                                        + XmlUtils.prettyPrint(event));
                    }
                    Calendar startCal = CoordELFunctions.resolveOffsetRawTime(startIndex, startU, eval);
                    Calendar endCal = CoordELFunctions.resolveOffsetRawTime(endIndex, endU, eval);
                    if (startCal != null && endCal != null) {
                        List<Integer> expandedFreqs = CoordELFunctions.expandOffsetTimes(startCal, endCal, eval);
                        for (int i = expandedFreqs.size() - 1; i >= 0; i--) {
                            //we need to use DS timeout, bcz expandOffsetTimes will expand offset in Freqs in DS timeunit
                            String matInstance = materializeInstance(event, "${coord:offset(" + expandedFreqs.get(i)
                                    + ", \"" + CoordELFunctions.getDSTimeUnit(eval) + "\")}", appInst, conf, eval);
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
                }
                else {
                    if (startIndex > endIndex) {
                        throw new CommandException(ErrorCode.E1010,
                                " start-instance should be equal or earlier than the end-instance \n"
                                        + XmlUtils.prettyPrint(event));
                    }
                    if (funcType == CURRENT) {
                        // Everything could be resolved NOW. no latest() ELs
                        String matInstance = materializeInstance(event, "${coord:currentRange(" + startIndex + ","
                                + endIndex + ")}", appInst, conf, eval);
                        if (matInstance != null && !matInstance.isEmpty()) {
                            if (instances.length() > 0) {
                                instances.append(CoordELFunctions.INSTANCE_SEPARATOR);
                            }
                            instances.append(matInstance);
                        }
                    }

                    else { // latest(n)/future() EL is present
                        if (funcType == LATEST) {
                            instances.append("${coord:latestRange(").append(startIndex).append(",").append(endIndex)
                            .append(")}");
                        }
                        else if (funcType == FUTURE) {
                            instances.append("${coord:futureRange(").append(startIndex).append(",").append(endIndex)
                            .append(",'").append(endRestArg).append("')}");
                        }
                    }
                }
            }
            // Remove start-instance and end-instances
            event.removeChild("start-instance", event.getNamespace());
            event.removeChild("end-instance", event.getNamespace());
        }
    }

    private static void resolveAbsoluteRange(Element event, StringBuilder instances, SyncCoordAction appInst,
            Configuration conf, ELEvaluator eval, String strStart, int endIndex, String rangeStr) throws Exception {
        StringBuffer bf = new StringBuffer();
        bf.append("${coord:absoluteRange(\"").append(rangeStr).append("\",")
                .append(endIndex).append(")}");
        String matInstance = materializeInstance(event, bf.toString(), appInst, conf, eval);
        if (matInstance != null && !matInstance.isEmpty()) {
            if (instances.length() > 0) {
                instances.append(CoordELFunctions.INSTANCE_SEPARATOR);
            }
            instances.append(matInstance);
        }
    }

    private static void resolveInstanceRangeEndOfDuration(TimeUnit duration, Element event, StringBuilder instances,
            SyncCoordAction appInst, Configuration conf, ELEvaluator eval, String strStart, int startIndex,
            int endIndex) throws Exception {
        Calendar startInstance = new StartInstanceFinder(startIndex, duration, CoordELFunctions.getDatasetTZ(eval),
                appInst.getNominalTime()).getStartInstance();
        resolveAbsoluteRange(event, instances, appInst, conf, eval, strStart, endIndex,
                DateUtils.formatDateOozieTZ(startInstance));
    }

    /**
     * Materialize one instance like current(-2)
     *
     * @param event : &lt;data-in&gt;
     * @param expr : instance like current(-1)
     * @param appInst : application specific info
     * @param conf
     * @param evalInst :ELEvaluator
     * @return materialized date string
     * @throws Exception
     */
    public static String materializeInstance(Element event, String expr, SyncCoordAction appInst, Configuration conf,
            ELEvaluator evalInst) throws Exception {
        if (event == null) {
            return null;
        }
        // ELEvaluator eval = CoordELEvaluator.createInstancesELEvaluator(event,
        // appInst, conf);
        return CoordELFunctions.evalAndWrap(evalInst, expr);
    }

    /**
     * Create two new tags with &lt;uris&gt; and &lt;unresolved-instances&gt;.
     *
     * @param event
     * @param instances
     * @throws Exception
     */
    private static String separateResolvedAndUnresolved(Element event, StringBuilder instances)
            throws Exception {
        StringBuilder unresolvedInstances = new StringBuilder();
        StringBuilder urisWithDoneFlag = new StringBuilder();
        StringBuilder depList = new StringBuilder();
        String uris = createEarlyURIs(event, instances.toString(), unresolvedInstances, urisWithDoneFlag);
        if (uris.length() > 0) {
            Element uriInstance = new Element("uris", event.getNamespace());
            uriInstance.addContent(uris);
            event.getContent().add(1, uriInstance);
            if (depList.length() > 0) {
                depList.append(CoordELFunctions.INSTANCE_SEPARATOR);
            }
            depList.append(urisWithDoneFlag);
        }
        if (unresolvedInstances.length() > 0) {
            Element elemInstance = new Element(UNRESOLVED_INSTANCES_TAG, event.getNamespace());
            elemInstance.addContent(unresolvedInstances.toString());
            event.getContent().add(1, elemInstance);
        }
        return depList.toString();
    }

    /**
     * The function create a list of URIs separated by "," using the instances
     * time stamp and URI-template
     *
     * @param event : &lt;data-in&gt; event
     * @param instances : List of time stamp separated by ","
     * @param unresolvedInstances : list of instance with latest function
     * @param urisWithDoneFlag : list of URIs with the done flag appended
     * @return : list of URIs separated by ";" as a string.
     * @throws Exception
     */
    public static String createEarlyURIs(Element event, String instances, StringBuilder unresolvedInstances,
            StringBuilder urisWithDoneFlag) throws Exception {
        if (instances == null || instances.length() == 0) {
            return "";
        }
        String[] instanceList = instances.split(CoordELFunctions.INSTANCE_SEPARATOR);
        StringBuilder uris = new StringBuilder();

        Element doneFlagElement = event.getChild("dataset", event.getNamespace()).getChild("done-flag",
                event.getNamespace());
        URIHandlerService uriService = Services.get().get(URIHandlerService.class);

        for (int i = 0; i < instanceList.length; i++) {
            if (instanceList[i].trim().length() == 0) {
                continue;
            }
            int funcType = getFuncType(instanceList[i]);
            if (funcType == LATEST || funcType == FUTURE) {
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
            URIHandler uriHandler = uriService.getURIHandler(uriPath);
            uriHandler.validate(uriPath);
            uris.append(uriPath);
            urisWithDoneFlag.append(uriHandler.getURIWithDoneFlag(uriPath, CoordUtils.getDoneFlag(doneFlagElement)));
        }
        return uris.toString();
    }

    /**
     * @param eAction
     * @param coordAction
     * @param conf
     * @return boolean to determine whether the SLA element is present or not
     * @throws CoordinatorJobException
     */
    public static boolean materializeSLA(Element eAction, CoordinatorActionBean coordAction, Configuration conf)
            throws CoordinatorJobException {
        Element eSla = eAction.getChild("action", eAction.getNamespace()).getChild("info", eAction.getNamespace("sla"));
        if (eSla == null) {
            // eAppXml.getNamespace("sla"));
            return false;
        }
        try {
            ELEvaluator evalSla = CoordELEvaluator.createSLAEvaluator(eAction, coordAction, conf);
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

    /**
     * Materialize one instance for specific nominal time. It includes: 1.
     * Materialize data events (i.e. &lt;data-in&gt; and &lt;data-out&gt;) 2. Materialize
     * data properties (i.e dataIn(&lt;DS&gt;) and dataOut(&lt;DS&gt;) 3. remove 'start' and
     * 'end' tag 4. Add 'instance_number' and 'nominal-time' tag
     *
     * @param jobId coordinator job id
     * @param dryrun true if it is dryrun
     * @param eAction frequency unexploded-job
     * @param nominalTime materialization time
     * @param actualTime action actual time
     * @param instanceCount instance numbers
     * @param conf job configuration
     * @param actionBean CoordinatorActionBean to materialize
     * @return one materialized action for specific nominal time
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public static String materializeOneInstance(String jobId, boolean dryrun, Element eAction, Date nominalTime,
            Date actualTime, int instanceCount, Configuration conf, CoordinatorActionBean actionBean) throws Exception {
        String actionId = Services.get().get(UUIDService.class).generateChildId(jobId, instanceCount + "");
        SyncCoordAction appInst = new SyncCoordAction();
        appInst.setActionId(actionId);
        appInst.setName(eAction.getAttributeValue("name"));
        appInst.setNominalTime(nominalTime);
        appInst.setActualTime(actualTime);
        String frequency = eAction.getAttributeValue("frequency");
        appInst.setFrequency(frequency);
        appInst.setTimeUnit(TimeUnit.valueOf(eAction.getAttributeValue("freq_timeunit")));
        appInst.setTimeZone(DateUtils.getTimeZone(eAction.getAttributeValue("timezone")));
        appInst.setEndOfDuration(TimeUnit.valueOf(eAction.getAttributeValue("end_of_duration")));

        boolean isInputLogicSpecified = CoordUtils.isInputLogicSpecified(eAction);

        Element inputList = eAction.getChild("input-events", eAction.getNamespace());
        List<Element> dataInList = null;
        if (inputList != null) {
            dataInList = inputList.getChildren("data-in", eAction.getNamespace());
            materializeInputDataEvents(dataInList, appInst, conf, actionBean, isInputLogicSpecified);
        }

        if(isInputLogicSpecified){
            evaluateInputCheck(eAction.getChild(CoordInputLogicEvaluator.INPUT_LOGIC, eAction.getNamespace()),
                    CoordELEvaluator.createDataEvaluator(eAction, conf, actionId));
        }
        Element outputList = eAction.getChild("output-events", eAction.getNamespace());
        List<Element> dataOutList = null;
        if (outputList != null) {
            dataOutList = outputList.getChildren("data-out", eAction.getNamespace());
            materializeOutputDataEvents(dataOutList, appInst, conf);
        }

        eAction.removeAttribute("start");
        eAction.removeAttribute("end");
        eAction.setAttribute("instance-number", Integer.toString(instanceCount));
        eAction.setAttribute("action-nominal-time", DateUtils.formatDateOozieTZ(nominalTime));
        eAction.setAttribute("action-actual-time", DateUtils.formatDateOozieTZ(actualTime));

        // Setting up action bean
        actionBean.setCreatedConf(XmlUtils.prettyPrint(conf).toString());
        actionBean.setRunConf(XmlUtils.prettyPrint(conf).toString());
        actionBean.setCreatedTime(actualTime);
        actionBean.setJobId(jobId);
        actionBean.setId(actionId);
        actionBean.setLastModifiedTime(new Date());
        actionBean.setStatus(CoordinatorAction.Status.WAITING);
        actionBean.setActionNumber(instanceCount);
        actionBean.setNominalTime(nominalTime);
        boolean isSla = CoordCommandUtils.materializeSLA(eAction, actionBean, conf);
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
            return dryRunCoord(eAction, actionBean);
        }
    }


    /**
     * @param eAction the actionXml related element
     * @param actionBean the coordinator action bean
     * @return actionXml returns actionXml as String
     * @throws Exception
     */
    static String dryRunCoord(Element eAction, CoordinatorActionBean actionBean) throws Exception {
        String action = XmlUtils.prettyPrint(eAction).toString();
        StringBuilder actionXml = new StringBuilder(action);
        Configuration actionConf = new XConfiguration(new StringReader(actionBean.getRunConf()));
        actionBean.setActionXml(action);

        if (CoordUtils.isInputLogicSpecified(eAction)) {
            new CoordInputLogicEvaluatorUtil(actionBean).validateInputLogic();
        }

        boolean isPushDepAvailable = true;
        String pushMissingDependencies = actionBean.getPushInputDependencies().getMissingDependencies();
        if (pushMissingDependencies != null) {
            ActionDependency actionDependencies = DependencyChecker.checkForAvailability(pushMissingDependencies,
                    actionConf, true);
            if (actionDependencies.getMissingDependencies().size() != 0) {
                isPushDepAvailable = false;
            }

        }
        boolean isPullDepAvailable = true;
        CoordActionInputCheckXCommand coordActionInput = new CoordActionInputCheckXCommand(actionBean.getId(),
                actionBean.getJobId());
        if (actionBean.getMissingDependencies() != null) {
            StringBuilder existList = new StringBuilder();
            StringBuilder nonExistList = new StringBuilder();
            StringBuilder nonResolvedList = new StringBuilder();
            getResolvedList(actionBean.getPullInputDependencies().getMissingDependencies(), nonExistList, nonResolvedList);
            isPullDepAvailable = actionBean.getPullInputDependencies().checkPullMissingDependencies(actionBean,
                    existList, nonExistList);

        }

        if (isPullDepAvailable && isPushDepAvailable) {
            // Check for latest/future
            boolean isLatestFutureDepAvailable = coordActionInput.checkUnResolvedInput(actionBean, actionXml,
                    actionConf);
            if (isLatestFutureDepAvailable) {
                String newActionXml = CoordActionInputCheckXCommand.resolveCoordConfiguration(actionXml, actionConf,
                        actionBean.getId());
                actionXml.replace(0, actionXml.length(), newActionXml);
            }
        }

        return actionXml.toString();
    }

    /**
     * Materialize all &lt;input-events&gt;/&lt;data-in&gt; or &lt;output-events&gt;/&lt;data-out&gt;
     * tags Create uris for resolved instances. Create unresolved instance for
     * latest()/future().
     *
     * @param events
     * @param appInst
     * @param conf
     * @throws Exception
     */
    private static void materializeOutputDataEvents(List<Element> events, SyncCoordAction appInst, Configuration conf)
            throws Exception {

        if (events == null) {
            return;
        }

        for (Element event : events) {
            StringBuilder instances = new StringBuilder();
            ELEvaluator eval = CoordELEvaluator.createInstancesELEvaluator(event, appInst, conf);
            // Handle list of instance tag
            resolveInstances(event, instances, appInst, conf, eval);
            // Handle start-instance and end-instance
            resolveInstanceRange(event, instances, appInst, conf, eval);
            // Separate out the unresolved instances
            separateResolvedAndUnresolved(event, instances);

        }
    }

    private static void evaluateInputCheck(Element root, ELEvaluator evalInputLogic) throws Exception {
        for (Object event : root.getChildren()) {
            Element inputElement = (Element) event;

            resolveAttribute("dataset", inputElement, evalInputLogic);
            resolveAttribute("name", inputElement, evalInputLogic);
            resolveAttribute("min", inputElement, evalInputLogic);
            resolveAttribute("wait", inputElement, evalInputLogic);
            if (!inputElement.getChildren().isEmpty()) {
                evaluateInputCheck(inputElement, evalInputLogic);
            }
        }
    }

    private static String resolveAttribute(String attrName, Element elem, ELEvaluator eval) throws CoordinatorJobException {
        Attribute attr = elem.getAttribute(attrName);
        String val = null;
        if (attr != null) {
            try {
                val = CoordELFunctions.evalAndWrap(eval, attr.getValue().trim());
            }
            catch (Exception e) {
                throw new CoordinatorJobException(ErrorCode.E1004, e.getMessage(), e);
            }
            attr.setValue(val);
        }
        return val;
    }

    public static void materializeInputDataEvents(List<Element> events, SyncCoordAction appInst, Configuration conf,
            CoordinatorActionBean actionBean, boolean isInputLogicSpecified) throws Exception {

        if (events == null) {
            return;
        }
        CoordInputDependency coordPullInputDependency = CoordInputDependencyFactory
                .createPullInputDependencies(isInputLogicSpecified);
        CoordInputDependency coordPushInputDependency = CoordInputDependencyFactory
                .createPushInputDependencies(isInputLogicSpecified);
        List<Pair<String, String>> unresolvedList = new ArrayList<Pair<String, String>>();

        URIHandlerService uriService = Services.get().get(URIHandlerService.class);

        for (Element event : events) {
            StringBuilder instances = new StringBuilder();
            ELEvaluator eval = CoordELEvaluator.createInstancesELEvaluator(event, appInst, conf);
            // Handle list of instance tag
            resolveInstances(event, instances, appInst, conf, eval);
            // Handle start-instance and end-instance
            resolveInstanceRange(event, instances, appInst, conf, eval);
            // Separate out the unresolved instances
            String resolvedList = separateResolvedAndUnresolved(event, instances);
            String name = event.getAttribute("name").getValue();

            if (!resolvedList.isEmpty()) {
                Element uri = event.getChild("dataset", event.getNamespace()).getChild("uri-template",
                        event.getNamespace());

                String uriTemplate = uri.getText();
                URI baseURI = uriService.getAuthorityWithScheme(uriTemplate);
                URIHandler handler = uriService.getURIHandler(baseURI);
                List<CoordInputInstance> inputInstanceList = new ArrayList<CoordInputInstance>();

                for (String inputInstance : resolvedList.split("#")) {
                    inputInstanceList.add(new CoordInputInstance(inputInstance, false));
                }

                if (handler.getDependencyType(baseURI).equals(DependencyType.PULL)) {
                    coordPullInputDependency.addInputInstanceList(name, inputInstanceList);
                }
                else {
                    coordPushInputDependency.addInputInstanceList(name, inputInstanceList);

                }
            }

            String tmpUnresolved = event.getChildTextTrim(UNRESOLVED_INSTANCES_TAG, event.getNamespace());
            if (tmpUnresolved != null) {
                unresolvedList.add(new Pair<String,String>(name, tmpUnresolved));
            }
        }
        for (Pair<String, String> unresolvedDataset : unresolvedList) {
            coordPullInputDependency.addUnResolvedList(unresolvedDataset.getFirst(), unresolvedDataset.getSecond());
        }
        actionBean.setPullInputDependencies(coordPullInputDependency);
        actionBean.setPushInputDependencies(coordPushInputDependency);
        actionBean.setMissingDependencies(coordPullInputDependency.serialize());
        actionBean.setPushMissingDependencies(coordPushInputDependency.serialize());

    }
    /**
     * Get resolved string from missDepList
     *
     * @param missDepList
     * @param resolved
     * @param unresolved
     * @return resolved string
     */
    public static String getResolvedList(String missDepList, StringBuilder resolved, StringBuilder unresolved) {
        if (missDepList != null) {
            int index = missDepList.indexOf(RESOLVED_UNRESOLVED_SEPARATOR);
            if (index < 0) {
                resolved.append(missDepList);
            }
            else {
                resolved.append(missDepList.substring(0, index));
                unresolved.append(missDepList.substring(index + RESOLVED_UNRESOLVED_SEPARATOR.length()));
            }
        }
        return resolved.toString();
    }

    /**
     * Get the next action time after a given time
     *
     * @param targetDate
     * @param coordJob
     * @return the next valid action time
     */
    public static Date getNextValidActionTimeForCronFrequency(Date targetDate, CoordinatorJobBean coordJob) throws ParseException {

        String freq = coordJob.getFrequency();
        TimeZone tz = DateUtils.getOozieProcessingTimeZone();
        String[] cronArray = freq.split(" ");
        Date nextTime = null;

        // Current CronExpression doesn't support operations
        // where both date of months and day of weeks are specified.
        // As a result, we need to split this scenario into two cases
        // and return the earlier time
        if (!cronArray[2].trim().equals("?") && !cronArray[4].trim().equals("?")) {

            // When any one of day of month or day of week fields is a wildcard
            // we need to replace the wildcard with "?"
            if (cronArray[2].trim().equals("*") || cronArray[4].trim().equals("*")) {
                if (cronArray[2].trim().equals("*")) {
                    cronArray[2] = "?";
                }
                else {
                    cronArray[4] = "?";
                }
                freq= StringUtils.join(cronArray, " ");

                // The cronExpression class takes second
                // as the first field where oozie is operating on
                // minute basis
                CronExpression expr = new CronExpression("0 " + freq);
                expr.setTimeZone(tz);
                nextTime = expr.getNextValidTimeAfter(targetDate);
            }
            // If both fields are specified by non-wildcards,
            // we need to split it into two expressions
            else {
                String[] cronArray1 = freq.split(" ");
                String[] cronArray2 = freq.split(" ");

                cronArray1[2] = "?";
                cronArray2[4] = "?";

                String freq1 = StringUtils.join(cronArray1, " ");
                String freq2 = StringUtils.join(cronArray2, " ");

                // The cronExpression class takes second
                // as the first field where oozie is operating on
                // minute basis
                CronExpression expr1 = new CronExpression("0 " + freq1);
                expr1.setTimeZone(tz);
                CronExpression expr2 = new CronExpression("0 " + freq2);
                expr2.setTimeZone(tz);
                nextTime = expr1.getNextValidTimeAfter(targetDate);
                Date nextTime2 = expr2.getNextValidTimeAfter(targetDate);
                nextTime = nextTime.compareTo(nextTime2) < 0 ? nextTime: nextTime2;
            }
        }
        else {
            // The cronExpression class takes second
            // as the first field where oozie is operating on
            // minute basis
            CronExpression expr  = new CronExpression("0 " + freq);
            expr.setTimeZone(tz);
            nextTime = expr.getNextValidTimeAfter(targetDate);
        }

        return nextTime;
    }

    /**
     * Computes the nominal time of the next action.
     * Based on CoordMaterializeTransitionXCommand#materializeActions
     *
     * The Coordinator Job needs to have the frequency, time unit, time zone, start time, end time, and job xml.
     * The Coordinator Action needs to have the nominal time and action number.
     *
     * @param coordJob The Coordinator Job
     * @param coordAction The Coordinator Action
     * @return the nominal time of the next action
     * @throws ParseException
     * @throws JDOMException
     */
    public static Date computeNextNominalTime(CoordinatorJobBean coordJob, CoordinatorActionBean coordAction)
            throws ParseException, JDOMException {
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
            TimeZone appTz = DateUtils.getTimeZone(coordJob.getTimeZone());
            Calendar nextNominalTimeCal = Calendar.getInstance(appTz);
            nextNominalTimeCal.setTime(coordJob.getStartTimestamp());
            TimeUnit freqTU = TimeUnit.valueOf(coordJob.getTimeUnitStr());
            // Action Number is indexed by 1, so no need to +1 here
            nextNominalTimeCal.add(freqTU.getCalendarUnit(), coordAction.getActionNumber() * freq);
            String jobXml = coordJob.getJobXml();
            Element eJob = XmlUtils.parseXml(jobXml);
            TimeUnit endOfFlag = TimeUnit.valueOf(eJob.getAttributeValue("end_of_duration"));
            // Move to the End of duration, if needed.
            DateUtils.moveToEnd(nextNominalTimeCal, endOfFlag);
            nextNominalTime = nextNominalTimeCal.getTime();
        }

        // If the next nominal time is after the job's end time, then this is the last action, so return null
        if (nextNominalTime.after(coordJob.getEndTime())) {
            nextNominalTime = null;
        }
        return nextNominalTime;
    }

    public static boolean pathExists(String sPath, Configuration actionConf, String user) throws IOException,
            URISyntaxException, URIHandlerException {
        URI uri = new URI(sPath);
        URIHandlerService service = Services.get().get(URIHandlerService.class);
        URIHandler handler = service.getURIHandler(uri);
        return handler.exists(uri, actionConf, user);
    }

    public static boolean pathExists(String sPath, Configuration actionConf) throws IOException, URISyntaxException,
            URIHandlerException {
        String user = ParamChecker.notEmpty(actionConf.get(OozieClient.USER_NAME), OozieClient.USER_NAME);
        return pathExists(sPath, actionConf, user);
    }

    public static String getFirstMissingDependency(CoordinatorActionBean coordAction) {
        CoordInputDependency coordPullInputDependency = coordAction.getPullInputDependencies();
        CoordInputDependency coordPushInputDependency = coordAction.getPushInputDependencies();
        String firstMissingDependencies = coordPullInputDependency.getFirstMissingDependency();
        if (StringUtils.isEmpty(firstMissingDependencies) || firstMissingDependencies.trim().startsWith("${coord:")) {
            firstMissingDependencies = coordPushInputDependency.getFirstMissingDependency();
        }
        return firstMissingDependencies;
    }

    /**
     * Class to find get start instance
     */
    private static class StartInstanceFinder {

        private int startIndex;
        private TimeUnit timeUnit;
        private TimeZone datasetTimeZone;
        private Date nominalTime;

        /**
         * @param startIndex dataset index
         * @param timeUnit
         * @param datasetTimeZone
         * @param nominalTime nominal time of action
         */
        public StartInstanceFinder(int startIndex, TimeUnit timeUnit, TimeZone datasetTimeZone, Date nominalTime) {
            this.startIndex = startIndex;
            this.timeUnit = timeUnit;
            this.datasetTimeZone = datasetTimeZone;
            this.nominalTime = nominalTime;
        }

        /**
         * Calculates the start instance. It put the start instance to the start
         * of a day i.e. 00:00:00.
         */
        public Calendar getStartInstance() throws Exception {
            Calendar startInstance = Calendar.getInstance(datasetTimeZone);
            startInstance.setTime(nominalTime);
            startInstance.set(Calendar.HOUR_OF_DAY, 0);
            startInstance.set(Calendar.MINUTE, 0);
            startInstance.set(Calendar.SECOND, 0);
            switch (timeUnit) {
                case WEEK:
                    startInstance.set(Calendar.DAY_OF_WEEK, startInstance.getFirstDayOfWeek());
                    startInstance.add(Calendar.WEEK_OF_YEAR, startIndex + 1);
                    break;
                case MONTH:
                    int FIRST_DAY_OF_MONTH = 1;
                    startInstance.set(Calendar.DAY_OF_MONTH, FIRST_DAY_OF_MONTH);
                    startInstance.add(Calendar.MONTH, startIndex + 1);
                    break;
                case DAY:
                    startInstance.add(Calendar.DATE, startIndex + 1);
                    break;
                default:
            }
            return startInstance;
        }
    }

}
