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

import java.io.StringReader;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.coord.CoordELEvaluator;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.coord.CoordUtils;
import org.apache.oozie.coord.CoordinatorJobException;
import org.apache.oozie.coord.SyncCoordAction;
import org.apache.oozie.coord.TimeUnit;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;

public class CoordCommandUtils {
    public static int CURRENT = 0;
    public static int LATEST = 1;
    public static int FUTURE = 2;
    public static int UNEXPECTED = -1;
    public static final String RESOLVED_UNRESOLVED_SEPARATOR = ";";

    /**
     * parse a function like coord:latest(n)/future() and return the 'n'.
     * <p/>
     * @param function
     * @param event
     * @param appInst
     * @param conf
     * @param restArg
     * @return int instanceNumber
     * @throws Exception
     */
    public static int getInstanceNumber(String function, Element event, SyncCoordAction appInst, Configuration conf,
            StringBuilder restArg) throws Exception {
        ELEvaluator eval = CoordELEvaluator
                .createInstancesELEvaluator("coord-action-create-inst", event, appInst, conf);
        String newFunc = CoordELFunctions.evalAndWrap(eval, function);
        int funcType = getFuncType(newFunc);
        if (funcType == CURRENT || funcType == LATEST) {
            return parseOneArg(newFunc);
        }
        else {
            return parseMoreArgs(newFunc, restArg);
        }
    }

    private static int parseOneArg(String funcName) throws Exception {
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
     * @param EL function name
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
        return UNEXPECTED;
        // throw new RuntimeException("Unexpected instance name "+ function);
    }

    /**
     * @param startInst: EL function name
     * @param endInst: EL function name
     * @throws CommandException if both are not the same function
     */
    public static void checkIfBothSameType(String startInst, String endInst) throws CommandException {
        if (getFuncType(startInst) != getFuncType(endInst)) {
            throw new CommandException(ErrorCode.E1010,
                    " start-instance and end-instance both should be either latest or current or future\n"
                            + " start " + startInst + " and end " + endInst);
        }
    }

    /**
     * Resolve list of <instance> </instance> tags.
     *
     * @param event
     * @param instances
     * @param actionInst
     * @param conf
     * @param eval: ELEvalautor
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
     * Resolve <start-instance> <end-insatnce> tag. Don't resolve any
     * latest()/future()
     *
     * @param event
     * @param instances
     * @param appInst
     * @param conf
     * @param eval: ELEvalautor
     * @throws Exception
     */
    public static void resolveInstanceRange(Element event, StringBuilder instances, SyncCoordAction appInst,
            Configuration conf, ELEvaluator eval) throws Exception {
        Element eStartInst = event.getChild("start-instance", event.getNamespace());
        Element eEndInst = event.getChild("end-instance", event.getNamespace());
        if (eStartInst != null && eEndInst != null) {
            String strStart = eStartInst.getTextTrim();
            String strEnd = eEndInst.getTextTrim();
            checkIfBothSameType(strStart, strEnd);
            StringBuilder restArg = new StringBuilder(); // To store rest
                                                         // arguments for
                                                         // future
                                                         // function
            int startIndex = getInstanceNumber(strStart, event, appInst, conf, restArg);
            restArg.delete(0, restArg.length());
            int endIndex = getInstanceNumber(strEnd, event, appInst, conf, restArg);
            if (startIndex > endIndex) {
                throw new CommandException(ErrorCode.E1010,
                        " start-instance should be equal or earlier than the end-instance \n"
                                + XmlUtils.prettyPrint(event));
            }
            int funcType = getFuncType(strStart);
            if (funcType == CURRENT) {
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
            else { // latest(n)/future() EL is present
                for (; startIndex <= endIndex; startIndex++) {
                    if (instances.length() > 0) {
                        instances.append(CoordELFunctions.INSTANCE_SEPARATOR);
                    }
                    if (funcType == LATEST) {
                        instances.append("${coord:latest(" + startIndex + ")}");
                    }
                    else { // For future
                        instances.append("${coord:future(" + startIndex + ",'" + restArg + "')}");
                    }
                }
            }
            // Remove start-instance and end-instances
            event.removeChild("start-instance", event.getNamespace());
            event.removeChild("end-instance", event.getNamespace());
        }
    }

    /**
     * Materialize one instance like current(-2)
     *
     * @param event : <data-in>
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
     * Create two new tags with <uris> and <unresolved-instances>.
     *
     * @param event
     * @param instances
     * @param dependencyList
     * @throws Exception
     */
    public static void separateResolvedAndUnresolved(Element event, StringBuilder instances, StringBuffer dependencyList)
            throws Exception {
        StringBuilder unresolvedInstances = new StringBuilder();
        StringBuilder urisWithDoneFlag = new StringBuilder();
        String uris = createEarlyURIs(event, instances.toString(), unresolvedInstances, urisWithDoneFlag);
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
     * The function create a list of URIs separated by "," using the instances
     * time stamp and URI-template
     *
     * @param event : <data-in> event
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
        String doneFlag = CoordUtils.getDoneFlag(doneFlagElement);

        for (int i = 0; i < instanceList.length; i++) {
            if(instanceList[i].trim().length() == 0) {
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
            uris.append(uriPath);
            if (doneFlag.length() > 0) {
                uriPath += "/" + doneFlag;
            }
            urisWithDoneFlag.append(uriPath);
        }
        return uris.toString();
    }

    /**
     * @param eSla
     * @param nominalTime
     * @param conf
     * @return boolean to determine whether the SLA element is present or not
     * @throws CoordinatorJobException
     */
    public static boolean materializeSLA(Element eSla, Date nominalTime, Configuration conf)
            throws CoordinatorJobException {
        if (eSla == null) {
            // eAppXml.getNamespace("sla"));
            return false;
        }
        try {
            ELEvaluator evalSla = CoordELEvaluator.createSLAEvaluator(nominalTime, conf);
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
     * Materialize data events (i.e. <data-in> and <data-out>) 2. Materialize
     * data properties (i.e dataIn(<DS>) and dataOut(<DS>) 3. remove 'start' and
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
        int frequency = Integer.parseInt(eAction.getAttributeValue("frequency"));
        appInst.setFrequency(frequency);
        appInst.setTimeUnit(TimeUnit.valueOf(eAction.getAttributeValue("freq_timeunit")));
        appInst.setTimeZone(DateUtils.getTimeZone(eAction.getAttributeValue("timezone")));
        appInst.setEndOfDuration(TimeUnit.valueOf(eAction.getAttributeValue("end_of_duration")));

        StringBuffer dependencyList = new StringBuffer();

        Element inputList = eAction.getChild("input-events", eAction.getNamespace());
        List<Element> dataInList = null;
        if (inputList != null) {
            dataInList = inputList.getChildren("data-in", eAction.getNamespace());
            materializeDataEvents(dataInList, appInst, conf, dependencyList);
        }

        Element outputList = eAction.getChild("output-events", eAction.getNamespace());
        List<Element> dataOutList = null;
        if (outputList != null) {
            dataOutList = outputList.getChildren("data-out", eAction.getNamespace());
            StringBuffer tmp = new StringBuffer();
            // no dependency checks
            materializeDataEvents(dataOutList, appInst, conf, tmp);
        }

        eAction.removeAttribute("start");
        eAction.removeAttribute("end");
        eAction.setAttribute("instance-number", Integer.toString(instanceCount));
        eAction.setAttribute("action-nominal-time", DateUtils.formatDateUTC(nominalTime));
        eAction.setAttribute("action-actual-time", DateUtils.formatDateUTC(actualTime));

        boolean isSla = CoordCommandUtils.materializeSLA(eAction.getChild("action", eAction.getNamespace()).getChild(
                "info", eAction.getNamespace("sla")), nominalTime, conf);

        // Setting up action bean
        actionBean.setCreatedConf(XmlUtils.prettyPrint(conf).toString());
        actionBean.setRunConf(XmlUtils.prettyPrint(conf).toString());
        actionBean.setCreatedTime(actualTime);
        actionBean.setJobId(jobId);
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
            CoordActionInputCheckXCommand coordActionInput = new CoordActionInputCheckXCommand(actionBean.getId(), actionBean.getJobId());
            StringBuilder actionXml = new StringBuilder(action);
            StringBuilder existList = new StringBuilder();
            StringBuilder nonExistList = new StringBuilder();
            StringBuilder nonResolvedList = new StringBuilder();
            getResolvedList(actionBean.getMissingDependencies(), nonExistList, nonResolvedList);
            Configuration actionConf = new XConfiguration(new StringReader(actionBean.getRunConf()));
            coordActionInput.checkInput(actionXml, existList, nonExistList, actionConf);
            return actionXml.toString();
        }
    }

    /**
     * Materialize all <input-events>/<data-in> or <output-events>/<data-out>
     * tags Create uris for resolved instances. Create unresolved instance for
     * latest()/future().
     *
     * @param events
     * @param appInst
     * @param conf
     * @throws Exception
     */
    public static void materializeDataEvents(List<Element> events, SyncCoordAction appInst, Configuration conf,
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
                unresolved.append(missDepList.substring(index + 1));
            }
        }
        return resolved.toString();
    }

}
