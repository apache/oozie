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

package org.apache.oozie.coord.input.dependency;

import java.io.IOException;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.coord.CoordCommandUtils;
import org.apache.oozie.coord.CoordELConstants;
import org.apache.oozie.coord.CoordELEvaluator;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.coord.CoordUtils;
import org.apache.oozie.dependency.ActionDependency;
import org.apache.oozie.dependency.DependencyChecker;
import org.apache.oozie.dependency.URIHandler;
import org.apache.oozie.dependency.URIHandlerException;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.URIHandlerService;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.JDOMException;

/**
 * Old approach where dependencies are stored as String.
 */
public class CoordOldInputDependency implements CoordInputDependency {

    private XLog log = XLog.getLog(getClass());

    protected transient String missingDependencies = "";

    public CoordOldInputDependency(String missingDependencies) {
        this.missingDependencies = missingDependencies;
    }

    public CoordOldInputDependency() {
    }

    @Override
    public void addInputInstanceList(String inputEventName, List<CoordInputInstance> inputInstanceList) {
        appendToDependencies(inputInstanceList);
    }

    @Override
    public String getMissingDependencies() {
        return missingDependencies;
    }

    @Override
    public boolean isDependencyMet() {
        return StringUtils.isEmpty(missingDependencies);
    }

    @Override
    public boolean isUnResolvedDependencyMet() {
        return false;
    }

    @Override
    public void setDependencyMet(boolean isDependencyMeet) {
        if (isDependencyMeet) {
            missingDependencies = "";
        }

    }

    @Override
    public String serialize() throws IOException {
        return missingDependencies;
    }

    @Override
    public List<String> getMissingDependenciesAsList() {
        return Arrays.asList(DependencyChecker.dependenciesAsArray(missingDependencies));
    }

    @Override
    public List<String> getAvailableDependenciesAsList() {
        return new ArrayList<String>();
    }

    @Override
    public void setMissingDependencies(String missingDependencies) {
        this.missingDependencies = missingDependencies;

    }

    public void appendToDependencies(List<CoordInputInstance> inputInstanceList) {
        StringBuilder sb = new StringBuilder(missingDependencies);
        boolean isFirst = true;
        for (CoordInputInstance coordInputInstance : inputInstanceList) {
            if (isFirst) {
                if (!StringUtils.isEmpty(sb.toString())) {
                    sb.append(CoordELFunctions.INSTANCE_SEPARATOR);
                }
            }
            else {
                sb.append(CoordELFunctions.INSTANCE_SEPARATOR);

            }
            sb.append(coordInputInstance.getInputDataInstance());
            isFirst = false;
        }
        missingDependencies = sb.toString();
    }

    @Override
    public void addUnResolvedList(String name, String unresolvedDependencies) {
        StringBuilder sb = new StringBuilder(missingDependencies);
        sb.append(CoordCommandUtils.RESOLVED_UNRESOLVED_SEPARATOR).append(unresolvedDependencies);
        missingDependencies = sb.toString();
    }

    @Override
    public List<String> getAvailableDependencies(String dataSet) {
        return null;
    }

    @Override
    public void addToAvailableDependencies(Collection<String> availableList) {

        if (StringUtils.isEmpty(missingDependencies)) {
            return;
        }
        List<String> missingDependenciesList = new ArrayList<String>(
                Arrays.asList((DependencyChecker.dependenciesAsArray(missingDependencies))));
        missingDependenciesList.removeAll(availableList);
        missingDependencies = DependencyChecker.dependenciesAsString(missingDependenciesList);

    }

    @Override
    public boolean checkPullMissingDependencies(CoordinatorActionBean coordAction, StringBuilder existList,
            StringBuilder nonExistList) throws IOException, JDOMException {
        Configuration actionConf = new XConfiguration(new StringReader(coordAction.getRunConf()));
        Element eAction = XmlUtils.parseXml(coordAction.getActionXml());

        Element inputList = eAction.getChild("input-events", eAction.getNamespace());
        if (inputList != null) {
            if (nonExistList.length() > 0) {
                checkListOfPaths(coordAction, existList, nonExistList, actionConf);
            }
            return nonExistList.length() == 0;
        }
        return true;
    }

    public ActionDependency checkPushMissingDependencies(CoordinatorActionBean coordAction,
            boolean registerForNotification) throws CommandException, IOException {
        return DependencyChecker.checkForAvailability(getMissingDependenciesAsList(),
                new XConfiguration(new StringReader(coordAction.getRunConf())), !registerForNotification);
    }

    private boolean checkListOfPaths(CoordinatorActionBean coordAction, StringBuilder existList,
            StringBuilder nonExistList, Configuration conf) throws IOException {

        String[] uriList = nonExistList.toString().split(CoordELFunctions.INSTANCE_SEPARATOR);
        if (uriList[0] != null) {
            log.info("[" + coordAction.getId() + "]::ActionInputCheck:: In checkListOfPaths: " + uriList[0]
                    + " is Missing.");
        }

        nonExistList.delete(0, nonExistList.length());
        boolean allExists = true;
        String existSeparator = "", nonExistSeparator = "";
        String user = ParamChecker.notEmpty(conf.get(OozieClient.USER_NAME), OozieClient.USER_NAME);
        for (int i = 0; i < uriList.length; i++) {
            if (allExists) {
                allExists = pathExists(coordAction, uriList[i], conf, user);
                log.info("[" + coordAction.getId() + "]::ActionInputCheck:: File:" + uriList[i] + ", Exists? :"
                        + allExists);
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

    public boolean pathExists(CoordinatorActionBean coordAction, String sPath, Configuration actionConf, String user)
            throws IOException {
        log.debug("checking for the file " + sPath);
        try {
            return CoordCommandUtils.pathExists(sPath, actionConf, user);
        }
        catch (URIHandlerException e) {
            if (coordAction != null) {
                coordAction.setErrorCode(e.getErrorCode().toString());
                coordAction.setErrorMessage(e.getMessage());
            }
            if (e.getCause() != null && e.getCause() instanceof AccessControlException) {
                throw (AccessControlException) e.getCause();
            }
            else {
                log.error(e);
                throw new IOException(e);
            }
        }
        catch (URISyntaxException e) {
            if (coordAction != null) {
                coordAction.setErrorCode(ErrorCode.E0906.toString());
                coordAction.setErrorMessage(e.getMessage());
            }
            log.error(e);
            throw new IOException(e);
        }
    }

    public boolean isChangeInDependency(StringBuilder nonExistList, String missingDependencies,
            StringBuilder nonResolvedList, boolean status) {
        if ((!nonExistList.toString().equals(missingDependencies) || missingDependencies.isEmpty())) {
            setMissingDependencies(nonExistList.toString());
            return true;
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    public boolean checkUnresolved(CoordinatorActionBean coordAction, Element eAction) throws Exception {
        Date nominalTime = DateUtils.parseDateOozieTZ(eAction.getAttributeValue("action-nominal-time"));
        String actualTimeStr = eAction.getAttributeValue("action-actual-time");
        Element inputList = eAction.getChild("input-events", eAction.getNamespace());

        if (inputList == null) {
            return true;
        }

        List<Element> eDataEvents = inputList.getChildren("data-in", eAction.getNamespace());
        Configuration actionConf = new XConfiguration(new StringReader(coordAction.getRunConf()));

        if (eDataEvents != null) {
            Date actualTime = null;
            if (actualTimeStr == null) {
                actualTime = new Date();
            }
            else {
                actualTime = DateUtils.parseDateOozieTZ(actualTimeStr);
            }

            for (Element dEvent : eDataEvents) {
                if (dEvent.getChild(CoordCommandUtils.UNRESOLVED_INSTANCES_TAG, dEvent.getNamespace()) == null) {
                    continue;
                }
                ELEvaluator eval = CoordELEvaluator.createLazyEvaluator(actualTime, nominalTime, dEvent, actionConf);
                String unResolvedInstance = dEvent
                        .getChild(CoordCommandUtils.UNRESOLVED_INSTANCES_TAG, dEvent.getNamespace()).getTextTrim();
                String unresolvedList[] = unResolvedInstance.split(CoordELFunctions.INSTANCE_SEPARATOR);
                StringBuffer resolvedTmp = new StringBuffer();
                for (int i = 0; i < unresolvedList.length; i++) {
                    String returnData = CoordELFunctions.evalAndWrap(eval, unresolvedList[i]);
                    Boolean isResolved = (Boolean) eval.getVariable(CoordELConstants.IS_RESOLVED);
                    if (isResolved == false) {
                        log.info("[" + coordAction.getId() + "] :: Cannot resolve : " + returnData);
                        return false;
                    }
                    if (resolvedTmp.length() > 0) {
                        resolvedTmp.append(CoordELFunctions.INSTANCE_SEPARATOR);
                    }
                    resolvedTmp.append((String) eval.getVariable(CoordELConstants.RESOLVED_PATH));
                }
                if (resolvedTmp.length() > 0) {
                    if (dEvent.getChild("uris", dEvent.getNamespace()) != null) {
                        resolvedTmp.append(CoordELFunctions.INSTANCE_SEPARATOR)
                                .append(dEvent.getChild("uris", dEvent.getNamespace()).getTextTrim());
                        dEvent.removeChild("uris", dEvent.getNamespace());
                    }
                    Element uriInstance = new Element("uris", dEvent.getNamespace());
                    uriInstance.addContent(resolvedTmp.toString());
                    dEvent.getContent().add(1, uriInstance);
                }
                dEvent.removeChild(CoordCommandUtils.UNRESOLVED_INSTANCES_TAG, dEvent.getNamespace());
            }
        }

        return true;
    }

    public Map<String, ActionDependency> getMissingDependencies(CoordinatorActionBean coordAction)
            throws CommandException, IOException, JDOMException {

        Map<String, ActionDependency> dependenciesMap = null;
        try {
            dependenciesMap = getDependency(coordAction);
        }
        catch (URIHandlerException e) {
            throw new IOException(e);
        }

        StringBuilder nonExistList = new StringBuilder();
        StringBuilder nonResolvedList = new StringBuilder();
        CoordCommandUtils.getResolvedList(getMissingDependencies(), nonExistList, nonResolvedList);

        Set<String> missingSets = new HashSet<String>(
                Arrays.asList(nonExistList.toString().split(CoordELFunctions.INSTANCE_SEPARATOR)));

        missingSets.addAll(
                Arrays.asList(nonResolvedList.toString().split(CoordCommandUtils.RESOLVED_UNRESOLVED_SEPARATOR)));

        for (Iterator<Map.Entry<String, ActionDependency>> it = dependenciesMap.entrySet().iterator(); it.hasNext();) {
            Map.Entry<String, ActionDependency> entry = it.next();
            ActionDependency dependency = entry.getValue();
            dependency.getMissingDependencies().retainAll(missingSets);

            if (dependency.getUriTemplate() != null) {
                for (int i = 0; i < dependency.getMissingDependencies().size(); i++) {
                    if (dependency.getMissingDependencies().get(i).trim().startsWith("${coord:")) {
                        dependency.getMissingDependencies().set(i,
                                dependency.getMissingDependencies().get(i) + " -> " + dependency.getUriTemplate());
                    }
                }
            }

            if (dependenciesMap.get(entry.getKey()).getMissingDependencies().isEmpty()) {
                it.remove();
            }

        }
        return dependenciesMap;
    }

    @SuppressWarnings("unchecked")
    private Map<String, ActionDependency> getDependency(CoordinatorActionBean coordAction)
            throws JDOMException, URIHandlerException {
        Map<String, ActionDependency> dependenciesMap = new HashMap<String, ActionDependency>();
        URIHandlerService uriService = Services.get().get(URIHandlerService.class);

        Element eAction = XmlUtils.parseXml(coordAction.getActionXml());
        Element inputList = eAction.getChild("input-events", eAction.getNamespace());

        if (inputList == null || inputList.getChildren().isEmpty()) {
            return dependenciesMap;
        }

        List<Element> eDataEvents = inputList.getChildren("data-in", eAction.getNamespace());
        for (Element event : eDataEvents) {
            Element uri = event.getChild("uris", event.getNamespace());
            ActionDependency dependency = new ActionDependency();
            if (uri != null) {
                Element doneFlagElement = event.getChild("dataset", event.getNamespace()).getChild("done-flag",
                        event.getNamespace());
                String[] dataSets = uri.getText().split(CoordELFunctions.INSTANCE_SEPARATOR);
                String doneFlag = CoordUtils.getDoneFlag(doneFlagElement);

                for (String dataSet : dataSets) {
                    URIHandler uriHandler;
                    uriHandler = uriService.getURIHandler(dataSet);
                    dependency.getMissingDependencies().add(uriHandler.getURIWithDoneFlag(dataSet, doneFlag));
                }
            }
            if (event.getChildTextTrim(CoordCommandUtils.UNRESOLVED_INSTANCES_TAG, event.getNamespace()) != null) {
                ActionDependency unResolvedDependency = getUnResolvedDependency(coordAction, event);
                dependency.getMissingDependencies()
                        .addAll(unResolvedDependency.getMissingDependencies());
                dependency.setUriTemplate(unResolvedDependency.getUriTemplate());
            }
            dependenciesMap.put(event.getAttributeValue("name"), dependency);
        }
        return dependenciesMap;
    }

    private ActionDependency getUnResolvedDependency(CoordinatorActionBean coordAction, Element event)
            throws JDOMException, URIHandlerException {
        String tmpUnresolved = event.getChildTextTrim(CoordCommandUtils.UNRESOLVED_INSTANCES_TAG, event.getNamespace());
        StringBuilder nonResolvedList = new StringBuilder();
        CoordCommandUtils.getResolvedList(getMissingDependencies(), new StringBuilder(), nonResolvedList);
        ActionDependency dependency = new ActionDependency();

        if (nonResolvedList.length() > 0) {
            Element eDataset = event.getChild("dataset", event.getNamespace());
            dependency.setUriTemplate(eDataset.getChild("uri-template", eDataset.getNamespace()).getTextTrim());
            dependency.getMissingDependencies().add(tmpUnresolved);
        }
        return dependency;
    }

    @Override
    public String getFirstMissingDependency() {
        StringBuilder nonExistList = new StringBuilder();
        String missingDependencies = getMissingDependencies();
        StringBuilder nonResolvedList = new StringBuilder();
        CoordCommandUtils.getResolvedList(missingDependencies, nonExistList, nonResolvedList);
        String firstMissingDependency = "";
        if (nonExistList.length() > 0) {
            firstMissingDependency = nonExistList.toString().split(CoordELFunctions.INSTANCE_SEPARATOR)[0];
        }
        else {
            if (nonResolvedList.length() > 0) {
                firstMissingDependency = nonResolvedList.toString().split(CoordCommandUtils.RESOLVED_UNRESOLVED_SEPARATOR)[0];
            }
        }
        return firstMissingDependency;
    }
}
