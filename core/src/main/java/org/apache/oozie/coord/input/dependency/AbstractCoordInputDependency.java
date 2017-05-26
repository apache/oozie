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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Writable;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.coord.CoordCommandUtils;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.coord.input.logic.CoordInputLogicEvaluatorUtil;
import org.apache.oozie.dependency.ActionDependency;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.WritableUtils;
import org.jdom.Element;
import org.jdom.JDOMException;

public abstract class AbstractCoordInputDependency implements Writable, CoordInputDependency {
    protected boolean isDependencyMet = false;
    /*
     * Transient variables only used for processing, not stored in DB.
     */
    protected transient Map<String, List<String>> missingDependenciesSet = new HashMap<String, List<String>>();
    protected transient Map<String, List<String>> availableDependenciesSet = new HashMap<String, List<String>>();
    protected Map<String, List<CoordInputInstance>> dependencyMap = new HashMap<String, List<CoordInputInstance>>();

    public AbstractCoordInputDependency() {
    }


    public AbstractCoordInputDependency(Map<String, List<CoordInputInstance>> dependencyMap) {
        this.dependencyMap = dependencyMap;
        generateDependencies();
    }

    public void addInputInstanceList(String inputEventName, List<CoordInputInstance> inputInstanceList) {
        dependencyMap.put(inputEventName, inputInstanceList);
    }

    public Map<String, List<CoordInputInstance>> getDependencyMap() {
        return dependencyMap;
    }

    public void setDependencyMap(Map<String, List<CoordInputInstance>> dependencyMap) {
        this.dependencyMap = dependencyMap;
    }

    public void addToAvailableDependencies(String dataSet, CoordInputInstance coordInputInstance) {
        coordInputInstance.setAvailability(true);
        List<String> availableSet = availableDependenciesSet.get(dataSet);
        if (availableSet == null) {
            availableSet = new ArrayList<String>();
            availableDependenciesSet.put(dataSet, availableSet);
        }
        availableSet.add(coordInputInstance.getInputDataInstance());
        removeFromMissingDependencies(dataSet, coordInputInstance);
    }

    public void removeFromMissingDependencies(String dataSet, CoordInputInstance coordInputInstance) {
        coordInputInstance.setAvailability(true);
        List<String> missingSet = missingDependenciesSet.get(dataSet);
        if (missingSet != null) {
            missingSet.remove(coordInputInstance.getInputDataInstance());
            if (missingSet.isEmpty()) {
                missingDependenciesSet.remove(dataSet);
            }
        }

    }

    public void addToMissingDependencies(String dataSet, CoordInputInstance coordInputInstance) {
        List<String> availableSet = missingDependenciesSet.get(dataSet);
        if (availableSet == null) {
            availableSet = new ArrayList<String>();
        }
        availableSet.add(coordInputInstance.getInputDataInstance());
        missingDependenciesSet.put(dataSet, availableSet);

    }

    protected void generateDependencies() {
        try {
            missingDependenciesSet = new HashMap<String, List<String>>();
            availableDependenciesSet = new HashMap<String, List<String>>();

            for (Entry<String, List<CoordInputInstance>> entry : dependencyMap.entrySet()) {
                String key = entry.getKey();
                for (CoordInputInstance coordInputInstance : entry.getValue()) {
                    if (coordInputInstance.isAvailable()) {
                        addToAvailableDependencies(key, coordInputInstance);
                    }
                    else {
                        addToMissingDependencies(key, coordInputInstance);
                    }
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public List<String> getAvailableDependencies(String dataSet) {
        if (availableDependenciesSet.get(dataSet) != null) {
            return availableDependenciesSet.get(dataSet);
        }
        else {
            return new ArrayList<String>();
        }

    }

    public String getMissingDependencies(String dataSet) {
        StringBuilder sb = new StringBuilder();
        for (String dependencies : missingDependenciesSet.get(dataSet)) {
            sb.append(dependencies).append("#");
        }
        return sb.toString();
    }

    public void addToAvailableDependencies(String dataSet, String availableSet) {
        List<CoordInputInstance> list = dependencyMap.get(dataSet);
        if (list == null) {
            list = new ArrayList<CoordInputInstance>();
            dependencyMap.put(dataSet, list);
        }

        for (String available : availableSet.split(CoordELFunctions.INSTANCE_SEPARATOR)) {
            CoordInputInstance coordInstance = new CoordInputInstance(available, true);
            list.add(coordInstance);
            addToAvailableDependencies(dataSet, coordInstance);
        }

    }

    public String getMissingDependencies() {
        StringBuilder sb = new StringBuilder();
        if (missingDependenciesSet != null) {
            for (List<String> dependenciesList : missingDependenciesSet.values()) {
                for (String dependencies : dependenciesList) {
                    sb.append(dependencies).append("#");
                }
            }
        }
        return sb.toString();
    }

    public List<String> getMissingDependenciesAsList() {
        List<String> missingDependencies = new ArrayList<String>();
        for (List<String> dependenciesList : missingDependenciesSet.values()) {
            missingDependencies.addAll(dependenciesList);
        }
        return missingDependencies;
    }

    public List<String> getAvailableDependenciesAsList() {
        List<String> availableDependencies = new ArrayList<String>();
        for (List<String> dependenciesList : availableDependenciesSet.values()) {
            availableDependencies.addAll(dependenciesList);

        }
        return availableDependencies;
    }

    public String serialize() throws IOException {
        return CoordInputDependencyFactory.getMagicNumber()
                + new String(WritableUtils.toByteArray(this), CoordInputDependencyFactory.CHAR_ENCODING);

    }

    public String getListAsString(List<String> dataSets) {
        StringBuilder sb = new StringBuilder();
        for (String dependencies : dataSets) {
            sb.append(dependencies).append("#");
        }

        return sb.toString();
    }

    public void setDependencyMet(boolean isDependencyMeet) {
        this.isDependencyMet = isDependencyMeet;
    }

    public boolean isDependencyMet() {
        return missingDependenciesSet.isEmpty() || isDependencyMet;
    }

    public boolean isUnResolvedDependencyMet() {
        return false;
    }


    @Override
    public void addToAvailableDependencies(Collection<String> availableList) {
        for (Entry<String, List<CoordInputInstance>> dependenciesList : dependencyMap.entrySet()) {
            for (CoordInputInstance coordInputInstance : dependenciesList.getValue()) {
                if (availableList.contains(coordInputInstance.getInputDataInstance()))
                    addToAvailableDependencies(dependenciesList.getKey(), coordInputInstance);
            }
        }
    }

    @Override
    public ActionDependency checkPushMissingDependencies(CoordinatorActionBean coordAction,
            boolean registerForNotification) throws CommandException, IOException,
            JDOMException {
        boolean status = new CoordInputLogicEvaluatorUtil(coordAction).checkPushDependencies();
        if (status) {
            coordAction.getPushInputDependencies().setDependencyMet(true);
        }
        return new ActionDependency(coordAction.getPushInputDependencies().getMissingDependenciesAsList(), coordAction
                .getPushInputDependencies().getAvailableDependenciesAsList());

    }

    public boolean checkPullMissingDependencies(CoordinatorActionBean coordAction,
            StringBuilder existList, StringBuilder nonExistList) throws IOException, JDOMException {
        boolean status = new CoordInputLogicEvaluatorUtil(coordAction).checkPullMissingDependencies();
        if (status) {
            coordAction.getPullInputDependencies().setDependencyMet(true);
        }
        return status;

    }

    public boolean isChangeInDependency(StringBuilder nonExistList, String missingDependencies,
            StringBuilder nonResolvedList, boolean status) {
        if (!StringUtils.isEmpty(missingDependencies)) {
            return !missingDependencies.equals(getMissingDependencies());
        }
        else {
            return true;
        }
    }

    @SuppressWarnings("unchecked")
    public boolean checkUnresolved(CoordinatorActionBean coordAction, Element eAction)
            throws Exception {
        String actualTimeStr = eAction.getAttributeValue("action-actual-time");
        Element inputList = eAction.getChild("input-events", eAction.getNamespace());
        Date actualTime = null;
        if (actualTimeStr == null) {
            actualTime = new Date();
        }
        else {
            actualTime = DateUtils.parseDateOozieTZ(actualTimeStr);
        }
        if (inputList == null) {
            return true;
        }
        List<Element> eDataEvents = inputList.getChildren("data-in", eAction.getNamespace());
        for (Element dEvent : eDataEvents) {
            if (dEvent.getChild(CoordCommandUtils.UNRESOLVED_INSTANCES_TAG, dEvent.getNamespace()) == null) {
                continue;
            }
            String unResolvedInstance = dEvent.getChild(CoordCommandUtils.UNRESOLVED_INSTANCES_TAG,
                    dEvent.getNamespace()).getTextTrim();
            String name = dEvent.getAttribute("name").getValue();
            addUnResolvedList(name, unResolvedInstance);
        }
        return new CoordInputLogicEvaluatorUtil(coordAction).checkUnResolved(actualTime);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeStringAsBytes(out,INTERNAL_VERSION_ID);
        out.writeBoolean(isDependencyMet);
        WritableUtils.writeMapWithList(out, dependencyMap);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        WritableUtils.readBytesAsString(in);
        this.isDependencyMet = in.readBoolean();
        dependencyMap = WritableUtils.readMapWithList(in, CoordInputInstance.class);
        generateDependencies();
    }

    public boolean isDataSetResolved(String dataSet){
        if(getAvailableDependencies(dataSet) ==null|| getDependencyMap().get(dataSet) == null){
            return false;
        }
        return getAvailableDependencies(dataSet).size() == getDependencyMap().get(dataSet).size();
    }

    @Override
    public Map<String, ActionDependency> getMissingDependencies(CoordinatorActionBean coordAction)
            throws CommandException, IOException, JDOMException {
        Map<String, ActionDependency> missingDependenciesMap = new HashMap<String, ActionDependency>();
        for (String key : missingDependenciesSet.keySet()) {
            missingDependenciesMap.put(key, new ActionDependency(missingDependenciesSet.get(key), new ArrayList<String>()));
        }
        return missingDependenciesMap;
    }

    public String getFirstMissingDependency() {
        return null;
    }

}
