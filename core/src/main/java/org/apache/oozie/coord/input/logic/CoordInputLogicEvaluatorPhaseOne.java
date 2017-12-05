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

package org.apache.oozie.coord.input.logic;

import java.io.IOException;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.command.coord.CoordCommandUtils;
import org.apache.oozie.coord.input.dependency.AbstractCoordInputDependency;
import org.apache.oozie.coord.input.dependency.CoordInputDependency;
import org.apache.oozie.coord.input.dependency.CoordInputInstance;
import org.apache.oozie.coord.input.dependency.CoordPullInputDependency;
import org.apache.oozie.coord.input.logic.CoordInputLogicEvaluatorResult.STATUS;
import org.apache.oozie.dependency.URIHandlerException;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;

/**
 * PhaseOne is for all dependencies check, except unresolved. Unresolved will be checked as part of phaseTwo.
 * Phasethree is only to get dependencies from dataset, no hdfs/hcat check.
 */
public class CoordInputLogicEvaluatorPhaseOne implements CoordInputLogicEvaluator {

    protected AbstractCoordInputDependency coordInputDependency;
    protected Map<String, List<CoordInputInstance>> dependencyMap;
    protected CoordinatorActionBean coordAction = null;
    protected XLog log = XLog.getLog(getClass());

    public CoordInputLogicEvaluatorPhaseOne(CoordinatorActionBean coordAction) {
        this(coordAction, coordAction.getPullInputDependencies());
    }

    public CoordInputLogicEvaluatorPhaseOne(CoordinatorActionBean coordAction, CoordInputDependency coordInputDependency) {
        this.coordAction = coordAction;
        this.coordInputDependency = (AbstractCoordInputDependency) coordInputDependency;
        dependencyMap = ((AbstractCoordInputDependency) coordInputDependency).getDependencyMap();
        LogUtils.setLogInfo(coordAction.getId());

    }

    public CoordInputLogicEvaluatorResult evalInput(String dataSet, int min, int wait) {
        return input(coordInputDependency, dataSet, min, wait);

    }

    /**
     * Evaluate input function with min and wait
     *
     * @param coordInputDependency the dependency
     * @param dataSet the dataset
     * @param min the minimum number of available dataset
     * @param wait time to wait in minutes
     * @return the coord input logic evaluator result
     */
    public CoordInputLogicEvaluatorResult input(AbstractCoordInputDependency coordInputDependency, String dataSet,
            int min, int wait) {

        List<String> availableList = new ArrayList<String>();
        if (coordInputDependency.getDependencyMap().get(dataSet) == null) {
            CoordInputLogicEvaluatorResult retData = new CoordInputLogicEvaluatorResult();
            if (((CoordPullInputDependency) coordAction.getPullInputDependencies()).getUnResolvedDependency(dataSet) != null) {
                log.debug("Data set [{0}] is unresolved set, will get resolved in phase two", dataSet);
                retData.setStatus(CoordInputLogicEvaluatorResult.STATUS.PHASE_TWO_EVALUATION);
            }
            else {
                return getResultFromPullPush(coordAction, dataSet, min);
            }
            return retData;
        }
        boolean allFound = true;
        try {
            Configuration actionConf = new XConfiguration(new StringReader(coordAction.getRunConf()));
            List<CoordInputInstance> firstInputSetList = coordInputDependency.getDependencyMap().get(dataSet);
            for (int i = 0; i < firstInputSetList.size(); i++) {
                CoordInputInstance coordInputInstance = firstInputSetList.get(i);
                if (!coordInputInstance.isAvailable()) {
                    if (pathExists(coordInputInstance.getInputDataInstance(), actionConf)) {
                        availableList.add(coordInputInstance.getInputDataInstance());
                        coordInputDependency.addToAvailableDependencies(dataSet, coordInputInstance);
                    }
                    else {
                        log.debug("[{0} is not found ", coordInputInstance.getInputDataInstance());
                        allFound = false;
                        // Stop looking for dependencies, if min is not specified.
                        if (min < 0) {
                            break;
                        }
                    }
                }
                else {
                    availableList.add(coordInputInstance.getInputDataInstance());
                }
            }
        }
        catch (Exception e) {
            log.error(e);
            throw new RuntimeException(ErrorCode.E1028.format("Error executing input function " + e.getMessage()));
        }
        CoordInputLogicEvaluatorResult retData = getEvalResult(allFound, min, wait, availableList);

        log.debug("Resolved status of Data set [{0}] with min [{1}] and wait [{2}]  =  [{3}]", dataSet, min, wait,
                retData.getStatus());
        return retData;
    }

    public boolean isInputWaitElapsed(int timeInMin) {

        if (timeInMin == -1) {
            return true;
        }
        long waitingTime = (new Date().getTime() - Math.max(coordAction.getNominalTime().getTime(), coordAction
                .getCreatedTime().getTime()))
                / (60 * 1000);
        return timeInMin <= waitingTime;
    }

    public CoordInputLogicEvaluatorResult evalCombineInput(String[] inputSets, int min, int wait) {
        return combine(coordInputDependency, inputSets, min, wait);
    }

    public CoordInputLogicEvaluatorResult combine(AbstractCoordInputDependency coordInputDependency,
            String[] inputSets, int min, int wait) {

        List<String> availableList = new ArrayList<String>();

        if (coordInputDependency.getDependencyMap().get(inputSets[0]) == null) {
            return new CoordInputLogicEvaluatorResult(CoordInputLogicEvaluatorResult.STATUS.TIMED_WAITING);
        }

        try {

            Configuration jobConf = new XConfiguration(new StringReader(coordAction.getRunConf()));
            String firstInputSet = inputSets[0];
            List<CoordInputInstance> firstInputSetList = coordInputDependency.getDependencyMap().get(firstInputSet);
            for (int i = 0; i < firstInputSetList.size(); i++) {
                CoordInputInstance coordInputInstance = firstInputSetList.get(i);
                boolean found = false;
                if (!coordInputInstance.isAvailable()) {
                    if (!pathExists(coordInputInstance.getInputDataInstance(), jobConf)) {
                        log.debug(MessageFormat.format("{0} is not found. Looking from other datasets.",
                                coordInputInstance.getInputDataInstance()));
                        for (int j = 1; j < inputSets.length; j++) {
                            if (!coordInputDependency.getDependencyMap().get(inputSets[j]).get(i).isAvailable()) {
                                if (pathExists(coordInputDependency.getDependencyMap().get(inputSets[j]).get(i)
                                        .getInputDataInstance(), jobConf)) {
                                    coordInputDependency.addToAvailableDependencies(inputSets[j], coordInputDependency
                                            .getDependencyMap().get(inputSets[j]).get(i));
                                    availableList.add(coordInputDependency.getDependencyMap().get(inputSets[j]).get(i)
                                            .getInputDataInstance());
                                    log.debug(MessageFormat.format("{0} is found.",
                                            coordInputInstance.getInputDataInstance()));
                                    found = true;
                                }

                            }
                            else {
                                coordInputDependency.addToAvailableDependencies(inputSets[j], coordInputDependency
                                        .getDependencyMap().get(inputSets[j]).get(i));
                                availableList.add(coordInputDependency.getDependencyMap().get(inputSets[j]).get(i)
                                        .getInputDataInstance());
                                found = true;

                            }
                        }
                    }
                    else {
                        coordInputDependency.addToAvailableDependencies(firstInputSet, coordInputInstance);
                        availableList.add(coordInputInstance.getInputDataInstance());
                        found = true;
                    }
                }
                else {
                    availableList.add(coordInputInstance.getInputDataInstance());
                    found = true;
                }

                if (min < 0 && !found) {
                    // Stop looking for dependencies, if min is not specified.
                    break;
                }

            }
        }
        catch (Exception e) {
            log.error(e);
            throw new RuntimeException(ErrorCode.E1028.format("Error executing combine function " + e.getMessage()));
        }
        boolean allFound = availableList.size() == coordInputDependency.getDependencyMap().get(inputSets[0]).size();
        CoordInputLogicEvaluatorResult retData = getEvalResult(allFound, min, wait, availableList);
        log.debug("Resolved status of Data set [{0}] with min [{1}] and wait [{2}]  =  [{3}]",
                Arrays.toString(inputSets), min, wait, retData.getStatus());
        return retData;

    }

    public Configuration getConf() throws IOException {
        return new XConfiguration(new StringReader(coordAction.getRunConf()));

    }

    public String getListAsString(List<String> list, String dataset) {
        if (list == null || list.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i < list.size(); i++) {
            sb.append(list.get(i - 1)).append(",");
        }
        sb.append(list.get(list.size() - 1));
        return sb.toString();
    }

    protected CoordInputLogicEvaluatorResult getEvalResult(boolean found, int min, int wait, List<String> availableList) {
        CoordInputLogicEvaluatorResult retData = new CoordInputLogicEvaluatorResult();
        if (!found && wait > 0) {
            if (!isInputWaitElapsed(wait)) {
                return new CoordInputLogicEvaluatorResult(STATUS.TIMED_WAITING);
            }
        }

        if (found || (min > 0 && availableList.size() >= min)) {
            retData.setStatus(CoordInputLogicEvaluatorResult.STATUS.TRUE);
            retData.setDataSets(getListAsString(availableList, null));
        }

        if (min == 0) {
            retData.setStatus(CoordInputLogicEvaluatorResult.STATUS.TRUE);
        }

        return retData;
    }

    protected boolean pathExists(String sPath, Configuration jobConf) throws IOException, URISyntaxException,
            URIHandlerException {
        return CoordCommandUtils.pathExists(sPath, jobConf);

    }

    public CoordInputLogicEvaluatorResult getResultFromPullPush(CoordinatorActionBean coordAction, String dataSet, int min) {
        CoordInputLogicEvaluatorResult result = new CoordInputLogicEvaluatorResult();
        CoordInputLogicEvaluatorResult pullResult = getEvalResult(
                (AbstractCoordInputDependency) coordAction.getPullInputDependencies(), dataSet, min);
        CoordInputLogicEvaluatorResult pushResult = getEvalResult(
                (AbstractCoordInputDependency) coordAction.getPushInputDependencies(), dataSet, min);
        result.appendDataSets(pullResult.getDataSets());
        result.appendDataSets(pushResult.getDataSets());

        if (pullResult.isWaiting() || pushResult.isWaiting()) {
            result.setStatus(STATUS.TIMED_WAITING);
        }

        else if (pullResult.isPhaseTwoEvaluation() || pushResult.isPhaseTwoEvaluation()) {
            result.setStatus(STATUS.PHASE_TWO_EVALUATION);
        }

        else if (pullResult.isTrue() || pushResult.isTrue()) {
            result.setStatus(STATUS.TRUE);
        }
        else {
            result.setStatus(STATUS.FALSE);
        }
        return result;

    }

    /**
     * Gets evaluator Result
     *
     * @param coordInputDependencies the coord dependencies
     * @param dataSet the data set
     * @param min the min
     * @return the coord input logic evaluator result
     */
    public CoordInputLogicEvaluatorResult getEvalResult(AbstractCoordInputDependency coordInputDependencies,
            String dataSet, int min) {
        CoordInputLogicEvaluatorResult result = new CoordInputLogicEvaluatorResult();
        if ((coordInputDependencies.getAvailableDependencies(dataSet) == null || coordInputDependencies
                .getAvailableDependencies(dataSet).isEmpty())) {
            if (min == 0) {
                result.setStatus(CoordInputLogicEvaluatorResult.STATUS.TRUE);
            }
            else {
                result.setStatus(CoordInputLogicEvaluatorResult.STATUS.FALSE);
            }
        }

        if (min > -1 && coordInputDependencies.getAvailableDependencies(dataSet).size() >= min) {
            result.setStatus(CoordInputLogicEvaluatorResult.STATUS.TRUE);
            result.appendDataSets(getListAsString(coordInputDependencies.getAvailableDependencies(dataSet), dataSet));
        }

        else if (coordInputDependencies.isDataSetResolved(dataSet)) {
            result.setStatus(CoordInputLogicEvaluatorResult.STATUS.TRUE);
            result.appendDataSets(getListAsString(coordInputDependencies.getAvailableDependencies(dataSet), dataSet));
        }
        return result;
    }
}
