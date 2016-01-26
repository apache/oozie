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

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.coord.CoordELConstants;
import org.apache.oozie.coord.CoordELEvaluator;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.coord.input.dependency.AbstractCoordInputDependency;
import org.apache.oozie.coord.input.dependency.CoordPullInputDependency;
import org.apache.oozie.coord.input.logic.CoordInputLogicEvaluatorResult.STATUS;
import org.apache.oozie.dependency.DependencyChecker;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.JDOMException;

public class CoordInputLogicEvaluatorPhaseTwo extends CoordInputLogicEvaluatorPhaseOne {

    Date actualTime;

    public CoordInputLogicEvaluatorPhaseTwo(CoordinatorActionBean coordAction, Date actualTime) {
        super(coordAction);
        this.actualTime = actualTime;
    }

    public CoordInputLogicEvaluatorPhaseTwo(CoordinatorActionBean coordAction,
            AbstractCoordInputDependency coordInputDependency) {
        super(coordAction, coordInputDependency);
    }

    @Override
    public CoordInputLogicEvaluatorResult evalInput(String dataSet, int min, int wait) {
        try {
            CoordPullInputDependency coordPullInputDependency = (CoordPullInputDependency) coordInputDependency;
            ELEvaluator eval = CoordELEvaluator.createLazyEvaluator(actualTime, coordAction.getNominalTime(),
                    getInputSetEvent(dataSet), getConf());
            if (coordPullInputDependency.getUnResolvedDependency(dataSet) == null) {
                return super.evalInput(dataSet, min, wait);

            }
            else {
                cleanPreviousCheckData(coordPullInputDependency, dataSet);
                List<String> unresolvedList = coordPullInputDependency.getUnResolvedDependency(dataSet)
                        .getDependencies();
                for (String unresolved : unresolvedList) {
                    String resolvedPath = "";

                    CoordELFunctions.evalAndWrap(eval, unresolved);
                    boolean isResolved = (Boolean) eval.getVariable(CoordELConstants.IS_RESOLVED);

                    coordPullInputDependency.setDependencyMap(dependencyMap);
                    if (eval.getVariable(CoordELConstants.RESOLVED_PATH) != null) {
                        resolvedPath = eval.getVariable(CoordELConstants.RESOLVED_PATH).toString();
                    }
                    if (resolvedPath != null) {
                        resolvedPath = getEvalResult(isResolved, min, wait,
                                Arrays.asList(DependencyChecker.dependenciesAsArray(resolvedPath.toString())))
                                .getDataSets();

                    }

                    log.trace(MessageFormat.format("Return data is {0}", resolvedPath));
                    log.debug(MessageFormat.format("Resolved status of Data set {0} with min {1} and wait {2}  =  {3}",
                            dataSet, min, wait, !StringUtils.isEmpty(resolvedPath)));

                    if ((isInputWaitElapsed(wait) || isResolved) && !StringUtils.isEmpty(resolvedPath)) {
                        coordPullInputDependency.addResolvedList(dataSet, resolvedPath.toString());
                    }
                    else {
                        cleanPreviousCheckData(coordPullInputDependency, dataSet);
                        if (!isInputWaitElapsed(wait)) {
                            return new CoordInputLogicEvaluatorResult(
                                    CoordInputLogicEvaluatorResult.STATUS.TIMED_WAITING);
                        }
                        else {
                            return new CoordInputLogicEvaluatorResult(CoordInputLogicEvaluatorResult.STATUS.FALSE);
                        }
                    }
                }
                coordPullInputDependency.getUnResolvedDependency(dataSet).setResolved(true);
                return new CoordInputLogicEvaluatorResult(STATUS.TRUE, getListAsString(coordPullInputDependency
                        .getUnResolvedDependency(dataSet).getResolvedList(), dataSet));

            }
        }
        catch (Exception e) {
            throw new RuntimeException(" event not found" + e, e);

        }

    }

    private void cleanPreviousCheckData(CoordPullInputDependency coordPullInputDependency, String dataSet) {
        // Previous check might have resolved and added resolved list. Cleanup any resolved list stored by previous
        // check.
        if (coordPullInputDependency.getUnResolvedDependency(dataSet) != null) {
            coordPullInputDependency.getUnResolvedDependency(dataSet).setResolvedList(new ArrayList<String>());
        }

    }

    @Override
    public CoordInputLogicEvaluatorResult evalCombineInput(String[] inputSets, int min, int wait) {
        throw new RuntimeException("Combine is not supported for latest/future");

    }

    @SuppressWarnings("unchecked")
    private Element getInputSetEvent(String name) throws JDOMException {
        Element eAction = XmlUtils.parseXml(coordAction.getActionXml().toString());
        Element inputList = eAction.getChild("input-events", eAction.getNamespace());
        List<Element> eDataEvents = inputList.getChildren("data-in", eAction.getNamespace());
        for (Element dEvent : eDataEvents) {
            if (dEvent.getAttribute("name").getValue().equals(name)) {
                return dEvent;
            }
        }
        throw new RuntimeException("Event not found");
    }
}
