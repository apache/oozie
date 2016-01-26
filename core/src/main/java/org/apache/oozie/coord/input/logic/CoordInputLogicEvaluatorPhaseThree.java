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
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.coord.input.dependency.AbstractCoordInputDependency;
import org.apache.oozie.coord.input.dependency.CoordInputInstance;
import org.apache.oozie.dependency.URIHandler;
import org.apache.oozie.dependency.URIHandlerException;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.URIHandlerService;
import org.apache.oozie.util.ELEvaluator;

public class CoordInputLogicEvaluatorPhaseThree extends CoordInputLogicEvaluatorPhaseOne {

    ELEvaluator eval;

    public CoordInputLogicEvaluatorPhaseThree(CoordinatorActionBean coordAction, ELEvaluator eval) {
        super(coordAction, (AbstractCoordInputDependency) coordAction.getPullInputDependencies());
        this.eval = eval;
    }

    public CoordInputLogicEvaluatorResult evalInput(String dataSet, int min, int wait) {
        return getResultFromPullPush(coordAction, dataSet, min);

    }

    public CoordInputLogicEvaluatorResult evalCombineInput(String[] inputSets, int min, int wait) {
        return combine(coordInputDependency, inputSets, min, wait);
    }

    public CoordInputLogicEvaluatorResult combine(AbstractCoordInputDependency coordInputDependency,
            String[] inputSets, int min, int wait) {

        List<String> availableList = new ArrayList<String>();

        if (coordInputDependency.getDependencyMap().get(inputSets[0]) == null) {
            return new CoordInputLogicEvaluatorResult(CoordInputLogicEvaluatorResult.STATUS.FALSE);
        }

        try {
            String firstInputSet = inputSets[0];
            List<CoordInputInstance> firstInputSetList = coordInputDependency.getDependencyMap().get(firstInputSet);
            for (int i = 0; i < firstInputSetList.size(); i++) {
                CoordInputInstance coordInputInstance = firstInputSetList.get(i);
                if (!coordInputInstance.isAvailable()) {
                    for (int j = 1; j < inputSets.length; j++) {
                        if (coordInputDependency.getDependencyMap().get(inputSets[j]).get(i).isAvailable()) {
                            availableList.add(getPathWithoutDoneFlag(
                                    coordInputDependency.getDependencyMap().get(inputSets[j]).get(i)
                                            .getInputDataInstance(), inputSets[j]));
                        }
                    }
                }

                else {
                    availableList.add(getPathWithoutDoneFlag(coordInputInstance.getInputDataInstance(), firstInputSet));
                }
            }
        }
        catch (Exception e) {
            log.error(e);
            throw new RuntimeException(ErrorCode.E1028.format("Error executing combine function " + e.getMessage()));
        }
        boolean allFound = availableList.size() == coordInputDependency.getDependencyMap().get(inputSets[0]).size();
        return getEvalResult(allFound, min, wait, availableList);
    }

    protected boolean pathExists(String sPath, Configuration actionConf) throws IOException, URISyntaxException,
            URIHandlerException {
        return false;
    }

    public boolean isInputWaitElapsed(int timeInMin) {
        return true;
    }

    public String getListAsString(List<String> input, String dataSet) {
        if (input == null || input.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        try {

            for (int i = 1; i < input.size(); i++) {
                sb.append(getPathWithoutDoneFlag(input.get(i - 1), dataSet)).append(",");
            }
            sb.append(getPathWithoutDoneFlag(input.get(input.size() - 1), dataSet));
        }
        catch (URIHandlerException e) {
            log.error(e);
            throw new RuntimeException(ErrorCode.E1028.format("Error finding path without done flag " + e.getMessage()));
        }

        return sb.toString();
    }

    private String getPathWithoutDoneFlag(String sPath, String dataSet) throws URIHandlerException {
        if (dataSet == null) {
            return sPath;
        }
        URIHandlerService service = Services.get().get(URIHandlerService.class);
        URIHandler handler = service.getURIHandler(sPath);
        return handler.getURIWithoutDoneFlag(sPath, eval.getVariable(".datain." + dataSet + ".doneFlag").toString());
    }

}
