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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.coord.input.dependency.CoordInputInstance;
import org.apache.oozie.coord.input.dependency.CoordPullInputDependency;
import org.apache.oozie.coord.input.dependency.CoordPushInputDependency;

public class CoordInputLogicEvaluatorPhaseValidate implements CoordInputLogicEvaluator {

    CoordPullInputDependency coordPullInputDependency;
    CoordPushInputDependency coordPushInputDependency;

    protected Map<String, List<CoordInputInstance>> dependencyMap;
    protected CoordinatorActionBean coordAction = null;
    protected CoordinatorJob coordJob = null;

    public CoordInputLogicEvaluatorPhaseValidate(CoordinatorActionBean coordAction) {
        this.coordAction = coordAction;
        coordPullInputDependency = (CoordPullInputDependency) coordAction.getPullInputDependencies();
        coordPushInputDependency = (CoordPushInputDependency) coordAction.getPushInputDependencies();

    }

    @Override
    public CoordInputLogicEvaluatorResult evalInput(String dataSet, int min, int wait) {
        getDataSetLen(dataSet);
        return new CoordInputLogicEvaluatorResult(CoordInputLogicEvaluatorResult.STATUS.FALSE);
    }

    @Override
    public CoordInputLogicEvaluatorResult evalCombineInput(String[] inputSets, int min, int wait) {
        if (inputSets.length <= 1) {
            throw new RuntimeException("Combine should have at least two input sets. DataSets : "
                    + Arrays.toString(inputSets));
        }
        int firstInputSetLen = getDataSetLen(inputSets[0]);
        for (int i = 1; i < inputSets.length; i++) {
            if (getDataSetLen(inputSets[i]) != firstInputSetLen) {
                throw new RuntimeException("Combine should have same range. DataSets : " + Arrays.toString(inputSets));
            }
            if (coordPullInputDependency.getUnResolvedDependency(inputSets[i]) != null) {
                throw new RuntimeException("Combine is not supported for latest/future");
            }
        }
        return new CoordInputLogicEvaluatorResult(CoordInputLogicEvaluatorResult.STATUS.FALSE);
    }

    private int getDataSetLen(String dataset) {
        if (coordAction.getPullInputDependencies() != null) {
            if (coordPullInputDependency.getDependencyMap().get(dataset) != null) {
                return coordPullInputDependency.getDependencyMap().get(dataset).size();
            }

            if (coordPullInputDependency.getUnResolvedDependency(dataset) != null) {
                return 1;
            }

        }
        if (coordAction.getPushInputDependencies() != null) {
            if (coordPushInputDependency.getDependencyMap().get(dataset) != null) {
                return coordPushInputDependency.getDependencyMap().get(dataset).size();
            }
        }
        throw new RuntimeException(" Data set not found : " + dataset);
    }
}
