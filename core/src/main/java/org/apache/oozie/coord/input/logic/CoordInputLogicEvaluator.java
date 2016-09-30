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

public interface CoordInputLogicEvaluator {
    String INPUT_LOGIC = "input-logic";


    /**
     * Eval input.
     *
     * @param inputDataSet the input data set
     * @param min the min
     * @param wait the wait
     * @return the coord input logic evaluator result
     */
    CoordInputLogicEvaluatorResult evalInput(String inputDataSet, int min, int wait);

    /**
     * Eval combine input.
     *
     * @param combineDatasets the combine datasets
     * @param min the min
     * @param wait the wait
     * @return the coord input logic evaluator result
     */
    CoordInputLogicEvaluatorResult evalCombineInput(String[] combineDatasets, int min, int wait);
}
