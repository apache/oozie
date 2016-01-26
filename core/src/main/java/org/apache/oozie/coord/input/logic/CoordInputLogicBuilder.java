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

import org.apache.commons.lang.StringUtils;

public class CoordInputLogicBuilder {

    StringBuffer bf = new StringBuffer();

    CoordInputLogicEvaluator coordInputlogicEvaluator;

    /** The Dependency builder. */
    public CoordInputDependencyBuilder dependencyBuilder;

    public CoordInputLogicBuilder(CoordInputLogicEvaluator coordInputlogicEvaluator) {
        this.coordInputlogicEvaluator = coordInputlogicEvaluator;
        dependencyBuilder = new CoordInputDependencyBuilder(coordInputlogicEvaluator);
    }

    /**
     * Input function of input-logic
     *
     * @param inputDataset the dataset
     * @return the string
     */
    public CoordInputLogicEvaluatorResult input(String inputDataset) {
        return coordInputlogicEvaluator.evalInput(inputDataset, -1, -1);

    }

    /**
     * Combine function of dataset
     *
     * @param combineDatasets the combine
     * @return the string
     */
    public CoordInputLogicEvaluatorResult combine(String... combineDatasets) {
        return coordInputlogicEvaluator.evalCombineInput(combineDatasets, -1, -1);
    }

    /**
     * The Class CoordInputDependencyBuilder.
     */
    public static class CoordInputDependencyBuilder {

        CoordInputLogicEvaluator coordInputLogicEvaluator;

        public CoordInputDependencyBuilder(CoordInputLogicEvaluator coordInputLogicEvaluator) {
            this.coordInputLogicEvaluator = coordInputLogicEvaluator;

        }

        private int minValue = -1;
        private String wait;
        private String inputDataset;
        private String[] combineDatasets;

        /**
         * Construct min function
         *
         * @param minValue the min value
         * @return the coord input dependency builder
         */
        public CoordInputDependencyBuilder min(int minValue) {
            this.minValue = minValue;
            return this;
        }

        /**
         * Construct  wait function
         *
         * @param wait the wait
         * @return the coord input dependency builder
         */
        public CoordInputDependencyBuilder inputWait(String wait) {
            this.wait = wait;
            return this;
        }

        /**
         * Construct wait function
         *
         * @param wait the wait
         * @return the coord input dependency builder
         */
        public CoordInputDependencyBuilder inputWait(int wait) {
            this.wait = String.valueOf(wait);
            return this;
        }

        /**
         * Construct input function
         *
         * @param dataset the input
         * @return the coord input dependency builder
         */
        public CoordInputDependencyBuilder input(String dataset) {
            this.inputDataset = dataset;
            return this;
        }

        /**
         * Construct complie function
         *
         * @param combineDatasets the combine
         * @return the coord input dependency builder
         */
        public CoordInputDependencyBuilder combine(String... combineDatasets) {
            this.combineDatasets = combineDatasets;
            return this;
        }

        /**
         * Build inputlogic expression
         *
         * @return the string
         * @throws IOException Signals that an I/O exception has occurred.
         */
        public CoordInputLogicEvaluatorResult build() throws IOException {
            if (combineDatasets != null) {
                return coordInputLogicEvaluator.evalCombineInput(combineDatasets, minValue, getTime(wait));
            }
            else {
                return coordInputLogicEvaluator.evalInput(inputDataset, minValue, getTime(wait));
            }
        }

        /**
         * Gets the time in min.
         *
         * @param value the value
         * @return the time in min
         * @throws IOException Signals that an I/O exception has occurred.
         */
        private int getTime(String value) throws IOException {
            if (StringUtils.isEmpty(value)) {
                return -1;
            }
            if (StringUtils.isNumeric(value)) {
                return Integer.parseInt(value);
            }
            else {
                throw new IOException("Unsupported time : " + value);
            }
        }
    }

}
