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

import org.apache.commons.lang.StringUtils;
import org.apache.oozie.coord.CoordELFunctions;

public class CoordInputLogicEvaluatorResult {

    private STATUS status;
    private String dataSets;

    public static enum STATUS {
        TRUE, FALSE, PHASE_TWO_EVALUATION, TIMED_WAITING
    }

    public CoordInputLogicEvaluatorResult() {
    }

    public CoordInputLogicEvaluatorResult(STATUS status, String dataSets) {
        this.status = status;
        this.dataSets = dataSets;
    }

    public CoordInputLogicEvaluatorResult(STATUS status) {
        this.status = status;
    }

    public String getDataSets() {
        return dataSets;
    }

    public void setDataSets(String dataSets) {
        this.dataSets = dataSets;
    }

    public void appendDataSets(String inputDataSets) {
        if (StringUtils.isEmpty(inputDataSets)) {
            return;
        }
        if (StringUtils.isEmpty(this.dataSets)) {
            this.dataSets = inputDataSets;
        }
        else {
            this.dataSets = this.dataSets + CoordELFunctions.DIR_SEPARATOR + inputDataSets;
        }
    }

    public void setStatus(STATUS status) {
        this.status = status;
    }

    public STATUS getStatus() {
        return status;
    }

    public boolean isTrue() {
        if (status == null) {
            return false;
        }
        switch (status) {
            case TIMED_WAITING:
            case PHASE_TWO_EVALUATION:
            case TRUE:
                return true;
            default:
                return false;
        }

    }

    public boolean isWaiting() {
        if (status == null) {
            return false;
        }
        return status.equals(STATUS.TIMED_WAITING);

    }

    public boolean isPhaseTwoEvaluation() {
        if (status == null) {
            return false;
        }
        return status.equals(STATUS.PHASE_TWO_EVALUATION);

    }

}
