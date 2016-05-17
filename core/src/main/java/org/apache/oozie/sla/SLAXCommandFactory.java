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

package org.apache.oozie.sla;

import org.apache.oozie.command.sla.SLACoordActionJobEventXCommand;
import org.apache.oozie.command.sla.SLACoordActionJobHistoryXCommand;
import org.apache.oozie.command.sla.SLAJobEventXCommand;
import org.apache.oozie.command.sla.SLAJobHistoryXCommand;
import org.apache.oozie.command.sla.SLAWorkflowActionJobEventXCommand;
import org.apache.oozie.command.sla.SLAWorkflowActionJobHistoryXCommand;
import org.apache.oozie.command.sla.SLAWorkflowJobEventXCommand;
import org.apache.oozie.command.sla.SLAWorkflowJobHistoryXCommand;

/**
 * A factory for creating SLACommand objects.
 */
public class SLAXCommandFactory {

    /**
     * Gets the SLA job history  command.
     *
     * @param jobId the job id
     * @return the SLA job history x command
     */
    public static SLAJobHistoryXCommand getSLAJobHistoryXCommand(String jobId) {
        if (jobId.endsWith("-W")) {
            return new SLAWorkflowJobHistoryXCommand(jobId);
        }
        else if (jobId.contains("-W@")) {
            return new SLAWorkflowActionJobHistoryXCommand(jobId);

        }
        else if (jobId.contains("-C@")) {
            return new SLACoordActionJobHistoryXCommand(jobId);
        }

        else {
            return null;
        }
    }

    /**
     * Gets the SLAevent  command.
     *
     * @param slaCalc the sla calc
     * @return the SLA event x command
     */
    public static SLAJobEventXCommand getSLAEventXCommand(SLACalcStatus slaCalc) {
        return getSLAEventXCommand(slaCalc, 0);
    }

    /**
     * Gets the SLA event x command.
     *
     * @param slaCalc the sla calc
     * @param lockTimeOut the lock time out
     * @return the SLA event x command
     */
    public static SLAJobEventXCommand getSLAEventXCommand(SLACalcStatus slaCalc, long lockTimeOut) {
        if (slaCalc.getId().endsWith("-W")) {
            return new SLAWorkflowJobEventXCommand(slaCalc, lockTimeOut);
        }
        else if (slaCalc.getId().contains("-W@")) {
            return new SLAWorkflowActionJobEventXCommand(slaCalc, lockTimeOut);

        }
        else if (slaCalc.getId().contains("-C@")) {
            return new SLACoordActionJobEventXCommand(slaCalc, lockTimeOut);
        }

        else {
            return null;
        }
    }

}
