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
package org.apache.oozie.command.coord;

import java.util.ArrayList;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.service.SLAService;
import org.apache.oozie.util.XConfiguration;

public class CoordSLAAlertsEnableXCommand extends CoordSLAAlertsXCommand {

    public CoordSLAAlertsEnableXCommand(String id, String actions, String dates) {
        super(id, "SLA.alerts.enable", "SLA.alerts.enable", actions, dates);
    }

    @SuppressWarnings("serial")
    @Override
    protected boolean executeSlaCommand() throws ServiceException, CommandException {
        if (getActionList() == null) {
            // if getActionList() == null, means enable command is for all child job.
            return Services.get().get(SLAService.class).enableChildJobAlert(new ArrayList<String>() {
                {
                    add(getJobId());
                }
            });
        }
        else {
            return Services.get().get(SLAService.class).enableAlert(getActionList());
        }
    }

    @Override
    protected void updateJob() throws CommandException {
        XConfiguration conf = new XConfiguration();
        if (isJobRequest()) {
            conf.set(OozieClient.SLA_DISABLE_ALERT, "");
            LOG.debug("Updating job property " + OozieClient.SLA_DISABLE_ALERT + " = ");
        }
        else {
            conf.set(OozieClient.SLA_ENABLE_ALERT, getActionDateListAsString());
            LOG.debug("Updating job property " + OozieClient.SLA_DISABLE_ALERT + " = " + getActionDateListAsString());

        }
        updateJobConf(conf);
    }
}
