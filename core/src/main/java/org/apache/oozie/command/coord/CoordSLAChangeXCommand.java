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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.coord.CoordELEvaluator;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.service.SLAService;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.Pair;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;

public class CoordSLAChangeXCommand extends CoordSLAAlertsXCommand {

    Map<String, String> newParams;

    public CoordSLAChangeXCommand(String jobId, String actions, String dates, Map<String, String> newParams) {
        super(jobId, "SLA.alerts.change", "SLA.alerts.change", actions, dates);
        this.newParams = newParams;
    }

    @Override
    protected boolean executeSlaCommand() throws ServiceException, CommandException {
        try {
            List<Pair<String, Map<String, String>>> idSlaDefinitionList = new ArrayList<Pair<String, Map<String, String>>>();
            List<CoordinatorActionBean> coordinatorActionBeanList = getNotTerminatedActions();
            Configuration conf = getJobConf();
            for (CoordinatorActionBean coordAction : coordinatorActionBeanList) {
                Map<String, String> slaDefinitionMap = new HashMap<String, String>(newParams);
                for (String key : slaDefinitionMap.keySet()) {
                    Element eAction = XmlUtils.parseXml(coordAction.getActionXml().toString());
                    ELEvaluator evalSla = CoordELEvaluator.createSLAEvaluator(eAction, coordAction, conf);
                    String updateValue = CoordELFunctions.evalAndWrap(evalSla, slaDefinitionMap.get(key));
                    slaDefinitionMap.put(key, updateValue);
                }
                idSlaDefinitionList.add(new Pair<String, Map<String, String>>(coordAction.getId(), slaDefinitionMap));
            }
            return Services.get().get(SLAService.class).changeDefinition(idSlaDefinitionList);
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E1027, e.getMessage(), e);
        }

    }

    @Override
    protected void updateJob() throws CommandException {
        if (isJobRequest()) {
            updateJobSLA(newParams);
        }
    }

    private List<CoordinatorActionBean> getNotTerminatedActions() throws JPAExecutorException {
        if (isJobRequest()) {
            return CoordActionQueryExecutor.getInstance().getList(
                    CoordActionQuery.GET_ACTIVE_ACTIONS_JOBID_FOR_SLA_CHANGE, getJobId());
        }
        else {
            return CoordActionQueryExecutor.getInstance().getList(
                    CoordActionQuery.GET_ACTIVE_ACTIONS_IDS_FOR_SLA_CHANGE, getActionList());
        }

    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        validateSLAChangeParam(newParams);
    }
}
