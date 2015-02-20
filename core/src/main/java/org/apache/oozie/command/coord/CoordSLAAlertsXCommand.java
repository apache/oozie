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

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.SLAAlertsXCommand;
import org.apache.oozie.coord.CoordUtils;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.sla.SLAOperations;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.JDOMException;

public abstract class CoordSLAAlertsXCommand extends SLAAlertsXCommand {

    private String scope;
    private String dates;
    private List<String> actionIds;

    @Override
    protected void loadState() throws CommandException {
        actionIds = getActionListForScopeAndDate(getJobId(), scope, dates);

    }

    public CoordSLAAlertsXCommand(String jobId, String name, String type, String actions, String dates) {
        super(jobId, name, type);
        this.scope = actions;
        this.dates = dates;

    }

    /**
     * Update job conf.
     *
     * @param newConf the new conf
     * @throws CommandException the command exception
     */
    protected void updateJobConf(Configuration newConf) throws CommandException {

        try {
            CoordinatorJobBean job = new CoordinatorJobBean();
            XConfiguration conf = null;
            conf = getJobConf();
            XConfiguration.copy(newConf, conf);
            job.setId(getJobId());
            job.setConf(XmlUtils.prettyPrint(conf).toString());
            CoordJobQueryExecutor.getInstance().executeUpdate(
                    CoordJobQueryExecutor.CoordJobQuery.UPDATE_COORD_JOB_CONF, job);
        }

        catch (XException e) {
            throw new CommandException(e);
        }
    }

    /**
     * Update job sla.
     *
     * @param newParams the new params
     * @throws CommandException the command exception
     */
    protected void updateJobSLA(Map<String, String> newParams) throws CommandException {

        try {

            CoordinatorJobBean job = CoordJobQueryExecutor.getInstance().get(
                    CoordJobQueryExecutor.CoordJobQuery.GET_COORD_JOB_XML, getJobId());

            Element eAction;
            try {
                eAction = XmlUtils.parseXml(job.getJobXml());
            }
            catch (JDOMException e) {
                throw new CommandException(ErrorCode.E1005, e.getMessage(), e);
            }
            Element eSla = eAction.getChild("action", eAction.getNamespace()).getChild("info",
                    eAction.getNamespace("sla"));

            if (newParams != null) {
                if (newParams.get(RestConstants.SLA_NOMINAL_TIME) != null) {
                    updateSlaTagElement(eSla, SLAOperations.NOMINAL_TIME,
                            newParams.get(RestConstants.SLA_NOMINAL_TIME));
                }
                if (newParams.get(RestConstants.SLA_SHOULD_START) != null) {
                    updateSlaTagElement(eSla, SLAOperations.SHOULD_START,
                            newParams.get(RestConstants.SLA_SHOULD_START));
                }
                if (newParams.get(RestConstants.SLA_SHOULD_END) != null) {
                    updateSlaTagElement(eSla, SLAOperations.SHOULD_END, newParams.get(RestConstants.SLA_SHOULD_END));
                }
                if (newParams.get(RestConstants.SLA_MAX_DURATION) != null) {
                    updateSlaTagElement(eSla, SLAOperations.MAX_DURATION,
                            newParams.get(RestConstants.SLA_MAX_DURATION));
                }
            }

            String actualXml = XmlUtils.prettyPrint(eAction).toString();
            job.setJobXml(actualXml);
            job.setId(getJobId());

            CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQueryExecutor.CoordJobQuery.UPDATE_COORD_JOB_XML,
                    job);
        }
        catch (XException e) {
            throw new CommandException(e);
        }

    }

    /**
     * Gets the action and date list as string.
     *
     * @return the action date list as string
     */
    protected String getActionDateListAsString() {
        StringBuffer bf = new StringBuffer();
        if (!StringUtils.isEmpty(dates)) {
            bf.append(dates);
        }

        if (!StringUtils.isEmpty(scope)) {
            if (!StringUtils.isEmpty(bf.toString())) {
                bf.append(",");
            }
            bf.append(scope);
        }

        return bf.toString();

    }

    /**
     * Gets the action list for scope and date.
     *
     * @param id the id
     * @param scope the scope
     * @param dates the dates
     * @return the action list for scope and date
     * @throws CommandException the command exception
     */
    private List<String> getActionListForScopeAndDate(String id, String scope, String dates) throws CommandException {
        List<String> actionIds = new ArrayList<String>();

        if (scope == null && dates == null) {
            return null;
        }
        List<String> parsed = new ArrayList<String>();
        if (dates != null) {
            List<CoordinatorActionBean> actionSet = CoordUtils.getCoordActionsFromDates(id, dates, true);
            for (CoordinatorActionBean action : actionSet) {
                actionIds.add(action.getId());
            }
            parsed.addAll(actionIds);
        }
        if (scope != null) {
            parsed.addAll(CoordUtils.getActionsIds(id, scope));
        }
        return parsed;
    }

    /**
     * Gets the action list.
     *
     * @return the action list
     */
    protected List<String> getActionList() {
        return actionIds;
    }

    protected boolean isJobRequest() {
        return StringUtils.isEmpty(dates) && StringUtils.isEmpty(scope);
    }


    /**
     * Update Sla tag element.
     *
     * @param elem the elem
     * @param tagName the tag name
     * @param value the value
     */
    public void updateSlaTagElement(Element elem, String tagName, String value) {
        if (elem != null && elem.getChild(tagName, elem.getNamespace("sla")) != null) {
            elem.getChild(tagName, elem.getNamespace("sla")).setText(value);
        }
    }

    protected XConfiguration getJobConf() throws JPAExecutorException, CommandException {
        CoordinatorJobBean job = CoordJobQueryExecutor.getInstance().get(
                CoordJobQueryExecutor.CoordJobQuery.GET_COORD_JOB_CONF, getJobId());
        String jobConf = job.getConf();
        XConfiguration conf = null;
        try {
            conf = new XConfiguration(new StringReader(jobConf));
        }
        catch (IOException e) {
            throw new CommandException(ErrorCode.E1005, e.getMessage(), e);
        }
        return conf;
    }

}
