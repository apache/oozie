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

package org.apache.oozie.servlet;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.ParseException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.sla.SLASummaryGetForFilterJPAExecutor;
import org.apache.oozie.executor.jpa.sla.SLASummaryGetForFilterJPAExecutor.SLASummaryFilter;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.SLASummaryBean;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.XLog;
import org.json.simple.JSONObject;

@SuppressWarnings("serial")
public class V2SLAServlet extends SLAServlet {

    private static final String INSTRUMENTATION_NAME = "v2sla";
    private static final JsonRestServlet.ResourceInfo RESOURCES_INFO[] = new JsonRestServlet.ResourceInfo[1];
    private static final Set<String> SLA_FILTER_NAMES = new HashSet<String>();

    static {
        SLA_FILTER_NAMES.add(OozieClient.FILTER_SLA_ID);
        SLA_FILTER_NAMES.add(OozieClient.FILTER_SLA_PARENT_ID);
        SLA_FILTER_NAMES.add(OozieClient.FILTER_SLA_APPNAME);
        SLA_FILTER_NAMES.add(OozieClient.FILTER_SLA_NOMINAL_START);
        SLA_FILTER_NAMES.add(OozieClient.FILTER_SLA_NOMINAL_END);
    }

    static {
        RESOURCES_INFO[0] = new JsonRestServlet.ResourceInfo("", Arrays.asList("GET"),
                Arrays.asList(new JsonRestServlet.ParameterInfo(RestConstants.JOBS_FILTER_PARAM, String.class, false,
                        Arrays.asList("GET"))));
    }

    public V2SLAServlet() {
        super(INSTRUMENTATION_NAME, RESOURCES_INFO);
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        XLog.getLog(getClass()).debug("Got SLA GET request:" + request.getQueryString());
        try {
            stopCron();
            JSONObject json = getSLASummaryList(request, response);
            startCron();
            if (json == null) {
                response.setStatus(HttpServletResponse.SC_OK);
            }
            else {
                sendJsonResponse(response, HttpServletResponse.SC_OK, json);
            }
        }
        catch (CommandException ce) {
            XLog.getLog(getClass()).error("Command exception ", ce);
            throw new XServletException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, ce);
        }
        catch (RuntimeException re) {
            XLog.getLog(getClass()).error("Runtime error ", re);
            throw new XServletException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, ErrorCode.E0307, re.getMessage());
        }
    }

    private JSONObject getSLASummaryList(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, CommandException {
        String timeZoneId = request.getParameter(RestConstants.TIME_ZONE_PARAM) == null ? null : request
                .getParameter(RestConstants.TIME_ZONE_PARAM);
        String filterString = request.getParameter(RestConstants.JOBS_FILTER_PARAM);
        String maxResults = request.getParameter(RestConstants.LEN_PARAM);
        int numMaxResults = 1000; // Default

        if (maxResults != null) {
            numMaxResults = Integer.parseInt(maxResults);
        }

        if (filterString == null || filterString.equals("")) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0305,
                    RestConstants.JOBS_FILTER_PARAM);
        }

        try {
            Map<String, List<String>> filterList = parseFilter(URLDecoder.decode(filterString, "UTF-8"), SLA_FILTER_NAMES);
            SLASummaryFilter filter = new SLASummaryFilter();
            if (filterList.containsKey(OozieClient.FILTER_SLA_ID)) {
                filter.setJobId(filterList.get(OozieClient.FILTER_SLA_ID).get(0));
            }
            if (filterList.containsKey(OozieClient.FILTER_SLA_PARENT_ID)) {
                filter.setParentId(filterList.get(OozieClient.FILTER_SLA_PARENT_ID).get(0));
            }
            if (filterList.containsKey(OozieClient.FILTER_SLA_APPNAME)) {
                filter.setAppName(filterList.get(OozieClient.FILTER_SLA_APPNAME).get(0));
            }
            if (filterList.containsKey(OozieClient.FILTER_SLA_NOMINAL_START)) {
                filter.setNominalStart(DateUtils.parseDateUTC(filterList.get(OozieClient.FILTER_SLA_NOMINAL_START).get(0)));
            }
            if (filterList.containsKey(OozieClient.FILTER_SLA_NOMINAL_END)) {
                filter.setNominalEnd(DateUtils.parseDateUTC(filterList.get(OozieClient.FILTER_SLA_NOMINAL_END).get(0)));
            }

            if (filter.getAppName() == null && filter.getJobId() == null && filter.getParentId() == null) {
                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0305,
                        "At least one of the filter parameters - " + OozieClient.FILTER_SLA_ID + ","
                                + OozieClient.FILTER_SLA_PARENT_ID + " or " + OozieClient.FILTER_SLA_APPNAME
                                + " should be specified in the filter query parameter");
            }

            JPAService jpaService = Services.get().get(JPAService.class);
            List<SLASummaryBean> slaSummaryList = null;
            if (jpaService != null) {
                slaSummaryList = jpaService.execute(new SLASummaryGetForFilterJPAExecutor(filter, numMaxResults));
            }
            else {
                XLog.getLog(getClass()).error(ErrorCode.E0610);
            }
            return SLASummaryBean.toJSONObject(slaSummaryList, timeZoneId);
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
        catch (UnsupportedEncodingException e) {
            throw new XServletException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, ErrorCode.E0307,
                    "Unsupported Encoding", e);
        }
        catch (ParseException e) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0303,
                    filterString, e);
        }

    }

}
