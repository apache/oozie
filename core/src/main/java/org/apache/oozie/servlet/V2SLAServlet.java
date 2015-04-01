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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.event.SLAEvent.EventStatus;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.SLARegistrationQueryExecutor;
import org.apache.oozie.executor.jpa.SLARegistrationQueryExecutor.SLARegQuery;
import org.apache.oozie.executor.jpa.sla.SLASummaryGetForFilterJPAExecutor;
import org.apache.oozie.executor.jpa.sla.SLASummaryGetForFilterJPAExecutor.SLASummaryFilter;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.SLARegistrationBean;
import org.apache.oozie.sla.SLASummaryBean;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.XLog;
import org.json.simple.JSONObject;

@SuppressWarnings("serial")
public class V2SLAServlet extends SLAServlet {

    private static final String INSTRUMENTATION_NAME = "v2sla";
    private static final JsonRestServlet.ResourceInfo RESOURCES_INFO[] = new JsonRestServlet.ResourceInfo[1];
    private static final Set<String> SLA_FILTER_NAMES = new HashSet<String>();
    private Pattern p = Pattern.compile("\\d{7}-\\d{15}-.*-B$");

    static {
        SLA_FILTER_NAMES.add(OozieClient.FILTER_SLA_ID);
        SLA_FILTER_NAMES.add(OozieClient.FILTER_SLA_PARENT_ID);
        SLA_FILTER_NAMES.add(OozieClient.FILTER_BUNDLE);
        SLA_FILTER_NAMES.add(OozieClient.FILTER_SLA_APPNAME);
        SLA_FILTER_NAMES.add(OozieClient.FILTER_SLA_NOMINAL_START);
        SLA_FILTER_NAMES.add(OozieClient.FILTER_SLA_NOMINAL_END);
        SLA_FILTER_NAMES.add(OozieClient.FILTER_SLA_EVENT_STATUS);
        SLA_FILTER_NAMES.add(OozieClient.FILTER_SLA_STATUS);
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

            if (!filterList.containsKey(OozieClient.FILTER_SLA_APPNAME)
                    && !filterList.containsKey(OozieClient.FILTER_SLA_ID)
                    && !filterList.containsKey(OozieClient.FILTER_SLA_PARENT_ID)
                    && !filterList.containsKey(OozieClient.FILTER_BUNDLE)
                    && !filterList.containsKey(OozieClient.FILTER_SLA_NOMINAL_START)
                    && !filterList.containsKey(OozieClient.FILTER_SLA_NOMINAL_END)) {
                StringBuffer st = new StringBuffer();
                st.append("At least one of the filter parameters - ").append(OozieClient.FILTER_SLA_APPNAME)
                        .append(",").append(OozieClient.FILTER_SLA_ID).append(",")
                        .append(OozieClient.FILTER_SLA_PARENT_ID).append(",").append(OozieClient.FILTER_BUNDLE)
                        .append(",").append(OozieClient.FILTER_SLA_NOMINAL_START).append(" or ")
                        .append(OozieClient.FILTER_SLA_NOMINAL_END)
                        .append(" should be specified in the filter query parameter");
                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0305, st.toString());
            }

            if (filterList.containsKey(OozieClient.FILTER_SLA_ID)) {
                filter.setJobId(filterList.get(OozieClient.FILTER_SLA_ID).get(0));
            }
            if (filterList.containsKey(OozieClient.FILTER_SLA_PARENT_ID)) {
                filter.setParentId(filterList.get(OozieClient.FILTER_SLA_PARENT_ID).get(0));
            }
            if (filterList.containsKey(OozieClient.FILTER_BUNDLE)) {
                String bundle = filterList.get(OozieClient.FILTER_BUNDLE).get(0);
                if (isBundleId(bundle)) {
                    filter.setBundleId(bundle);
                }
                else {
                    filter.setBundleName(bundle);
                }
            }
            if (filterList.containsKey(OozieClient.FILTER_SLA_EVENT_STATUS)) {
                filter.setEventStatus(filterList.get(OozieClient.FILTER_SLA_EVENT_STATUS).get(0));
            }
            if (filterList.containsKey(OozieClient.FILTER_SLA_STATUS)) {
                filter.setSLAStatus(filterList.get(OozieClient.FILTER_SLA_STATUS).get(0));
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

            JPAService jpaService = Services.get().get(JPAService.class);
            List<SLASummaryBean> slaSummaryList = null;
            if (jpaService != null) {
                slaSummaryList = jpaService.execute(new SLASummaryGetForFilterJPAExecutor(filter, numMaxResults));
            }
            else {
                XLog.getLog(getClass()).error(ErrorCode.E0610);
            }

            List<String> jobIds = new ArrayList<String>();
            for(SLASummaryBean summaryBean:slaSummaryList){
                jobIds.add(summaryBean.getId());
            }
            List<SLARegistrationBean> SLARegistrationList = SLARegistrationQueryExecutor.getInstance().getList(
                    SLARegQuery.GET_SLA_CONFIGS, jobIds);

            Map<String, Map<String, String>> jobIdSLAConfigMap = new HashMap<String, Map<String, String>>();
            for(SLARegistrationBean registrationBean:SLARegistrationList){
                jobIdSLAConfigMap.put(registrationBean.getId(), registrationBean.getSLAConfigMap());
            }

            return SLASummaryBean.toJSONObject(slaSummaryList, jobIdSLAConfigMap, timeZoneId);
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

    private boolean isBundleId(String id) {
        boolean ret = false;
        Matcher m = p.matcher(id);
        if (m.matches()) {
            return true;
        }
        return ret;
    }
}
