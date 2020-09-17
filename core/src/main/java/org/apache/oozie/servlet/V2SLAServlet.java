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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ListMultimap;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.FilterParser;
import org.apache.oozie.XException;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.SLARegistrationQueryExecutor;
import org.apache.oozie.executor.jpa.SLARegistrationQueryExecutor.SLARegQuery;
import org.apache.oozie.executor.jpa.sla.SLASummaryGetForFilterJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.SLARegistrationBean;
import org.apache.oozie.sla.SLASummaryBean;
import org.apache.oozie.util.XLog;
import org.json.simple.JSONObject;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("serial")
public class V2SLAServlet extends SLAServlet {

    private static final String INSTRUMENTATION_NAME = "v2sla";
    private static final JsonRestServlet.ResourceInfo RESOURCES_INFO[] = new JsonRestServlet.ResourceInfo[1];

    static {
        RESOURCES_INFO[0] = new JsonRestServlet.ResourceInfo("", Arrays.asList("GET"),
                Arrays.asList(new JsonRestServlet.ParameterInfo(RestConstants.JOBS_FILTER_PARAM, String.class, false,
                        Arrays.asList("GET")), new JsonRestServlet.ParameterInfo(RestConstants.ORDER_PARAM, String.class, false,
                                Arrays.asList("GET")), new JsonRestServlet.ParameterInfo(RestConstants.SORTBY_PARAM, String.class,
                                false, Arrays.asList("GET"))
                        ));
    }

    public V2SLAServlet() {
        super(INSTRUMENTATION_NAME, RESOURCES_INFO);
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        XLog.getLog(getClass()).debug("Got SLA GET request: {0}", request.getQueryString());
        try {
            stopCron();
            JSONObject json = getSLASummaryList(request);
            startCron();
            sendJsonResponse(response, HttpServletResponse.SC_OK, json);
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

    private JSONObject getSLASummaryList(final HttpServletRequest request) throws ServletException, CommandException {
        String timeZoneId = request.getParameter(RestConstants.TIME_ZONE_PARAM);
        String filterString = request.getParameter(RestConstants.JOBS_FILTER_PARAM);
        String orderString = request.getParameter(RestConstants.ORDER_PARAM);
        String sortbyString = request.getParameter(RestConstants.SORTBY_PARAM);
        String maxResults = request.getParameter(RestConstants.LEN_PARAM);
        int numMaxResults = 1000; // Default
        boolean isDescendingOrder = false; // Default

        if (maxResults != null) {
            numMaxResults = Integer.parseInt(maxResults);
        }

        if (Strings.isNullOrEmpty(filterString)) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0305,
                    RestConstants.JOBS_FILTER_PARAM);
        }

        if (!Strings.isNullOrEmpty(orderString)) {
            isDescendingOrder = getOrder(orderString);
        }

        try {
            ListMultimap<String, String> filterParams = FilterParser.parseFilter(filterString);
            return getSLASummaryListByFilterParams(timeZoneId, numMaxResults, filterParams, sortbyString, isDescendingOrder);
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
        catch (ParseException | IllegalArgumentException e) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0303,
                    filterString, e);
        }
    }

    private boolean getOrder(String orderString) throws XServletException {
        switch (orderString) {
            case "desc":
                return true;
            case "asc":
                return false;
            default:
                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0303, "order", orderString);
        }
    }

    @VisibleForTesting
    JSONObject getSLASummaryListByFilterParams(String timeZoneId, int numMaxResults, ListMultimap<String, String> filterList,
                                               String sortbyColumn, boolean isDescendingOrder) throws
            ServletException, ParseException, IllegalArgumentException, JPAExecutorException {
        SLASummaryGetForFilterJPAExecutor slaSummaryGetForFilterJPAExecutor =
                createSlaSummaryGetForFilterJPAExecutor(numMaxResults, filterList, sortbyColumn, isDescendingOrder);
        List<SLASummaryBean> slaSummaryList = filterForSlaSummaryBeans(slaSummaryGetForFilterJPAExecutor);

        List<String> jobIds = new ArrayList<>();
        if (slaSummaryList != null) {
            for (SLASummaryBean summaryBean : slaSummaryList) {
                jobIds.add(summaryBean.getId());
            }
        }
        List<SLARegistrationBean> SLARegistrationList = SLARegistrationQueryExecutor.getInstance().getList(
                SLARegQuery.GET_SLA_CONFIGS, jobIds);

        Map<String, Map<String, String>> jobIdSLAConfigMap = new HashMap<>();
        for(SLARegistrationBean registrationBean : SLARegistrationList){
            jobIdSLAConfigMap.put(registrationBean.getId(), registrationBean.getSLAConfigMap());
        }
        return SLASummaryBean.toJSONObject(slaSummaryList, jobIdSLAConfigMap, timeZoneId);
    }

    private List<SLASummaryBean> filterForSlaSummaryBeans(SLASummaryGetForFilterJPAExecutor slaSummaryGetForFilterJPAExecutor)
            throws JPAExecutorException, IllegalArgumentException {
        JPAService jpaService = Services.get().get(JPAService.class);
        List<SLASummaryBean> slaSummaryList = null;
        if (jpaService != null) {
            slaSummaryList = jpaService.execute(slaSummaryGetForFilterJPAExecutor);
        }
        else {
            XLog.getLog(getClass()).error(ErrorCode.E0610);
        }
        return slaSummaryList;
    }

    private SLASummaryGetForFilterJPAExecutor createSlaSummaryGetForFilterJPAExecutor(int numMaxResults,
                                                                                      ListMultimap<String, String> filterList,
                                                                                      String sortbyColumn,
                                                                                      boolean isDescendingOrder)
            throws ServletException, ParseException {
        SLASummaryGetForFilterJPAExecutor slaSummaryGetForFilterJPAExecutor =
                new SLASummaryGetForFilterJPAExecutor(numMaxResults);
        slaSummaryGetForFilterJPAExecutor.setSortbyColumn(sortbyColumn);
        slaSummaryGetForFilterJPAExecutor.setDescendingOrder(isDescendingOrder);

        for(String filterName : filterList.keySet()) {
            String filterValue = filterList.get(filterName).get(0);
            slaSummaryGetForFilterJPAExecutor.checkAndSetFilterField(filterName, filterValue);
        }
        return slaSummaryGetForFilterJPAExecutor;
    }
}
