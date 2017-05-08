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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.BaseEngine;
import org.apache.oozie.BaseEngineException;
import org.apache.oozie.BundleEngine;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorActionInfo;
import org.apache.oozie.CoordinatorEngine;
import org.apache.oozie.CoordinatorEngineException;
import org.apache.oozie.CoordinatorWfActionBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.DagEngine;
import org.apache.oozie.DagEngineException;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.coord.CoordCommandUtils;
import org.apache.oozie.command.wf.ActionXCommand;
import org.apache.oozie.dependency.ActionDependency;
import org.apache.oozie.service.BundleEngineService;
import org.apache.oozie.service.CoordinatorEngineService;
import org.apache.oozie.service.DagEngineService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.util.Pair;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

@SuppressWarnings("serial")
public class V2JobServlet extends V1JobServlet {

    private static final String INSTRUMENTATION_NAME = "v2job";

    public V2JobServlet() {
        super(INSTRUMENTATION_NAME);
    }

    @Override
    protected JsonBean getWorkflowJob(HttpServletRequest request, HttpServletResponse response) throws XServletException {
        JsonBean jobBean = super.getWorkflowJobBean(request, response);
        return jobBean;
    }

    @Override
    protected JsonBean getWorkflowAction(HttpServletRequest request, HttpServletResponse response) throws XServletException {
        JsonBean actionBean = super.getWorkflowActionBean(request, response);
        return actionBean;
    }

    @Override
    protected int getCoordinatorJobLength(int defaultLen, int len) {
        return (len < 0) ? defaultLen : len;
    }

    @Override
    protected String getJMSTopicName(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            IOException {
        String topicName;
        String jobId = getResourceName(request);
        DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(getUser(request));
        try {
            topicName = dagEngine.getJMSTopicName(jobId);
        }
        catch (DagEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
        return topicName;
    }

    @Override
    protected JSONObject getJobsByParentId(HttpServletRequest request, HttpServletResponse response)
            throws XServletException, IOException {
        return super.getJobsByParentId(request, response);
    }

    /**
     * Update coord job.
     *
     * @param request the request
     * @param response the response
     * @return the JSON object
     * @throws XServletException the x servlet exception
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @SuppressWarnings("unchecked")
    @Override
    protected JSONObject updateJob(HttpServletRequest request, HttpServletResponse response, Configuration conf)
            throws XServletException, IOException {
        CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class)
                .getCoordinatorEngine(getUser(request));
        JSONObject json = new JSONObject();
        try {
            String jobId = getResourceName(request);
            boolean dryrun = StringUtils.isEmpty(request.getParameter(RestConstants.JOB_ACTION_DRYRUN)) ? false
                    : Boolean.parseBoolean(request.getParameter(RestConstants.JOB_ACTION_DRYRUN));
            boolean showDiff = StringUtils.isEmpty(request.getParameter(RestConstants.JOB_ACTION_SHOWDIFF)) ? true
                    : Boolean.parseBoolean(request.getParameter(RestConstants.JOB_ACTION_SHOWDIFF));

            String diff = coordEngine.updateJob(conf, jobId, dryrun, showDiff);
            JSONObject diffJson = new JSONObject();
            diffJson.put(JsonTags.COORD_UPDATE_DIFF, diff);
            json.put(JsonTags.COORD_UPDATE, diffJson);
        }
        catch (CoordinatorEngineException e) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, e);
        }
        return json;
    }

    /**
     * Ignore a coordinator job
     * @param request request object
     * @param response response object
     * @throws XServletException
     * @throws IOException
     */
    @Override
    protected JSONObject ignoreJob(HttpServletRequest request, HttpServletResponse response) throws XServletException, IOException {
        String jobId = getResourceName(request);
        if (jobId.endsWith("-W")) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0302, "Workflow Ignore Not supported");
        } else if (jobId.endsWith("-B")) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0302, "Bundle Ignore Not supported");
        } else {
            return ignoreCoordinatorJob(request, response);
        }

    }

    @Override
    protected void slaEnableAlert(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            IOException {
        String jobId = getResourceName(request);
        String actions = request.getParameter(RestConstants.JOB_COORD_SCOPE_ACTION_LIST);
        String dates = request.getParameter(RestConstants.JOB_COORD_SCOPE_DATE);
        String childIds = request.getParameter(RestConstants.COORDINATORS_PARAM);
        try {
            getBaseEngine(jobId, getUser(request)).enableSLAAlert(jobId, actions, dates, childIds);
        }
        catch (BaseEngineException e) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, e);
        }

    }

    @Override
    protected void slaDisableAlert(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            IOException {
        String jobId = getResourceName(request);
        String actions = request.getParameter(RestConstants.JOB_COORD_SCOPE_ACTION_LIST);
        String dates = request.getParameter(RestConstants.JOB_COORD_SCOPE_DATE);
        String childIds = request.getParameter(RestConstants.COORDINATORS_PARAM);
        try {
            getBaseEngine(jobId, getUser(request)).disableSLAAlert(jobId, actions, dates, childIds);
        }
        catch (BaseEngineException e) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, e);
        }
    }

    @Override
    protected void slaChange(HttpServletRequest request, HttpServletResponse response) throws XServletException, IOException {
        String jobId = getResourceName(request);
        String actions = request.getParameter(RestConstants.JOB_COORD_SCOPE_ACTION_LIST);
        String dates = request.getParameter(RestConstants.JOB_COORD_SCOPE_DATE);
        String newParams = request.getParameter(RestConstants.JOB_CHANGE_VALUE);
        String coords = request.getParameter(RestConstants.COORDINATORS_PARAM);

        try {
            getBaseEngine(jobId, getUser(request)).changeSLA(jobId, actions, dates, coords, newParams);
        }
        catch (BaseEngineException e) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, e);
        }
    }

    /**
     * Ignore a coordinator job/action
     *
     * @param request servlet request
     * @param response servlet response
     * @throws XServletException
     */
    @SuppressWarnings("unchecked")
    private JSONObject ignoreCoordinatorJob(HttpServletRequest request, HttpServletResponse response)
            throws XServletException {
        JSONObject json = null;
        CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class).getCoordinatorEngine(
                getUser(request));
        String jobId = getResourceName(request);
        String type = request.getParameter(RestConstants.JOB_COORD_RANGE_TYPE_PARAM);
        String scope = request.getParameter(RestConstants.JOB_COORD_SCOPE_PARAM);
        String changeValue = "status=" + CoordinatorAction.Status.IGNORED;
        List<CoordinatorActionBean> coordActions = new ArrayList<CoordinatorActionBean>();
        try {
            if (type != null && !type.equals(RestConstants.JOB_COORD_SCOPE_ACTION)) {
                throw new CommandException(ErrorCode.E1024, "Currently ignore only support -action option");
            }
            CoordinatorActionInfo coordInfo = null;
            if(scope == null || scope.isEmpty()) {
                coordEngine.change(jobId, changeValue);
            } else{
                coordInfo = coordEngine.ignore(jobId, type, scope);
            }
            if(coordInfo != null) {
                coordActions = coordInfo.getCoordActions();
                json = new JSONObject();
                json.put(JsonTags.COORDINATOR_ACTIONS, CoordinatorActionBean.toJSONArray(coordActions, "GMT"));
            }
            return json;
        }
        catch (CommandException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
        catch (CoordinatorEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected String getJobStatus(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            IOException {
        String status;
        String jobId = getResourceName(request);
        try {
            if (jobId.endsWith("-B") || jobId.endsWith("-W")) {
                status = getBaseEngine(jobId, getUser(request)).getJobStatus(jobId);
            }
            else if (jobId.contains("C@")) {
                CoordinatorEngine engine = Services.get().get(CoordinatorEngineService.class)
                        .getCoordinatorEngine(getUser(request));
                status = engine.getActionStatus(jobId);
            }
            else {
                status = getBaseEngine(jobId, getUser(request)).getJobStatus(jobId);
            }

        } catch (BaseEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
        return status;
    }
    @SuppressWarnings("unchecked")
    @Override
    protected void streamJobErrorLog(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            IOException {

        String jobId = getResourceName(request);
        try {
            getBaseEngine(jobId, getUser(request)).streamErrorLog(jobId, response.getWriter(), request.getParameterMap());
        }
        catch (DagEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
        catch (BaseEngineException e) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void streamJobAuditLog(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            IOException {

        String jobId = getResourceName(request);
        try {
            getBaseEngine(jobId, getUser(request)).streamAuditLog(jobId, response.getWriter(), request.getParameterMap());
        }
        catch (DagEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
        catch (BaseEngineException e) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    JSONArray getActionRetries(HttpServletRequest request, HttpServletResponse response)
            throws XServletException, IOException {
        JSONArray jsonArray = new JSONArray();
        String jobId = getResourceName(request);
        try {
            jsonArray.addAll(Services.get().get(DagEngineService.class).getDagEngine(getUser(request))
                    .getWorkflowActionRetries(jobId));
            return jsonArray;
        }
        catch (BaseEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected JSONObject getCoordActionMissingDependencies(HttpServletRequest request, HttpServletResponse response)
            throws XServletException, IOException {
        String jobId = getResourceName(request);
        String actions = request.getParameter(RestConstants.JOB_COORD_SCOPE_ACTION_LIST);
        String dates = request.getParameter(RestConstants.JOB_COORD_SCOPE_DATE);

        try {
            List<Pair<CoordinatorActionBean, Map<String, ActionDependency>>> dependenciesList = Services.get()
                    .get(CoordinatorEngineService.class).getCoordinatorEngine(getUser(request))
                    .getCoordActionMissingDependencies(jobId, actions, dates);
            JSONArray dependenciesArray = new JSONArray();
            for (Pair<CoordinatorActionBean, Map<String, ActionDependency>> dependencies : dependenciesList) {
                JSONObject json = new JSONObject();
                JSONArray parentJsonArray = new JSONArray();

                for (String key : dependencies.getSecond().keySet()) {
                    JSONObject dependencyList = new JSONObject();
                    JSONArray jsonArray = new JSONArray();
                    jsonArray.addAll(dependencies.getSecond().get(key).getMissingDependencies());
                    dependencyList.put(JsonTags.COORDINATOR_ACTION_MISSING_DEPS, jsonArray);
                    dependencyList.put(JsonTags.COORDINATOR_ACTION_DATASET, key);
                    parentJsonArray.add(dependencyList);
                }
                json.put(JsonTags.COORD_ACTION_FIRST_MISSING_DEPENDENCIES,
                        CoordCommandUtils.getFirstMissingDependency(dependencies.getFirst()));
                json.put(JsonTags.COORDINATOR_ACTION_ID, dependencies.getFirst().getActionNumber());
                json.put(JsonTags.COORDINATOR_ACTION_DATASETS, parentJsonArray);
                dependenciesArray.add(json);
            }
            JSONObject jsonObject = new JSONObject();
            jsonObject.put(JsonTags.COORD_ACTION_MISSING_DEPENDENCIES, dependenciesArray);
            return jsonObject;
        }
        catch (CommandException e) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, e);
        }
    }

    /**
     * Gets the base engine based on jobId.
     *
     * @param jobId the jobId
     * @param user the user
     * @return the baseEngine
     */
    final public BaseEngine getBaseEngine(String jobId, String user) {
        if (jobId.endsWith("-W")) {
            return Services.get().get(DagEngineService.class).getDagEngine(user);
        }
        else if (jobId.endsWith("-B")) {
            return Services.get().get(BundleEngineService.class).getBundleEngine(user);
        }
        else if (jobId.contains("-C")) {
            return Services.get().get(CoordinatorEngineService.class).getCoordinatorEngine(user);
        }
        else {
            throw new RuntimeException("Unknown job Type");
        }
    }

    @Override
    protected JSONObject getWfActionByJobIdAndName(HttpServletRequest request, HttpServletResponse response)
            throws XServletException, IOException {
        CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class).getCoordinatorEngine(
                getUser(request));
        String jobId = getResourceName(request);
        String action = request.getParameter(RestConstants.ACTION_NAME_PARAM);
        String startStr = request.getParameter(RestConstants.OFFSET_PARAM);
        String lenStr = request.getParameter(RestConstants.LEN_PARAM);
        String timeZoneId = request.getParameter(RestConstants.TIME_ZONE_PARAM);
        timeZoneId = (timeZoneId == null) ? "GMT" : timeZoneId;

        if (action == null) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST,
                    ErrorCode.E0305, RestConstants.ACTION_NAME_PARAM);
        }

        int offset = (startStr != null) ? Integer.parseInt(startStr) : 1;
        offset = (offset < 1) ? 1 : offset;
        /**
         * set default number of wf actions to be retrieved to
         * default number of coordinator actions to be retrieved
         **/
        int defaultLen = ConfigurationService.getInt(COORD_ACTIONS_DEFAULT_LENGTH);
        int len = (lenStr != null) ? Integer.parseInt(lenStr) : 0;
        len = getCoordinatorJobLength(defaultLen, len);

        try {
            JSONObject json = new JSONObject();
            List<CoordinatorWfActionBean> coordWfActions = coordEngine.getWfActionByJobIdAndName(jobId, action, offset, len);
            JSONArray array = new JSONArray();
            for (CoordinatorWfActionBean coordWfAction : coordWfActions) {
                array.add(coordWfAction.toJSONObject(timeZoneId));
            }
            json.put(JsonTags.COORDINATOR_JOB_ID, jobId);
            json.put(JsonTags.COORDINATOR_WF_ACTIONS, array);
            return json;
        }
        catch (CoordinatorEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }
}
