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
import org.apache.oozie.DagEngine;
import org.apache.oozie.DagEngineException;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.service.BundleEngineService;
import org.apache.oozie.service.CoordinatorEngineService;
import org.apache.oozie.service.DagEngineService;
import org.apache.oozie.service.Services;
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

}
