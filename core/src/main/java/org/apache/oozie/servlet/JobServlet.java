/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.servlet;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.BaseEngineException;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorActionInfo;
import org.apache.oozie.CoordinatorEngine;
import org.apache.oozie.DagEngine;
import org.apache.oozie.DagEngineException;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.JsonWorkflowJob;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.AuthorizationException;
import org.apache.oozie.service.AuthorizationService;
import org.apache.oozie.service.CoordinatorEngineService;
import org.apache.oozie.service.DagEngineService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.XLogService;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.json.simple.JSONObject;

public class JobServlet extends JsonRestServlet {
    private static final String INSTRUMENTATION_NAME = "job";

    private static final ResourceInfo RESOURCES_INFO[] = new ResourceInfo[1];

    static {
        RESOURCES_INFO[0] = new ResourceInfo("*", Arrays.asList("PUT", "GET"), Arrays.asList(new ParameterInfo(
                RestConstants.ACTION_PARAM, String.class, true, Arrays.asList("PUT")), new ParameterInfo(
                RestConstants.JOB_SHOW_PARAM, String.class, false, Arrays.asList("GET"))));
    }

    public JobServlet() {
        super(INSTRUMENTATION_NAME, RESOURCES_INFO);
    }

    /**
     * Perform various job related actions - start, suspend, resume, kill, etc.
     */
    @SuppressWarnings("unchecked")
    @Override
    protected void doPut(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String jobId = getResourceName(request);
        request.setAttribute(AUDIT_PARAM, jobId);
        request.setAttribute(AUDIT_OPERATION, request.getParameter(RestConstants.ACTION_PARAM));
        try {
            AuthorizationService auth = Services.get().get(AuthorizationService.class);
            auth.authorizeForJob(getUser(request), jobId, true);
        }
        catch (AuthorizationException ex) {
            throw new XServletException(HttpServletResponse.SC_UNAUTHORIZED, ex);
        }

        DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(getUser(request),
                                                                                      getAuthToken(request));
        CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class).getCoordinatorEngine(
                getUser(request), getAuthToken(request));
        try {
            String action = request.getParameter(RestConstants.ACTION_PARAM);
            if (action.equals(RestConstants.JOB_ACTION_START)) {
                stopCron();
                dagEngine.start(jobId);
                startCron();
                response.setStatus(HttpServletResponse.SC_OK);
            }
            else if (action.equals(RestConstants.JOB_ACTION_RESUME)) {
                stopCron();
                dagEngine.resume(jobId);
                startCron();
                response.setStatus(HttpServletResponse.SC_OK);
            }
            else if (action.equals(RestConstants.JOB_ACTION_SUSPEND)) {
                stopCron();
                dagEngine.suspend(jobId);
                startCron();
                response.setStatus(HttpServletResponse.SC_OK);
            }
            else if (action.equals(RestConstants.JOB_ACTION_KILL)) {
                stopCron();
                dagEngine.kill(jobId);
                startCron();
                response.setStatus(HttpServletResponse.SC_OK);
            }
            else if (action.equals(RestConstants.JOB_ACTION_RERUN)) {
                validateContentType(request, RestConstants.XML_CONTENT_TYPE);
                XConfiguration conf = new XConfiguration(request.getInputStream());
                stopCron();
                conf = conf.trim();
                conf = conf.resolve();
                JobsServlet.validateJobConfiguration(conf);
                checkAuthorizationForApp(getUser(request), conf);
                dagEngine.reRun(jobId, conf);
                startCron();
                response.setStatus(HttpServletResponse.SC_OK);
            }
            else if (action.equals(RestConstants.JOB_COORD_ACTION_RERUN)) {
                validateContentType(request, RestConstants.XML_CONTENT_TYPE);
                stopCron();
                String rerunType = request.getParameter(RestConstants.JOB_COORD_RERUN_TYPE_PARAM);
                String scope = request.getParameter(RestConstants.JOB_COORD_RERUN_SCOPE_PARAM);
                String refresh = request.getParameter(RestConstants.JOB_COORD_RERUN_REFRESH_PARAM);
                String noCleanup = request.getParameter(RestConstants.JOB_COORD_RERUN_NOCLEANUP_PARAM);
                CoordinatorActionInfo coordInfo = coordEngine.reRun(jobId, rerunType, scope, Boolean.valueOf(refresh),
                        Boolean.valueOf(noCleanup));
                List<CoordinatorActionBean> actions = coordInfo.getCoordActions();
                JSONObject json = new JSONObject();
                json.put(JsonTags.COORDINATOR_ACTIONS, CoordinatorActionBean.toJSONArray(actions));
                startCron();
                sendJsonResponse(response, HttpServletResponse.SC_OK, json);
            }
            else {
                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0303,
                        RestConstants.ACTION_PARAM, action);
            }
        }
        catch (DagEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
        catch (BaseEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }

    /**
     * Validate the configuration user/group. <p/>
     *
     * @param requestUser user in request.
     * @param conf configuration.
     * @throws XServletException thrown if the configuration does not have a property {@link
     * org.apache.oozie.client.OozieClient#USER_NAME}.
     */
    static void checkAuthorizationForApp(String requestUser, Configuration conf) throws XServletException {
        String user = conf.get(OozieClient.USER_NAME);
        String group = conf.get(OozieClient.GROUP_NAME);
        try {
            if (user == null) {
                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0401, OozieClient.USER_NAME);
            }
            if (!requestUser.equals(UNDEF) && !user.equals(requestUser)) {
                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0400, requestUser, user);
            }
            AuthorizationService auth = Services.get().get(AuthorizationService.class);
            if (group == null) {
                group = auth.getDefaultGroup(user);
                conf.set(OozieClient.GROUP_NAME, group);
            }
            else {
                auth.authorizeForGroup(user, group);
            }
            XLog.Info.get().setParameter(XLogService.GROUP, group);
            auth.authorizeForApp(user, group, conf.get(OozieClient.APP_PATH), conf);
        }
        catch (AuthorizationException ex) {
            throw new XServletException(HttpServletResponse.SC_UNAUTHORIZED, ex);
        }
    }

    /**
     * Return information about jobs.
     */
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String jobId = getResourceName(request);
        String show = request.getParameter(RestConstants.JOB_SHOW_PARAM);

        try {
            AuthorizationService auth = Services.get().get(AuthorizationService.class);
            auth.authorizeForJob(getUser(request), jobId, false);
        }
        catch (AuthorizationException ex) {
            throw new XServletException(HttpServletResponse.SC_UNAUTHORIZED, ex);
        }

        DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(getUser(request),
                                                                                      getAuthToken(request));
        try {
            if (show == null || show.equals(RestConstants.JOB_SHOW_INFO)) {
                stopCron();
                JsonWorkflowJob job = (JsonWorkflowJob) dagEngine.getJob(jobId);
                startCron();
                sendJsonResponse(response, HttpServletResponse.SC_OK, job);
            }
            else {
                if (show.equals(RestConstants.JOB_SHOW_LOG)) {
                    response.setContentType(TEXT_UTF8);
                    dagEngine.streamLog(jobId, response.getWriter());
                }
                else {
                    if (show.equals(RestConstants.JOB_SHOW_DEFINITION)) {
                        stopCron();
                        response.setContentType(XML_UTF8);
                        String wfDefinition = dagEngine.getDefinition(jobId);
                        startCron();
                        response.setStatus(HttpServletResponse.SC_OK);
                        response.getWriter().write(wfDefinition);
                    }
                    else {
                        throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0303,
                                                    RestConstants.JOB_SHOW_PARAM, show);
                    }
                }
            }
        }
        catch (DagEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }

}
