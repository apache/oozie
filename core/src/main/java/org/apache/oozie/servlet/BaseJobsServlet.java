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
import java.util.Arrays;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.AuthorizationException;
import org.apache.oozie.service.AuthorizationService;
import org.apache.oozie.util.JobUtils;
import org.apache.oozie.util.JobsFilterUtils;
import org.apache.oozie.util.XConfiguration;
import org.json.simple.JSONObject;

public abstract class BaseJobsServlet extends JsonRestServlet {

    private static final JsonRestServlet.ResourceInfo RESOURCES_INFO[] = new JsonRestServlet.ResourceInfo[1];

    static {
        RESOURCES_INFO[0] = new JsonRestServlet.ResourceInfo("", Arrays.asList(
                "POST", "GET", "PUT"), Arrays.asList(
                new JsonRestServlet.ParameterInfo(RestConstants.ACTION_PARAM,
                                                  String.class, false, Arrays.asList("POST", "PUT")),
                new JsonRestServlet.ParameterInfo(
                        RestConstants.JOBS_FILTER_PARAM, String.class, false,
                        Arrays.asList("GET", "PUT")),
                new JsonRestServlet.ParameterInfo(RestConstants.JOBTYPE_PARAM,
                                                  String.class, false, Arrays.asList("GET", "POST", "PUT")),
                new JsonRestServlet.ParameterInfo(RestConstants.OFFSET_PARAM,
                                                  String.class, false, Arrays.asList("GET", "PUT")),
                new JsonRestServlet.ParameterInfo(RestConstants.LEN_PARAM,
                                                  String.class, false, Arrays.asList("GET", "PUT")),
                new JsonRestServlet.ParameterInfo(RestConstants.JOBS_BULK_PARAM,
                                                  String.class, false, Arrays.asList("GET", "PUT")),
                new JsonRestServlet.ParameterInfo(
                        RestConstants.JOBS_EXTERNAL_ID_PARAM, String.class,
                        false, Arrays.asList("GET"))));
    }

    public BaseJobsServlet(String instrumentationName) {
        super(instrumentationName, RESOURCES_INFO);
    }

    /**
     * Create a job.
     */
    @Override
    @SuppressWarnings("unchecked")
    protected void doPost(HttpServletRequest request,
            HttpServletResponse response) throws ServletException, IOException {
        /*
         * Enumeration p = request.getAttributeNames();
         * for(;p.hasMoreElements();){ String key = (String)p.nextElement();
         * XLog.getLog(getClass()).warn(" key "+ key + " val "+ (String)
         * request.getAttribute(key)); }
         */
        validateContentType(request, RestConstants.XML_CONTENT_TYPE);

        request.setAttribute(AUDIT_OPERATION, request
                .getParameter(RestConstants.ACTION_PARAM));

        XConfiguration conf = new XConfiguration(request.getInputStream());

        stopCron();

        conf = conf.trim();
        conf = conf.resolve();

        validateJobConfiguration(conf);
        String requestUser = getUser(request);
        if (!requestUser.equals(UNDEF)) {
            conf.set(OozieClient.USER_NAME, requestUser);
        }
        BaseJobServlet.checkAuthorizationForApp(conf);
        JobUtils.normalizeAppPath(conf.get(OozieClient.USER_NAME), conf.get(OozieClient.GROUP_NAME), conf);

        JSONObject json = submitJob(request, conf);
        startCron();
        sendJsonResponse(response, HttpServletResponse.SC_CREATED, json);
    }

    /**
     * Return information about jobs.
     */
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
        String externalId = request
        .getParameter(RestConstants.JOBS_EXTERNAL_ID_PARAM);
        if (externalId != null) {
            stopCron();
            JSONObject json = getJobIdForExternalId(request, externalId);
            startCron();
            sendJsonResponse(response, HttpServletResponse.SC_OK, json);
        }
        else {
            stopCron();
            JSONObject json = getJobs(request);
            startCron();
            sendJsonResponse(response, HttpServletResponse.SC_OK, json);
        }
    }

    /**
     * Perform various job related actions - suspend, resume, kill, etc.
     */
    @Override
    protected void doPut(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        request.setAttribute(AUDIT_PARAM, request.getParameter(RestConstants.JOBS_FILTER_PARAM));
        request.setAttribute(AUDIT_OPERATION, request.getParameter(RestConstants.ACTION_PARAM));
        try {
            AuthorizationService auth = Services.get().get(AuthorizationService.class);
            String filter = request.getParameter(RestConstants.JOBS_FILTER_PARAM);
            String startStr = request.getParameter(RestConstants.OFFSET_PARAM);
            String lenStr = request.getParameter(RestConstants.LEN_PARAM);
            String jobType = request.getParameter(RestConstants.JOBTYPE_PARAM);

            if (filter == null) {
                throw new IllegalArgumentException("filter params must be specified for bulk write API");
            }
            int start = (startStr != null) ? Integer.parseInt(startStr) : 1;
            start = (start < 1) ? 1 : start;
            int len = (lenStr != null) ? Integer.parseInt(lenStr) : 50;
            len = (len < 1) ? 50 : len;
            auth.authorizeForJobs(getUser(request), JobsFilterUtils.parseFilter(filter), jobType, start, len, true);
        }
        catch (AuthorizationException ex) {
            throw new XServletException(HttpServletResponse.SC_UNAUTHORIZED, ex);
        }

        String action = request.getParameter(RestConstants.ACTION_PARAM);
        JSONObject json = null;
        if (action.equals(RestConstants.JOB_ACTION_KILL)) {
            stopCron();
            json = killJobs(request, response);
            startCron();
        }
        else if (action.equals(RestConstants.JOB_ACTION_RESUME)) {
            stopCron();
            json = resumeJobs(request, response);
            startCron();
        }
        else if (action.equals(RestConstants.JOB_ACTION_SUSPEND)) {
            stopCron();
            json = suspendJobs(request, response);
            startCron();
        }
        else {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0303,
                    RestConstants.ACTION_PARAM, action);
        }
        response.setStatus(HttpServletResponse.SC_OK);
        sendJsonResponse(response, HttpServletResponse.SC_OK, json);
    }

    /**
     * abstract method to kill jobs based ona filter param. The jobs could be workflow, coordinator or bundle jobs
     *
     * @param request
     * @param response
     * @return JSONObject of all jobs being killed
     * @throws XServletException
     * @throws IOException
     */
    abstract JSONObject killJobs(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            IOException;

    /**
     * abstract method to suspend jobs based ona filter param. The jobs could be workflow, coordinator or bundle jobs
     *
     * @param request
     * @param response
     * @return JSONObject of all jobs being suspended
     * @throws XServletException
     * @throws IOException
     */
    abstract JSONObject suspendJobs(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            IOException;

    /**
     * abstract method to resume jobs based ona filter param. The jobs could be workflow, coordinator or bundle jobs
     *
     * @param request
     * @param response
     * @return JSONObject of all jobs being resumed
     * @throws XServletException
     * @throws IOException
     */
    abstract JSONObject resumeJobs(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            IOException;
    /**
     * abstract method to submit a job, either workflow or coordinator in the case of workflow job, there is an optional
     * flag in request to indicate if want this job to be started immediately or not
     *
     * @param request
     * @param conf
     * @return JSONObject of job id
     * @throws XServletException
     * @throws IOException
     */
    abstract JSONObject submitJob(HttpServletRequest request, Configuration conf)
    throws XServletException, IOException;

    /**
     * abstract method to get a job from external ID
     *
     * @param request
     * @param externalId
     * @return JSONObject for the requested job
     * @throws XServletException
     * @throws IOException
     */
    abstract JSONObject getJobIdForExternalId(HttpServletRequest request,
            String externalId) throws XServletException, IOException;

    /**
     * abstract method to get a list of workflow jobs
     *
     * @param request
     * @return JSONObject of the requested jobs
     * @throws XServletException
     * @throws IOException
     */
    abstract JSONObject getJobs(HttpServletRequest request)
    throws XServletException, IOException;

    static void validateJobConfiguration(Configuration conf) throws XServletException {
        if (conf.get(OozieClient.USER_NAME) == null) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0401,
                    OozieClient.USER_NAME);
        }
    }
}
