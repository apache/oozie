/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.servlet;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.DagEngineService;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.DagEngine;
import org.apache.oozie.DagEngineException;
import org.apache.oozie.WorkflowsInfo;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.ErrorCode;
import org.json.simple.JSONObject;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class    JobsServlet extends JsonRestServlet {
    private static final String INSTRUMENTATION_NAME = "jobs";

    private static final JsonRestServlet.ResourceInfo RESOURCES_INFO[] = new JsonRestServlet.ResourceInfo[1];

    static {
        RESOURCES_INFO[0] =
        new JsonRestServlet.ResourceInfo("", Arrays.asList("POST", "GET"), Arrays.asList(
        new JsonRestServlet.ParameterInfo(RestConstants.ACTION_PARAM, String.class, false, Arrays.asList("POST")),
        new JsonRestServlet.ParameterInfo(RestConstants.JOBS_FILTER_PARAM, String.class, false, Arrays.asList("GET")),
        new JsonRestServlet.ParameterInfo(RestConstants.OFFSET_PARAM, String.class, false, Arrays.asList("GET")),
        new JsonRestServlet.ParameterInfo(RestConstants.LEN_PARAM, String.class, false, Arrays.asList("GET")),
        new JsonRestServlet.ParameterInfo(RestConstants.JOBS_EXTERNAL_ID_PARAM, String.class, false, Arrays.asList("GET"))));
    }

    public JobsServlet() {
        super(INSTRUMENTATION_NAME, RESOURCES_INFO);
    }

    /**
     * Create a job.
     */
    @SuppressWarnings("unchecked")
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        validateContentType(request, RestConstants.XML_CONTENT_TYPE);

        request.setAttribute(AUDIT_OPERATION, request.getParameter(RestConstants.ACTION_PARAM));

        Configuration conf = new XConfiguration(request.getInputStream());

        stopCron();

        conf = XConfiguration.trim(conf);

        validateJobConfiguration(conf);

        JobServlet.checkAuthorizationForApp(getUser(request), conf);

        String action = request.getParameter(RestConstants.ACTION_PARAM);
        if (action != null && !action.equals(RestConstants.JOB_ACTION_START)) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0303, RestConstants.ACTION_PARAM,
                                        action);
        }
        try {
            boolean startJob = (action != null);
            String user = conf.get(OozieClient.USER_NAME);
            DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(user, getAuthToken(request));
            String id = dagEngine.submitJob(conf, startJob);
            JSONObject json = new JSONObject();
            json.put(JsonTags.JOB_ID, id);
            startCron();
            sendJsonResponse(response, HttpServletResponse.SC_CREATED, json);
        }
        catch (DagEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }

    static void validateJobConfiguration(Configuration conf) throws XServletException {
        if (conf.get(OozieClient.USER_NAME) == null) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0401,
                                        OozieClient.USER_NAME);
        }

        //TODO: it should use KerberosHadoopAccessorService.KERBEROS_AUTH_ENABLED once 20.1 is not used anymore
        if (Services.get().getConf().getBoolean("oozie.service.HadoopAccessorService.kerberos.enabled", false)) {
            if (conf.get(WorkflowAppService.HADOOP_JT_KERBEROS_NAME) == null) {
                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0401,
                                            WorkflowAppService.HADOOP_JT_KERBEROS_NAME);
            }
            if (conf.get(WorkflowAppService.HADOOP_NN_KERBEROS_NAME) == null) {
                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0401,
                                            WorkflowAppService.HADOOP_NN_KERBEROS_NAME);
            }
        }
        else {
            conf.set(WorkflowAppService.HADOOP_JT_KERBEROS_NAME, "");
            conf.set(WorkflowAppService.HADOOP_NN_KERBEROS_NAME, "");
        }
    }

    /**
     * Return information about jobs.
     */
    @SuppressWarnings("unchecked")
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        try {
            String externalId = request.getParameter(RestConstants.JOBS_EXTERNAL_ID_PARAM);
            if (externalId != null) {
                stopCron();
                DagEngine dagEngine = Services.get().get(DagEngineService.class)
                        .getDagEngine(getUser(request), getAuthToken(request));
                String jobId = dagEngine.getJobIdForExternalId(externalId);
                JSONObject json = new JSONObject();
                json.put(JsonTags.JOB_ID, jobId);
                startCron();
                sendJsonResponse(response, HttpServletResponse.SC_OK, json);
            }
            else {
                String filter = request.getParameter(RestConstants.JOBS_FILTER_PARAM);
                String startStr = request.getParameter(RestConstants.OFFSET_PARAM);
                String lenStr = request.getParameter(RestConstants.LEN_PARAM);
                int start = (startStr != null) ? Integer.parseInt(startStr) : 1;
                start = (start < 1) ? 1 : start;
                int len = (lenStr != null) ? Integer.parseInt(lenStr) : 50;
                len = (len < 1) ? 50 : len;
                stopCron();
                DagEngine dagEngine = Services.get().get(DagEngineService.class)
                        .getDagEngine(getUser(request), getAuthToken(request));
                WorkflowsInfo jobs = dagEngine.getJobs(filter, start, len);
                List<WorkflowJobBean> jsonWorkflows = jobs.getWorkflows();
                startCron();
                JSONObject json = new JSONObject();
                json.put(JsonTags.WORKFLOWS_JOBS, WorkflowJobBean.toJSONArray(jsonWorkflows));
                json.put(JsonTags.WORKFLOWS_TOTAL, jobs.getTotal());
                json.put(JsonTags.WORKFLOWS_OFFSET, jobs.getStart());
                json.put(JsonTags.WORKFLOWS_LEN, jobs.getLen());
                sendJsonResponse(response, HttpServletResponse.SC_OK, json);
            }
        }
        catch (DagEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }

}