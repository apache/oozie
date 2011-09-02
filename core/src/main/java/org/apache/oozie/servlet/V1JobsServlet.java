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

import java.io.IOException;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.CoordinatorJobInfo;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.DagEngine;
import org.apache.oozie.DagEngineException;
import org.apache.oozie.CoordinatorEngine;
import org.apache.oozie.CoordinatorEngineException;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.WorkflowsInfo;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.DagEngineService;
import org.apache.oozie.service.CoordinatorEngineService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.json.simple.JSONObject;

public class V1JobsServlet extends BaseJobsServlet {

    private static final String INSTRUMENTATION_NAME = "v1jobs";

    public V1JobsServlet() {
        super(INSTRUMENTATION_NAME);
    }

    /**
     * v1 service implementation to submit a job, either workflow or coordinator
     */
    protected JSONObject submitJob(HttpServletRequest request, Configuration conf) throws XServletException,
            IOException {
        JSONObject json = null;
        String wfPath = conf.get(OozieClient.APP_PATH);
        String coordPath = conf.get(OozieClient.COORDINATOR_APP_PATH);

        ServletUtilities.ValidateAppPath(wfPath, coordPath);

        if (wfPath != null) {
            json = submitWorkflowJob(request, conf);
        }
        else {
            json = submitCoordinatorJob(request, conf);
        }
        return json;
    }

    /**
     * v1 service implementation to get a JSONObject representation of a job from its external ID
     */
    protected JSONObject getJobIdForExternalId(HttpServletRequest request, String externalId) throws XServletException,
            IOException {
        JSONObject json = null;
        /*
         * Configuration conf = new XConfiguration(); String wfPath =
         * conf.get(OozieClient.APP_PATH); String coordPath =
         * conf.get(OozieClient.COORDINATOR_APP_PATH);
         * 
         * ServletUtilities.ValidateAppPath(wfPath, coordPath);
         */
        String jobtype = request.getParameter(RestConstants.JOBTYPE_PARAM);
        jobtype = (jobtype != null) ? jobtype : "wf";
        if (jobtype.contains("wf")) {
            getWorkflowJobIdForExternalId(request, externalId);
        }
        else {
            getCoordinatorJobIdForExternalId(request, externalId);
        }
        return json;
    }

    /**
     * v1 service implementation to get a list of workflows, with filtering or interested windows embedded in the
     * request object
     */
    protected JSONObject getJobs(HttpServletRequest request) throws XServletException, IOException {
        JSONObject json = null;
        /*
         * json = getWorkflowJobs(request); if (json != null) { return json; }
         * else { json = getCoordinatorJobs(request); } return json;
         */
        // Configuration conf = new XConfiguration();

        String jobtype = request.getParameter(RestConstants.JOBTYPE_PARAM);
        jobtype = (jobtype != null) ? jobtype : "wf";

        if (jobtype.contains("wf")) {
            json = getWorkflowJobs(request);
        }
        else {
            json = getCoordinatorJobs(request);
        }
        return json;
    }

    /**
     * v1 service implementation to submit a workflow job
     */
    private JSONObject submitWorkflowJob(HttpServletRequest request, Configuration conf) throws XServletException {

        JSONObject json = new JSONObject();

        try {
            String action = request.getParameter(RestConstants.ACTION_PARAM);
            if (action != null && !action.equals(RestConstants.JOB_ACTION_START)) {
                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0303,
                                            RestConstants.ACTION_PARAM, action);
            }
            boolean startJob = (action != null);
            String user = conf.get(OozieClient.USER_NAME);
            DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(user, getAuthToken(request));
            String id = dagEngine.submitJob(conf, startJob);
            json.put(JsonTags.JOB_ID, id);
        }
        catch (DagEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }

        return json;
    }

    /**
     * v1 service implementation to submit a coordinator job
     */
    private JSONObject submitCoordinatorJob(HttpServletRequest request, Configuration conf) throws XServletException {

        JSONObject json = new JSONObject();
        XLog.getLog(getClass()).warn("submitCoordinatorJob " + XmlUtils.prettyPrint(conf).toString());
        try {
            String action = request.getParameter(RestConstants.ACTION_PARAM);
            if (action != null && !action.equals(RestConstants.JOB_ACTION_START)
                    && !action.equals(RestConstants.JOB_ACTION_DRYRUN)) {
                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0303,
                                            RestConstants.ACTION_PARAM, action);
            }
            boolean startJob = (action != null);
            String user = conf.get(OozieClient.USER_NAME);
            CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class).getCoordinatorEngine(
                    user, getAuthToken(request));
            String id = null;
            boolean dryrun = false;
            if (action != null) {
                dryrun = (action.equals(RestConstants.JOB_ACTION_DRYRUN));
            }
            if (dryrun) {
                id = coordEngine.dryrunSubmit(conf, startJob);
            }
            else {
                id = coordEngine.submitJob(conf, startJob);
            }
            json.put(JsonTags.JOB_ID, id);
        }
        catch (CoordinatorEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }

        return json;
    }

    /**
     * v1 service implementation to get a JSONObject representation of a job from its external ID
     */
    private JSONObject getWorkflowJobIdForExternalId(HttpServletRequest request, String externalId)
            throws XServletException {
        JSONObject json = new JSONObject();
        try {
            DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(getUser(request),
                                                                                          getAuthToken(request));
            String jobId = dagEngine.getJobIdForExternalId(externalId);
            json.put(JsonTags.JOB_ID, jobId);
        }
        catch (DagEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
        return json;
    }

    /**
     * v1 service implementation to get a JSONObject representation of a job from its external ID
     */
    private JSONObject getCoordinatorJobIdForExternalId(HttpServletRequest request, String externalId)
            throws XServletException {
        JSONObject json = new JSONObject();
        // TODO
        return json;
    }

    /**
     * v1 service implementation to get a list of workflows, with filtering or interested windows embedded in the
     * request object
     */
    private JSONObject getWorkflowJobs(HttpServletRequest request) throws XServletException {
        JSONObject json = new JSONObject();
        try {
            String filter = request.getParameter(RestConstants.JOBS_FILTER_PARAM);
            String startStr = request.getParameter(RestConstants.OFFSET_PARAM);
            String lenStr = request.getParameter(RestConstants.LEN_PARAM);
            int start = (startStr != null) ? Integer.parseInt(startStr) : 1;
            start = (start < 1) ? 1 : start;
            int len = (lenStr != null) ? Integer.parseInt(lenStr) : 50;
            len = (len < 1) ? 50 : len;
            DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(getUser(request),
                                                                                          getAuthToken(request));
            WorkflowsInfo jobs = dagEngine.getJobs(filter, start, len);
            List<WorkflowJobBean> jsonWorkflows = jobs.getWorkflows();
            json.put(JsonTags.WORKFLOWS_JOBS, WorkflowJobBean.toJSONArray(jsonWorkflows));
            json.put(JsonTags.WORKFLOWS_TOTAL, jobs.getTotal());
            json.put(JsonTags.WORKFLOWS_OFFSET, jobs.getStart());
            json.put(JsonTags.WORKFLOWS_LEN, jobs.getLen());

        }
        catch (DagEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }

        return json;
    }

    /**
     * v1 service implementation to get a list of workflows, with filtering or interested windows embedded in the
     * request object
     */
    @SuppressWarnings("unchecked")
    private JSONObject getCoordinatorJobs(HttpServletRequest request) throws XServletException {
        JSONObject json = new JSONObject();
        try {
            String filter = request.getParameter(RestConstants.JOBS_FILTER_PARAM);
            String startStr = request.getParameter(RestConstants.OFFSET_PARAM);
            String lenStr = request.getParameter(RestConstants.LEN_PARAM);
            int start = (startStr != null) ? Integer.parseInt(startStr) : 1;
            start = (start < 1) ? 1 : start;
            int len = (lenStr != null) ? Integer.parseInt(lenStr) : 50;
            len = (len < 1) ? 50 : len;
            CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class).getCoordinatorEngine(
                    getUser(request), getAuthToken(request));
            CoordinatorJobInfo jobs = coordEngine.getCoordJobs(filter, start, len);
            List<CoordinatorJobBean> jsonJobs = jobs.getCoordJobs();
            json.put(JsonTags.COORDINATOR_JOBS, CoordinatorJobBean.toJSONArray(jsonJobs));
            json.put(JsonTags.COORD_JOB_TOTAL, jobs.getTotal());
            json.put(JsonTags.COORD_JOB_OFFSET, jobs.getStart());
            json.put(JsonTags.COORD_JOB_LEN, jobs.getLen());

        }
        catch (CoordinatorEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
        return json;
    }
}
