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
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.BundleJobInfo;
import org.apache.oozie.CoordinatorEngine;
import org.apache.oozie.BundleEngine;
import org.apache.oozie.CoordinatorEngineException;
import org.apache.oozie.BundleEngineException;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.CoordinatorJobInfo;
import org.apache.oozie.DagEngine;
import org.apache.oozie.DagEngineException;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.WorkflowsInfo;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.CoordinatorEngineService;
import org.apache.oozie.service.DagEngineService;
import org.apache.oozie.service.BundleEngineService;
import org.apache.oozie.service.Services;
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
    @Override
    protected JSONObject submitJob(HttpServletRequest request, Configuration conf) throws XServletException,
            IOException {
        JSONObject json = null;

        String jobType = request.getParameter(RestConstants.JOBTYPE_PARAM);

        if (jobType == null) {
            String wfPath = conf.get(OozieClient.APP_PATH);
            String coordPath = conf.get(OozieClient.COORDINATOR_APP_PATH);
            String bundlePath = conf.get(OozieClient.BUNDLE_APP_PATH);

            ServletUtilities.ValidateAppPath(wfPath, coordPath, bundlePath);

            if (wfPath != null) {
                json = submitWorkflowJob(request, conf);
            }
            else if (coordPath != null) {
                json = submitCoordinatorJob(request, conf);
            }
            else {
                json = submitBundleJob(request, conf);
            }
        }
        else { // This is a http submission job
            if (jobType.equals("pig") || jobType.equals("mapreduce")) {
                json = submitHttpJob(request, conf, jobType);
            }
            else {
                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0303,
                        RestConstants.JOBTYPE_PARAM, jobType);
            }
        }
        return json;
    }

    /**
     * v1 service implementation to get a JSONObject representation of a job from its external ID
     */
    @Override
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
            json = getWorkflowJobIdForExternalId(request, externalId);
        }
        else {
            json = getCoordinatorJobIdForExternalId(request, externalId);
        }
        return json;
    }

    /**
     * v1 service implementation to get a list of workflows, coordinators, or bundles, with filtering or interested
     * windows embedded in the request object
     */
    @Override
    protected JSONObject getJobs(HttpServletRequest request) throws XServletException, IOException {
        JSONObject json = null;
        String jobtype = request.getParameter(RestConstants.JOBTYPE_PARAM);
        jobtype = (jobtype != null) ? jobtype : "wf";

        if (jobtype.contains("wf")) {
            json = getWorkflowJobs(request);
        }
        else if (jobtype.contains("coord")) {
            json = getCoordinatorJobs(request);
        }
        else if (jobtype.contains("bundle")) {
            json = getBundleJobs(request);
        }
        return json;
    }

    /**
     * v1 service implementation to submit a workflow job
     */
    @SuppressWarnings("unchecked")
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
    @SuppressWarnings("unchecked")
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
     * v1 service implementation to submit a bundle job
     */
    @SuppressWarnings("unchecked")
    private JSONObject submitBundleJob(HttpServletRequest request, Configuration conf) throws XServletException {
        JSONObject json = new JSONObject();
        XLog.getLog(getClass()).warn("submitBundleJob " + XmlUtils.prettyPrint(conf).toString());
        try {
            String action = request.getParameter(RestConstants.ACTION_PARAM);
            if (action != null && !action.equals(RestConstants.JOB_ACTION_START)
                    && !action.equals(RestConstants.JOB_ACTION_DRYRUN)) {
                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0303,
                        RestConstants.ACTION_PARAM, action);
            }
            boolean startJob = (action != null);
            String user = conf.get(OozieClient.USER_NAME);
            BundleEngine bundleEngine = Services.get().get(BundleEngineService.class).getBundleEngine(user,
                    getAuthToken(request));
            String id = null;
            boolean dryrun = false;
            if (action != null) {
                dryrun = (action.equals(RestConstants.JOB_ACTION_DRYRUN));
            }
            if (dryrun) {
                id = bundleEngine.dryrunSubmit(conf, startJob);
            }
            else {
                id = bundleEngine.submitJob(conf, startJob);
            }
            json.put(JsonTags.JOB_ID, id);
        }
        catch (BundleEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }

        return json;
    }

    /**
     * v1 service implementation to get a JSONObject representation of a job from its external ID
     */
    @SuppressWarnings("unchecked")
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

    @SuppressWarnings("unchecked")
    private JSONObject getBundleJobs(HttpServletRequest request) throws XServletException {
        JSONObject json = new JSONObject();
        try {
            String filter = request.getParameter(RestConstants.JOBS_FILTER_PARAM);
            String startStr = request.getParameter(RestConstants.OFFSET_PARAM);
            String lenStr = request.getParameter(RestConstants.LEN_PARAM);
            int start = (startStr != null) ? Integer.parseInt(startStr) : 1;
            start = (start < 1) ? 1 : start;
            int len = (lenStr != null) ? Integer.parseInt(lenStr) : 50;
            len = (len < 1) ? 50 : len;

            BundleEngine bundleEngine = Services.get().get(BundleEngineService.class).getBundleEngine(getUser(request),
                    getAuthToken(request));
            BundleJobInfo jobs = bundleEngine.getBundleJobs(filter, start, len);
            List<BundleJobBean> jsonJobs = jobs.getBundleJobs();

            json.put(JsonTags.BUNDLE_JOBS, BundleJobBean.toJSONArray(jsonJobs));
            json.put(JsonTags.BUNDLE_JOB_TOTAL, jobs.getTotal());
            json.put(JsonTags.BUNDLE_JOB_OFFSET, jobs.getStart());
            json.put(JsonTags.BUNDLE_JOB_LEN, jobs.getLen());

        }
        catch (BundleEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
        return json;
    }

    /**
     * service implementation to submit a http job
     */
    private JSONObject submitHttpJob(HttpServletRequest request, Configuration conf, String jobType)
            throws XServletException {
        JSONObject json = new JSONObject();

        try {
            String user = conf.get(OozieClient.USER_NAME);
            DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(user, getAuthToken(request));
            String id = dagEngine.submitHttpJob(conf, jobType);
            json.put(JsonTags.JOB_ID, id);
        }
        catch (DagEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }

        return json;
    }
}
