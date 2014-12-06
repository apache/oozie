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
import org.apache.oozie.DagEngine;
import org.apache.oozie.DagEngineException;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.WorkflowsInfo;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.DagEngineService;
import org.apache.oozie.service.Services;
import org.json.simple.JSONObject;

public class V0JobsServlet extends BaseJobsServlet {

    private static final String INSTRUMENTATION_NAME = "v0jobs";

    public V0JobsServlet() {
        super(INSTRUMENTATION_NAME);
    }


    /**
     * v0 service implementation to submit a workflow job
     */
    @Override
    protected JSONObject submitJob(HttpServletRequest request, Configuration conf) throws XServletException, IOException {

        JSONObject json = new JSONObject();

        try {
            String action = request.getParameter(RestConstants.ACTION_PARAM);
            if (action != null && !action.equals(RestConstants.JOB_ACTION_START)) {
                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0303, RestConstants.ACTION_PARAM, action);
            }
            boolean startJob = (action != null);
            String user = conf.get(OozieClient.USER_NAME);
            DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(user);
            String id = dagEngine.submitJob(conf, startJob);
            json.put(JsonTags.JOB_ID, id);
        }
        catch (DagEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }

        return json;
    }

    /**
     * v0 service implementation to get a JSONObject representation of a job from its external ID
     */
    @Override
    protected JSONObject getJobIdForExternalId(HttpServletRequest request, String externalId) throws XServletException, IOException {
        JSONObject json = new JSONObject();
        try {
            DagEngine dagEngine = Services.get().get(DagEngineService.class)
            .getDagEngine(getUser(request));
            String jobId = dagEngine.getJobIdForExternalId(externalId);
            json.put(JsonTags.JOB_ID, jobId);
        }
        catch (DagEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
        return json;
    }

    /**
     * v0 service implementation to get a list of workflows, with filtering or interested windows embedded in the
     * request object
     */
    @Override
    protected JSONObject getJobs(HttpServletRequest request) throws XServletException, IOException {
        JSONObject json = new JSONObject();
        try {
            String filter = request.getParameter(RestConstants.JOBS_FILTER_PARAM);
            String startStr = request.getParameter(RestConstants.OFFSET_PARAM);
            String lenStr = request.getParameter(RestConstants.LEN_PARAM);
            int start = (startStr != null) ? Integer.parseInt(startStr) : 1;
            start = (start < 1) ? 1 : start;
            int len = (lenStr != null) ? Integer.parseInt(lenStr) : 50;
            len = (len < 1) ? 50 : len;
            DagEngine dagEngine = Services.get().get(DagEngineService.class)
            .getDagEngine(getUser(request));
            WorkflowsInfo jobs = dagEngine.getJobs(filter, start, len);
            List<WorkflowJobBean> jsonWorkflows = jobs.getWorkflows();
            json.put(JsonTags.WORKFLOWS_JOBS, WorkflowJobBean.toJSONArray(jsonWorkflows, "GMT"));
            json.put(JsonTags.WORKFLOWS_TOTAL, jobs.getTotal());
            json.put(JsonTags.WORKFLOWS_OFFSET, jobs.getStart());
            json.put(JsonTags.WORKFLOWS_LEN, jobs.getLen());

        }
        catch (DagEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }

        return json;
    }
}
