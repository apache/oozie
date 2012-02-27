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
import java.io.Writer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.DagEngine;
import org.apache.oozie.DagEngineException;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowsInfo;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.rest.JsonWorkflowAction;
import org.apache.oozie.client.rest.JsonWorkflowJob;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.DagEngineService;
import org.apache.oozie.util.XmlUtils;

public class MockDagEngineService extends DagEngineService {
    public static final String JOB_ID = "job-";
    public static final String JOB_ID_END = "-W";
    public static final String ACTION_ID = "action-";
    public static final String EXT_ID = "ext-";
    public static final String WORKFLOW_APP = "<workflow-app/>";
    public static final String CONFIGURATION = "<configuration/>";
    public static final String GROUP = "group";
    public static final String USER = "user";

    public static final String LOG = "log";

    public static Configuration submittedConf;
    public static String did = null;
    public static String user = null;
    public static Properties properties;
    public static List<WorkflowJob> workflows;
    public static List<Boolean> started;
    public static final int INIT_WF_COUNT = 4;

    static {
        reset();
    }

    public static void reset() {
        user = null;
        did = null;
        properties = null;
        workflows = new ArrayList<WorkflowJob>();
        started = new ArrayList<Boolean>();
        for (int i = 0; i < INIT_WF_COUNT; i++) {
            workflows.add(createDummyWorkflow(i));
            started.add(false);
        }
    }

    @Override
    public DagEngine getSystemDagEngine() {
        return new MockDagEngine();
    }

    @Override
    public DagEngine getDagEngine(String user, String authToken) {
        return new MockDagEngine(user, authToken);
    }

    private static class MockDagEngine extends DagEngine {

        public MockDagEngine() {
        }

        public MockDagEngine(String user, String authToken) {
            super(user, authToken);
        }

        @Override
        public String submitJob(Configuration conf, boolean startJob) throws DagEngineException {
            did = "submit";
            submittedConf = conf;
            int idx = workflows.size();
            WorkflowJob job = createDummyWorkflow(idx, XmlUtils.prettyPrint(conf).toString());
            workflows.add(job);
            started.add(startJob);
            MockDagEngineService.user = getUser();
            return job.getId();
        }

        @Override
        public String submitHttpJob(Configuration conf, String jobType) throws DagEngineException {
            if (jobType.equals("pig")) {
                did = "submitPig";
            }
            else if (jobType.equals("mapreduce")) {
                did = "submitMR";
            }
            int idx = workflows.size();
            WorkflowJob job = createDummyWorkflow(idx, XmlUtils.prettyPrint(conf).toString());
            workflows.add(job);
            started.add(true);
            return job.getId();
        }

        @Override
        public void start(String jobId) throws DagEngineException {
            did = RestConstants.JOB_ACTION_START;
            int idx = validateWorkflowIdx(jobId);
            started.set(idx, true);
        }

        @Override
        public void resume(String jobId) throws DagEngineException {
            did = RestConstants.JOB_ACTION_RESUME;
            int idx = validateWorkflowIdx(jobId);
            started.set(idx, true);
        }

        @Override
        public void suspend(String jobId) throws DagEngineException {
            did = RestConstants.JOB_ACTION_SUSPEND;
            int idx = validateWorkflowIdx(jobId);
            started.set(idx, false);
        }

        @Override
        public void kill(String jobId) throws DagEngineException {
            did = RestConstants.JOB_ACTION_KILL;
            int idx = validateWorkflowIdx(jobId);
            started.set(idx, false);
        }

        @Override
        public void reRun(String jobId, Configuration conf) throws DagEngineException {
            did = RestConstants.JOB_ACTION_RERUN;
            int idx = validateWorkflowIdx(jobId);
            started.set(idx, true);
        }

        @Override
        public void processCallback(String actionId, String externalStatus, Properties actionData)
                throws DagEngineException {
            if (actionId.equals("ok") && externalStatus.equals("ok")) {
                properties = actionData;
                return;
            }
            throw new DagEngineException(ErrorCode.ETEST, actionId);
        }

        @Override
        public WorkflowJob getJob(String jobId) throws DagEngineException {
            did = RestConstants.JOB_SHOW_INFO;
            int idx = validateWorkflowIdx(jobId);
            return workflows.get(idx);
        }

        @Override
        public WorkflowJob getJob(String jobId, int start, int length) throws DagEngineException {
            return getJob(jobId);
        }

        @Override
        public String getDefinition(String jobId) throws DagEngineException {
            did = RestConstants.JOB_SHOW_DEFINITION;
            int idx = validateWorkflowIdx(jobId);
            //FIXME:
            return WORKFLOW_APP;
        }

        @Override
        public void streamLog(String jobId, Writer writer) throws IOException, DagEngineException {
            did = RestConstants.JOB_SHOW_LOG;
            validateWorkflowIdx(jobId);
            writer.write(LOG);
        }

        @Override
        public WorkflowsInfo getJobs(String filter, int start, int len) throws DagEngineException {
            parseFilter(filter);
            did = RestConstants.JOBS_FILTER_PARAM;
            return new WorkflowsInfo((List) workflows, start, len, workflows.size());
        }

        @Override
        public String getJobIdForExternalId(String externalId) throws DagEngineException {
            did = RestConstants.JOBS_EXTERNAL_ID_PARAM;
            return (externalId.equals("external-valid")) ? "id-valid" : null;
        }

        private int validateWorkflowIdx(String jobId) throws DagEngineException {
            int idx = -1;
            try {
                if (jobId.endsWith(JOB_ID_END)) {
                    jobId = jobId.replace(JOB_ID, "");
                    jobId = jobId.replace(JOB_ID_END, "");
                }
                else {
                    jobId = jobId.replace(JOB_ID, "");
                }
                idx = Integer.parseInt(jobId);
            }
            catch (Exception e) {
                throw new DagEngineException(ErrorCode.ETEST, jobId);
            }

            if (idx >= workflows.size()) {
                throw new DagEngineException(ErrorCode.ETEST, jobId);
            }

            return idx;
        }
    }

    private static WorkflowJob createDummyWorkflow(int idx) {
        JsonWorkflowJob workflow = new JsonWorkflowJob();
        workflow.setId(JOB_ID + idx + JOB_ID_END);
        workflow.setAppPath("hdfs://blah/blah/" + idx + "-blah");
        workflow.setStatus((idx % 2) == 0 ? WorkflowJob.Status.RUNNING : WorkflowJob.Status.SUCCEEDED);
        workflow.setRun(idx);
        workflow.setCreatedTime(new Date());
        workflow.setStartTime(new Date());
        workflow.setEndTime((idx % 2) == 0 ? null : (new Date()));
        workflow.setConf(CONFIGURATION);
        workflow.setAppName("workflow-" + idx);
        workflow.setGroup(GROUP);
        workflow.setUser(USER);

        List<JsonWorkflowAction> actions = new ArrayList<JsonWorkflowAction>();
        for (int i = 0; i < idx; i++) {
            actions.add(createDummyAction(i));
        }

        workflow.setActions(actions);
        return workflow;
    }

    private static WorkflowJob createDummyWorkflow(int idx, String conf) {
        JsonWorkflowJob workflow = new JsonWorkflowJob();
        workflow.setId(JOB_ID + idx + JOB_ID_END);
        workflow.setAppPath("hdfs://blah/blah/" + idx + "-blah");
        workflow.setStatus((idx % 2) == 0 ? WorkflowJob.Status.RUNNING : WorkflowJob.Status.SUCCEEDED);
        workflow.setRun(idx);
        workflow.setCreatedTime(new Date());
        workflow.setStartTime(new Date());
        workflow.setEndTime((idx % 2) == 0 ? null : (new Date()));
        workflow.setConf(conf);
        workflow.setAppName("workflow-" + idx);
        workflow.setGroup(GROUP);
        workflow.setUser(USER);

        List<JsonWorkflowAction> actions = new ArrayList<JsonWorkflowAction>();
        for (int i = 0; i < idx; i++) {
            actions.add(createDummyAction(i));
        }

        workflow.setActions(actions);
        return workflow;
    }

    private static JsonWorkflowAction createDummyAction(int idx) {
        JsonWorkflowAction action = new JsonWorkflowAction();
        int mod = idx % 5;
        action.setId(ACTION_ID + idx);
        action.setExternalId(EXT_ID + idx);
        action.setConsoleUrl("http://blah:blah/blah/" + idx);
        action.setConf("<configuration/>");
        action.setData(null);
        action.setStartTime(new Date());
        action.setEndTime(new Date());
        action.setErrorInfo(null, null);
        action.setExternalStatus((idx % 2) == 0 ? "RUNNING" : "OK");
        action.setName(ACTION_ID + idx);
        action.setRetries(idx);
        action.setTrackerUri("http://trackerhost:blah/blah/" + idx);
        action.setTransition("OK");
        switch (mod) {
            case 0: {
                action.setType("hadoop");
                break;
            }
            case 1: {
                action.setType("fs");
                break;
            }
            case 2: {
                action.setType("pig");
                break;
            }
            case 3: {
                action.setType("ssh");
                break;
            }
            case 4: {
                action.setType("decision");
                break;
            }
        }

        return action;
    }
}
