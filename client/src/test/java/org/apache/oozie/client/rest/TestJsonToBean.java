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
package org.apache.oozie.client.rest;

import junit.framework.TestCase;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.List;

public class TestJsonToBean extends TestCase {

    static String CREATED_TIME = "Thu, 01 Jan 2009 00:00:00 GMT";
    static String START_TIME = "Thu, 01 Jan 2009 00:00:00 GMT";
    static String END_TIME = "Fri, 02 Jan 2009 00:00:00 GMT";
    static String PAUSE_TIME = "Fri, 02 Jan 2009 00:00:00 GMT";
    static String NOMINAL_TIME = "Fri, 02 Jan 2009 01:00:00 GMT";
    static String LAST_MODIFIED = "Fri, 02 Jan 2009 02:00:00 GMT";
    static String LAST_ACTION = "Fri, 02 Jan 2009 03:00:00 GMT";
    static String NEXT_MATERIALIZED = "Fri, 02 Jan 2009 04:00:00 GMT";

    @SuppressWarnings("unchecked")
    private JSONObject createJsonWorkflowAction() {
        JSONObject json = new JSONObject();
        json.put(JsonTags.WORKFLOW_ACTION_ID, "a");
        json.put(JsonTags.WORKFLOW_ACTION_NAME, "b");
        json.put(JsonTags.WORKFLOW_ACTION_TYPE, "c");
        json.put(JsonTags.WORKFLOW_ACTION_CONF, "d");
        json.put(JsonTags.WORKFLOW_ACTION_STATUS, WorkflowAction.Status.RUNNING.toString());
        json.put(JsonTags.WORKFLOW_ACTION_RETRIES, (long)1);
        json.put(JsonTags.WORKFLOW_ACTION_START_TIME, START_TIME);
        json.put(JsonTags.WORKFLOW_ACTION_END_TIME, END_TIME);
        json.put(JsonTags.WORKFLOW_ACTION_TRANSITION, "e");
        json.put(JsonTags.WORKFLOW_ACTION_DATA, "ee");
        json.put(JsonTags.WORKFLOW_ACTION_EXTERNAL_ID, "f");
        json.put(JsonTags.WORKFLOW_ACTION_EXTERNAL_STATUS, "g");
        json.put(JsonTags.WORKFLOW_ACTION_TRACKER_URI, "h");
        json.put(JsonTags.WORKFLOW_ACTION_CONSOLE_URL, "i");
        json.put(JsonTags.WORKFLOW_ACTION_ERROR_CODE, "j");
        json.put(JsonTags.WORKFLOW_ACTION_ERROR_MESSAGE, "k");
        return json;
    }

    @SuppressWarnings("unchecked")
    private JSONArray createJsonWorkflowActionList() {
        JSONObject json1 = createJsonWorkflowAction();
        json1.put(JsonTags.WORKFLOW_ACTION_ID, "a1");
        JSONObject json2 = createJsonWorkflowAction();
        json2.put(JsonTags.WORKFLOW_ACTION_ID, "a2");
        JSONArray array = new JSONArray();
        array.add(json1);
        array.add(json2);
        return array;
    }

    public void testParseWorkflowAction() {
        JSONObject json = createJsonWorkflowAction();
        WorkflowAction action = JsonToBean.createWorkflowAction(json);

        assertEquals("a", action.getId());
        assertEquals("b", action.getName());
        assertEquals("c", action.getType());
        assertEquals("d", action.getConf());
        assertEquals(WorkflowAction.Status.RUNNING, action.getStatus());
        assertEquals(1, action.getRetries());
        assertEquals(JsonUtils.parseDateRfc822(START_TIME), action.getStartTime());
        assertEquals(JsonUtils.parseDateRfc822(END_TIME), action.getEndTime());
        assertEquals("e", action.getTransition());
        assertEquals("ee", action.getData());
        assertEquals("f", action.getExternalId());
        assertEquals("g", action.getExternalStatus());
        assertEquals("h", action.getTrackerUri());
        assertEquals("i", action.getConsoleUrl());
        assertEquals("j", action.getErrorCode());
        assertEquals("k", action.getErrorMessage());
    }

    @SuppressWarnings("unchecked")
    public void testParseWorkflowActions() {
        JSONArray array = createJsonWorkflowActionList();
        List<WorkflowAction> list = JsonToBean.createWorkflowActionList(array);

        assertEquals(2, list.size());
        assertEquals("a1", list.get(0).getId());
        assertEquals("a2", list.get(1).getId());
    }

    @SuppressWarnings("unchecked")
    private JSONObject createJsonWorkflowJob() {
        JSONObject json = new JSONObject();
        json.put(JsonTags.WORKFLOW_APP_PATH, "a");
        json.put(JsonTags.WORKFLOW_APP_NAME, "b");
        json.put(JsonTags.WORKFLOW_ID, "c");
        json.put(JsonTags.WORKFLOW_CONF, "d");
        json.put(JsonTags.WORKFLOW_STATUS, WorkflowJob.Status.PREP.toString());
        json.put(JsonTags.WORKFLOW_CREATED_TIME, CREATED_TIME);
        json.put(JsonTags.WORKFLOW_START_TIME, START_TIME);
        json.put(JsonTags.WORKFLOW_END_TIME, END_TIME);
        json.put(JsonTags.WORKFLOW_USER, "e");
        json.put(JsonTags.WORKFLOW_GROUP, "f");
        json.put(JsonTags.WORKFLOW_RUN, (long)1);
        json.put(JsonTags.WORKFLOW_CONSOLE_URL, "g");
        json.put(JsonTags.WORKFLOW_ACTIONS, createJsonWorkflowActionList());
        return json;
    }

    @SuppressWarnings("unchecked")
    private JSONArray createJsonWorkflowJobList() {
        JSONObject json1 = createJsonWorkflowJob();
        json1.put(JsonTags.WORKFLOW_ID, "j1");
        JSONObject json2 = createJsonWorkflowJob();
        json2.put(JsonTags.WORKFLOW_ID, "j2");
        JSONArray array = new JSONArray();
        array.add(json1);
        array.add(json2);
        return array;
    }

    @SuppressWarnings("unchecked")
    public void testParseWorkflowJob() {
        JSONObject json = createJsonWorkflowJob();
        WorkflowJob wf = JsonToBean.createWorkflowJob(json);

        assertEquals("a", wf.getAppPath());
        assertEquals("b", wf.getAppName());
        assertEquals("c", wf.getId());
        assertEquals("d", wf.getConf());
        assertEquals(WorkflowJob.Status.PREP, wf.getStatus());
        assertEquals(JsonUtils.parseDateRfc822(CREATED_TIME), wf.getCreatedTime());
        assertEquals(JsonUtils.parseDateRfc822(START_TIME), wf.getStartTime());
        assertEquals(JsonUtils.parseDateRfc822(END_TIME), wf.getEndTime());
        assertEquals("e", wf.getUser());
        assertEquals("f", wf.getGroup());
        assertEquals(1, wf.getRun());
        assertEquals("g", wf.getConsoleUrl());
        assertEquals(2, wf.getActions().size());
        assertEquals("a1", wf.getActions().get(0).getId());
        assertEquals("a2", wf.getActions().get(1).getId());
    }

    @SuppressWarnings("unchecked")
    public void testParseWorkflowJobs() {
        JSONArray array = createJsonWorkflowJobList();
        List<WorkflowJob> list = JsonToBean.createWorkflowJobList(array);

        assertEquals(2, list.size());
        assertEquals("j1", list.get(0).getId());
        assertEquals("j2", list.get(1).getId());
    }

    @SuppressWarnings("unchecked")
    private JSONObject createJsonCoordinatorAction() {
        JSONObject json = new JSONObject();
        json.put(JsonTags.COORDINATOR_ACTION_ID, "a");
        json.put(JsonTags.COORDINATOR_JOB_ID, "b");
        json.put(JsonTags.COORDINATOR_ACTION_NUMBER, (long)1);
        json.put(JsonTags.COORDINATOR_ACTION_CREATED_CONF, "c");
        json.put(JsonTags.COORDINATOR_ACTION_CREATED_TIME, CREATED_TIME);
        json.put(JsonTags.COORDINATOR_ACTION_NOMINAL_TIME, NOMINAL_TIME);
        json.put(JsonTags.COORDINATOR_ACTION_EXTERNALID, "d");
        json.put(JsonTags.COORDINATOR_ACTION_STATUS, CoordinatorAction.Status.DISCARDED.toString());
        json.put(JsonTags.COORDINATOR_ACTION_RUNTIME_CONF, "e");
        json.put(JsonTags.COORDINATOR_ACTION_LAST_MODIFIED_TIME, LAST_MODIFIED);
        json.put(JsonTags.COORDINATOR_ACTION_MISSING_DEPS, "f");
        json.put(JsonTags.COORDINATOR_ACTION_EXTERNAL_STATUS, "g");
        json.put(JsonTags.COORDINATOR_ACTION_TRACKER_URI, "h");
        json.put(JsonTags.COORDINATOR_ACTION_CONSOLE_URL, "i");
        json.put(JsonTags.COORDINATOR_ACTION_ERROR_CODE, "j");
        json.put(JsonTags.COORDINATOR_ACTION_ERROR_MESSAGE, "k");
        return json;
    }

    @SuppressWarnings("unchecked")
    private JSONArray createJsonCoordinatorActionList() {
        JSONObject json1 = createJsonCoordinatorAction();
        json1.put(JsonTags.COORDINATOR_ACTION_ID, "ca1");
        JSONObject json2 = createJsonCoordinatorAction();
        json2.put(JsonTags.COORDINATOR_ACTION_ID, "ca2");
        JSONArray array = new JSONArray();
        array.add(json1);
        array.add(json2);
        return array;
    }

    @SuppressWarnings("unchecked")
    public void testParseCoordinatorAction() {
        JSONObject json = createJsonCoordinatorAction();
        CoordinatorAction action = JsonToBean.createCoordinatorAction(json);

        assertEquals("a", action.getId());
        assertEquals("b", action.getJobId());
        assertEquals(1, action.getActionNumber());
        assertEquals("c", action.getCreatedConf());
        assertEquals(JsonUtils.parseDateRfc822(CREATED_TIME), action.getCreatedTime());
        assertEquals(JsonUtils.parseDateRfc822(NOMINAL_TIME), action.getNominalTime());
        assertEquals("d", action.getExternalId());
        assertEquals(CoordinatorAction.Status.DISCARDED, action.getStatus());
        assertEquals("e", action.getRunConf());
        assertEquals(JsonUtils.parseDateRfc822(LAST_MODIFIED), action.getLastModifiedTime());
        assertEquals("f", action.getMissingDependencies());
        assertEquals("g", action.getExternalStatus());
        assertEquals("h", action.getTrackerUri());
        assertEquals("i", action.getConsoleUrl());
        assertEquals("j", action.getErrorCode());
        assertEquals("k", action.getErrorMessage());
    }

    @SuppressWarnings("unchecked")
    public void testParseCoordinatorActions() {
        JSONArray array = createJsonCoordinatorActionList();
        List<CoordinatorAction> list = JsonToBean.createCoordinatorActionList(array);

        assertEquals(2, list.size());
        assertEquals("ca1", list.get(0).getId());
        assertEquals("ca2", list.get(1).getId());
    }

    @SuppressWarnings("unchecked")
    private JSONObject createJsonCoordinatorJob() {
        JSONObject json = new JSONObject();
        json.put(JsonTags.COORDINATOR_JOB_PATH, "a");
        json.put(JsonTags.COORDINATOR_JOB_NAME, "b");
        json.put(JsonTags.COORDINATOR_JOB_ID, "c");
        json.put(JsonTags.COORDINATOR_JOB_CONF, "d");
        json.put(JsonTags.COORDINATOR_JOB_STATUS, CoordinatorJob.Status.RUNNING.toString());
        json.put(JsonTags.COORDINATOR_JOB_EXECUTIONPOLICY, CoordinatorJob.Execution.FIFO.toString());
        json.put(JsonTags.COORDINATOR_JOB_FREQUENCY, (long)1);
        json.put(JsonTags.COORDINATOR_JOB_TIMEUNIT, CoordinatorJob.Timeunit.DAY.toString());
        json.put(JsonTags.COORDINATOR_JOB_TIMEZONE, "e");
        json.put(JsonTags.COORDINATOR_JOB_CONCURRENCY, (long)2);
        json.put(JsonTags.COORDINATOR_JOB_TIMEOUT, (long)3);
        json.put(JsonTags.COORDINATOR_JOB_LAST_ACTION_TIME, LAST_ACTION);
        json.put(JsonTags.COORDINATOR_JOB_NEXT_MATERIALIZED_TIME, NEXT_MATERIALIZED);
        json.put(JsonTags.COORDINATOR_JOB_START_TIME, START_TIME);
        json.put(JsonTags.COORDINATOR_JOB_PAUSE_TIME, PAUSE_TIME);
        json.put(JsonTags.COORDINATOR_JOB_END_TIME, END_TIME);
        json.put(JsonTags.COORDINATOR_JOB_USER, "f");
        json.put(JsonTags.COORDINATOR_JOB_GROUP, "g");
        json.put(JsonTags.COORDINATOR_JOB_CONSOLE_URL, "h");
        json.put(JsonTags.COORDINATOR_ACTIONS, createJsonCoordinatorActionList());
        return json;
    }

    @SuppressWarnings("unchecked")
    private JSONArray createJsonCoordinatorJobList() {
        JSONObject json1 = createJsonCoordinatorJob();
        json1.put(JsonTags.COORDINATOR_JOB_ID, "cj1");
        JSONObject json2 = createJsonCoordinatorJob();
        json2.put(JsonTags.COORDINATOR_JOB_ID, "cj2");
        JSONArray array = new JSONArray();
        array.add(json1);
        array.add(json2);
        return array;
    }

    @SuppressWarnings("unchecked")
    public void testParseCoordinatorJob() {
        JSONObject json = createJsonCoordinatorJob();
        CoordinatorJob job = JsonToBean.createCoordinatorJob(json);
        assertEquals("a", job.getAppPath());
        assertEquals("b", job.getAppName());
        assertEquals("c", job.getId());
        assertEquals("d", job.getConf());
        assertEquals(CoordinatorJob.Status.RUNNING, job.getStatus());
        assertEquals(CoordinatorJob.Execution.FIFO, job.getExecutionOrder());
        assertEquals(1, job.getFrequency());
        assertEquals(CoordinatorJob.Timeunit.DAY, job.getTimeUnit());
        assertEquals("e", job.getTimeZone());
        assertEquals(2, job.getConcurrency());
        assertEquals(3, job.getTimeout());
        assertEquals(JsonUtils.parseDateRfc822(LAST_ACTION), job.getLastActionTime());
        assertEquals(JsonUtils.parseDateRfc822(NEXT_MATERIALIZED), job.getNextMaterializedTime());
        assertEquals(JsonUtils.parseDateRfc822(START_TIME), job.getStartTime());
        assertEquals(JsonUtils.parseDateRfc822(END_TIME), job.getEndTime());
        assertEquals(JsonUtils.parseDateRfc822(PAUSE_TIME), job.getPauseTime());
        assertEquals("f", job.getUser());
        assertEquals("g", job.getGroup());
        assertEquals("h", job.getConsoleUrl());
        assertEquals(2, job.getActions().size());
        assertEquals("ca1", job.getActions().get(0).getId());
        assertEquals("ca2", job.getActions().get(1).getId());
    }

    @SuppressWarnings("unchecked")
    public void testParseCoordinatorJobs() {
        JSONArray array = createJsonCoordinatorJobList();
        List<CoordinatorJob> list = JsonToBean.createCoordinatorJobList(array);

        assertEquals(2, list.size());
        assertEquals("cj1", list.get(0).getId());
        assertEquals("cj2", list.get(1).getId());
    }

}
