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
package org.apache.oozie.client.rest;

import junit.framework.TestCase;
import org.apache.oozie.client.WorkflowAction;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;

public class TestJsonWorkflowAction extends TestCase {
    static String START_TIME = "Thu, 01 Jan 2009 00:00:00 GMT";
    static String END_TIME = "Fri, 02 Jan 2009 00:00:00 GMT";

    static JsonWorkflowAction createNode() {
        JsonWorkflowAction action = new JsonWorkflowAction();
        action.setId("a");
        action.setName("b");
        action.setType("c");
        action.setConf("d");
        action.setRetries(1);
        action.setStatus(WorkflowAction.Status.RUNNING);
        action.setStartTime(JsonUtils.parseDateRfc822(START_TIME));
        action.setEndTime(JsonUtils.parseDateRfc822(END_TIME));
        action.setTransition("e");
        action.setData("ee");
        action.setExternalId("f");
        action.setExternalStatus("g");
        action.setTrackerUri("h");
        action.setConsoleUrl("i");
        action.setErrorInfo("j", "k");
        return action;
    }

    public void testProperties() {
        WorkflowAction action = createNode();
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

    public void testJsonAndBack() throws Exception {
        JsonWorkflowAction action = createNode();
        StringWriter sw = new StringWriter();
        action.toJSONObject().writeJSONString(sw);
        sw.close();
        JSONObject json = (JSONObject) JSONValue.parse(new StringReader(sw.toString()));
        action = new JsonWorkflowAction(json);

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

    public void testList() throws Exception {
        List<JsonWorkflowAction> actions = Arrays.asList(createNode(), createNode());
        JSONArray array = JsonWorkflowAction.toJSONArray(actions);
        StringWriter sw = new StringWriter();
        array.writeJSONString(sw);
        sw.close();
        array = (JSONArray) JSONValue.parse(new StringReader(sw.toString()));
        List<JsonWorkflowAction> readNodes = JsonWorkflowAction.fromJSONArray(array);
        assertEquals(2, readNodes.size());
    }

}
