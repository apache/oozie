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

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.client.WorkflowAction;

public class TestJsonWorkflowAction extends TestCase {
    static String START_TIME = "Thu, 01 Jan 2009 00:00:00 GMT";
    static String END_TIME = "Fri, 02 Jan 2009 00:00:00 GMT";

    static WorkflowActionBean createNode() {
        WorkflowActionBean action = new WorkflowActionBean();
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
        action.setStats("stats");
        action.setExternalChildIDs("extChIDs");
        action.setExternalId("f");
        action.setExternalStatus("g");
        action.setTrackerUri("h");
        action.setConsoleUrl("i");
        action.setErrorInfo("j", "k");
        return action;
    }

    public void testProperties() {
        WorkflowAction action = createNode();
        Assert.assertEquals("a", action.getId());
        Assert.assertEquals("b", action.getName());
        Assert.assertEquals("c", action.getType());
        Assert.assertEquals("d", action.getConf());
        Assert.assertEquals(WorkflowAction.Status.RUNNING, action.getStatus());
        Assert.assertEquals(1, action.getRetries());
        Assert.assertEquals(JsonUtils.parseDateRfc822(START_TIME), action.getStartTime());
        Assert.assertEquals(JsonUtils.parseDateRfc822(END_TIME), action.getEndTime());
        Assert.assertEquals("e", action.getTransition());
        Assert.assertEquals("ee", action.getData());
        Assert.assertEquals("stats", action.getStats());
        Assert.assertEquals("extChIDs", action.getExternalChildIDs());
        Assert.assertEquals("f", action.getExternalId());
        Assert.assertEquals("g", action.getExternalStatus());
        Assert.assertEquals("h", action.getTrackerUri());
        Assert.assertEquals("i", action.getConsoleUrl());
        Assert.assertEquals("j", action.getErrorCode());
        Assert.assertEquals("k", action.getErrorMessage());
    }

}
