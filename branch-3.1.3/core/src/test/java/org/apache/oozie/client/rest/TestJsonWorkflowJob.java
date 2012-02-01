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
import org.apache.oozie.client.WorkflowJob;
import java.util.Arrays;

public class TestJsonWorkflowJob extends TestCase {

    static String CREATED_TIME = "Thu, 01 Jan 2009 00:00:00 GMT";
    static String START_TIME = "Fri, 02 Jan 2009 00:00:00 GMT";
    static String END_TIME = "Sat, 03 Jan 2009 00:00:00 GMT";

    static JsonWorkflowJob createWorkflow() {
        JsonWorkflowJob wf = new JsonWorkflowJob();
        wf.setAppPath("a");
        wf.setAppName("b");
        wf.setId("c");
        wf.setConf("cc");
        wf.setStatus(WorkflowJob.Status.PREP);
        wf.setCreatedTime(JsonUtils.parseDateRfc822(CREATED_TIME));
        wf.setStartTime(JsonUtils.parseDateRfc822(START_TIME));
        wf.setEndTime(JsonUtils.parseDateRfc822(END_TIME));
        wf.setUser("d");
        wf.setGroup("e");
        wf.setRun(2);
        wf.setConsoleUrl("cu");
        return wf;
    }

    public void testProperties() {
        JsonWorkflowJob wf = createWorkflow();
        assertEquals("a", wf.getAppPath());
        assertEquals("b", wf.getAppName());
        assertEquals("c", wf.getId());
        assertEquals("cc", wf.getConf());
        assertEquals(WorkflowJob.Status.PREP, wf.getStatus());
        assertEquals(JsonUtils.parseDateRfc822(CREATED_TIME), wf.getCreatedTime());
        assertEquals(JsonUtils.parseDateRfc822(START_TIME), wf.getStartTime());
        assertEquals(JsonUtils.parseDateRfc822(END_TIME), wf.getEndTime());
        assertEquals("d", wf.getUser());
        assertEquals("e", wf.getGroup());
        assertEquals(2, wf.getRun());
        assertEquals("cu", wf.getConsoleUrl());
        assertEquals(0, wf.getActions().size());

        wf.setActions(Arrays.asList((JsonWorkflowAction) TestJsonWorkflowAction.createNode()));
        assertEquals(1, wf.getActions().size());
    }

}
