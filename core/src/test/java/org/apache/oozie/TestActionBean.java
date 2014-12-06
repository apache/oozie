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

package org.apache.oozie;

import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.WorkflowActionBean;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Date;

public class TestActionBean extends XTestCase {

    public void testAction() {
        WorkflowActionBean action = new WorkflowActionBean();
        action.setJobId("id");
        action.setExecutionPath("executionPath");
        action.setPending();
        action.setPendingAge(new Date());
        action.setSignalValue("signal");
        action.setLogToken("logToken");
        assertEquals("id", action.getJobId());
        assertEquals("executionPath", action.getExecutionPath());
        assertTrue(action.isPending());
        assertNotNull(action.getPendingAge());
        assertEquals("signal", action.getSignalValue());
        assertEquals("logToken", action.getLogToken());

        action.setExecutionData("externalStatus", System.getProperties());
        assertEquals("externalStatus", action.getExternalStatus());
        assertNotNull(action.getData());

        action.setEndData(WorkflowAction.Status.OK, "signal");
        assertEquals(WorkflowAction.Status.OK, action.getStatus());
        assertEquals("externalStatus", action.getExternalStatus());

        action.setStartData("externalId", "trackerUri", "consoleUrl");
        assertEquals("externalId", action.getExternalId());
        assertEquals("trackerUri", action.getTrackerUri());
        assertEquals("consoleUrl", action.getConsoleUrl());

        action.setStats("jsonStats");
        action.setExternalChildIDs("job1,job2");
        assertEquals("jsonStats", action.getStats());
        assertEquals("job1,job2", action.getExternalChildIDs());
    }

    public void testEmptyWriteRead() throws Exception {
        WorkflowActionBean action = new WorkflowActionBean();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        action.write(dos);
        dos.close();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        action = new WorkflowActionBean();
        action.readFields(dis);
    }

    public void testFullWriteRead() throws Exception {
        //TODO
    }
}
