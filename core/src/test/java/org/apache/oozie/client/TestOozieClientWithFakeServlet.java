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


package org.apache.oozie.client;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.CoordinatorAction.Status;
import org.apache.oozie.client.rest.BulkResponseImpl;
import org.apache.oozie.client.rest.JsonTags;
import org.json.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.Test;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;


/**
 * Test some client functions with fake http connect
 *
 */
public class TestOozieClientWithFakeServlet {

    private int answer = 0;
    private boolean check = true;

    /**
     * Test method getJMSTopicName
     */
    @Test
    public void testGetJMSTopicName() throws Exception {
        answer = 0;
        check = true;
        FakeOozieClient client = new FakeOozieClient("http://url");
        String answer = client.getJMSTopicName("jobId");
        assertEquals("topicName", answer);

    }

    /**
     * Test method getJMSConnectionInfo
     */
    @Test
    public void testGetJMSConnectionInfo() throws Exception {
        answer = 1;
        check = true;
        FakeOozieClient client = new FakeOozieClient("http://url");
        JMSConnectionInfo answer = client.getJMSConnectionInfo();
        assertNotNull(answer);

    }

    /**
     * Test method getCoordActionInfo
     */
    @Test
    public void testGetCoordActionInfo() throws Exception {
        answer = 1;
        check = true;
        FakeOozieClient client = new FakeOozieClient("http://url");
        CoordinatorAction answer = client.getCoordActionInfo("actiomId");
        assertNotNull(answer);
    }

    /**
     * Test method getBundleJobsInfo
     */
    @Test
    public void testGetBundleJobsInfo() throws Exception {
        answer = 2;
        check = true;
        FakeOozieClient client = new FakeOozieClient("http://url");
        List<BundleJob> answer = client.getBundleJobsInfo("", 0, 10);
        assertNotNull(answer);
        assertEquals(1, answer.size());

    }

    /**
     * Test method getBulkInfo
     */
    @Test
    public void testGetBulkInfo() throws Exception {
        answer = 3;
        check = true;
        FakeOozieClient client = new FakeOozieClient("http://url");
        List<BulkResponse> answer = client.getBulkInfo("", 0, 10);
        assertNotNull(answer);
        assertEquals(2, answer.size());
        assertEquals(Status.READY, answer.get(0).getAction().getStatus());

    }

    /**
     * Test method FakeOozieClient
     */
    @Test
    public void testBundleRerun() throws Exception {
        answer = 1;
        check = true;
        FakeOozieClient client = new FakeOozieClient("http://url");
        Void answer = client.reRunBundle("jobId", "", "", true, true);
        assertNull(answer);

    }

    private class FakeOozieClient extends OozieClient {

        public FakeOozieClient(String oozieUrl) {
            super(oozieUrl);
        }

        @Override
        protected HttpURLConnection createConnection(URL url, String method) throws IOException, OozieClientException {
            HttpURLConnection result = mock(HttpURLConnection.class);
            when(result.getResponseCode()).thenReturn(200);
            when(result.getInputStream()).thenReturn(getIs());
            return result;
        }

        @SuppressWarnings("unchecked")
        private InputStream getIs() {
            ByteArrayInputStream result = new ByteArrayInputStream("".getBytes());
            if (check) {
                JSONArray array = new JSONArray();
                array.put(2L);
                String s = JSONValue.toJSONString(array);
                result = new ByteArrayInputStream(s.getBytes());
                check = false;
                return result;
            }
            if (answer == 0) {
                JSONObject json = new JSONObject();
                json.put(JsonTags.JMS_TOPIC_NAME, "topicName");
                result = new ByteArrayInputStream(json.toJSONString().getBytes());

            }
            if (answer == 1) {
                JSONObject json = new JSONObject();
                result = new ByteArrayInputStream(json.toJSONString().getBytes());

            }
            if (answer == 2) {
                JSONObject json = new JSONObject();
                List<WorkflowJobBean> jsonWorkflows = new ArrayList<WorkflowJobBean>();
                jsonWorkflows.add(new WorkflowJobBean());
                json.put(JsonTags.BUNDLE_JOBS, WorkflowJobBean.toJSONArray(jsonWorkflows, "GMT"));
                result = new ByteArrayInputStream(json.toJSONString().getBytes());

            }
            if (answer == 3) {
                JSONObject json = new JSONObject();
                List<BulkResponseImpl> jsonWorkflows = new ArrayList<BulkResponseImpl>();
                BulkResponseImpl bulk = new BulkResponseImpl();
                bulk.setBundle(new BundleJobBean());
                bulk.setCoordinator(new CoordinatorJobBean());
                CoordinatorActionBean action = new CoordinatorActionBean();
                action.setStatus(Status.READY);
                bulk.setAction(action);
                jsonWorkflows.add(bulk);
                jsonWorkflows.add(bulk);
                json.put(JsonTags.BULK_RESPONSES, BulkResponseImpl.toJSONArray(jsonWorkflows, "GMT"));
                result = new ByteArrayInputStream(json.toJSONString().getBytes());

            }

            return result;
        }

    }
}
