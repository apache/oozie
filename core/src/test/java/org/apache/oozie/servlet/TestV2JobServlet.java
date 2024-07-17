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

import org.apache.oozie.client.CoordinatorWfAction;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.service.ConfigurationService;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import javax.servlet.http.HttpServletResponse;

import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class TestV2JobServlet extends DagServletTestCase {

    static {
        new V2JobServlet();
    }

    private static final boolean IS_SECURITY_ENABLED = false;

    protected void setUp() throws Exception {
        super.setUp();
    }

    public void testJobInfo() throws Exception {
        runTest("/v2/job/*", V2JobServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                MockCoordinatorEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.JOB_SHOW_PARAM, RestConstants.JOB_SHOW_INFO);
                URL url = createURL(MockCoordinatorEngineService.JOB_ID + 1, params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
                JSONObject obj = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream(),
                        StandardCharsets.UTF_8));
                assertEquals(MockCoordinatorEngineService.JOB_ID + 1, obj.get(JsonTags.COORDINATOR_JOB_ID));
                assertEquals(RestConstants.JOB_SHOW_INFO, MockCoordinatorEngineService.did);

                MockCoordinatorEngineService.reset();
                params = new HashMap<String, String>();
                params.put(RestConstants.JOB_SHOW_PARAM, RestConstants.JOB_SHOW_INFO);
                params.put(RestConstants.OFFSET_PARAM, "2");
                params.put(RestConstants.LEN_PARAM, "50");
                url = createURL(MockCoordinatorEngineService.JOB_ID + 1, params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
                obj = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream(),
                        StandardCharsets.UTF_8));
                assertEquals(MockCoordinatorEngineService.JOB_ID + 1, obj.get(JsonTags.COORDINATOR_JOB_ID));
                assertEquals(RestConstants.JOB_SHOW_INFO, MockCoordinatorEngineService.did);
                assertEquals(MockCoordinatorEngineService.offset, new Integer(2));
                assertEquals(MockCoordinatorEngineService.length, new Integer(50));

                MockCoordinatorEngineService.reset();
                params = new HashMap<String, String>();
                params.put(RestConstants.JOB_SHOW_PARAM, RestConstants.JOB_SHOW_INFO);
                params.put(RestConstants.OFFSET_PARAM, "-1"); //OozieCLI
                params.put(RestConstants.LEN_PARAM, "-1");
                url = createURL(MockCoordinatorEngineService.JOB_ID + 1, params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
                obj = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream(),
                        StandardCharsets.UTF_8));
                assertEquals(MockCoordinatorEngineService.JOB_ID + 1, obj.get(JsonTags.COORDINATOR_JOB_ID));
                assertEquals(RestConstants.JOB_SHOW_INFO, MockCoordinatorEngineService.did);
                assertEquals(MockCoordinatorEngineService.offset, new Integer(1));
                assertEquals(MockCoordinatorEngineService.length, new Integer(1000));

                MockCoordinatorEngineService.reset();
                params = new HashMap<String, String>();
                params.put(RestConstants.JOB_SHOW_PARAM, RestConstants.JOB_SHOW_INFO);
                params.put(RestConstants.OFFSET_PARAM, "0");  //oozie-console.js
                params.put(RestConstants.LEN_PARAM, "0");
                url = createURL(MockCoordinatorEngineService.JOB_ID + 1, params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
                obj = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream(),
                        StandardCharsets.UTF_8));
                assertEquals(MockCoordinatorEngineService.JOB_ID + 1, obj.get(JsonTags.COORDINATOR_JOB_ID));
                assertEquals(RestConstants.JOB_SHOW_INFO, MockCoordinatorEngineService.did);
                assertEquals(MockCoordinatorEngineService.offset, new Integer(1));
                assertEquals(MockCoordinatorEngineService.length, new Integer(0));

                MockCoordinatorEngineService.reset();
                params = new HashMap<String, String>();
                params.put(RestConstants.JOB_SHOW_PARAM, RestConstants.JOB_SHOW_INFO);
                url = createURL(MockCoordinatorEngineService.JOB_ID + (MockCoordinatorEngineService.coordJobs.size() + 1), params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertBadRequestOrInternalServerError(conn.getResponseCode());
                assertEquals(RestConstants.JOB_SHOW_INFO, MockCoordinatorEngineService.did);
                return null;
            }
        });
    }

    private void assertBadRequestOrInternalServerError(final int responseCode) {
        assertTrue(String.format("HTTP response code [%d] is unexpected, should be one of [%d, %d]",
                            responseCode, HttpServletResponse.SC_BAD_REQUEST, HttpServletResponse.SC_INTERNAL_SERVER_ERROR),
                HttpServletResponse.SC_BAD_REQUEST == responseCode
                || HttpServletResponse.SC_INTERNAL_SERVER_ERROR == responseCode);
    }

    public void testGetCoordActionReruns() throws Exception {
        runTest("/v2/job/*", V1JobServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {

                MockDagEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.JOB_SHOW_PARAM, RestConstants.ALL_WORKFLOWS_FOR_COORD_ACTION);

                // url - oozie/v2/coord-action-id?show=allruns
                URL url = createURL(MockCoordinatorEngineService.ACTION_ID + 1, params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());

                MockDagEngineService.reset();
                params.put(RestConstants.JOB_COORD_RANGE_TYPE_PARAM, RestConstants.JOB_COORD_SCOPE_ACTION);
                params.put(RestConstants.JOB_COORD_SCOPE_PARAM, "2");

                // url - oozie/v2/coord-job-id?type=action&scope=action-num&show=allruns
                url = createURL(MockCoordinatorEngineService.JOB_ID + 2, params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                return null;
            }
        });
    }

    public void testCoordJobIgnore() throws Exception {
        runTest("/v2/job/*", V2JobServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {

                MockDagEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.ACTION_PARAM, RestConstants.JOB_ACTION_IGNORE);

                // url - oozie/v2/coord_job_id?action=ignore
                URL url = createURL(MockCoordinatorEngineService.JOB_ID + 1, params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("PUT");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertEquals(RestConstants.JOB_ACTION_CHANGE, MockCoordinatorEngineService.did);

                MockCoordinatorEngineService.reset();
                params = new HashMap<String, String>();
                params.put(RestConstants.ACTION_PARAM, RestConstants.JOB_ACTION_IGNORE);
                url = createURL(MockCoordinatorEngineService.JOB_ID
                        + (MockCoordinatorEngineService.coordJobs.size() + 1), params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("PUT");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                assertBadRequestOrInternalServerError(conn.getResponseCode());
                assertEquals(RestConstants.JOB_ACTION_CHANGE, MockCoordinatorEngineService.did);

                return null;
            }
        });
    }
    public void testCoordActionIgnore() throws Exception {
        runTest("/v2/job/*", V2JobServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {

                MockDagEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.ACTION_PARAM, RestConstants.JOB_ACTION_IGNORE);
                params.put(RestConstants.JOB_COORD_RANGE_TYPE_PARAM, RestConstants.JOB_COORD_SCOPE_ACTION);
                params.put(RestConstants.JOB_COORD_SCOPE_PARAM, "1");

                // url - oozie/v2/coord_job_id?action=ignore
                URL url = createURL(MockCoordinatorEngineService.JOB_ID + 1, params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("PUT");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertEquals(RestConstants.JOB_ACTION_IGNORE, MockCoordinatorEngineService.did);

                // negative test for non-existent action
                MockCoordinatorEngineService.reset();
                params = new HashMap<String, String>();
                params.put(RestConstants.ACTION_PARAM, RestConstants.JOB_ACTION_IGNORE);
                params.put(RestConstants.JOB_COORD_RANGE_TYPE_PARAM, RestConstants.JOB_COORD_SCOPE_ACTION);
                params.put(RestConstants.JOB_COORD_SCOPE_PARAM, "1");
                url = createURL(MockCoordinatorEngineService.JOB_ID
                        + (MockCoordinatorEngineService.coordJobs.size() + 1), params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("PUT");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                assertBadRequestOrInternalServerError(conn.getResponseCode());
                assertEquals(RestConstants.JOB_ACTION_IGNORE, MockCoordinatorEngineService.did);

                return null;
            }
        });
    }

    public void testJobStatus() throws Exception {
        runTest("/v2/job/*", V2JobServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                MockDagEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.JOB_SHOW_PARAM, RestConstants.JOB_SHOW_STATUS);
                URL url = createURL(MockDagEngineService.JOB_ID + "1" + MockDagEngineService.JOB_ID_END, params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
                JSONObject obj = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream(),
                        StandardCharsets.UTF_8));
                assertEquals("SUCCEEDED", obj.get(JsonTags.STATUS));
                assertEquals(RestConstants.JOB_SHOW_STATUS, MockDagEngineService.did);

                MockCoordinatorEngineService.reset();
                params = new HashMap<String, String>();
                params.put(RestConstants.JOB_SHOW_PARAM, RestConstants.JOB_SHOW_STATUS);
                url = createURL(MockCoordinatorEngineService.JOB_ID + 1, params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
                obj = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream(),
                        StandardCharsets.UTF_8));
                assertEquals("RUNNING", obj.get(JsonTags.STATUS));
                assertEquals(RestConstants.JOB_SHOW_STATUS, MockCoordinatorEngineService.did);

                return null;
            }
        });
    }

    //test normal request
    public void testGetWfActionByJobIdAndNameNormal() throws Exception {
        runTest("/v2/job/*", V2JobServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                MockCoordinatorEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                params = new HashMap<String, String>();
                params.put(RestConstants.JOB_SHOW_PARAM, RestConstants.JOB_SHOW_WF_ACTIONS_IN_COORD);
                params.put(RestConstants.OFFSET_PARAM, "2");
                params.put(RestConstants.LEN_PARAM, "2");
                params.put(RestConstants.ACTION_NAME_PARAM, "actionTest");
                URL url = createURL(MockCoordinatorEngineService.JOB_ID + 1, params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
                JSONObject obj = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream(),
                        StandardCharsets.UTF_8));
                assertEquals(MockCoordinatorEngineService.JOB_ID+1, obj.get(JsonTags.COORDINATOR_JOB_ID));
                assertEquals(RestConstants.JOB_SHOW_WF_ACTIONS_IN_COORD, MockCoordinatorEngineService.did);
                assertEquals(MockCoordinatorEngineService.offset.intValue(), 2);
                assertEquals(MockCoordinatorEngineService.length.intValue(), 2);
                JSONArray coordWfActions = (JSONArray) obj.get(JsonTags.COORDINATOR_WF_ACTIONS);
                assertEquals(coordWfActions.size(), 3);
                for(int i = 0; i < coordWfActions.size(); i++) {
                    JSONObject coordWfAction = (JSONObject) coordWfActions.get(i);
                    JSONObject wfAction = (JSONObject) coordWfAction.get(JsonTags.COORDINATOR_WF_ACTION);
                    if (i == (coordWfActions.size() - 1)) {
                        assertEquals(null, wfAction);
                        String nullReason = CoordinatorWfAction.NullReason.ACTION_NULL.getNullReason("actionTest2", "wf1");
                        assertEquals(nullReason, coordWfAction.get(JsonTags.COORDINATOR_WF_ACTION_NULL_REASON));
                    }
                    else {
                        assertEquals("actionTest", wfAction.get(JsonTags.WORKFLOW_ACTION_NAME));
                    }
                }

                return null;
            }
        });
    }

    //test missing parameter action-name
    public void testGetWfActionByJobIdAndNameActionNameMissing() throws Exception {
        runTest("/v2/job/*", V2JobServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                MockCoordinatorEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.JOB_SHOW_PARAM, RestConstants.JOB_SHOW_WF_ACTIONS_IN_COORD);
                URL url = createURL(MockCoordinatorEngineService.JOB_ID + 1, params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertBadRequestOrInternalServerError(conn.getResponseCode());

                return null;
            }
        });
    }

    //test unparseable offset
    public void testGetWfActionByJobIdAndNameUnparseableOffset() throws Exception {
        runTest("/v2/job/*", V2JobServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                MockCoordinatorEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.JOB_SHOW_PARAM, RestConstants.JOB_SHOW_WF_ACTIONS_IN_COORD);
                params.put(RestConstants.OFFSET_PARAM, "2abc");
                params.put(RestConstants.LEN_PARAM, "2");
                params.put(RestConstants.ACTION_NAME_PARAM, "actionTest");
                URL url = createURL(MockCoordinatorEngineService.JOB_ID + 1, params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertBadRequestOrInternalServerError(conn.getResponseCode());

                return null;
            }
        });
    }

    //test unparseable len
    public void testGetWfActionByJobIdAndNameUnparseableLen() throws Exception {
        runTest("/v2/job/*", V2JobServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                MockCoordinatorEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.JOB_SHOW_PARAM, RestConstants.JOB_SHOW_WF_ACTIONS_IN_COORD);
                params.put(RestConstants.OFFSET_PARAM, "2");
                params.put(RestConstants.LEN_PARAM, "2abc");
                params.put(RestConstants.ACTION_NAME_PARAM, "actionTest");
                URL url = createURL(MockCoordinatorEngineService.JOB_ID + 1, params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertBadRequestOrInternalServerError(conn.getResponseCode());

                return null;
            }
        });
    }

    public void testGetWfActionByJobIdAndNameOffsetOutOfRange() throws Exception {
        runTest("/v2/job/*", V2JobServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                MockCoordinatorEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.JOB_SHOW_PARAM, RestConstants.JOB_SHOW_WF_ACTIONS_IN_COORD);
                params.put(RestConstants.OFFSET_PARAM, "-1");
                params.put(RestConstants.LEN_PARAM, "2");
                params.put(RestConstants.ACTION_NAME_PARAM, "actionTest");
                URL url = createURL(MockCoordinatorEngineService.JOB_ID + 1, params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertEquals(MockCoordinatorEngineService.offset.intValue(), 1);

                return null;
            }
        });
    }

    public void testGetWfActionByJobIdAndNameLenOutOfRange() throws Exception {
        runTest("/v2/job/*", V2JobServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                MockCoordinatorEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.JOB_SHOW_PARAM, RestConstants.JOB_SHOW_WF_ACTIONS_IN_COORD);
                params.put(RestConstants.OFFSET_PARAM, "1");
                params.put(RestConstants.LEN_PARAM, "-1");
                params.put(RestConstants.ACTION_NAME_PARAM, "actionTest");
                URL url = createURL(MockCoordinatorEngineService.JOB_ID + 1, params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertEquals(MockCoordinatorEngineService.length.intValue(),
                        ConfigurationService.getInt("oozie.coord.actions.default.length"));

                return null;
            }
        });
    }

    public void testGetWfActionFromV0JobServlet() throws Exception {
        runTest("/v0/job/*", V0JobServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                MockCoordinatorEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                params = new HashMap<String, String>();
                params.put(RestConstants.JOB_SHOW_PARAM, RestConstants.JOB_SHOW_WF_ACTIONS_IN_COORD);
                params.put(RestConstants.OFFSET_PARAM, "2");
                params.put(RestConstants.LEN_PARAM, "2");
                params.put(RestConstants.ACTION_NAME_PARAM, "actionTest");
                URL url = createURL(MockCoordinatorEngineService.JOB_ID + 1, params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertBadRequestOrInternalServerError(conn.getResponseCode());

                return null;
            }
        });
    }

    public void testGetWfActionFromV1JobServlet() throws Exception {
        runTest("/v1/job/*", V1JobServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                MockCoordinatorEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                params = new HashMap<String, String>();
                params.put(RestConstants.JOB_SHOW_PARAM, RestConstants.JOB_SHOW_WF_ACTIONS_IN_COORD);
                params.put(RestConstants.OFFSET_PARAM, "2");
                params.put(RestConstants.LEN_PARAM, "2");
                params.put(RestConstants.ACTION_NAME_PARAM, "actionTest");
                URL url = createURL(MockCoordinatorEngineService.JOB_ID + 1, params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertBadRequestOrInternalServerError(conn.getResponseCode());

                return null;
            }
        });
    }

    public void testCoordJobIgnoreWithScopeValidation() throws Exception {
        runTest("/v2/job/*", V2JobServlet.class, IS_SECURITY_ENABLED, () -> {

            MockDagEngineService.reset();
            final Map<String, String> params = new HashMap<>();
            params.put(RestConstants.ACTION_PARAM, RestConstants.JOB_ACTION_IGNORE);
            params.put(RestConstants.JOB_COORD_RANGE_TYPE_PARAM, "action");
            params.put(RestConstants.JOB_COORD_SCOPE_PARAM, "1-300");

            // url - oozie/v2/coord_job_id?action=ignore&scope=1-300&type=action
            final URL url = createURL("0000001-1234567890-C", params);
            final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("PUT");
            conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
            conn.setDoOutput(true);

            // conn.getResponseCode() is also needed to send the request
            final int responseCode = conn.getResponseCode();
            final String error = conn.getHeaderField(RestConstants.OOZIE_ERROR_CODE);
            final String message = conn.getHeaderField(RestConstants.OOZIE_ERROR_MESSAGE);

            assertEquals("Unexpected error code: " + conn.getResponseMessage(),
                    HttpServletResponse.SC_BAD_REQUEST, responseCode);
            assertEquals("Unexpected Oozie error code", "E0309", error);
            assertEquals(
                    "Unexpected error message",
                    "E0309: Invalid parameter value, [scope] = [1-300], " +
                            "too many elements are requested: 300, maximum allowed: 50",
                    message
            );


            return null;
        });
    }

}
