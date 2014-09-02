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

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.servlet.V1JobServlet;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class TestV1JobServlet extends DagServletTestCase {

    static {
        new V1JobServlet();
    }

    private static final boolean IS_SECURITY_ENABLED = false;

    protected void setUp() throws Exception {
        super.setUp();
    }

    private void _testAction(final String action, final Configuration conf) throws Exception {
        runTest("/v1/job/*", V1JobServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                MockCoordinatorEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.ACTION_PARAM, action);
                URL url = createURL(MockCoordinatorEngineService.JOB_ID + 1, params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("PUT");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                if (conf != null) {
                    conf.writeXml(conn.getOutputStream());
                }
                if (conf == null || conf.get(OozieClient.USER_NAME) != null) {
                    assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                    assertEquals(action, MockCoordinatorEngineService.did);
                }
                else {
                    assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
                }

                MockCoordinatorEngineService.reset();
                params = new HashMap<String, String>();
                params.put(RestConstants.ACTION_PARAM, action);
                url = createURL(MockCoordinatorEngineService.JOB_ID + (MockCoordinatorEngineService.coordJobs.size() + 1), params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("PUT");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                if (conf != null) {
                    conf.writeXml(conn.getOutputStream());
                }
                if (conf == null || conf.get(OozieClient.USER_NAME) != null) {
                    assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
                    assertEquals(action, MockCoordinatorEngineService.did);
                }
                else {
                    assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
                }
                return null;
            }
        });
    }

    public void testSuspend() throws Exception {
        _testAction(RestConstants.JOB_ACTION_SUSPEND, null);
    }

    public void testResume() throws Exception {
        _testAction(RestConstants.JOB_ACTION_RESUME, null);
    }

    public void testKill() throws Exception {
        _testAction(RestConstants.JOB_ACTION_KILL, null);
    }

    /**
     * This test will invoke the start action on a coordinator job using HTTP.
     * Checks for 400 response
     * @throws Exception
     */
    public void testStart() throws Exception {
        runTest("/v1/job/*", V1JobServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                MockCoordinatorEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                //testing for the start action
                params.put(RestConstants.ACTION_PARAM, RestConstants.JOB_ACTION_START);
                URL url = createURL(MockCoordinatorEngineService.JOB_ID + 1, params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("PUT");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                //check if the response is 400
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
                //check if the state remains uninitialized
                assertEquals(null, MockCoordinatorEngineService.did);
                return null;
            }
        });
    }

    /**
     * This test will invoke the start action on a coordinator job. Since the coordinator job
     * does not support a start action, the test case is expected to catch the error and verify
     * the error message.
     * @throws Exception
     */
    public void testStartForErrorCode() throws Exception {
        @SuppressWarnings("serial")
        V1JobServlet testV1JobServlet = new V1JobServlet() {

            @Override
            protected String getResourceName(HttpServletRequest request) {
                return "-C";
            }
        };
        try {
            testV1JobServlet.startJob(null, null);
            // should not get here!
            fail("Negative test to test an exception. Should not be succeeding!");
        } catch (XServletException xse) {
            // check for the error code and the message
            assertEquals(xse.getErrorCode(), ErrorCode.E0303);
            assertTrue(xse.getMessage().contains("Invalid parameter value, [action] = [start]"));
        } catch (Exception e) {
            // should not get here!
            fail("Did not expect a generic exception. Was expecting XServletException");
        }

    }

    private void _testNonJsonResponses(final String show, final String contentType, final String response)
            throws Exception {
        runTest("/v1/job/*", V1JobServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                MockCoordinatorEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.JOB_SHOW_PARAM, show);
                URL url = createURL(MockCoordinatorEngineService.JOB_ID + 1, params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(contentType));
                String output = IOUtils.getReaderAsString(new InputStreamReader(conn.getInputStream()), 1000);
                assertEquals(response, output);
                assertEquals(show, MockCoordinatorEngineService.did);

                MockCoordinatorEngineService.reset();
                params = new HashMap<String, String>();
                params.put(RestConstants.JOB_SHOW_PARAM, show);
                url = createURL(MockCoordinatorEngineService.JOB_ID + (MockCoordinatorEngineService.coordJobs.size() + 1), params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
                assertEquals(show, MockCoordinatorEngineService.did);
                return null;
            }
        });
    }

    public void testJobDef() throws Exception {
        _testNonJsonResponses(RestConstants.JOB_SHOW_DEFINITION, RestConstants.XML_CONTENT_TYPE,
                              MockCoordinatorEngineService.COORD_APP);
    }

    public void testJobLog() throws Exception {
        _testNonJsonResponses(RestConstants.JOB_SHOW_LOG, RestConstants.TEXT_CONTENT_TYPE,
                              MockCoordinatorEngineService.LOG);
    }

    public void testJobInfo() throws Exception {
        runTest("/v1/job/*", V1JobServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                MockCoordinatorEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.JOB_SHOW_PARAM, RestConstants.JOB_SHOW_INFO);
                URL url = createURL(MockCoordinatorEngineService.JOB_ID + 1, params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
                JSONObject obj = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
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
                obj = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertEquals(MockCoordinatorEngineService.JOB_ID + 1, obj.get(JsonTags.COORDINATOR_JOB_ID));
                assertEquals(RestConstants.JOB_SHOW_INFO, MockCoordinatorEngineService.did);
                assertEquals(MockCoordinatorEngineService.offset, new Integer(2));
                assertEquals(MockCoordinatorEngineService.length, new Integer(50));

                MockCoordinatorEngineService.reset();
                params = new HashMap<String, String>();
                params.put(RestConstants.JOB_SHOW_PARAM, RestConstants.JOB_SHOW_INFO);
                params.put(RestConstants.OFFSET_PARAM, "0");  //oozie-console.js and older clients
                params.put(RestConstants.LEN_PARAM, "0");
                url = createURL(MockCoordinatorEngineService.JOB_ID + 1, params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
                obj = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertEquals(MockCoordinatorEngineService.JOB_ID + 1, obj.get(JsonTags.COORDINATOR_JOB_ID));
                assertEquals(RestConstants.JOB_SHOW_INFO, MockCoordinatorEngineService.did);
                assertEquals(MockCoordinatorEngineService.offset, new Integer(1));
                assertEquals(MockCoordinatorEngineService.length, new Integer(1000));

                MockCoordinatorEngineService.reset();
                params = new HashMap<String, String>();
                params.put(RestConstants.JOB_SHOW_PARAM, RestConstants.JOB_SHOW_INFO);
                url = createURL(MockCoordinatorEngineService.JOB_ID + (MockCoordinatorEngineService.coordJobs.size() + 1), params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
                assertEquals(RestConstants.JOB_SHOW_INFO, MockCoordinatorEngineService.did);
                return null;
            }
        });
    }

    public void testCoordChange() throws Exception {
        runTest("/v1/job/*", V1JobServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                MockCoordinatorEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.ACTION_PARAM, RestConstants.JOB_ACTION_CHANGE);
                String changeValue = "endtime=2011-12-01T05:00Z";
                params.put(RestConstants.JOB_CHANGE_VALUE, changeValue);
                URL url = createURL(MockCoordinatorEngineService.JOB_ID + 1, params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("PUT");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertEquals(RestConstants.JOB_ACTION_CHANGE, MockCoordinatorEngineService.did);

                MockCoordinatorEngineService.reset();
                params = new HashMap<String, String>();
                params.put(RestConstants.ACTION_PARAM, RestConstants.JOB_ACTION_CHANGE);
                changeValue = "concurrency=200";
                params.put(RestConstants.JOB_CHANGE_VALUE, changeValue);
                url = createURL(MockCoordinatorEngineService.JOB_ID + 1, params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("PUT");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertEquals(RestConstants.JOB_ACTION_CHANGE, MockCoordinatorEngineService.did);

                MockCoordinatorEngineService.reset();
                params = new HashMap<String, String>();
                params.put(RestConstants.ACTION_PARAM, RestConstants.JOB_ACTION_CHANGE);
                changeValue = "endtime=2011-12-01T05:00Z;concurrency=200";
                params.put(RestConstants.JOB_CHANGE_VALUE, changeValue);
                url = createURL(MockCoordinatorEngineService.JOB_ID + 1, params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("PUT");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertEquals(RestConstants.JOB_ACTION_CHANGE, MockCoordinatorEngineService.did);

                MockCoordinatorEngineService.reset();
                params = new HashMap<String, String>();
                params.put(RestConstants.ACTION_PARAM, RestConstants.JOB_ACTION_CHANGE);
                changeValue = "endtime=2011-12-01T05:00";
                params.put(RestConstants.JOB_CHANGE_VALUE, changeValue);
                url = createURL(MockCoordinatorEngineService.JOB_ID
                        + (MockCoordinatorEngineService.coordJobs.size() + 1), params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("PUT");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
                assertEquals(RestConstants.JOB_ACTION_CHANGE, MockCoordinatorEngineService.did);

                return null;
            }
        });
    }

    /**
     * Test Coord Action kill feature via REST API
     * @throws Exception
     */
    public void testCoordActionKill() throws Exception {
        runTest("/v1/job/*", V1JobServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                MockCoordinatorEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.ACTION_PARAM, RestConstants.JOB_ACTION_KILL);
                String rangeType = "action";
                params.put(RestConstants.JOB_COORD_RANGE_TYPE_PARAM, rangeType);
                String scope = "1-3";
                params.put(RestConstants.JOB_COORD_SCOPE_PARAM, scope);
                URL url = createURL(MockCoordinatorEngineService.JOB_ID + 1, params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("PUT");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertEquals(RestConstants.JOB_ACTION_KILL, MockCoordinatorEngineService.did);

                MockCoordinatorEngineService.reset();
                params = new HashMap<String, String>();
                params.put(RestConstants.ACTION_PARAM, RestConstants.JOB_ACTION_KILL);
                rangeType = "date";
                params.put(RestConstants.JOB_COORD_RANGE_TYPE_PARAM, rangeType);
                scope = "2009-12-15T01:00Z::2009-12-16T01:00Z, 2009-12-20T01:00Z";
                params.put(RestConstants.JOB_COORD_SCOPE_PARAM, scope);
                url = createURL(MockCoordinatorEngineService.JOB_ID + 1, params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("PUT");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertEquals(RestConstants.JOB_ACTION_KILL, MockCoordinatorEngineService.did);

                return null;
            }
        });
    }

    public void testGraph() throws Exception {
        runTest("/v1/job/*", V1JobServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {

                MockDagEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.JOB_SHOW_PARAM, RestConstants.JOB_SHOW_GRAPH);
                URL url = createURL(MockDagEngineService.JOB_ID + 1 + MockDagEngineService.JOB_ID_END, params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.PNG_IMAGE_CONTENT_TYPE));
                //Assert.assertNotNull(ImageIO.read(conn.getInputStream())); // Can't check this as the XML is just <workflow/>

                // Negative test..  should fail
                MockCoordinatorEngineService.reset();
                params.clear();
                params.put(RestConstants.JOB_SHOW_PARAM, RestConstants.JOB_SHOW_GRAPH);
                url = createURL(MockCoordinatorEngineService.JOB_ID + 1, params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
                assertEquals(ErrorCode.E0306.name(), conn.getHeaderField(RestConstants.OOZIE_ERROR_CODE));

                return null;
            }
        });
    }
}
