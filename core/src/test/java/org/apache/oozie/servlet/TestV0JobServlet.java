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
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.servlet.V0JobServlet;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import javax.servlet.http.HttpServletResponse;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import java.util.List;
import org.apache.oozie.ErrorCode;

public class TestV0JobServlet extends DagServletTestCase {

    static {
        new V0JobServlet();
    }

    private static final boolean IS_SECURITY_ENABLED = false;

    protected void setUp() throws Exception {
        super.setUp();
    }

    private void _testAction(final String action, final Configuration conf) throws Exception {
        runTest("/v0/job/*", V0JobServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                MockDagEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.ACTION_PARAM, action);
                URL url = createURL(MockDagEngineService.JOB_ID + 1, params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("PUT");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                if (conf != null) {
                    conf.writeXml(conn.getOutputStream());
                }
                if (conf == null || conf.get(OozieClient.USER_NAME) != null) {
                    assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                    assertEquals(action, MockDagEngineService.did);
                }
                else {
                    assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
                }

                MockDagEngineService.reset();
                params = new HashMap<String, String>();
                params.put(RestConstants.ACTION_PARAM, action);
                url = createURL(MockDagEngineService.JOB_ID + (MockDagEngineService.workflows.size() + 1) +
                                MockDagEngineService.JOB_ID_END, params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("PUT");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                if (conf != null) {
                    conf.writeXml(conn.getOutputStream());
                }
                if (conf == null || conf.get(OozieClient.USER_NAME) != null) {
                    assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
                    assertEquals(action, MockDagEngineService.did);
                }
                else {
                    assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
                }
                return null;
            }
        });
    }

    public void testStart() throws Exception {
        _testAction(RestConstants.JOB_ACTION_START, null);
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

    public void testReRun() throws Exception {
        Configuration conf = new XConfiguration();
        conf.set(OozieClient.USER_NAME, getTestUser());
        Path appPath = new Path(getFsTestCaseDir(), "app");
        getFileSystem().mkdirs(appPath);
        getFileSystem().create(new Path(appPath, "workflow.xml")).close();
        conf.set(OozieClient.APP_PATH, appPath.toString());

        _testAction(RestConstants.JOB_ACTION_RERUN, conf);
    }

    private void _testNonJsonResponses(final String show, final String contentType, final String response)
            throws Exception {
        runTest("/v0/job/*", V0JobServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                MockDagEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.JOB_SHOW_PARAM, show);
                URL url = createURL(MockDagEngineService.JOB_ID + 1 + MockDagEngineService.JOB_ID_END, params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(contentType));
                String output = IOUtils.getReaderAsString(new InputStreamReader(conn.getInputStream()), 1000);
                assertEquals(response, output);
                assertEquals(show, MockDagEngineService.did);

                MockDagEngineService.reset();
                params = new HashMap<String, String>();
                params.put(RestConstants.JOB_SHOW_PARAM, show);
                url = createURL(MockDagEngineService.JOB_ID + (MockDagEngineService.workflows.size() + 1) +
                                MockDagEngineService.JOB_ID_END, params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
                assertEquals(show, MockDagEngineService.did);
                return null;
            }
        });
    }

    public void testJobDef() throws Exception {
        _testNonJsonResponses(RestConstants.JOB_SHOW_DEFINITION, RestConstants.XML_CONTENT_TYPE,
                              MockDagEngineService.WORKFLOW_APP);
    }

    public void testJobLog() throws Exception {
        _testNonJsonResponses(RestConstants.JOB_SHOW_LOG, RestConstants.TEXT_CONTENT_TYPE,
                              MockDagEngineService.LOG);
    }

    public void testJobInfo() throws Exception {
        runTest("/v0/job/*", V0JobServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                MockDagEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.JOB_SHOW_PARAM, RestConstants.JOB_SHOW_INFO);
                URL url = createURL(MockDagEngineService.JOB_ID + 1 + MockDagEngineService.JOB_ID_END, params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
                JSONObject obj = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertEquals(MockDagEngineService.JOB_ID + 1 + MockDagEngineService.JOB_ID_END,
                             obj.get(JsonTags.WORKFLOW_ID));
                assertEquals(RestConstants.JOB_SHOW_INFO, MockDagEngineService.did);

                MockDagEngineService.reset();
                params = new HashMap<String, String>();
                params.put(RestConstants.JOB_SHOW_PARAM, RestConstants.JOB_SHOW_INFO);
                url = createURL(MockDagEngineService.JOB_ID + (MockDagEngineService.workflows.size() + 1) +
                                MockDagEngineService.JOB_ID_END, params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
                assertEquals(RestConstants.JOB_SHOW_INFO, MockDagEngineService.did);
                return null;
            }
        });
    }

    public void testGraph() throws Exception {
        runTest("/v0/job/*", V0JobServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {

                MockDagEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.JOB_SHOW_PARAM, RestConstants.JOB_SHOW_GRAPH);
                URL url = createURL(MockDagEngineService.JOB_ID + 1 + MockDagEngineService.JOB_ID_END, params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");

                assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
                assertEquals(ErrorCode.E0306.name(), conn.getHeaderField(RestConstants.OOZIE_ERROR_CODE));

                return null;
            }
        });
    }
}
