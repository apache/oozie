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

import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.Services;
import org.apache.oozie.BuildInfo;
import org.apache.oozie.servlet.V0AdminServlet;
import org.apache.oozie.servlet.V0JobServlet;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import javax.servlet.http.HttpServletResponse;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class TestAdminServlet extends DagServletTestCase {

    static {
        new V0AdminServlet();
        new V0JobServlet();
    }
    private static final boolean IS_SECURITY_ENABLED = false;

    protected void setUp() throws Exception {
        super.setUp();
    }

    public void testStatus() throws Exception {
        runTest("/v0/admin/*", V0AdminServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                URL url = createURL(RestConstants.ADMIN_STATUS_RESOURCE, Collections.EMPTY_MAP);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
                JSONObject json = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertEquals(false, json.get(JsonTags.OOZIE_SAFE_MODE));
                return null;
            }
        });
    }

    public void testOsEnv() throws Exception {
        runTest("/v0/admin/*", V0AdminServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                URL url = createURL(RestConstants.ADMIN_OS_ENV_RESOURCE, Collections.EMPTY_MAP);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
                JSONObject json = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertTrue(json.containsKey("USER"));
                return null;
            }
        });
    }

    public void testJavaSysProps() throws Exception {
        runTest("/v0/admin/*", V0AdminServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                URL url = createURL(RestConstants.ADMIN_JAVA_SYS_PROPS_RESOURCE, Collections.EMPTY_MAP);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
                JSONObject json = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertTrue(json.containsKey("java.version"));
                return null;
            }
        });
    }

    public void testConfiguration() throws Exception {
        runTest("/v0/admin/*", V0AdminServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                URL url = createURL(RestConstants.ADMIN_CONFIG_RESOURCE, Collections.EMPTY_MAP);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
                JSONObject json = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertTrue(json.containsKey(Services.CONF_SERVICE_CLASSES));
                return null;
            }
        });
    }

    public void testInstrumentation() throws Exception {
        runTest("/v0/admin/*", V0AdminServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                URL url = createURL(RestConstants.ADMIN_INSTRUMENTATION_RESOURCE, Collections.EMPTY_MAP);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
                JSONObject json = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertTrue(json.containsKey(JsonTags.INSTR_VARIABLES));
                return null;
            }
        });
    }

    public void testSafeMode() throws Exception {
        runTest(new String[]{"/v0/admin/*", "/v0/job/*"}, new Class[]{V0AdminServlet.class, V0JobServlet.class},
                IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {

                MockDagEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.ACTION_PARAM, RestConstants.JOB_ACTION_START);
                URL url = createURL("/v0/job/*", MockDagEngineService.JOB_ID + 1, params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("PUT");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());

                MockDagEngineService.reset();
                url = createURL("/v0/admin/*", RestConstants.ADMIN_STATUS_RESOURCE, Collections.EMPTY_MAP);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
                JSONObject json = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertTrue(json.containsKey(JsonTags.OOZIE_SAFE_MODE));
                assertFalse((Boolean) json.get(JsonTags.OOZIE_SAFE_MODE));


                MockDagEngineService.reset();
                params = new HashMap<String, String>();
                params.put(RestConstants.ADMIN_SAFE_MODE_PARAM, "true");
                url = createURL("/v0/admin/*", RestConstants.ADMIN_STATUS_RESOURCE, params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("PUT");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());

                MockDagEngineService.reset();
                url = createURL("/v0/admin/*", RestConstants.ADMIN_STATUS_RESOURCE, Collections.EMPTY_MAP);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
                json = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertTrue(json.containsKey(JsonTags.OOZIE_SAFE_MODE));
                assertTrue((Boolean) json.get(JsonTags.OOZIE_SAFE_MODE));

                MockDagEngineService.reset();
                params = new HashMap<String, String>();
                params.put(RestConstants.ACTION_PARAM, RestConstants.JOB_ACTION_START);
                url = createURL("/v0/job/*", MockDagEngineService.JOB_ID + 1, params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("PUT");
                assertEquals(HttpServletResponse.SC_SERVICE_UNAVAILABLE, conn.getResponseCode());

                MockDagEngineService.reset();
                params = new HashMap<String, String>();
                params.put(RestConstants.ADMIN_SAFE_MODE_PARAM, "false");
                url = createURL("/v0/admin/*", RestConstants.ADMIN_STATUS_RESOURCE, params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("PUT");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());

                MockDagEngineService.reset();
                url = createURL("/v0/admin/*", RestConstants.ADMIN_STATUS_RESOURCE, Collections.EMPTY_MAP);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
                json = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertTrue(json.containsKey(JsonTags.OOZIE_SAFE_MODE));
                assertFalse((Boolean) json.get(JsonTags.OOZIE_SAFE_MODE));

                MockDagEngineService.reset();
                params = new HashMap<String, String>();
                params.put(RestConstants.ACTION_PARAM, RestConstants.JOB_ACTION_START);
                url = createURL("/v0/job/*", MockDagEngineService.JOB_ID + 1, params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("PUT");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());

                return null;
            }
        });
    }

    public void testVersion() throws Exception {
        runTest("/v0/admin/*", V0AdminServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                URL url = createURL(RestConstants.ADMIN_BUILD_VERSION_RESOURCE, Collections.EMPTY_MAP);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
                JSONObject json = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertEquals(BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_VERSION),
                             json.get(JsonTags.BUILD_VERSION));
                return null;
            }
        });
    }

}
