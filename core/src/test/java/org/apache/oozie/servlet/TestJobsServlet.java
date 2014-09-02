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

import org.apache.oozie.service.AuthorizationService;

import java.io.StringReader;

import org.apache.oozie.service.DagEngineService;
import org.apache.oozie.DagEngine;
import org.apache.oozie.servlet.V0JobsServlet;
import org.apache.oozie.service.Services;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.util.XConfiguration;
import org.json.simple.JSONArray;
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

public class TestJobsServlet extends DagServletTestCase {

    static {
        new V0JobsServlet();
    }

    private static final boolean IS_SECURITY_ENABLED = false;

    protected void setUp() throws Exception {
        super.setUp();
    }

    public void testSubmit() throws Exception {
        //runTest("/jobs", BaseJobsServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
        runTest("/v0/jobs", V0JobsServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                MockDagEngineService.reset();

                String appPath = getFsTestCaseDir().toString() + "/app";

                FileSystem fs = getFileSystem();
                Path jobXmlPath = new Path(appPath, "workflow.xml");
                fs.create(jobXmlPath);

                int wfCount = MockDagEngineService.workflows.size();
                Configuration jobConf = new XConfiguration();
                jobConf.set(OozieClient.USER_NAME, getTestUser());
                jobConf.set(OozieClient.APP_PATH, appPath);

                Map<String, String> params = new HashMap<String, String>();
                URL url = createURL("", params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                jobConf.writeXml(conn.getOutputStream());
                assertEquals(HttpServletResponse.SC_CREATED, conn.getResponseCode());
                JSONObject obj = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertEquals(MockDagEngineService.JOB_ID + wfCount + MockDagEngineService.JOB_ID_END,
                             obj.get(JsonTags.JOB_ID));
                assertFalse(MockDagEngineService.started.get(wfCount));
                wfCount++;

                jobConf = new XConfiguration();
                jobConf.set(OozieClient.USER_NAME, getTestUser());
                jobConf.set(OozieClient.APP_PATH, appPath);

                params = new HashMap<String, String>();
                params.put(RestConstants.ACTION_PARAM, RestConstants.JOB_ACTION_START);
                url = createURL("", params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                jobConf.writeXml(conn.getOutputStream());
                assertEquals(HttpServletResponse.SC_CREATED, conn.getResponseCode());
                obj = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertEquals(MockDagEngineService.JOB_ID + wfCount + MockDagEngineService.JOB_ID_END,
                             obj.get(JsonTags.JOB_ID));
                assertTrue(MockDagEngineService.started.get(wfCount));
                Services services = Services.get();
                DagEngine de = services.get(DagEngineService.class).getDagEngine(getTestUser());
                StringReader sr = new StringReader(de.getJob(MockDagEngineService.JOB_ID + wfCount).getConf());
                Configuration conf1 = new XConfiguration(sr);
                return null;
            }
        });
    }

    public void testJobs() throws Exception {
        //runTest("/jobs", BaseJobsServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
        runTest("/v0/jobs", V0JobsServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                MockDagEngineService.reset();

                int wfCount = MockDagEngineService.workflows.size();
                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.JOBS_FILTER_PARAM, "name=x");
                URL url = createURL("", params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
                JSONObject json = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                JSONArray array = (JSONArray) json.get(JsonTags.WORKFLOWS_JOBS);
                assertEquals(MockDagEngineService.INIT_WF_COUNT, array.size());
                for (int i = 0; i < MockDagEngineService.INIT_WF_COUNT; i++) {
                    assertEquals(MockDagEngineService.JOB_ID + i + MockDagEngineService.JOB_ID_END,
                                 ((JSONObject) array.get(i)).get(JsonTags.WORKFLOW_ID));
                    assertNotNull(((JSONObject) array.get(i)).get(JsonTags.WORKFLOW_APP_PATH));
                }

                params = new HashMap<String, String>();
                params.put(RestConstants.JOBS_FILTER_PARAM, "name=x");
                params.put(RestConstants.OFFSET_PARAM, "2");
                params.put(RestConstants.LEN_PARAM, "100");
                url = createURL("", params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
                json = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                array = (JSONArray) json.get(JsonTags.WORKFLOWS_JOBS);

                assertEquals(MockDagEngineService.INIT_WF_COUNT, array.size());
                for (int i = 0; i < MockDagEngineService.INIT_WF_COUNT; i++) {
                    assertEquals(MockDagEngineService.JOB_ID + i + MockDagEngineService.JOB_ID_END,
                                 ((JSONObject) array.get(i)).get(JsonTags.WORKFLOW_ID));
                    assertNotNull(((JSONObject) array.get(i)).get(JsonTags.WORKFLOW_APP_PATH));
                }

                params = new HashMap<String, String>();
                params.put(RestConstants.JOBS_EXTERNAL_ID_PARAM, "external-valid");
                url = createURL("", params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
                JSONObject obj = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertEquals("id-valid", obj.get(JsonTags.JOB_ID));

                params = new HashMap<String, String>();
                params.put(RestConstants.JOBS_EXTERNAL_ID_PARAM, "external-invalid");
                url = createURL("", params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
                obj = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertNull(obj.get(JsonTags.JOB_ID));

                return null;
            }
        });
    }

    public static class V0JobsServletWithDefUser extends V0JobsServlet {

        protected String getUser(HttpServletRequest request) {
            return getTestUser2();
        }
    }

    public void testDiffUser() throws Exception {
        //runTest("/jobs", BaseJobsServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
        runTest("/v0/jobs", V0JobsServletWithDefUser.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                MockDagEngineService.reset();

                String appPath = getFsTestCaseDir().toString() + "/app";

                FileSystem fs = getFileSystem();
                Path jobXmlPath = new Path(appPath, "workflow.xml");
                fs.create(jobXmlPath);

                Configuration jobConf = new XConfiguration();
                jobConf.set(OozieClient.USER_NAME, getTestUser());
                jobConf.set(OozieClient.APP_PATH, appPath);

                Map<String, String> params = new HashMap<String, String>();
                URL url = createURL("", params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                jobConf.writeXml(conn.getOutputStream());
                assertEquals(HttpServletResponse.SC_CREATED, conn.getResponseCode());
                assertEquals(getTestUser2(), MockDagEngineService.user);
                return null;
            }
        });
    }

}
