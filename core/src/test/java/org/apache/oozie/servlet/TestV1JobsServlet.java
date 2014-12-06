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

import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.DagEngine;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.AuthorizationService;
import org.apache.oozie.service.DagEngineService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XConfiguration;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class TestV1JobsServlet extends DagServletTestCase {

    static {
        new V1JobsServlet();
    }

    private static final boolean IS_SECURITY_ENABLED = false;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    public void testSubmit() throws Exception {
        runTest("/v1/jobs", V1JobsServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
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
                wfCount++;

                jobConf = new XConfiguration();
                jobConf.set(OozieClient.USER_NAME, getTestUser());

                params = new HashMap<String, String>();
                url = createURL("", params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                jobConf.writeXml(conn.getOutputStream());
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());

                Path libPath1 = new Path(getFsTestCaseDir(), "libpath1");
                fs.mkdirs(libPath1);
                Path jobXmlPath1 = new Path(libPath1, "workflow.xml");
                fs.create(jobXmlPath1);
                jobConf = new XConfiguration();
                jobConf.set(OozieClient.USER_NAME, getTestUser());
                jobConf.set(OozieClient.LIBPATH, libPath1.toString());

                params = new HashMap<String, String>();
                url = createURL("", params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                jobConf.writeXml(conn.getOutputStream());
                assertEquals(HttpServletResponse.SC_CREATED, conn.getResponseCode());
                assertEquals(HttpServletResponse.SC_CREATED, conn.getResponseCode());
                obj = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertEquals(MockDagEngineService.JOB_ID + wfCount + MockDagEngineService.JOB_ID_END,
                             obj.get(JsonTags.JOB_ID));
                assertFalse(MockDagEngineService.started.get(wfCount));
                wfCount++;

                Path libPath2 = new Path(getFsTestCaseDir(), "libpath2");
                fs.mkdirs(libPath2);
                jobConf = new XConfiguration();
                jobConf.set(OozieClient.USER_NAME, getTestUser());
                jobConf.set(OozieClient.LIBPATH, libPath1.toString() + "," + libPath2.toString());

                params = new HashMap<String, String>();
                url = createURL("", params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                jobConf.writeXml(conn.getOutputStream());
                assertEquals(HttpServletResponse.SC_CREATED, conn.getResponseCode());
                assertEquals(HttpServletResponse.SC_CREATED, conn.getResponseCode());
                obj = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertEquals(MockDagEngineService.JOB_ID + wfCount + MockDagEngineService.JOB_ID_END,
                             obj.get(JsonTags.JOB_ID));
                assertFalse(MockDagEngineService.started.get(wfCount));
                wfCount++;

                return null;
            }
        });
    }

    public void testJobs() throws Exception {
        runTest("/v1/jobs", V1JobsServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
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
                params.put(RestConstants.TIME_ZONE_PARAM, "PST");
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
                    String startTime = (((JSONObject) array.get(i)).get(JsonTags.WORKFLOW_START_TIME)).toString();
                    assertTrue(startTime.endsWith("PST") || startTime.endsWith("PDT")); // PDT if on daylight saving time
                }

                params = new HashMap<String, String>();
                params.put(RestConstants.JOBTYPE_PARAM, "wf");
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

}
