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

import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.hadoop.fs.Path;
import org.apache.oozie.BuildInfo;
import org.apache.oozie.client.OozieClient.SYSTEM_MODE;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.servlet.DagServletTestCase;
import org.apache.oozie.servlet.MockCoordinatorEngineService;
import org.apache.oozie.servlet.MockDagEngineService;
import org.apache.oozie.servlet.SLAServlet;
import org.apache.oozie.servlet.V0JobServlet;
import org.apache.oozie.servlet.V0JobsServlet;
import org.apache.oozie.servlet.V1AdminServlet;
import org.apache.oozie.servlet.V1JobServlet;
import org.apache.oozie.servlet.V1JobsServlet;
import org.apache.oozie.servlet.V2AdminServlet;
import org.apache.oozie.servlet.V2JobServlet;
import org.apache.oozie.servlet.V2SLAServlet;
import org.json.simple.JSONArray;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Iterator;

import static org.mockito.Mockito.*;

public class TestWorkflowClient extends DagServletTestCase {

    static {
        new HeaderTestingVersionServlet();
        new V0JobsServlet();
        new V1JobsServlet();
        new V0JobServlet();
        new V1JobServlet();
        new V2JobServlet();
        new V1AdminServlet();
        new V2AdminServlet();
        new SLAServlet();
        new V2SLAServlet();
    }

    private static final boolean IS_SECURITY_ENABLED = false;
    static final String VERSION_0 = "/v" + OozieClient.WS_PROTOCOL_VERSION_0;
    static final String VERSION_1 = "/v" + OozieClient.WS_PROTOCOL_VERSION_1;
    static final String VERSION_2 = "/v" + OozieClient.WS_PROTOCOL_VERSION;
    static final String[] END_POINTS = { "/versions",
            VERSION_0 + "/jobs", VERSION_1 + "/jobs", VERSION_2 + "/jobs",
            VERSION_0 + "/job/*", VERSION_1 + "/job/*", VERSION_2 + "/job/*",
            VERSION_1 + "/admin/*", VERSION_2 + "/admin/*",
            VERSION_1 + "/sla/*", VERSION_2 + "/sla/*" };
    @SuppressWarnings("rawtypes")
    static final Class[] SERVLET_CLASSES = {HeaderTestingVersionServlet.class,
            V0JobsServlet.class, V1JobsServlet.class, V1JobsServlet.class,
            V0JobServlet.class, V1JobServlet.class, V2JobServlet.class,
            V1AdminServlet.class, V2AdminServlet.class,
            SLAServlet.class, V2SLAServlet.class};

    protected void setUp() throws Exception {
        super.setUp();
        MockDagEngineService.reset();
    }

    // UNCOMMENT TO QUICKLY RUN THE WS WITH MOCK engine
    //    public void testRunning() throws Exception {
    //        runTest(END_POINTS, SERVLET_CLASSES, new Callable<Void>() {
    //            public Void call() throws Exception {
    //                Thread.sleep(Long.MAX_VALUE);
    //                return null;
    //            }
    //        });
    //
    //    }

    /**
     * Test methods for headers manipulation
     */
    public void testHeaders() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();
                String oozieUrl = getContextURL();
                OozieClient wc = new OozieClient(oozieUrl);
                wc.setHeader("header", "test");
                assertEquals("test", wc.getHeader("header"));
                assertEquals("test", wc.getHeaders().get("header"));

                boolean found = false;
                for (Iterator<String> headers = wc.getHeaderNames(); headers.hasNext();) {
                    if ("header".equals(headers.next())) {
                        found = true;
                    }
                }
                assertTrue("headers does not contain header!", found);
                wc.validateWSVersion();
                assertTrue(HeaderTestingVersionServlet.OOZIE_HEADERS.containsKey("header"));
                assertTrue(HeaderTestingVersionServlet.OOZIE_HEADERS.containsValue("test"));
                wc.removeHeader("header");
                assertNull(wc.getHeader("header"));
                return null;
            }
        });

    }

    public void testUrls() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                OozieClient wc = new OozieClient(oozieUrl);
                assertEquals(oozieUrl, wc.getOozieUrl().substring(0, wc.getOozieUrl().length() - 1));
                assertTrue(wc.getProtocolUrl().startsWith(wc.getOozieUrl() + "v"));

                try {
                    wc = new OozieClientForTest(oozieUrl);
                    wc.getProtocolUrl();
                    fail("wrong version should run throw exception");
                }
                catch (OozieClientException e) {
                    assertEquals("UNSUPPORTED_VERSION : Supported version [2] or less, Unsupported versions[-11-10]", e.toString());
                }
                return null;
            }
        });

    }

    public void testValidateVersion() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                OozieClient wc = new OozieClient(oozieUrl);
                wc.validateWSVersion();
                return null;
            }
        });
    }

    public void testSubmit() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                MockDagEngineService.reset();
                int wfCount = MockDagEngineService.INIT_WF_COUNT;
                String oozieUrl = getContextURL();
                OozieClient wc = new OozieClient(oozieUrl);
                Properties conf = wc.createConfiguration();
                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "workflow.xml")).close();
                conf.setProperty(OozieClient.APP_PATH, appPath.toString());

                assertEquals(MockDagEngineService.JOB_ID + wfCount + MockDagEngineService.JOB_ID_END, wc.submit(conf));
                assertFalse(MockDagEngineService.started.get(wfCount));
                return null;
            }
        });
    }

    public void testRun() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                int wfCount = MockDagEngineService.INIT_WF_COUNT;
                OozieClient wc = new OozieClient(oozieUrl);
                Properties conf = wc.createConfiguration();
                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "workflow.xml")).close();
                conf.setProperty(OozieClient.APP_PATH, appPath.toString());

                assertEquals(MockDagEngineService.JOB_ID + wfCount + MockDagEngineService.JOB_ID_END, wc.run(conf));
                assertTrue(MockDagEngineService.started.get(wfCount));
                return null;
            }
        });
    }

    public void testStart() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                OozieClient wc = new OozieClient(oozieUrl);
                Properties conf = wc.createConfiguration();
                conf.setProperty(OozieClient.USER_NAME, "x");
                wc.start(MockDagEngineService.JOB_ID + "1" + MockDagEngineService.JOB_ID_END);
                assertEquals(RestConstants.JOB_ACTION_START, MockDagEngineService.did);
                return null;
            }
        });
    }

    public void testSuspend() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                OozieClient wc = new OozieClient(oozieUrl);
                Properties conf = wc.createConfiguration();
                conf.setProperty(OozieClient.USER_NAME, "x");
                wc.suspend(MockDagEngineService.JOB_ID + "1" + MockDagEngineService.JOB_ID_END);
                assertEquals(RestConstants.JOB_ACTION_SUSPEND, MockDagEngineService.did);
                return null;
            }
        });
    }

    public void testResume() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                OozieClient wc = new OozieClient(oozieUrl);
                Properties conf = wc.createConfiguration();
                conf.setProperty(OozieClient.USER_NAME, "x");
                wc.resume(MockDagEngineService.JOB_ID + "1" + MockDagEngineService.JOB_ID_END);
                assertEquals(RestConstants.JOB_ACTION_RESUME, MockDagEngineService.did);
                return null;
            }
        });
    }

    public void testKill() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                OozieClient wc = new OozieClient(oozieUrl);
                Properties conf = wc.createConfiguration();
                conf.setProperty(OozieClient.USER_NAME, "x");
                wc.kill(MockDagEngineService.JOB_ID + "1" + MockDagEngineService.JOB_ID_END);
                assertEquals(RestConstants.JOB_ACTION_KILL, MockDagEngineService.did);
                return null;
            }
        });
    }

    public void testReRun() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                OozieClient wc = new OozieClient(oozieUrl);
                Properties conf = wc.createConfiguration();
                conf.setProperty(OozieClient.USER_NAME, getTestUser());
                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "workflow.xml")).close();
                conf.setProperty(OozieClient.APP_PATH, appPath.toString());

                wc.reRun(MockDagEngineService.JOB_ID + "1" + MockDagEngineService.JOB_ID_END, conf);
                assertEquals(RestConstants.JOB_ACTION_RERUN, MockDagEngineService.did);
                assertTrue(MockDagEngineService.started.get(1));
                return null;
            }
        });
    }

    public void testJobStatus() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                OozieClient wc = new OozieClient(oozieUrl);
                WorkflowJob wf = wc.getJobInfo(MockDagEngineService.JOB_ID + "1" + MockDagEngineService.JOB_ID_END);
                assertEquals(RestConstants.JOB_SHOW_INFO, MockDagEngineService.did);
                assertEquals(MockDagEngineService.JOB_ID + "1" + MockDagEngineService.JOB_ID_END, wf.getId());
                return null;
            }
        });
    }

    public void testJobsStatus() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                OozieClient wc = new OozieClient(oozieUrl);

                List<WorkflowJob> list = wc.getJobsInfo(null);
                assertEquals(MockDagEngineService.INIT_WF_COUNT, list.size());
                for (int i = 0; i < MockDagEngineService.INIT_WF_COUNT; i++) {
                    assertNotNull(list.get(i).getAppPath());
                    assertEquals(MockDagEngineService.JOB_ID + i + MockDagEngineService.JOB_ID_END, list.get(i).getId());
                }

                MockDagEngineService.reset();
                list = wc.getJobsInfo("name=x", 3, 4);
                assertEquals(MockDagEngineService.INIT_WF_COUNT, list.size());
                for (int i = 0; i < MockDagEngineService.INIT_WF_COUNT; i++) {
                    assertNotNull(list.get(i).getAppPath());
                    assertEquals(MockDagEngineService.JOB_ID + i + MockDagEngineService.JOB_ID_END, list.get(i).getId());
                }
                return null;
            }
        });
    }

    public void testExternalId() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                OozieClient wc = new OozieClient(oozieUrl);
                assertEquals("id-valid", wc.getJobId("external-valid"));
                assertEquals(RestConstants.JOBS_EXTERNAL_ID_PARAM, MockDagEngineService.did);
                assertNull(wc.getJobId("external-invalid"));
                return null;
            }
        });
    }

    public void testJobsStatusFilter() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                OozieClient wc = new OozieClient(oozieUrl);

                wc.getJobsInfo("name=x");
                wc.getJobsInfo("user=x");
                wc.getJobsInfo("group=x");
                wc.getJobsInfo("status=RUNNING");
                wc.getJobsInfo("name=x;name=y");

                try {
                    wc.getJobsInfo("name=");
                    fail();
                }
                catch (OozieClientException ex) {
                    //nop
                }

                try {
                    wc.getJobsInfo("x=x");
                    fail();
                }
                catch (OozieClientException ex) {
                    //nop
                }
                try {
                    wc.getJobsInfo("status=X");
                    fail();
                }
                catch (OozieClientException ex) {
                    //nop
                }


                return null;
            }
        });
    }

    public void testSafeMode() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                OozieClient wc = new OozieClient(oozieUrl);
                SYSTEM_MODE systemMode = wc.getSystemMode();
                assertEquals(systemMode, SYSTEM_MODE.NORMAL);

                wc.setSystemMode(SYSTEM_MODE.SAFEMODE);
                systemMode = wc.getSystemMode();
                assertEquals(systemMode, SYSTEM_MODE.SAFEMODE);

                wc.setSystemMode(SYSTEM_MODE.NOWEBSERVICE);
                systemMode = wc.getSystemMode();
                assertEquals(systemMode, SYSTEM_MODE.NOWEBSERVICE);
                return null;
            }
        });
    }

    public void testWSErrors() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                String oozieUrl = getContextURL() + "dummy";
                OozieClient wc = new OozieClient(oozieUrl);

                try {
                    wc.getJobInfo(MockDagEngineService.JOB_ID + 1);
                    fail();
                }
                catch (OozieClientException e) {
                    assertNotNull(e.getErrorCode());
                    assertNotNull(e.getMessage());
                }

                return null;
            }

        });
    }

    public void testServerBuildVersion() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                OozieClient wc = new OozieClient(oozieUrl);
                assertEquals(BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_VERSION),
                             wc.getServerBuildVersion());
                return null;
            }
        });
    }

    public void testClientBuildVersion() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                OozieClient wc = new OozieClient(oozieUrl);
                assertEquals(BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_VERSION),
                             wc.getClientBuildVersion());
                return null;
            }
        });
    }

    /**
     * Test client's methods getWorkflowActionInfo and getBundleJobInfo
     */
    public void testJobInformation() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                OozieClient wc = new OozieClient(oozieUrl);
                String jobId = MockDagEngineService.JOB_ID + "1" + MockDagEngineService.JOB_ID_END;
                assertEquals(RestConstants.JOB_SHOW_LOG, wc.getJobLog(jobId));

                WorkflowAction wfAction = wc.getWorkflowActionInfo(jobId);

                assertEquals(jobId, wfAction.getId());
                CoordinatorJob job = wc.getCoordJobInfo(MockCoordinatorEngineService.JOB_ID + "1"
                        + MockCoordinatorEngineService.JOB_ID_END);

                assertEquals("group", job.getAcl());
                assertEquals("RUNNING", job.getStatus().toString());
                assertEquals("user", job.getUser());
                assertEquals(MockCoordinatorEngineService.offset, new Integer(1));
                assertEquals(MockCoordinatorEngineService.length, new Integer(1000));

                BundleJob bundleJob = wc.getBundleJobInfo(jobId);
                assertEquals("SUCCEEDED", bundleJob.getStatus().toString());
                assertEquals("user", bundleJob.getUser());

                return null;
            }
        });
    }


    /**
     * Test SlaServlet and client's method getSlaInfo
     */
    public void testSla() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                cleanUpDBTables();
                String oozieUrl = getContextURL();
                OozieClient wc = new OozieClient(oozieUrl);

                PrintStream oldStream = System.out;
                ByteArrayOutputStream data = new ByteArrayOutputStream();
                System.setOut(new PrintStream(data));
                try {
                    wc.getSlaInfo(0, 10, null);
                }
                finally {
                    System.setOut(oldStream);
                }
                assertTrue(data.toString().contains("<sla-message>"));
                assertTrue(data.toString().contains("<last-sequence-id>0</last-sequence-id>"));
                assertTrue(data.toString().contains("</sla-message>"));

                return null;
            }
        });
    }

    /**
     * Fake class for test reaction on a bad version
     */
    private class OozieClientForTest extends OozieClient {

        public OozieClientForTest(String oozieUrl) {
            super(oozieUrl);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected HttpURLConnection createConnection(URL url, String method) throws IOException, OozieClientException {
            HttpURLConnection result = mock(HttpURLConnection.class);
            when(result.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);

            JSONArray versions = new JSONArray();
            versions.add(-11);
            versions.add(-10);
            Writer writer = new StringWriter();
            versions.writeJSONString(writer);
            writer.flush();

            when(result.getInputStream()).thenReturn(new ByteArrayInputStream(writer.toString().getBytes()));
            return result;
        }

    }
}
