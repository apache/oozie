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

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.cli.OozieCLI;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.servlet.DagServletTestCase;
import org.apache.oozie.servlet.MockCoordinatorEngineService;
import org.apache.oozie.servlet.MockDagEngineService;
import org.apache.oozie.servlet.V1AdminServlet;
import org.apache.oozie.servlet.V1JobServlet;
import org.apache.oozie.servlet.V1JobsServlet;
import org.apache.oozie.util.XConfiguration;

//hardcoding options instead using constants on purpose, to detect changes to option names if any and correct docs.
public class TestOozieCLI extends DagServletTestCase {

    static {
        new HeaderTestingVersionServlet();
        new V1JobServlet();
        new V1JobsServlet();
        new V1AdminServlet();
    }

    static final boolean IS_SECURITY_ENABLED = false;
    static final String VERSION = "/v" + OozieClient.WS_PROTOCOL_VERSION;
    static final String[] END_POINTS = {"/versions", VERSION + "/jobs", VERSION + "/job/*", VERSION + "/admin/*"};
    static final Class[] SERVLET_CLASSES =
 { HeaderTestingVersionServlet.class, V1JobsServlet.class, V1JobServlet.class,
            V1AdminServlet.class };

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        MockDagEngineService.reset();
        MockCoordinatorEngineService.reset();
    }

    private String createConfigFile(String appPath) throws Exception {
        String path = getTestCaseDir() + "/" + getName() + ".xml";
        Configuration conf = new Configuration(false);
        conf.set(OozieClient.APP_PATH, appPath);
        conf.set(OozieClient.RERUN_SKIP_NODES, "node");

        OutputStream os = new FileOutputStream(path);
        conf.writeXml(os);
        os.close();
        return path;
    }

    private String createPropertiesFile(String appPath) throws Exception {
        String path = getTestCaseDir() + "/" + getName() + ".properties";
        Properties props = new Properties();
        props.setProperty(OozieClient.USER_NAME, getTestUser());
        props.setProperty(OozieClient.GROUP_NAME, getTestGroup());
        props.setProperty(OozieClient.APP_PATH, appPath);
        props.setProperty(OozieClient.RERUN_SKIP_NODES, "node");
        props.setProperty("a", "A");

        OutputStream os = new FileOutputStream(path);
        props.store(os, "");
        os.close();
        return path;
    }

    private String createPropertiesFileWithTrailingSpaces(String appPath) throws Exception {
        String path = getTestCaseDir() + "/" + getName() + ".properties";
        Properties props = new Properties();
        props.setProperty(OozieClient.USER_NAME, getTestUser());
        props.setProperty(OozieClient.GROUP_NAME, getTestGroup());

        props.setProperty(OozieClient.APP_PATH, appPath);
        //add spaces to string
        props.setProperty(OozieClient.RERUN_SKIP_NODES + " ", " node ");
        OutputStream os = new FileOutputStream(path);
        props.store(os, "");
        os.close();
        return path;
    }

    private String createPigPropertiesFile(String appPath) throws Exception {
        String path = getTestCaseDir() + "/" + getName() + ".properties";
        Properties props = new Properties();
        props.setProperty(OozieClient.USER_NAME, getTestUser());
        props.setProperty(XOozieClient.NN, "localhost:9000");
        props.setProperty(XOozieClient.JT, "localhost:9001");
        props.setProperty("oozie.libpath", appPath);
        props.setProperty("mapred.output.dir", appPath);
        props.setProperty("a", "A");

        OutputStream os = new FileOutputStream(path);
        props.store(os, "");
        os.close();
        return path;
    }

    private String createMRProperties(String appPath) throws Exception {
        String path = getTestCaseDir() + "/" + getName() + ".properties";
        Properties props = new Properties();
        props.setProperty(OozieClient.USER_NAME, getTestUser());
        props.setProperty(OozieClient.GROUP_NAME, getTestGroup());
        props.setProperty(OozieClient.APP_PATH, appPath);
        props.setProperty(OozieClient.RERUN_SKIP_NODES, "node");
        props.setProperty(XOozieClient.NN, "localhost:9000");
        props.setProperty(XOozieClient.JT, "localhost:9001");
        props.setProperty("mapred.mapper.class", "mapper.class");
        props.setProperty("mapred.reducer.class", "reducer.class");
        props.setProperty("mapred.input.dir", "input");
        props.setProperty("mapred.output.dir", "output");
        props.setProperty("oozie.libpath", appPath);
        props.setProperty("a", "A");

        OutputStream os = new FileOutputStream(path);
        props.store(os, "");
        os.close();
        return path;
    }

    private String createPigScript(String appPath) throws Exception {
        String path = getTestCaseDir() + "/" + getName() + ".properties";

        DataOutputStream dos = new DataOutputStream(new FileOutputStream(path));

        String pigScript = "A = load \'/user/data\' using PigStorage(:);\n" +
        		           "B = foreach A generate $0" +
                           "dumb B;";

        dos.writeBytes(pigScript);
        dos.close();

        return path;
    }

    public void testSubmit() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                int wfCount = MockDagEngineService.INIT_WF_COUNT;

                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "workflow.xml")).close();

                String[] args = new String[]{"job", "-submit", "-oozie", oozieUrl, "-config",
                        createConfigFile(appPath.toString())};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals("submit", MockDagEngineService.did);
                assertFalse(MockDagEngineService.started.get(wfCount));
                wfCount++;

                args = new String[]{"job", "-submit", "-oozie", oozieUrl, "-config",
                        createPropertiesFile(appPath.toString())};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals("submit", MockDagEngineService.did);
                assertFalse(MockDagEngineService.started.get(wfCount));

                MockDagEngineService.reset();
                wfCount = MockDagEngineService.INIT_WF_COUNT;
                args = new String[]{"job", "-submit", "-oozie", oozieUrl, "-config",
                        createPropertiesFile(appPath.toString()) + "x"};
                assertEquals(-1, new OozieCLI().run(args));
                assertEquals(null, MockDagEngineService.did);
                try {
                    MockDagEngineService.started.get(wfCount);
                    //job was not created, then how did this extra job come after reset? fail!!
                    fail();
                }
                catch (Exception e) {
                    //job was not submitted, so its fine
                }
                return null;
            }
        });
    }

    public void testSubmitPig() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                int wfCount = MockDagEngineService.INIT_WF_COUNT;

                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);

                String[] args = new String[]{"pig", "-oozie", oozieUrl, "-file", createPigScript(appPath.toString()), "-config",
                        createPigPropertiesFile(appPath.toString())};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals("submitPig", MockDagEngineService.did);
                assertTrue(MockDagEngineService.started.get(wfCount));
                return null;
            }
        });
    }

    public void testSubmitMapReduce() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                int wfCount = MockDagEngineService.INIT_WF_COUNT;

                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);

                String[] args = new String[]{"mapreduce", "-oozie", oozieUrl, "-config", createMRProperties(appPath.toString())};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals("submitMR", MockDagEngineService.did);
                assertTrue(MockDagEngineService.started.get(wfCount));
                return null;
            }
        });
    }

    public void testSubmitDoAs() throws Exception {
        setSystemProperty("oozie.authentication.simple.anonymous.allowed", "false");
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();

                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "workflow.xml")).close();

                String[] args = new String[]{"job", "-submit", "-oozie", oozieUrl, "-config",
                    createConfigFile(appPath.toString()), "-doas", getTestUser2() };
                assertEquals(0, new OozieCLI().run(args));
                assertEquals("submit", MockDagEngineService.did);
                assertEquals(getTestUser2(), MockDagEngineService.user);
                return null;
            }
        });
    }

    public void testSubmitWithPropertyArguments() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                int wfCount = MockDagEngineService.INIT_WF_COUNT;

                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "workflow.xml")).close();

                String[] args = new String[]{"job", "-submit", "-oozie", oozieUrl, "-config",
                        createConfigFile(appPath.toString()), "-Da=X", "-Db=B"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals("submit", MockDagEngineService.did);
                assertFalse(MockDagEngineService.started.get(wfCount));

                assertEquals("X", MockDagEngineService.submittedConf.get("a"));
                assertEquals("B", MockDagEngineService.submittedConf.get("b"));
                return null;
            }
        });
    }

    public void testRun() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "workflow.xml")).close();
                String oozieUrl = getContextURL();
                int wfCount = MockDagEngineService.INIT_WF_COUNT;
                String[] args = new String[]{"job", "-run", "-oozie", oozieUrl, "-config",
                        createConfigFile(appPath.toString())};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals("submit", MockDagEngineService.did);
                assertTrue(MockDagEngineService.started.get(wfCount));

                return null;
            }
        });
    }

    /**
     * Check if "-debug" option is accepted at CLI with job run command
     * 
     * @throws Exception
     */
    public void testRunWithDebug() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "workflow.xml")).close();
                String oozieUrl = getContextURL();
                int wfCount = MockDagEngineService.INIT_WF_COUNT;
                String[] args = new String[]{"job", "-run", "-oozie", oozieUrl, "-config",
                        createConfigFile(appPath.toString()), "-debug"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals("submit", MockDagEngineService.did);
                assertTrue(MockDagEngineService.started.get(wfCount));

                return null;
            }
        });
    }

    public void testStart() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[]{"job", "-oozie", oozieUrl, "-start", MockDagEngineService.JOB_ID + "1" +
                  MockDagEngineService.JOB_ID_END};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_ACTION_START, MockDagEngineService.did);
                assertTrue(MockDagEngineService.started.get(1));

                args = new String[]{"job", "-oozie", oozieUrl, "-start",
                        MockDagEngineService.JOB_ID + (MockDagEngineService.workflows.size() + 1)};
                assertEquals(-1, new OozieCLI().run(args));
                return null;
            }
        });
    }

    public void testSuspend() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[]{"job", "-oozie", oozieUrl, "-suspend", MockDagEngineService.JOB_ID + "1" +
                    MockDagEngineService.JOB_ID_END};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_ACTION_SUSPEND, MockDagEngineService.did);

                args = new String[]{"job", "-oozie", oozieUrl, "-suspend",
                        MockDagEngineService.JOB_ID + (MockDagEngineService.workflows.size() + 1)};
                assertEquals(-1, new OozieCLI().run(args));
                return null;
            }
        });
    }

    public void testResume() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[]{"job", "-oozie", oozieUrl, "-resume", MockDagEngineService.JOB_ID + "1" +
                    MockDagEngineService.JOB_ID_END};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_ACTION_RESUME, MockDagEngineService.did);

                args = new String[]{"job", "-oozie", oozieUrl, "-resume",
                        MockDagEngineService.JOB_ID + (MockDagEngineService.workflows.size() + 1)};
                assertEquals(-1, new OozieCLI().run(args));
                return null;
            }
        });
    }

    public void testKill() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[]{"job", "-oozie", oozieUrl, "-kill", MockDagEngineService.JOB_ID + "1" +
                    MockDagEngineService.JOB_ID_END};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_ACTION_KILL, MockDagEngineService.did);

                args = new String[]{"job", "-oozie", oozieUrl, "-kill",
                        MockDagEngineService.JOB_ID + (MockDagEngineService.workflows.size() + 1)};
                assertEquals(-1, new OozieCLI().run(args));
                return null;
            }
        });
    }

    public void testReRun() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "workflow.xml")).close();
                String oozieUrl = getContextURL();
                String[] args = new String[]{"job", "-oozie", oozieUrl, "-config", createConfigFile(appPath.toString()),
                        "-rerun", MockDagEngineService.JOB_ID + "1" + MockDagEngineService.JOB_ID_END};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_ACTION_RERUN, MockDagEngineService.did);
                assertTrue(MockDagEngineService.started.get(1));
                return null;
            }
        });
    }

    /**
     * Test: oozie -rerun coord_job_id -action 1
     *
     * @throws Exception
     */
    public void testCoordReRun1() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "coordinator.xml")).close();
                String oozieUrl = getContextURL();
                String[] args = new String[] { "job", "-oozie", oozieUrl, "-rerun",
                        MockCoordinatorEngineService.JOB_ID + "1",
                        "-action", "1" };
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_COORD_ACTION_RERUN, MockCoordinatorEngineService.did);
                assertTrue(MockCoordinatorEngineService.started.get(1));
                return null;
            }
        });
    }

    /**
     * Test: oozie -rerun coord_job_id -date 2009-12-15T01:00Z::2009-12-16T01:00Z
     *
     * @throws Exception
     */
    public void testCoordReRun2() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "coordinator.xml")).close();
                String oozieUrl = getContextURL();
                String[] args = new String[] { "job", "-oozie", oozieUrl, "-rerun",
                        MockCoordinatorEngineService.JOB_ID + "1",
                        "-date", "2009-12-15T01:00Z::2009-12-16T01:00Z" };
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_COORD_ACTION_RERUN, MockCoordinatorEngineService.did);
                assertTrue(MockCoordinatorEngineService.started.get(1));
                return null;
            }
        });
    }

    /**
     * Negative Test: oozie -rerun coord_job_id -date 2009-12-15T01:00Z -action 1
     *
     * @throws Exception
     */
    public void testCoordReRunNeg1() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "coordinator.xml")).close();
                String oozieUrl = getContextURL();
                String[] args = new String[] { "job", "-oozie", oozieUrl, "-rerun",
                        MockCoordinatorEngineService.JOB_ID + "1" + MockDagEngineService.JOB_ID_END,
                        "-date", "2009-12-15T01:00Z", "-action", "1" };
                assertEquals(-1, new OozieCLI().run(args));
                assertNull(MockCoordinatorEngineService.did);
                assertFalse(MockCoordinatorEngineService.started.get(1));
                return null;
            }
        });
    }

    /**
     * Negative Test: oozie -rerun coord_job_id
     *
     * @throws Exception
     */
    public void testCoordReRunNeg2() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "coordinator.xml")).close();
                String oozieUrl = getContextURL();
                String[] args = new String[] { "job", "-oozie", oozieUrl, "-rerun",
                        MockCoordinatorEngineService.JOB_ID + "1" + MockDagEngineService.JOB_ID_END};
                assertEquals(-1, new OozieCLI().run(args));
                assertNull(MockCoordinatorEngineService.did);
                assertFalse(MockCoordinatorEngineService.started.get(1));
                return null;
            }
        });
    }

    public void testJobStatus() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                MockDagEngineService.reset();
                String[] args = new String[]{"job", "-oozie", oozieUrl, "-info", MockDagEngineService.JOB_ID + "0" +
                    MockDagEngineService.JOB_ID_END};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_SHOW_INFO, MockDagEngineService.did);

                args = new String[]{"job", "-localtime", "-oozie", oozieUrl, "-info", MockDagEngineService.JOB_ID +
                    "1" + MockDagEngineService.JOB_ID_END};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_SHOW_INFO, MockDagEngineService.did);

                args = new String[]{"job", "-timezone", "PST", "-oozie", oozieUrl, "-info", 
                    MockDagEngineService.JOB_ID + "1" + MockDagEngineService.JOB_ID_END};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_SHOW_INFO, MockDagEngineService.did);
                
                args = new String[]{"job", "-oozie", oozieUrl, "-info", MockDagEngineService.JOB_ID + "2" +
                    MockDagEngineService.JOB_ID_END};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_SHOW_INFO, MockDagEngineService.did);
                
                args = new String[]{"job", "-oozie", oozieUrl, "-info",
                        MockDagEngineService.JOB_ID + (MockDagEngineService.workflows.size() + 1)};
                assertEquals(-1, new OozieCLI().run(args));
                return null;
            }
        });
    }

    public void testJobsStatus() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[]{"jobs", "-len", "3", "-offset", "2", "-oozie", oozieUrl, "-filter",
                        "name=x"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOBS_FILTER_PARAM, MockDagEngineService.did);

                args = new String[]{"jobs", "-localtime", "-len", "3", "-offset", "2", "-oozie", oozieUrl, "-filter",
                        "name=x"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOBS_FILTER_PARAM, MockDagEngineService.did);
                
                args = new String[]{"jobs", "-timezone", "PST", "-len", "3", "-offset", "2", "-oozie", oozieUrl, 
                    "-filter", "name=x"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOBS_FILTER_PARAM, MockDagEngineService.did);
                
                args = new String[]{"jobs", "-jobtype", "coord",  "-filter", "status=FAILED", "-oozie", oozieUrl};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOBS_FILTER_PARAM, MockDagEngineService.did);                
                return null;
            }
        });
    }

    public void testHeaderPropagation() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();
                setSystemProperty(OozieCLI.WS_HEADER_PREFIX + "header", "test");

                String oozieUrl = getContextURL();
                String[] args = new String[]{"job", "-oozie", oozieUrl, "-start", MockDagEngineService.JOB_ID + "1" +
                    MockDagEngineService.JOB_ID_END};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_ACTION_START, MockDagEngineService.did);
                assertTrue(HeaderTestingVersionServlet.OOZIE_HEADERS.containsKey("header"));
                assertTrue(HeaderTestingVersionServlet.OOZIE_HEADERS.containsValue("test"));

                return null;
            }
        });
    }

    public void testOozieStatus() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();

                String oozieUrl = getContextURL();
                String[] args = new String[]{"admin", "-status", "-oozie", oozieUrl};
                assertEquals(0, new OozieCLI().run(args));

                args = new String[]{"admin", "-oozie", oozieUrl, "-systemmode", "NORMAL"};
                assertEquals(0, new OozieCLI().run(args));
                return null;
            }
        });
    }

    public void testServerBuildVersion() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();

                String oozieUrl = getContextURL();
                String[] args = new String[]{"admin", "-version", "-oozie", oozieUrl};
                assertEquals(0, new OozieCLI().run(args));

                return null;
            }
        });
    }

    public void testClientBuildVersion() throws Exception {
        String[] args = new String[]{"version"};
        assertEquals(0, new OozieCLI().run(args));
    }

    public void testJobInfo() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                MockDagEngineService.reset();
                String[] args = new String[]{"job", "-oozie", oozieUrl, "-info", MockDagEngineService.JOB_ID + "0" +
                    MockDagEngineService.JOB_ID_END};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_SHOW_INFO, MockDagEngineService.did);

                args = new String[]{"job", "-oozie", oozieUrl, "-info", MockDagEngineService.JOB_ID + "1" +
                    MockDagEngineService.JOB_ID_END, "-len", "3", "-offset", "1"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_SHOW_INFO, MockDagEngineService.did);

                args = new String[]{"job", "-oozie", oozieUrl, "-info", MockDagEngineService.JOB_ID + "2" +
                    MockDagEngineService.JOB_ID_END, "-len", "2"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_SHOW_INFO, MockDagEngineService.did);

                args = new String[]{"job", "-oozie", oozieUrl, "-info", MockDagEngineService.JOB_ID + "3" +
                    MockDagEngineService.JOB_ID_END, "-offset", "3"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_SHOW_INFO, MockDagEngineService.did);

                return null;
            }
        });
    }

    public void testJobLog() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                MockDagEngineService.reset();
                String[] args = new String[]{"job", "-oozie", oozieUrl, "-log", MockDagEngineService.JOB_ID + "0" +
                    MockDagEngineService.JOB_ID_END};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_SHOW_LOG, MockDagEngineService.did);


                return null;
            }
        });
    }

    public void testJobDefinition() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                MockDagEngineService.reset();
                String[] args = new String[]{"job", "-oozie", oozieUrl, "-definition", MockDagEngineService.JOB_ID +
                    "0" + MockDagEngineService.JOB_ID_END};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_SHOW_DEFINITION, MockDagEngineService.did);


                return null;
            }
        });
    }

    public void testPropertiesWithTrailingSpaces() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                MockDagEngineService.reset();
                String oozieUrl = getContextURL();

                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "workflow.xml")).close();

                String[] args = new String[]{"job", "-submit", "-oozie", oozieUrl, "-config",
                        createPropertiesFileWithTrailingSpaces(appPath.toString())};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals("submit", MockDagEngineService.did);
                String confStr = MockDagEngineService.workflows.get(MockDagEngineService.INIT_WF_COUNT).getConf();
                XConfiguration conf = new XConfiguration(new StringReader(confStr));
                assertNotNull(conf.get(OozieClient.RERUN_SKIP_NODES));
                assertEquals("node", conf.get(OozieClient.RERUN_SKIP_NODES));
                return null;
            }
        });
    }

    public void testAdminQueueDump() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();

                String oozieUrl = getContextURL();
                String[] args = new String[]{"admin", "-queuedump", "-oozie", oozieUrl};
                assertEquals(0, new OozieCLI().run(args));

                return null;
            }
        });
    }
    
    public void testInfo() throws Exception {
        String[] args = new String[]{"info"};
        assertEquals(0, new OozieCLI().run(args));
    }
}
