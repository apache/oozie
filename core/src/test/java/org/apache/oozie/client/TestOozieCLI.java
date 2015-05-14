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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.BuildInfo;
import org.apache.oozie.cli.CLIParser;
import org.apache.oozie.cli.OozieCLI;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.InstrumentationService;
import org.apache.oozie.service.MetricsInstrumentationService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.ShareLibService;
import org.apache.oozie.servlet.DagServletTestCase;
import org.apache.oozie.servlet.MockCoordinatorEngineService;
import org.apache.oozie.servlet.MockDagEngineService;
import org.apache.oozie.servlet.V1AdminServlet;
import org.apache.oozie.servlet.V1JobServlet;
import org.apache.oozie.servlet.V1JobsServlet;
import org.apache.oozie.servlet.V2AdminServlet;
import org.apache.oozie.servlet.V2JobServlet;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;

//hardcoding options instead using constants on purpose, to detect changes to option names if any and correct docs.
public class TestOozieCLI extends DagServletTestCase {

    static {
        new HeaderTestingVersionServlet();
        new V1JobServlet();
        new V1JobsServlet();
        new V1AdminServlet();
        new V2AdminServlet();
        new V2JobServlet();
    }

    static final boolean IS_SECURITY_ENABLED = false;
    static final String VERSION = "/v" + OozieClient.WS_PROTOCOL_VERSION;
    static final String[] END_POINTS = {"/versions", VERSION + "/jobs", VERSION + "/job/*", VERSION + "/admin/*"};
    static final Class[] SERVLET_CLASSES = { HeaderTestingVersionServlet.class, V1JobsServlet.class,
            V2JobServlet.class, V2AdminServlet.class, V2JobServlet.class, V2AdminServlet.class };

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

    private String createCoodrConfigFile(String appPath) throws Exception {
        String path = getTestCaseDir() + "/" + getName() + ".xml";
        Configuration conf = new Configuration(false);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
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

    private String createMRProperties(String appPath, boolean useNewAPI) throws Exception {
        String path = getTestCaseDir() + "/" + getName() + ".properties";
        Properties props = new Properties();
        props.setProperty(OozieClient.USER_NAME, getTestUser());
        props.setProperty(OozieClient.GROUP_NAME, getTestGroup());
        props.setProperty(OozieClient.APP_PATH, appPath);
        props.setProperty(OozieClient.RERUN_SKIP_NODES, "node");
        props.setProperty(XOozieClient.NN, "localhost:9000");
        props.setProperty(XOozieClient.JT, "localhost:9001");
        if (useNewAPI) {
            props.setProperty("mapreduce.map.class", "mapper.class");
            props.setProperty("mapreduce.reduce.class", "reducer.class");
        }
        else {
            props.setProperty("mapred.mapper.class", "mapper.class");
            props.setProperty("mapred.reducer.class", "reducer.class");
        }
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
            @Override
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
            @Override
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
            @Override
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                int wfCount = MockDagEngineService.INIT_WF_COUNT;

                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);

                String[] args = new String[]{"mapreduce", "-oozie", oozieUrl, "-config",
                    createMRProperties(appPath.toString(), false)};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals("submitMR", MockDagEngineService.did);
                assertTrue(MockDagEngineService.started.get(wfCount));
                return null;
            }
        });
    }

    public void testSubmitMapReduce2() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                int wfCount = MockDagEngineService.INIT_WF_COUNT;

                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);

                String[] args = new String[]{"mapreduce", "-oozie", oozieUrl, "-config",
                        createMRProperties(appPath.toString(), true)};
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
            @Override
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
            @Override
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
            @Override
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
            @Override
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
            @Override
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
            @Override
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
            @Override
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
            @Override
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

    public void testBulkSuspendResumeKill1() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[]{"jobs", "-oozie", oozieUrl, "-suspend", "-filter",
                        "name=workflow-1"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOBS, MockDagEngineService.did);

                args = new String[]{"jobs", "-oozie", oozieUrl, "-resume", "-filter",
                        "name=workflow-1"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOBS, MockDagEngineService.did);

                args = new String[]{"jobs", "-oozie", oozieUrl, "-kill", "-filter",
                        "name=workflow-1"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOBS, MockDagEngineService.did);
                return null;
            }
        });
    }

    public void testBulkSuspendResumeKill2() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[]{"jobs", "-oozie", oozieUrl, "-suspend", "-filter",
                        "name=coordinator", "-jobtype", "coordinator"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOBS, MockCoordinatorEngineService.did);

                args = new String[]{"jobs", "-oozie", oozieUrl, "-resume", "-filter",
                        "name=coordinator", "-jobtype", "coordinator"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOBS, MockCoordinatorEngineService.did);

                args = new String[]{"jobs", "-oozie", oozieUrl, "-kill", "-filter",
                        "name=coordinator", "-jobtype", "coordinator"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOBS, MockCoordinatorEngineService.did);
                return null;
            }
        });
    }

    public void testBulkCommandWithoutFilterNegative() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[]{"jobs", "-oozie", oozieUrl, "-suspend", "-jobtype", "coordinator"};
                assertEquals(-1, new OozieCLI().run(args));
                assertNull(MockCoordinatorEngineService.did);

                args = new String[]{"jobs", "-oozie", oozieUrl, "-resume", "-jobtype", "coordinator"};
                assertEquals(-1, new OozieCLI().run(args));
                assertNull(MockCoordinatorEngineService.did);

                args = new String[]{"jobs", "-oozie", oozieUrl, "-kill", "-jobtype", "coordinator"};
                assertEquals(-1, new OozieCLI().run(args));
                assertNull(MockCoordinatorEngineService.did);
                return null;
            }
        });
    }

    /**
     * Test the working of coord action kill from Client with action numbers
     *
     * @throws Exception
     */
    public void testCoordActionKill1() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "coordinator.xml")).close();
                String oozieUrl = getContextURL();
                String[] args = new String[] { "job", "-oozie", oozieUrl, "-kill",
                        MockCoordinatorEngineService.JOB_ID + "1", "-action", "1" };
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_ACTION_KILL, MockCoordinatorEngineService.did);
                assertFalse(MockCoordinatorEngineService.started.get(1));
                return null;
            }
        });
    }

    /**
     * Test the working of coord action kill from Client with action nominal
     * date ranges
     *
     * @throws Exception
     */
    public void testCoordActionKill2() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "coordinator.xml")).close();
                String oozieUrl = getContextURL();
                String[] args = new String[] { "job", "-oozie", oozieUrl, "-kill",
                        MockCoordinatorEngineService.JOB_ID + "1", "-date", "2009-12-15T01:00Z::2009-12-16T01:00Z" };
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_ACTION_KILL, MockCoordinatorEngineService.did);
                assertFalse(MockCoordinatorEngineService.started.get(1));
                return null;
            }
        });
    }

    public void testReRun() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
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
            @Override
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
            @Override
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
     *
     * Test: oozie -rerun coord_job_id -action 0 -refresh
     *
     */
    public void testCoordReRun3() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "coordinator.xml")).close();
                String oozieUrl = getContextURL();
                String[] args = new String[] { "job", "-oozie", oozieUrl, "-rerun",
                            MockCoordinatorEngineService.JOB_ID + "0",
                            "-action", "0", "-refresh" };
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_COORD_ACTION_RERUN, MockCoordinatorEngineService.did);
                assertTrue(MockCoordinatorEngineService.started.get(0));
                return null;
            }
        });
    }
    /**
     *
     * Test: oozie -rerun coord_job_id -action 0 -nocleanup
     *
     */
    public void testCoordReRun4() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "coordinator.xml")).close();
                String oozieUrl = getContextURL();
                String[] args = new String[] { "job", "-oozie", oozieUrl, "-rerun",
                        MockCoordinatorEngineService.JOB_ID + "0",
                        "-action", "0", "-nocleanup" };
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_COORD_ACTION_RERUN, MockCoordinatorEngineService.did);
                assertTrue(MockCoordinatorEngineService.started.get(0));
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
            @Override
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
            @Override
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

    /**
     *
     * Negative Test: date or action option expected
     * @throws Exception
     *
     */
    public void testCoordReRunNeg3() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "coordinator.xml")).close();
                String oozieUrl = getContextURL();

                String[] args = new String[] {"job", "-oozie", oozieUrl, "-config", createConfigFile(appPath.toString()),
                        "-rerun", MockCoordinatorEngineService.JOB_ID + "0" };
                assertEquals(-1, new OozieCLI().run(args));
                assertNull(MockCoordinatorEngineService.did);
                assertFalse(MockCoordinatorEngineService.started.get(1));
                return null;
            }
        });
    }

    /**
     * Negative Test: Invalid options provided for rerun: eitherdate or action expected. Don't use both at the same time
     * @throws Exception
     */
    public void testCoordReRunNeg4() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "coordinator.xml")).close();
                String oozieUrl = getContextURL();

                String[] args = new String[] {"job", "-oozie", oozieUrl, "-config", createConfigFile(appPath.toString()),
                        "-rerun", MockCoordinatorEngineService.JOB_ID + "0",
                        "-date", "2009-12-15T01:00Z", "-action", "1"};

                assertEquals(-1, new OozieCLI().run(args));
                assertNull(MockCoordinatorEngineService.did);
                assertFalse(MockCoordinatorEngineService.started.get(1));
                return null;
            }
        });
    }

    public void testCoordJobIgnore() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[]{"job", "-oozie", oozieUrl, "-ignore", MockCoordinatorEngineService.JOB_ID + "1"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_ACTION_CHANGE, MockCoordinatorEngineService.did);
                assertTrue(MockCoordinatorEngineService.started.get(1));

                // negative test for "oozie job -ignore <non-existent coord>"
                MockCoordinatorEngineService.reset();
                args = new String[] {
                        "job","-oozie",oozieUrl,"ignore",
                        MockDagEngineService.JOB_ID + (MockCoordinatorEngineService.coordJobs.size() + 1)};
                assertEquals(-1, new OozieCLI().run(args));
                assertNull(MockCoordinatorEngineService.did);
                assertFalse(MockCoordinatorEngineService.started.get(1));
                return null;
            }
        });
    }

    public void testCoordActionsIgnore() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[]{"job", "-oozie", oozieUrl, "-ignore",
                        MockCoordinatorEngineService.JOB_ID + "1", "-action", "1"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_ACTION_IGNORE, MockCoordinatorEngineService.did);
                assertTrue(MockCoordinatorEngineService.started.get(1));

                // negative test for "oozie job -ignore <non-existent coord> -action 1"
                MockCoordinatorEngineService.reset();
                args = new String[]{"job", "-oozie", oozieUrl, "ignore",
                        MockDagEngineService.JOB_ID + (MockCoordinatorEngineService.coordJobs.size() + 1), "-action", "1" };
                assertEquals(-1, new OozieCLI().run(args));
                assertNull(MockCoordinatorEngineService.did);
                assertFalse(MockCoordinatorEngineService.started.get(1));

                // negative test for "oozie job -ignore <id> -action (action is empty)"
                MockCoordinatorEngineService.reset();
                args = new String[]{"job", "-oozie", oozieUrl, "-ignore",
                        MockCoordinatorEngineService.JOB_ID, "-action", ""};
                assertEquals(-1, new OozieCLI().run(args));
                assertNull(MockCoordinatorEngineService.did);
                assertFalse(MockCoordinatorEngineService.started.get(1));

                return null;
            }
        });
    }

    public void testJobStatus() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
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
            @Override
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

                args = new String[] { "jobs", "-filter",
                        "startcreatedtime=2014-04-01T00:00Z;endcreatedtime=2014-05-01T00:00Z", "-oozie", oozieUrl };
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOBS_FILTER_PARAM, MockDagEngineService.did);

                args = new String[] { "jobs", "-filter",
                        "startcreatedtime=-10d;endcreatedtime=-20m", "-oozie", oozieUrl };
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOBS_FILTER_PARAM, MockDagEngineService.did);

                return null;
            }
        });
    }

    public void testHeaderPropagation() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
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
            @Override
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();

                String oozieUrl = getContextURL();
                String[] args = new String[]{"admin", "-status", "-oozie", oozieUrl};
                String out = runOozieCLIAndGetStdout(args);
                assertEquals("System mode: NORMAL\n", out);

                args = new String[]{"admin", "-oozie", oozieUrl, "-systemmode", "NORMAL"};
                out = runOozieCLIAndGetStdout(args);
                assertEquals("System mode: NORMAL\n", out);
                return null;
            }
        });
    }

    public void testServerBuildVersion() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();

                String oozieUrl = getContextURL();
                String[] args = new String[]{"admin", "-version", "-oozie", oozieUrl};
                String out = runOozieCLIAndGetStdout(args);
                assertEquals("Oozie server build version: " + BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_VERSION)+ "\n",
                        out);

                return null;
            }
        });
    }

    public void testClientBuildVersion() throws Exception {
        String[] args = new String[]{"version"};
        String out = runOozieCLIAndGetStdout(args);
        assertEquals("Oozie client build version: " + BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_VERSION) + "\n", out);
    }

    public void testJobInfo() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
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


                args = new String[] { "job", "-oozie", oozieUrl, "-info",
                        MockCoordinatorEngineService.JOB_ID + 1 + MockCoordinatorEngineService.JOB_ID_END };
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_SHOW_INFO, MockCoordinatorEngineService.did);
                assertEquals(MockCoordinatorEngineService.offset, new Integer(1));
                assertEquals(MockCoordinatorEngineService.length, new Integer(1000));

                MockCoordinatorEngineService.reset();
                args = new String[] { "job", "-oozie", oozieUrl, "-info",
                        MockCoordinatorEngineService.JOB_ID + 1 + MockCoordinatorEngineService.JOB_ID_END,
                        "-len", "10", "-offset", "5", "-order", "desc", "-filter", "status=FAILED"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_SHOW_INFO, MockCoordinatorEngineService.did);
                assertEquals(MockCoordinatorEngineService.offset, new Integer(5));
                assertEquals(MockCoordinatorEngineService.length, new Integer(10));
                assertEquals(MockCoordinatorEngineService.order, "desc");
                assertEquals(MockCoordinatorEngineService.filter, "status=FAILED");

                MockCoordinatorEngineService.reset();
                args = new String[] { "job", "-oozie", oozieUrl, "-info",
                        MockCoordinatorEngineService.JOB_ID + 1 + MockCoordinatorEngineService.JOB_ID_END,
                        "-len", "10", "-offset", "5", "-order", "desc", "-filter", "status!=FAILED"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_SHOW_INFO, MockCoordinatorEngineService.did);
                assertEquals(MockCoordinatorEngineService.offset, new Integer(5));
                assertEquals(MockCoordinatorEngineService.length, new Integer(10));
                assertEquals(MockCoordinatorEngineService.order, "desc");
                assertEquals(MockCoordinatorEngineService.filter, "status!=FAILED");

                return null;
            }
        });
    }

    public void testJobPoll() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                MockDagEngineService.reset();
                String[] args = new String[]{"job", "-oozie", oozieUrl, "-poll", MockDagEngineService.JOB_ID + "1" +
                    MockDagEngineService.JOB_ID_END};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_SHOW_STATUS, MockDagEngineService.did);

                args = new String[]{"job", "-oozie", oozieUrl, "-poll", MockDagEngineService.JOB_ID + "1" +
                    MockDagEngineService.JOB_ID_END, "-interval", "10"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_SHOW_STATUS, MockDagEngineService.did);

                args = new String[]{"job", "-oozie", oozieUrl, "-poll", MockDagEngineService.JOB_ID + "1" +
                    MockDagEngineService.JOB_ID_END, "-timeout", "60"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_SHOW_STATUS, MockDagEngineService.did);

                args = new String[]{"job", "-oozie", oozieUrl, "-poll", MockDagEngineService.JOB_ID + "1" +
                    MockDagEngineService.JOB_ID_END, "-interval", "10", "-timeout", "60"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_SHOW_STATUS, MockDagEngineService.did);

                return null;
            }
        });
    }

    public void testJobLog() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                MockDagEngineService.reset();
                String[] args = new String[]{"job", "-oozie", oozieUrl, "-log", MockDagEngineService.JOB_ID + "0" +
                    MockDagEngineService.JOB_ID_END};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_SHOW_LOG, MockDagEngineService.did);

                args = new String[]{"job", "-oozie", oozieUrl, "-log", MockCoordinatorEngineService.JOB_ID + "0"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_SHOW_LOG, MockDagEngineService.did);

                args = new String[]{"job", "-oozie", oozieUrl, "-log", MockCoordinatorEngineService.JOB_ID + "0",
                                   "-action", "0", "-date", "2009-12-16T01:00Z"};
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_SHOW_LOG, MockDagEngineService.did);

                return null;
            }
        });
    }

    public void testJobDefinition() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
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
            @Override
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
            @Override
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();

                String oozieUrl = getContextURL();
                String[] args = new String[]{"admin", "-queuedump", "-oozie", oozieUrl};
                String out = runOozieCLIAndGetStdout(args);
                assertTrue(out.contains("Server Queue Dump"));

                return null;
            }
        });
    }

    public void testInfo() throws Exception {
        String[] args = new String[]{"info"};
        assertEquals(0, new OozieCLI().run(args));

        args = new String[]{"info", "-timezones"};
        String out = runOozieCLIAndGetStdout(args);
        assertTrue(out.contains("Available Time Zones"));
    }

    public void testValidateWorkFlowCommand() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                String validFileName = "./test-workflow-app.xml";
                String invalidFileName = "./test-invalid-workflow-app.xml";

                String validContent = "<workflow-app xmlns=\"uri:oozie:workflow:0.2\" name=\"no-op-wf\"> "+
                        " <start to=\"end\"/> <end name=\"end\"/> </workflow-app>";
                String invalidContent = "<workflow-app xmlns=\"uri:oozie:workflow:0.2\" name=\"f\"> "+
                        " <tag=\"end\"/> <tag=\"end\"/> </workflow-app>";
                File validfile = new File(validFileName);
                File invalidfile = new File(invalidFileName);
                validfile.delete();
                invalidfile.delete();


                IOUtils.copyCharStream(new StringReader(validContent), new FileWriter(validfile));
                String [] args = new String[] { "validate", validFileName };
                String out = runOozieCLIAndGetStdout(args);
                assertTrue(out.contains("Valid"));

                IOUtils.copyCharStream(new StringReader(invalidContent), new FileWriter(invalidfile));
                args = new String[] { "validate", invalidFileName };
                out = runOozieCLIAndGetStderr(args);
                assertTrue(out.contains("Invalid"));

                return null;
          }
      });
    }

   /**
     *
     * oozie -change coord_job_id -value concurrency=10
     *
     */
    public void testChangeValue() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "coordinator.xml")).close();
                String oozieUrl = getContextURL();

                String[] args = new String[] {"job", "-oozie", oozieUrl, "-change",
                    MockCoordinatorEngineService.JOB_ID + "0", "-value", "concurrency=10" };

                assertEquals(0, new OozieCLI().run(args));
                assertEquals(RestConstants.JOB_ACTION_CHANGE, MockCoordinatorEngineService.did);

                return null;
            }
        });
    }

    /**
     * Could not authenticate, Authentication failed, status: 404, message: Not Found
     */
    public void testSlaEvents() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                String oozieUrl = getContextURL();
                String[] args = new String[] {"sla", "-oozie", oozieUrl, "-len", "1" };

                String out = runOozieCLIAndGetStderr(args);
                assertTrue(out.contains("Could not authenticate, Authentication failed, status: 404, message: Not Found"));

                return null;
            }
        });
    }

    public void testshareLibUpdate() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();
                Services.get().setService(ShareLibService.class);

                String oozieUrl = getContextURL();
                String[] args = new String[] { "admin", "-sharelibupdate", "-oozie", oozieUrl };
                String out = runOozieCLIAndGetStdout(args);
                assertTrue(out.contains("ShareLib update status"));

                return null;
            }
        });
    }

    public void testshareLibUpdate_withSecurity() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, true, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();

                String oozieUrl = getContextURL();
                String[] args = new String[] { "admin", "-sharelibupdate", "-oozie", oozieUrl };
                String out = runOozieCLIAndGetStderr(args);
                assertEquals("Error: E0503 : E0503: User [test] does not have admin privileges\n", out);

                return null;
            }
        });
    }

    public void testGetShareLib() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, false, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();
                Services.get().setService(ShareLibService.class);

                String oozieUrl = getContextURL();
                String[] args = new String[] { "admin", "-shareliblist", "-oozie", oozieUrl };
                String out = runOozieCLIAndGetStdout(args);
                assertTrue(out.contains("Available ShareLib"));

                return null;
            }
        });
    }

    public void testGetShareLib_withKey() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, false, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();
                Services.get().setService(ShareLibService.class);

                String oozieUrl = getContextURL();
                String[] args = new String[] { "admin", "-shareliblist", "pig", "-oozie", oozieUrl };
                String out = runOozieCLIAndGetStdout(args);
                assertTrue(out.contains("Available ShareLib"));

                return null;
            }
        });
    }

    public void testJobDryrun() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, false, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();
                Path appPath = new Path(getFsTestCaseDir(), "app");
                getFileSystem().mkdirs(appPath);
                getFileSystem().create(new Path(appPath, "coordinator.xml")).close();
                String oozieUrl = getContextURL();
                String[] args = new String[] { "job", "-dryrun", "-config", createCoodrConfigFile(appPath.toString()),
                        "-oozie", oozieUrl, "-Doozie.proxysubmission=true" };
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(MockCoordinatorEngineService.did, RestConstants.JOB_ACTION_DRYRUN);
                assertFalse(MockCoordinatorEngineService.started.get(1));
                return null;
            }
        });
    }

    public void testUpdate() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, false, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();
                String oozieUrl = getContextURL();
                String[] args = new String[] { "job", "-update", "aaa", "-oozie", oozieUrl };
                assertEquals(-1, new OozieCLI().run(args));
                assertEquals(MockCoordinatorEngineService.did, RestConstants.JOB_COORD_UPDATE );
                assertFalse(MockCoordinatorEngineService.started.get(1));
                return null;
            }
        });

    }

    public void testUpdateWithDryrun() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, false, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();

                String oozieUrl = getContextURL();
                String[] args = new String[] { "job", "-update", "aaa", "-dryrun", "-oozie", oozieUrl };
                assertEquals(-1, new OozieCLI().run(args));
                assertEquals(MockCoordinatorEngineService.did, RestConstants.JOB_COORD_UPDATE + "&"
                        + RestConstants.JOB_ACTION_DRYRUN);
                assertFalse(MockCoordinatorEngineService.started.get(1));
                return null;
            }
        });

    }

    public void testFailNoArg() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, false, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();

                String oozieUrl = getContextURL();
                String[] args = new String[] { "job", "-oozie", oozieUrl };
                String out = runOozieCLIAndGetStderr(args);
                assertTrue(out.contains("Invalid sub-command"));
                return null;
            }
        });
    }

    public void testRetryForTimeout() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, false, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();
                String oozieUrl = "http://localhost:11/oozie";
                String[] args = new String[] { "job", "-update", "aaa", "-dryrun", "-oozie", oozieUrl, "-debug" };
                OozieCLI cli = new OozieCLI();
                CLIParser parser = cli.getCLIParser();
                try {
                    final CLIParser.Command command = parser.parse(args);
                    cli.processCommand(parser, command);
                }
                catch (Exception e) {
                    assertTrue(e.getMessage().contains(
                            "Error while connecting Oozie server. No of retries = 4. Exception = Connection refused"));
                }
                return null;
            }
        });
    }

    public void testNoRetryForError() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, false, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();
                String oozieUrl = getContextURL();
                String[] args = new String[] { "job", "-info", "aaa", "-oozie", oozieUrl, "-debug" };
                OozieCLI cli = new OozieCLI();
                CLIParser parser = cli.getCLIParser();
                try {
                    final CLIParser.Command command = parser.parse(args);
                    cli.processCommand(parser, command);
                }
                catch (Exception e) {
                    //Create connection will be successful, no retry
                    assertFalse(e.getMessage().contains("Error while connecting Oozie server"));
                    assertTrue(e.getMessage().contains("invalid job id [aaa]"));
                }
                return null;
            }
        });
    }

    public void testRetryWithRetryCount() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, false, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();
                String oozieUrl = "http://localhost:11/oozie";
                String[] args = new String[] { "job", "-update", "aaa", "-dryrun", "-oozie", oozieUrl, "-debug" };
                OozieCLI cli = new OozieCLI() {
                    protected void setRetryCount(OozieClient wc) {
                        wc.setRetryCount(2);
                    }
                    public CLIParser getCLIParser(){
                        return super.getCLIParser();
                    }

                };
                CLIParser parser = cli.getCLIParser();
                try {
                    final CLIParser.Command command = parser.parse(args);
                    cli.processCommand(parser, command);
                }
                catch (Exception e) {
                    assertTrue(e.getMessage().contains(
                            "Error while connecting Oozie server. No of retries = 2. Exception = Connection refused"));
                }
                return null;
            }
        });
   }

    public void testAdminConfiguration() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();

                String oozieUrl = getContextURL();
                String[] args = new String[]{"admin", "-configuration", "-oozie", oozieUrl};
                String out = runOozieCLIAndGetStdout(args);
                assertTrue(out.contains("oozie.base.url"));

                return null;
            }
        });
    }

    public void testAdminOsEnv() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();

                String oozieUrl = getContextURL();
                String[] args = new String[]{"admin", "-osenv", "-oozie", oozieUrl};
                String out = runOozieCLIAndGetStdout(args);
                assertTrue(out.contains("JAVA_HOME"));

                return null;
            }
        });
    }

    public void testAdminJavaSystemProperties() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();

                String oozieUrl = getContextURL();
                String[] args = new String[]{"admin", "-javasysprops", "-oozie", oozieUrl};
                String out = runOozieCLIAndGetStdout(args);
                assertTrue(out.contains("java.vendor"));

                return null;
            }
        });
    }

    public void testAdminInstrumentation() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();
                Services.get().setService(InstrumentationService.class);

                String oozieUrl = getContextURL();
                String[] args = new String[]{"admin", "-instrumentation", "-oozie", oozieUrl};
                String out = runOozieCLIAndGetStdout(args);
                assertTrue(out.contains("webservices.version-GET"));

                args = new String[]{"admin", "-metrics", "-oozie", oozieUrl};
                out = runOozieCLIAndGetStdout(args);
                assertTrue(out.contains("Metrics are unavailable"));

                return null;
            }
        });
    }

    public void testAdminMetrics() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();
                Services.get().setService(MetricsInstrumentationService.class);

                String oozieUrl = getContextURL();
                String[] args = new String[]{"admin", "-metrics", "-oozie", oozieUrl};
                String out = runOozieCLIAndGetStdout(args);
                assertTrue(out.contains("webservices.version-GET"));

                args = new String[]{"admin", "-instrumentation", "-oozie", oozieUrl};
                out = runOozieCLIAndGetStdout(args);
                assertTrue(out.contains("Instrumentation is unavailable"));

                return null;
            }
        });
    }

    public void testSlaEnable() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, false, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();
                String oozieUrl = getContextURL();
                String[] args = new String[] { "job", "-slaenable", "aaa-C", "-oozie", oozieUrl };
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(MockCoordinatorEngineService.did, RestConstants.SLA_ENABLE_ALERT);
                return null;
            }
        });

    }

    public void testSlaDisable() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, false, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();
                String oozieUrl = getContextURL();
                String[] args = new String[] { "job", "-sladisable", "aaa-C", "-oozie", oozieUrl };
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(MockCoordinatorEngineService.did, RestConstants.SLA_DISABLE_ALERT);
                return null;
            }
        });

    }

    public void testSlaChange() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, false, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                HeaderTestingVersionServlet.OOZIE_HEADERS.clear();
                String oozieUrl = getContextURL();
                String[] args = new String[] { "job", "-slachange", "aaa-C", "-oozie", oozieUrl };
                assertEquals(0, new OozieCLI().run(args));
                assertEquals(MockCoordinatorEngineService.did, RestConstants.SLA_CHANGE);
                return null;
            }
        });

    }

    private String runOozieCLIAndGetStdout(String[] args) {
        PrintStream original = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        String outStr = null;
        System.out.flush();
        try {
            System.setOut(ps);
            assertEquals(0, new OozieCLI().run(args));
            System.out.flush();
            outStr = baos.toString();
        } finally {
            System.setOut(original);
            if (outStr != null) {
                System.out.print(outStr);
            }
            System.out.flush();
        }
        return outStr;
    }

    private String runOozieCLIAndGetStderr(String[] args) {
        PrintStream original = System.err;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        String outStr = null;
        System.err.flush();
        try {
            System.setErr(ps);
            assertEquals(-1, new OozieCLI().run(args));
            System.err.flush();
            outStr = baos.toString();
        } finally {
            System.setErr(original);
            if (outStr != null) {
                System.err.print(outStr);
            }
            System.err.flush();
        }
        return outStr;
    }
}
