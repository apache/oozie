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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.hadoop.fs.Path;
import org.apache.oozie.servlet.DagServletTestCase;
import org.apache.oozie.servlet.MockDagEngineService;
import org.apache.oozie.servlet.V1JobsServlet;
import org.apache.oozie.servlet.V1AdminServlet;

import java.io.File;

public class TestWorkflowXClient extends DagServletTestCase {

    static {
        new HeaderTestingVersionServlet();
        new V1JobsServlet();
        new V1AdminServlet();
    }

    private static final boolean IS_SECURITY_ENABLED = false;
    static final String VERSION = "/v" + OozieClient.WS_PROTOCOL_VERSION;
    static final String[] END_POINTS = { "/versions", VERSION + "/jobs", VERSION + "/admin/*" };
    @SuppressWarnings("rawtypes")
    static final Class[] SERVLET_CLASSES = { HeaderTestingVersionServlet.class, V1JobsServlet.class, V1AdminServlet.class };

    protected void setUp() throws Exception {
        super.setUp();
        MockDagEngineService.reset();
    }

    public void testSubmitPig() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                int wfCount = MockDagEngineService.INIT_WF_COUNT;
                XOozieClient wc = new XOozieClient(oozieUrl);
                Properties conf = wc.createConfiguration();
                Path libPath = new Path(getFsTestCaseDir(), "lib");
                getFileSystem().mkdirs(libPath);
                conf.setProperty(OozieClient.LIBPATH, libPath.toString());
                conf.setProperty(XOozieClient.JT, "localhost:9001");
                conf.setProperty(XOozieClient.NN, "hdfs://localhost:9000");
                String[] params = new String[]{"INPUT=input.txt"};



                String pigScriptFile = getTestCaseDir() + "/test";
                BufferedWriter writer = new BufferedWriter(new FileWriter(pigScriptFile));
                writer.write("a = load '${INPUT}';\n dump a;");
                writer.close();
                assertEquals(MockDagEngineService.JOB_ID + wfCount + MockDagEngineService.JOB_ID_END,
                             wc.submitScriptLanguage(conf, pigScriptFile, null, params, "pig"));

                assertTrue(MockDagEngineService.started.get(wfCount));
                return null;
            }
        });
    }

    public void testSubmitHive() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                int wfCount = MockDagEngineService.INIT_WF_COUNT;
                XOozieClient wc = new XOozieClient(oozieUrl);
                Properties conf = wc.createConfiguration();
                Path libPath = new Path(getFsTestCaseDir(), "lib");
                getFileSystem().mkdirs(libPath);
                System.out.println(libPath.toString());
                conf.setProperty(OozieClient.LIBPATH, libPath.toString());
                conf.setProperty(XOozieClient.JT, "localhost:9001");
                conf.setProperty(XOozieClient.NN, "hdfs://localhost:9000");
                String[] params = new String[]{"NAME=test"};


                String hiveScriptFile = getTestCaseDir() + "/test";
                System.out.println(hiveScriptFile);
                BufferedWriter writer = new BufferedWriter(new FileWriter(hiveScriptFile));
                writer.write("CREATE EXTERNAL TABLE ${NAME} (a INT);");
                writer.close();
                assertEquals(MockDagEngineService.JOB_ID + wfCount + MockDagEngineService.JOB_ID_END,
                        wc.submitScriptLanguage(conf, hiveScriptFile, null, params, "hive"));

                assertTrue(MockDagEngineService.started.get(wfCount));
                return null;
            }
        });
    }

    public void testSubmitSqoop() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                int wfCount = MockDagEngineService.INIT_WF_COUNT;
                XOozieClient wc = new XOozieClient(oozieUrl);
                Properties conf = wc.createConfiguration();
                Path libPath = new Path(getFsTestCaseDir(), "lib");
                getFileSystem().mkdirs(libPath);
                System.out.println(libPath.toString());
                conf.setProperty(OozieClient.LIBPATH, libPath.toString());
                conf.setProperty(XOozieClient.JT, "localhost:9001");
                conf.setProperty(XOozieClient.NN, "hdfs://localhost:9000");

                assertEquals(MockDagEngineService.JOB_ID + wfCount + MockDagEngineService.JOB_ID_END,
                        wc.submitSqoop(conf, new String[] {"import", "--connect",
                                "jdbc:mysql://localhost:3306/oozie"},
                                null));

                assertTrue(MockDagEngineService.started.get(wfCount));
                return null;
            }
        });
    }

    public void testSubmitMR() throws Exception {
        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                int wfCount = MockDagEngineService.INIT_WF_COUNT;
                XOozieClient wc = new XOozieClient(oozieUrl);
                Properties conf = wc.createConfiguration();
                Path libPath = new Path(getFsTestCaseDir(), "lib");
                getFileSystem().mkdirs(libPath);

                String localPath = libPath.toUri().getPath();
                int startPosition = libPath.toString().indexOf(localPath);
                String nn = libPath.toString().substring(0, startPosition);

                // try to submit without JT and NN
                try {
                    wc.submitMapReduce(conf);
                    fail("submit client without JT should throw exception");
                }
                catch (RuntimeException exception) {
                    assertEquals("java.lang.RuntimeException: jobtracker is not specified in conf", exception.toString());
                }
                conf.setProperty(XOozieClient.JT, "localhost:9001");
                try {
                    wc.submitMapReduce(conf);
                    fail("submit client without NN should throw exception");
                }
                catch (RuntimeException exception) {
                    assertEquals("java.lang.RuntimeException: namenode is not specified in conf", exception.toString());
                }
                // set fs.default.name
                conf.setProperty(XOozieClient.NN, nn);
                try {
                    wc.submitMapReduce(conf);
                    fail("submit client without LIBPATH should throw exception");
                }
                catch (RuntimeException exception) {
                    assertEquals("java.lang.RuntimeException: libpath is not specified in conf", exception.toString());
                }
                // set fs.defaultFS instead
                conf.remove(XOozieClient.NN);
                conf.setProperty(XOozieClient.NN_2, nn);
                try {
                    wc.submitMapReduce(conf);
                    fail("submit client without LIBPATH should throw exception");
                }
                catch (RuntimeException exception) {
                    assertEquals("java.lang.RuntimeException: libpath is not specified in conf", exception.toString());
                }

                conf.setProperty(OozieClient.LIBPATH, localPath.substring(1));
                try {
                    wc.submitMapReduce(conf);
                    fail("lib path can not be relative");
                }
                catch (RuntimeException e) {
                    assertEquals("java.lang.RuntimeException: libpath should be absolute", e.toString());
                }

                conf.setProperty(OozieClient.LIBPATH, localPath);

                assertEquals(MockDagEngineService.JOB_ID + wfCount + MockDagEngineService.JOB_ID_END,
                        wc.submitMapReduce(conf));

                assertTrue(MockDagEngineService.started.get(wfCount));
                return null;
            }
        });
    }

    /**
     * Test some simple clint's methods
     */
    public void testSomeMethods() throws Exception {

        runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                XOozieClient wc = new XOozieClient(oozieUrl);
                Properties configuration = wc.createConfiguration();
                try {
                    wc.addFile(configuration, null);
                }
                catch (IllegalArgumentException e) {
                    assertEquals("file cannot be null or empty", e.getMessage());
                }
                wc.addFile(configuration, "file1");
                wc.addFile(configuration, "file2");
                assertEquals("file1,file2", configuration.get(XOozieClient.FILES));
                // test archive
                try {
                    wc.addArchive(configuration, null);
                }
                catch (IllegalArgumentException e) {
                    assertEquals("file cannot be null or empty", e.getMessage());
                }
                wc.addArchive(configuration, "archive1");
                wc.addArchive(configuration, "archive2");
                assertEquals("archive1,archive2", configuration.get(XOozieClient.ARCHIVES));

                return null;
            }
        });
    }
}
