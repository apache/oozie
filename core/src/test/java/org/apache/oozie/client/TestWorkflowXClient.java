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

public class TestWorkflowXClient extends DagServletTestCase {

    static {
        new HeaderTestingVersionServlet();
        new V1JobsServlet();
        new V1AdminServlet();
    }

    private static final boolean IS_SECURITY_ENABLED = false;
    static final String VERSION = "/v" + OozieClient.WS_PROTOCOL_VERSION;
    static final String[] END_POINTS = { "/versions", VERSION + "/jobs", VERSION + "/admin/*" };
    static final Class[] SERVLET_CLASSES = { HeaderTestingVersionServlet.class, V1JobsServlet.class,
            V1AdminServlet.class };

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



                String pigScriptFile = getTestCaseDir() + "/test";
                BufferedWriter writer = new BufferedWriter(new FileWriter(pigScriptFile));
                writer.write("a = load 'input.txt';\n dump a;");
                writer.close();
                assertEquals(MockDagEngineService.JOB_ID + wfCount + MockDagEngineService.JOB_ID_END,
                             wc.submitPig(conf, pigScriptFile, null));

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
                conf.setProperty(OozieClient.LIBPATH, libPath.toString());
                conf.setProperty(XOozieClient.JT, "localhost:9001");
                conf.setProperty(XOozieClient.NN, "hdfs://localhost:9000");


                assertEquals(MockDagEngineService.JOB_ID + wfCount + MockDagEngineService.JOB_ID_END,
                             wc.submitMapReduce(conf));

                assertTrue(MockDagEngineService.started.get(wfCount));
                return null;
            }
        });
    }
}
