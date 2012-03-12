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

import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.local.LocalOozie;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.Writer;
import java.io.OutputStreamWriter;
import java.util.Properties;

public class TestLocalOozie extends XFsTestCase {

    protected void setUp() throws Exception {
        super.setUp();
        setSystemProperty("oozielocal.log", "/tmp/oozielocal.log");
    }

    public void testLocalOozieInitDestroy() throws Exception {
        try {
            LocalOozie.stop();
            LocalOozie.getClient();
            fail();
        }
        catch (IllegalStateException ex) {
            //nop
        }
        catch (Exception ex) {
            fail();
        }
        try {
            LocalOozie.start();
            LocalOozie.start();
            fail();
        }
        catch (IllegalStateException ex) {
            //nop
        }
        catch (Exception ex) {
            fail();
        }
        try {
            LocalOozie.stop();
            LocalOozie.start();
            OozieClient wc = LocalOozie.getClient();
            assertNotNull(wc);
            assertEquals("localoozie", wc.getOozieUrl());
        }
        finally {
            LocalOozie.stop();
        }
    }

    public void testWorkflowRun() throws Exception {
        String wfApp = "<workflow-app xmlns='uri:oozie:workflow:0.1' name='test-wf'>" +
                "    <start to='end'/>" +
                "    <end name='end'/>" +
                "</workflow-app>";

        FileSystem fs = getFileSystem();
        Path appPath = new Path(getFsTestCaseDir(), "app");
        fs.mkdirs(appPath);
        fs.mkdirs(new Path(appPath, "lib"));

        Writer writer = new OutputStreamWriter(fs.create(new Path(appPath, "workflow.xml")));
        writer.write(wfApp);
        writer.close();

        try {
            LocalOozie.start();
            final OozieClient wc = LocalOozie.getClient();
            Properties conf = wc.createConfiguration();
            conf.setProperty(OozieClient.APP_PATH, appPath.toString() + File.separator + "workflow.xml");
            conf.setProperty(OozieClient.USER_NAME, getTestUser());
            conf.setProperty(OozieClient.GROUP_NAME, getTestGroup());


            final String jobId = wc.submit(conf);
            assertNotNull(jobId);

            WorkflowJob wf = wc.getJobInfo(jobId);
            assertNotNull(wf);
            assertEquals(WorkflowJob.Status.PREP, wf.getStatus());

            wc.start(jobId);

            waitFor(1000, new Predicate() {
                public boolean evaluate() throws Exception {
                    WorkflowJob wf = wc.getJobInfo(jobId);
                    return wf.getStatus() == WorkflowJob.Status.SUCCEEDED;
                }
            });

            wf = wc.getJobInfo(jobId);
            assertNotNull(wf);
            assertEquals(WorkflowJob.Status.SUCCEEDED, wf.getStatus());
        }
        finally {
            LocalOozie.stop();
        }
    }

    //TODO test all WF states with a more complex WF

}
