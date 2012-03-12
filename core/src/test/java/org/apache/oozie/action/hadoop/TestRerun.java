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
package org.apache.oozie.action.hadoop;

import org.apache.oozie.service.XLogService;

import java.util.List;
import java.util.HashMap;

import org.apache.oozie.client.WorkflowAction;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Properties;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.test.XFsTestCase;

public class TestRerun extends XFsTestCase {

    protected void setUp() throws Exception {
        super.setUp();
        setSystemProperty(XLogService.LOG4J_FILE, "oozie-log4j.properties");
        setSystemProperty("oozie.service.ActionCheckerService.action.check.delay", "1");
        setSystemProperty("oozie.service.ActionCheckerService.action.check.interval", "10");
        LocalOozie.start();
        cleanUpDBTables();
    }

    protected void tearDown() throws Exception {
        LocalOozie.stop();
        super.tearDown();
    }

    public void testRerun() throws Exception {
        Path appPath = new Path(getFsTestCaseDir(), "app");
        FileSystem fs = getFileSystem();
        fs.mkdirs(new Path(appPath, "lib"));
        File jarFile = IOUtils.createJar(new File(getTestCaseDir()), "test.jar", MapperReducerForTest.class);
        InputStream is = new FileInputStream(jarFile);
        OutputStream os = fs.create(new Path(appPath, "lib/test.jar"));
        IOUtils.copyStream(is, os);
        Path input = new Path(appPath, "input");
        Path output = new Path(appPath, "output");
        fs.mkdirs(input);
        Writer writer = new OutputStreamWriter(fs.create(new Path(input, "test.txt")));
        writer.write("hello");
        writer.close();

        final String APP1 = "<workflow-app xmlns='uri:oozie:workflow:0.1' name='app'>" +
                "<start to='end'/>" +
                "<end name='end'/>" +
                "</workflow-app>";
        String subWorkflowAppPath = new Path(appPath, "subwf").toString();
        fs.mkdirs(new Path(appPath, "subwf"));
        Writer writer2 = new OutputStreamWriter(fs.create(new Path(subWorkflowAppPath, "workflow.xml")));
        writer2.write(APP1);
        writer2.close();

        Reader reader = IOUtils.getResourceAsReader("recovery-wf.xml", -1);
        Writer writer1 = new OutputStreamWriter(fs.create(new Path(appPath + "/workflow.xml")));
        IOUtils.copyCharStream(reader, writer1);

        final OozieClient wfClient = LocalOozie.getClient();
        Properties conf = wfClient.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, appPath.toString() + File.separator + "workflow.xml");
        conf.setProperty("jobTracker", getJobTrackerUri());
        conf.setProperty("nameNode", getNameNodeUri());
        conf.setProperty("mrclass", MapperReducerForTest.class.getName());
        conf.setProperty("input", input.toString());
        conf.setProperty("output", output.toString());
        conf.setProperty("delPath", output.toString());
        conf.setProperty("subWfApp", appPath.toString() + "/subwf/workflow.xml");
        //conf.setProperty("user.name", getTestUser());


        //first run
        final String jobId1 = wfClient.submit(conf);
        wfClient.start(jobId1);
        waitFor(120 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return wfClient.getJobInfo(jobId1).getStatus() == WorkflowJob.Status.SUCCEEDED;
            }
        });
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfClient.getJobInfo(jobId1).getStatus());

        //getting external IDs of all actions on first run
        Map<String, String> extId0 = loadExtIds(wfClient.getJobInfo(jobId1).getActions());

        //doing a rerun skipping no nodes
        conf.setProperty(OozieClient.RERUN_SKIP_NODES, "");
        wfClient.reRun(jobId1, conf);
        waitFor(120 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return wfClient.getJobInfo(jobId1).getStatus() == WorkflowJob.Status.SUCCEEDED;
            }
        });
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfClient.getJobInfo(jobId1).getStatus());

        //getting external IDs of all actions on rerun
        Map<String, String> extId1 = loadExtIds(wfClient.getJobInfo(jobId1).getActions());

        //comparing external IDs of first run and rerun are different.
        assertNotSame(extId0, extId1);
    }

    private Map<String, String> loadExtIds(List<WorkflowAction> actions) {
        Map<String, String> extIds = new HashMap<String, String>();
        for (WorkflowAction action : actions) {
            extIds.put(action.getName(), action.getExternalId());
        }
        return extIds;
    }

}
