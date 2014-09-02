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

package org.apache.oozie.command.wf;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Appender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.WriterAppender;
import org.apache.oozie.DagEngine;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.workflow.lite.LiteWorkflowAppParser;

public class TestSignalXCommand extends XDataTestCase {

    private Services services;
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.getConf().setBoolean(LiteWorkflowAppParser.VALIDATE_FORK_JOIN, false);
        services.init();

    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testJoinFail() throws Exception {
        Logger logger = Logger.getLogger(SignalXCommand.class);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Layout layout = new SimpleLayout();
        Appender appender = new WriterAppender(layout, out);
        logger.addAppender(appender);

        FileSystem fs = getFileSystem();
        Path appPath = new Path(getFsTestCaseDir(), "app");
        fs.mkdirs(appPath);
        Reader reader = IOUtils.getResourceAsReader("wf-fork.xml", -1);
        Writer writer = new OutputStreamWriter(fs.create(new Path(appPath, "workflow.xml")));
        IOUtils.copyCharStream(reader, writer);
        writer.close();
        reader.close();

        final DagEngine engine = new DagEngine("u");

        XConfiguration conf = new XConfiguration();
        conf.set(OozieClient.APP_PATH, appPath.toString() + File.separator + "workflow.xml");
        conf.set(OozieClient.USER_NAME, getTestUser());

        final String jobId = engine.submitJob(conf, false);


        assertNotNull(jobId);
        engine.start(jobId);

        Thread.sleep(2000);
        assertFalse(out.toString().contains("EntityExistsException"));
    }

    public void testSuspendPoints() throws Exception {
        services.destroy();
        LocalOozie.start();
        FileSystem fs = getFileSystem();
        Path appPath = new Path(getFsTestCaseDir(), "app");
        fs.mkdirs(appPath);
        Reader reader = IOUtils.getResourceAsReader("wf-suspendpoints.xml", -1);
        Writer writer = new OutputStreamWriter(fs.create(new Path(appPath, "workflow.xml")));
        IOUtils.copyCharStream(reader, writer);
        writer.close();
        reader.close();

        final OozieClient oc = LocalOozie.getClient();

        Properties conf = oc.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, new Path(appPath, "workflow.xml").toString());
        conf.setProperty(OozieClient.USER_NAME, getTestUser());
        conf.setProperty("oozie.suspend.on.nodes", "action1,nonexistant_action_name,decision1, action3,join1 ,fork1,action4b");

        final String jobId = oc.submit(conf);
        assertNotNull(jobId);

        WorkflowJob wf = oc.getJobInfo(jobId);
        assertEquals(WorkflowJob.Status.PREP, wf.getStatus());

        long beforeStart = System.currentTimeMillis();

        oc.start(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUSPENDED,
                new String[]{"action1"},
                new String[]{":start:"});

        // Check for creation time
        long afterStart = System.currentTimeMillis();
        WorkflowJob wf1 = oc.getJobInfo(jobId);
        for (WorkflowAction action : wf1.getActions()) {
            WorkflowActionBean bean = (WorkflowActionBean) action;
            assertNotNull(bean.getCreatedTime());
            assertTrue((bean.getCreatedTime().getTime() > beforeStart) && (bean.getCreatedTime().getTime() < afterStart));
        }

        oc.resume(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUSPENDED,
                new String[]{"decision1"},
                new String[]{":start:", "action1", "action2"});

        oc.resume(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUSPENDED,
                new String[]{"action3"},
                new String[]{":start:", "action1", "action2", "decision1"});

        oc.resume(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUSPENDED,
                new String[]{"fork1"},
                new String[]{":start:", "action1", "action2", "decision1", "action3"});

        oc.resume(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUSPENDED,
                new String[]{"action4a", "action4b", "action4c"},
                new String[]{":start:", "action1", "action2", "decision1", "action3", "fork1"});

        oc.resume(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUSPENDED,
                new String[]{"join1"},
                new String[]{":start:", "action1", "action2", "decision1", "action3", "fork1", "action4a", "action4b", "action4c"});

        oc.resume(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUCCEEDED,
                new String[]{},
                new String[]{":start:", "action1", "action2", "decision1", "action3", "fork1", "action4a", "action4b", "action4c",
                             "join1", "end"});
        LocalOozie.stop();
    }

    public void testSuspendPointsAll() throws Exception {
        services.destroy();
        LocalOozie.start();
        FileSystem fs = getFileSystem();
        Path appPath = new Path(getFsTestCaseDir(), "app");
        fs.mkdirs(appPath);
        Reader reader = IOUtils.getResourceAsReader("wf-suspendpoints.xml", -1);
        Writer writer = new OutputStreamWriter(fs.create(new Path(appPath, "workflow.xml")));
        IOUtils.copyCharStream(reader, writer);
        writer.close();
        reader.close();

        final OozieClient oc = LocalOozie.getClient();

        Properties conf = oc.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, new Path(appPath, "workflow.xml").toString());
        conf.setProperty(OozieClient.USER_NAME, getTestUser());
        conf.setProperty("oozie.suspend.on.nodes", "*");

        final String jobId = oc.submit(conf);
        assertNotNull(jobId);

        WorkflowJob wf = oc.getJobInfo(jobId);
        assertEquals(WorkflowJob.Status.PREP, wf.getStatus());

        oc.start(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUSPENDED,
                new String[]{"action1"},
                new String[]{":start:"});

        oc.resume(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUSPENDED,
                new String[]{"action2"},
                new String[]{":start:", "action1"});

        oc.resume(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUSPENDED,
                new String[]{"decision1"},
                new String[]{":start:", "action1", "action2"});

        oc.resume(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUSPENDED,
                new String[]{"action3"},
                new String[]{":start:", "action1", "action2", "decision1"});

        oc.resume(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUSPENDED,
                new String[]{"fork1"},
                new String[]{":start:", "action1", "action2", "decision1", "action3"});

        oc.resume(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUSPENDED,
                new String[]{"action4a", "action4b", "action4c"},
                new String[]{":start:", "action1", "action2", "decision1", "action3", "fork1"});

        oc.resume(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUSPENDED,
                new String[]{"join1"},
                new String[]{":start:", "action1", "action2", "decision1", "action3", "fork1", "action4a", "action4b", "action4c"});

        oc.resume(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUSPENDED,
                new String[]{"end"},
                new String[]{":start:", "action1", "action2", "decision1", "action3", "fork1", "action4a", "action4b", "action4c",
                             "join1"});

        oc.resume(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUCCEEDED,
                new String[]{},
                new String[]{":start:", "action1", "action2", "decision1", "action3", "fork1", "action4a", "action4b", "action4c",
                             "join1", "end"});
        LocalOozie.stop();
    }

    private void checkSuspendActions(WorkflowJob wf, final OozieClient oc, final String jobId, final WorkflowJob.Status status,
            String[] prepActions, String[] okActions) throws Exception {
        // Wait for the WF to transition to status
        waitFor(30 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                WorkflowJob wf = oc.getJobInfo(jobId);
                return wf.getStatus() == status;
            }
        });
        wf = oc.getJobInfo(jobId);
        assertEquals(status, wf.getStatus());

        // Check the actions' statuses
        int numPrep = 0;
        int numOK = 0;
        for (WorkflowAction action : wf.getActions()) {
            boolean checked = false;
            for (String name : prepActions) {
                if (!checked && name.equals(action.getName())) {
                    assertEquals("action [" + action.getName() + "] had incorrect status",
                            WorkflowAction.Status.PREP, action.getStatus());
                    numPrep++;
                    checked = true;
                }
            }
            if (!checked) {
                for (String name : okActions) {
                    if (!checked && name.equals(action.getName())) {
                        assertEquals("action [" + action.getName() + "] had incorrect status",
                                WorkflowAction.Status.OK, action.getStatus());
                    numOK++;
                    checked = true;
                    }
                }
            }
            if (!checked) {
                fail("Unexpected action [" + action.getName() + "] with status [" + action.getStatus() + "]");
            }
        }
        assertEquals(prepActions.length, numPrep);
        assertEquals(okActions.length, numOK);
    }
}
