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

import java.io.File;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Properties;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.IOUtils;

public class TestSignalXCommand extends XDataTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        LocalOozie.start();
    }

    @Override
    protected void tearDown() throws Exception {
        LocalOozie.stop();
        super.tearDown();
    }

    public void testSuspendPoints() throws Exception {
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
        conf.setProperty(OozieClient.APP_PATH, appPath.toString() + File.separator + "workflow.xml");
        conf.setProperty(OozieClient.USER_NAME, getTestUser());
        conf.setProperty("oozie.suspend.on.nodes", "action1,nonexistant_action_name,decision1, action3,join1 ,fork1,action4b");

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
    }

    public void testSuspendPointsAll() throws Exception {
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
        conf.setProperty(OozieClient.APP_PATH, appPath.toString() + File.separator + "workflow.xml");
        conf.setProperty(OozieClient.USER_NAME, getTestUser());
        conf.setProperty("oozie.suspend.on.nodes", "*");

        final String jobId = oc.submit(conf);
        assertNotNull(jobId);

        WorkflowJob wf = oc.getJobInfo(jobId);
        assertEquals(WorkflowJob.Status.PREP, wf.getStatus());

        oc.start(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUSPENDED,
                new String[]{":start:"},
                new String[]{});

        oc.resume(jobId);
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
