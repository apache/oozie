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

import java.util.Date;
import java.util.Properties;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.command.coord.CoordActionStartXCommand;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.SchemaService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.XLogService;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;

public class TestReRunXCommand extends XDataTestCase {
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        setSystemProperty(XLogService.LOG4J_FILE, "oozie-log4j.properties");
        LocalOozie.start();
    }

    @Override
    protected void tearDown() throws Exception {
        LocalOozie.stop();
        super.tearDown();
    }

    public void testRerun() throws IOException, OozieClientException {
        Reader reader = IOUtils.getResourceAsReader("rerun-wf.xml", -1);
        Writer writer = new FileWriter(new File(getTestCaseDir(), "workflow.xml"));
        IOUtils.copyCharStream(reader, writer);

        Path path = getFsTestCaseDir();

        getFileSystem().create(new Path(path, "p2"));

        final OozieClient wfClient = LocalOozie.getClient();
        Properties conf = wfClient.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, getTestCaseFileUri("workflow.xml"));
        conf.setProperty(OozieClient.USER_NAME, getTestUser());


        conf.setProperty("nnbase", path.toString());
        conf.setProperty("base", path.toUri().getPath());

        final String jobId1 = wfClient.submit(conf);
        wfClient.start(jobId1);
        waitFor(15 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return wfClient.getJobInfo(jobId1).getStatus() == WorkflowJob.Status.KILLED;
            }
        });
        assertEquals(WorkflowJob.Status.KILLED, wfClient.getJobInfo(jobId1).getStatus());

        // Skip a non-executed node
        conf.setProperty(OozieClient.RERUN_SKIP_NODES, "fs1,fs2,dec3");

        boolean failed = false;
        try {
            wfClient.reRun(jobId1, conf);
        }
        catch (OozieClientException e) {
            failed = true;
            assertTrue(e.getCause().getMessage().contains(ErrorCode.E0807.toString()));
        }
        assertEquals(true, failed);

        // Skip executed nodes
        getFileSystem().delete(new Path(path, "p2"), true);
        conf.setProperty(OozieClient.RERUN_SKIP_NODES, "fs1");

        wfClient.reRun(jobId1, conf);
        waitFor(15 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return wfClient.getJobInfo(jobId1).getStatus() == WorkflowJob.Status.SUCCEEDED;
            }
        });
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfClient.getJobInfo(jobId1).getStatus());
    }

    /**
     * This tests a specific edge case where rerun can fail when there's a fork, the actions in the fork succeed, but an action
     * after the fork fails.  Previously, the rerun would step through the forked actions in the order they were listed in the
     * fork action's XML; if they happened to finish in a different order, this would cause an error during rerun.  This is fixed by
     * enforcing the same order in LiteWorkflowInstance#signal, which this test verifies.
     *
     * @throws Exception
     */
    public void testRerunFork() throws Exception {
        // We need the shell schema and action for this test
        Services.get().setService(ActionService.class);
        Services.get().getConf().set(SchemaService.WF_CONF_EXT_SCHEMAS, "shell-action-0.3.xsd");
        Services.get().setService(SchemaService.class);

        Reader reader = IOUtils.getResourceAsReader("rerun-wf-fork.xml", -1);
        Writer writer = new FileWriter(new File(getTestCaseDir(), "workflow.xml"));
        IOUtils.copyCharStream(reader, writer);

        final OozieClient wfClient = LocalOozie.getClient();
        Properties conf = wfClient.createConfiguration();
        conf.setProperty("nameNode", getNameNodeUri());
        conf.setProperty("jobTracker", getJobTrackerUri());
        conf.setProperty(OozieClient.APP_PATH, getTestCaseFileUri("workflow.xml"));
        conf.setProperty(OozieClient.USER_NAME, getTestUser());
        conf.setProperty("cmd3", "echo1");      // expected to fail

        final String jobId1 = wfClient.submit(conf);
        wfClient.start(jobId1);
        waitFor(200 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return wfClient.getJobInfo(jobId1).getStatus() == WorkflowJob.Status.KILLED;
            }
        });
        assertEquals(WorkflowJob.Status.KILLED, wfClient.getJobInfo(jobId1).getStatus());
        List<WorkflowAction> actions = wfClient.getJobInfo(jobId1).getActions();
        assertEquals(WorkflowAction.Status.OK, actions.get(1).getStatus());     // fork
        assertEquals(WorkflowAction.Status.OK, actions.get(2).getStatus());     // sh1
        assertEquals(WorkflowAction.Status.OK, actions.get(3).getStatus());     // sh2
        assertEquals(WorkflowAction.Status.OK, actions.get(4).getStatus());     // join
        assertEquals(WorkflowAction.Status.ERROR, actions.get(5).getStatus());  // sh3

        // rerun failed node, which is after the fork
        conf.setProperty(OozieClient.RERUN_FAIL_NODES, "true");
        conf.setProperty("cmd3", "echo");      // expected to succeed

        wfClient.reRun(jobId1, conf);
        waitFor(200 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return wfClient.getJobInfo(jobId1).getStatus() == WorkflowJob.Status.SUCCEEDED;
            }
        });
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfClient.getJobInfo(jobId1).getStatus());
        actions = wfClient.getJobInfo(jobId1).getActions();
        assertEquals(WorkflowAction.Status.OK, actions.get(1).getStatus());     // fork
        assertEquals(WorkflowAction.Status.OK, actions.get(2).getStatus());     // sh1
        assertEquals(WorkflowAction.Status.OK, actions.get(3).getStatus());     // sh2
        assertEquals(WorkflowAction.Status.OK, actions.get(4).getStatus());     // join
        assertEquals(WorkflowAction.Status.OK, actions.get(5).getStatus());     // sh3
    }

    /*
     * Test to ensure parameterized configuration variables get resolved in workflow rerun
     */
    public void testRerunVariableSub() throws IOException, OozieClientException {
        Reader reader = IOUtils.getResourceAsReader("rerun-varsub-wf.xml", -1);
        Writer writer = new FileWriter(new File(getTestCaseDir(), "workflow.xml"));
        IOUtils.copyCharStream(reader, writer);

        Path path = getFsTestCaseDir();

        final OozieClient wfClient = LocalOozie.getClient();
        Properties conf = wfClient.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, getTestCaseFileUri("workflow.xml"));
        conf.setProperty(OozieClient.USER_NAME, getTestUser());


        conf.setProperty("nnbase", path.toString());
        conf.setProperty("base", conf.getProperty("nnbase"));
        // setting the variables "srcDir" and "dstDir", used as a file paths in the workflow, to parameterized expressions to test resolution.
        conf.setProperty("srcDir", "${base}/p1");
        conf.setProperty("dstDir", "${base}/p2");

        final String jobId1 = wfClient.submit(conf);
        wfClient.start(jobId1);

        wfClient.kill(jobId1);

        assertEquals(WorkflowJob.Status.KILLED, wfClient.getJobInfo(jobId1).getStatus());

        // Skip executed nodes
        getFileSystem().delete(new Path(path, "p2"), true);
        conf.setProperty(OozieClient.RERUN_FAIL_NODES, "false");

        wfClient.reRun(jobId1, conf);
        waitFor(15 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return wfClient.getJobInfo(jobId1).getStatus() == WorkflowJob.Status.SUCCEEDED;
            }
        });

        // workflow success reflects that rerun configuration contained correctly resolved variable values.
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfClient.getJobInfo(jobId1).getStatus());
    }

    public void testRerunFromFailNodes() throws IOException, OozieClientException {
        Reader reader = IOUtils.getResourceAsReader("rerun-wf.xml", -1);
        Writer writer = new FileWriter(new File(getTestCaseDir(), "workflow.xml"));
        IOUtils.copyCharStream(reader, writer);

        Path path = getFsTestCaseDir();

        getFileSystem().create(new Path(path, "p2"));

        final OozieClient wfClient = LocalOozie.getClient();
        Properties conf = wfClient.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, getTestCaseFileUri("workflow.xml"));
        conf.setProperty(OozieClient.USER_NAME, getTestUser());


        conf.setProperty("nnbase", path.toString());
        conf.setProperty("base", path.toUri().getPath());

        final String jobId1 = wfClient.submit(conf);
        wfClient.start(jobId1);
        waitFor(15 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return wfClient.getJobInfo(jobId1).getStatus() == WorkflowJob.Status.KILLED;
            }
        });
        assertEquals(WorkflowJob.Status.KILLED, wfClient.getJobInfo(jobId1).getStatus());

        // Skip succeeded nodes
        getFileSystem().delete(new Path(path, "p2"), true);
        conf.setProperty(OozieClient.RERUN_FAIL_NODES, "true");

        wfClient.reRun(jobId1, conf);
        waitFor(15 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return wfClient.getJobInfo(jobId1).getStatus() == WorkflowJob.Status.SUCCEEDED;
            }
        });
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfClient.getJobInfo(jobId1).getStatus());
    }

    public void testRedeploy() throws IOException, OozieClientException, InterruptedException {
        Reader reader = IOUtils.getResourceAsReader("rerun-elerr-wf.xml", -1);
        Writer writer = new FileWriter(new File(getTestCaseDir(), "workflow.xml"));
        IOUtils.copyCharStream(reader, writer);

        final OozieClient wfClient = LocalOozie.getClient();
        Properties conf = wfClient.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, getTestCaseFileUri("workflow.xml"));
        conf.setProperty(OozieClient.USER_NAME, getTestUser());


        conf.setProperty("inPath", getFsTestCaseDir().toString());
        conf.setProperty("checkDir", getFsTestCaseDir().toString() + "/check");

        final String jobId1 = wfClient.submit(conf);
        wfClient.start(jobId1);
        waitFor(15 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return wfClient.getJobInfo(jobId1).getStatus() == WorkflowJob.Status.FAILED;
            }
        });
        assertEquals(WorkflowJob.Status.FAILED, wfClient.getJobInfo(jobId1).getStatus());

        reader = IOUtils.getResourceAsReader("rerun-el-wf.xml", -1);
        writer = new FileWriter(new File(getTestCaseDir(), "workflow.xml"));
        IOUtils.copyCharStream(reader, writer);

        sleep(5000);

        conf.setProperty(OozieClient.RERUN_SKIP_NODES, "hdfs11");
        conf.setProperty("WF_NAME", "wf_test");
        conf.setProperty("FEED_NAME", "feed_test");
        wfClient.reRun(jobId1, conf);
        waitFor(15 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return wfClient.getJobInfo(jobId1).getStatus() == WorkflowJob.Status.SUCCEEDED;
            }
        });
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfClient.getJobInfo(jobId1).getStatus());
        assertEquals("wf_test-feed_test", wfClient.getJobInfo(jobId1).getAppName());
    }

    //rerun should use existing wf conf
    public void testRerunWithExistingConf() throws IOException, OozieClientException {
        Reader reader = IOUtils.getResourceAsReader("rerun-wf.xml", -1);
        Writer writer = new FileWriter(new File(getTestCaseDir(), "workflow.xml"));
        IOUtils.copyCharStream(reader, writer);
        Path path = getFsTestCaseDir();
        getFileSystem().create(new Path(path, "p2"));
        final OozieClient wfClient = LocalOozie.getClient();
        final Properties conf = wfClient.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, getTestCaseFileUri("workflow.xml"));
        conf.setProperty(OozieClient.USER_NAME, getTestUser());
        conf.setProperty("nnbase", path.toString());
        conf.setProperty("base", path.toUri().getPath());

        Properties newConf = wfClient.createConfiguration();
        newConf.setProperty("base", path.toUri().getPath());
        final String jobId = wfClient.submit(conf);
        wfClient.start(jobId);
        waitFor(15 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return wfClient.getJobInfo(jobId).getStatus() == WorkflowJob.Status.KILLED;
            }
        });
        assertEquals(WorkflowJob.Status.KILLED, wfClient.getJobInfo(jobId).getStatus());
        try {
            wfClient.reRun(jobId, newConf);
        }
        catch (OozieClientException e) {
            assertTrue(e.getCause().getMessage().contains(ErrorCode.E0401.toString()));
        }
        newConf = wfClient.createConfiguration();
        // Skip a non-executed node
        getFileSystem().delete(new Path(path, "p2"), true);
        newConf.setProperty(OozieClient.RERUN_SKIP_NODES, "fs1");
        wfClient.reRun(jobId, newConf);
        waitFor(15 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return wfClient.getJobInfo(jobId).getStatus() == WorkflowJob.Status.SUCCEEDED;
            }
        });
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfClient.getJobInfo(jobId).getStatus());
    }

    //rerun should use existing coord conf
    public void testRerunWithExistingCoodConf() throws Exception {
        final OozieClient wfClient = LocalOozie.getClient();

        Date start = DateUtils.parseDateOozieTZ("2009-12-15T01:00Z");
        Date end = DateUtils.parseDateOozieTZ("2009-12-16T01:00Z");
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, false, false,
                1);

        CoordinatorActionBean action = addRecordToCoordActionTable(coordJob.getId(), 1,
                CoordinatorAction.Status.SUBMITTED, "coord-action-start-escape-strings.xml", 0);

        String actionId = action.getId();
        new CoordActionStartXCommand(actionId, getTestUser(), "myapp", "myjob").call();

        final JPAService jpaService = Services.get().get(JPAService.class);
        action = jpaService.execute(new CoordActionGetJPAExecutor(actionId));

        if (action.getStatus() == CoordinatorAction.Status.SUBMITTED) {
            fail("CoordActionStartCommand didn't work because the status for action id" + actionId + " is :"
                    + action.getStatus() + " expected to be NOT SUBMITTED (i.e. RUNNING)");
        }
        final String wfId = action.getExternalId();
        wfClient.kill(wfId);
        waitFor(15 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return wfClient.getJobInfo(wfId).getStatus() == WorkflowJob.Status.KILLED;
            }
        });
        Properties newConf = wfClient.createConfiguration();
        newConf.setProperty(OozieClient.RERUN_FAIL_NODES, "true");
        wfClient.reRun(wfId, newConf);
        waitFor(15 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return wfClient.getJobInfo(wfId).getStatus() == WorkflowJob.Status.SUCCEEDED;
            }
        });
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfClient.getJobInfo(wfId).getStatus());

    }

    /**
     * Rerun workflow should run by parent only if configuration has been set to
     * true for oozie.wf.child.disable.rerun , Default it is disabled.
     * @throws Exception
     */
    public void testRerunDisableForChild() throws Exception {
        final OozieClient wfClient = LocalOozie.getClient();

        Date start = DateUtils.parseDateOozieTZ("2009-12-15T01:00Z");
        Date end = DateUtils.parseDateOozieTZ("2009-12-16T01:00Z");
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, false, false,
                1);

        CoordinatorActionBean action = addRecordToCoordActionTable(coordJob.getId(), 1,
                CoordinatorAction.Status.SUBMITTED, "coord-action-start-escape-strings.xml", 0);

        String actionId = action.getId();
        new CoordActionStartXCommand(actionId, getTestUser(), "myapp", "myjob").call();

        final JPAService jpaService = Services.get().get(JPAService.class);
        action = jpaService.execute(new CoordActionGetJPAExecutor(actionId));

        if (action.getStatus() == CoordinatorAction.Status.SUBMITTED) {
            fail("CoordActionStartCommand didn't work because the status for action id" + actionId + " is :"
                    + action.getStatus() + " expected to be NOT SUBMITTED (i.e. RUNNING)");
        }
        final String wfId = action.getExternalId();
        wfClient.kill(wfId);
        waitFor(15 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return wfClient.getJobInfo(wfId).getStatus() == WorkflowJob.Status.KILLED;
            }
        });
        Properties newConf = wfClient.createConfiguration();
        newConf.setProperty(OozieClient.RERUN_FAIL_NODES, "true");
        Services.get().getConf().setBoolean(ReRunXCommand.DISABLE_CHILD_RERUN, true);
        try {
            wfClient.reRun(wfId, newConf);
        } catch (OozieClientException ex){
            assertEquals(ErrorCode.E0755.toString(), ex.getErrorCode());
        }

        Services.get().getConf().setBoolean(ReRunXCommand.DISABLE_CHILD_RERUN, false);
        wfClient.reRun(wfId, newConf);
        waitFor(15 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return wfClient.getJobInfo(wfId).getStatus() == WorkflowJob.Status.SUCCEEDED;
            }
        });
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfClient.getJobInfo(wfId).getStatus());

    }
}
