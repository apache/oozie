/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.service;

import java.io.File;
import java.io.FileWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.List;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionInsertJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionsGetForJobJPAExecutor;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorEngine;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.DagEngine;
import org.apache.oozie.ForTestingActionExecutor;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.service.ActionCheckerService.ActionCheckRunnable;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.workflow.WorkflowInstance;

/**
 * Test cases for the Action Checker Service.
 */
public class TestActionCheckerService extends XDataTestCase {

    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        setSystemProperty(SchemaService.WF_CONF_EXT_SCHEMAS, "wf-ext-schema.xsd");
        services = new Services();
        services.init();
        cleanUpDBTables();
        services.get(ActionService.class).register(ForTestingActionExecutor.class);
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    /**
     * Tests functionality of the Action Checker Service Runnable. </p> Starts
     * an action which behaves like an Async Action (Action and Job state set to
     * Running). Verifies the action status to be RUNNING. </p> Runs the
     * ActionCheck runnable, and checks for thw job to complete.
     *
     * @throws Exception
     */
    public void testActionCheckerService() throws Exception {
        Reader reader = IOUtils.getResourceAsReader("wf-ext-schema-valid.xml", -1);
        Writer writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
        IOUtils.copyCharStream(reader, writer);

        final DagEngine engine = new DagEngine(getTestUser(), "a");
        Configuration conf = new XConfiguration();
        conf.set(OozieClient.APP_PATH, getTestCaseDir() + File.separator + "workflow.xml");
        conf.set(WorkflowAppService.HADOOP_USER, getTestUser());
        conf.set(OozieClient.GROUP_NAME, getTestGroup());
        conf.set(WorkflowAppService.HADOOP_UGI, getTestUser() + "," + getTestGroup());
        injectKerberosInfo(conf);
        conf.set(OozieClient.LOG_TOKEN, "t");

        conf.set("external-status", "ok");
        conf.set("signal-value", "based_on_action_status");
        conf.set("running-mode", "async");

        final String jobId = engine.submitJob(conf, true);

        waitFor(5000, new Predicate() {
            public boolean evaluate() throws Exception {
                return (engine.getJob(jobId).getStatus() == WorkflowJob.Status.RUNNING);
            }
        });
        Thread.sleep(2000);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        WorkflowActionsGetForJobJPAExecutor actionsGetExecutor = new WorkflowActionsGetForJobJPAExecutor(jobId);
        List<WorkflowActionBean> actions = jpaService.execute(actionsGetExecutor);
        WorkflowActionBean action = actions.get(0);
        assertEquals(WorkflowActionBean.Status.RUNNING, action.getStatus());

        Thread.sleep(2000);
        Runnable actionCheckRunnable = new ActionCheckRunnable(0);
        actionCheckRunnable.run();

        waitFor(20000, new Predicate() {
            public boolean evaluate() throws Exception {
                return (engine.getJob(jobId).getStatus() == WorkflowJob.Status.SUCCEEDED);
            }
        });

        List<WorkflowActionBean> actions2 = jpaService.execute(actionsGetExecutor);
        WorkflowActionBean action2 = actions2.get(0);
        assertEquals(WorkflowActionBean.Status.OK, action2.getStatus());
    }

    /**
     * Tests the delayed check functionality of the Action Check Service
     * Runnable. </p> Starts an action which behaves like an Async Action
     * (Action and Job state set to Running). Verifies the action status to be
     * RUNNING. </p> Updates the last check time to now, and attempts to run the
     * ActionCheckRunnable with the delay configured to 20 seconds.
     *
     * @throws Exception
     */
    public void testActionCheckerServiceDelay() throws Exception {
        Reader reader = IOUtils.getResourceAsReader("wf-ext-schema-valid.xml", -1);
        Writer writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
        IOUtils.copyCharStream(reader, writer);

        final DagEngine engine = new DagEngine("u", "a");
        Configuration conf = new XConfiguration();
        conf.set(OozieClient.APP_PATH, getTestCaseDir() + File.separator + "workflow.xml");
        conf.setStrings(WorkflowAppService.HADOOP_USER, getTestUser());
        conf.setStrings(OozieClient.GROUP_NAME, getTestGroup());
        conf.setStrings(WorkflowAppService.HADOOP_UGI, getTestUser() + "," + getTestGroup());
        injectKerberosInfo(conf);
        conf.set(OozieClient.LOG_TOKEN, "t");

        conf.set("external-status", "ok");
        conf.set("signal-value", "based_on_action_status");
        conf.set("running-mode", "async");

        final String jobId = engine.submitJob(conf, true);
        Thread.sleep(200);

        waitFor(5000, new Predicate() {
            public boolean evaluate() throws Exception {
                return (engine.getJob(jobId).getStatus() == WorkflowJob.Status.RUNNING);
            }
        });

        Thread.sleep(100);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        WorkflowActionsGetForJobJPAExecutor actionsGetExecutor = new WorkflowActionsGetForJobJPAExecutor(jobId);
        List<WorkflowActionBean> actions = jpaService.execute(actionsGetExecutor);
        WorkflowActionBean action = actions.get(0);
        assertEquals(WorkflowActionBean.Status.RUNNING, action.getStatus());

        action.setLastCheckTime(new Date());
        jpaService.execute(new WorkflowActionUpdateJPAExecutor(action));

        int actionCheckDelay = 20;

        Runnable actionCheckRunnable = new ActionCheckRunnable(actionCheckDelay);
        actionCheckRunnable.run();

        Thread.sleep(3000);

        List<WorkflowActionBean> actions2 = jpaService.execute(actionsGetExecutor);
        WorkflowActionBean action2 = actions2.get(0);
        assertEquals(WorkflowActionBean.Status.RUNNING, action2.getStatus());
        assertEquals(WorkflowJob.Status.RUNNING, engine.getJob(jobId).getStatus());
    }

    /**
     * Tests functionality of the Action Checker Service Runnable for
     * coordinator actions. </p> Inserts Coord Job, Coord Action, and Workflow
     * Job, and verifies the action status updated to SUCCEEDED. </p> Runs the
     * ActionCheck runnable, and checks for the action job.
     *
     * @throws Exception
     */
    public void testActionCheckerServiceCoord() throws Exception {
        final int actionNum = 1;
        final CoordinatorEngine ce = new CoordinatorEngine(getTestUser(), "UNIT_TESTING");
        final CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.PREMATER);
        final WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED,
                WorkflowInstance.Status.SUCCEEDED);
        final CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), actionNum,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", wfJob.getId());

        Thread.sleep(3000);
        Runnable actionCheckRunnable = new ActionCheckRunnable(1);
        actionCheckRunnable.run();
        Thread.sleep(3000);

        waitFor(200000, new Predicate() {
            public boolean evaluate() throws Exception {
                return (ce.getCoordAction(action.getId()).getStatus() == CoordinatorAction.Status.SUCCEEDED);
            }
        });

        JPAService jpaService = Services.get().get(JPAService.class);
        CoordinatorActionBean recoveredAction = jpaService.execute(new CoordActionGetJPAExecutor(action.getId()));
        assertEquals(CoordinatorAction.Status.SUCCEEDED, recoveredAction.getStatus());
    }

    protected CoordinatorActionBean addRecordToCoordActionTable(String jobId, int actionNum,
            CoordinatorAction.Status status, String resourceXmlName, String wfId) throws Exception {
        CoordinatorActionBean action = createCoordAction(jobId, actionNum, status, resourceXmlName);
        action.setExternalId(wfId);
        action.setExternalStatus("RUNNING");
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordActionInsertJPAExecutor coordActionInsertExecutor = new CoordActionInsertJPAExecutor(action);
            jpaService.execute(coordActionInsertExecutor);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test coord action record to table");
            throw je;
        }
        return action;
    }

    /*   private void addRecordToCoordJobTable(String jobId, CoordinatorStore store) throws StoreException {
           CoordinatorJobBean coordJob = new CoordinatorJobBean();
           coordJob.setId(jobId);
           coordJob.setAppName("testApp");
           coordJob.setAppPath("testAppPath");
           coordJob.setStatus(CoordinatorJob.Status.PREMATER);
           coordJob.setCreatedTime(new Date());
           coordJob.setLastModifiedTime(new Date());
           coordJob.setUser(getTestUser());
           coordJob.setGroup(getTestGroup());
           coordJob.setAuthToken("notoken");

           String confStr = "<configuration></configuration>";
           coordJob.setConf(confStr);
           coordJob.setLastActionNumber(0);
           coordJob.setFrequency(1);
           coordJob.setExecution(Execution.FIFO);
           coordJob.setConcurrency(1);
           try {
               coordJob.setEndTime(DateUtils.parseDateUTC("2009-02-03T23:59Z"));
               coordJob.setStartTime(DateUtils.parseDateUTC("2009-02-01T23:59Z"));
           }
           catch (Exception e) {
               e.printStackTrace();
               fail("Could not set Date/time");
           }

           try {
               store.beginTrx();
               store.insertCoordinatorJob(coordJob);
               store.commitTrx();
           }
           catch (StoreException se) {
               se.printStackTrace();
               store.rollbackTrx();
               fail("Unable to insert the test job record to table");
               throw se;
           }
       }

       private void addRecordToCoordActionTable(String jobId, int actionNum, String actionId, String wfId,
                                                CoordinatorStore store) throws StoreException, IOException {
           CoordinatorActionBean action = new CoordinatorActionBean();
           action.setJobId(jobId);
           action.setId(actionId);
           action.setExternalId(wfId);
           action.setActionNumber(actionNum);
           action.setNominalTime(new Date());
           action.setLastModifiedTime(new Date());
           action.setStatus(CoordinatorAction.Status.RUNNING);
           store.beginTrx();
           store.insertCoordinatorAction(action);
           store.commitTrx();
       }

       private void addRecordToWfJobTable(String wfId, WorkflowStore store) throws Exception {
           store.beginTrx();
           WorkflowApp app = new LiteWorkflowApp("testApp", "<workflow-app/>", new StartNodeDef("end"))
                   .addNode(new EndNodeDef("end"));
           Configuration conf = new Configuration();
           conf.set(OozieClient.APP_PATH, "testPath");
           conf.set(OozieClient.LOG_TOKEN, "testToken");
           conf.set(OozieClient.USER_NAME, getTestUser());
           conf.set(OozieClient.GROUP_NAME, getTestGroup());
           injectKerberosInfo(conf);
           WorkflowJobBean wfBean = createWorkflow(app, conf, "auth");
           wfBean.setId(wfId);
           wfBean.setStatus(WorkflowJob.Status.SUCCEEDED);

           store.insertWorkflow(wfBean);
           store.commitTrx();
       }

       private WorkflowJobBean createWorkflow(WorkflowApp app, Configuration conf, String authToken) throws Exception {
           WorkflowAppService wps = Services.get().get(WorkflowAppService.class);
           Configuration protoActionConf = wps.createProtoActionConf(conf, authToken, true);
           WorkflowLib workflowLib = Services.get().get(WorkflowStoreService.class).getWorkflowLibWithNoDB();
           WorkflowInstance wfInstance;
           wfInstance = workflowLib.createInstance(app, conf);
           WorkflowJobBean workflow = new WorkflowJobBean();
           workflow.setId(wfInstance.getId());
           workflow.setAppName(app.getName());
           workflow.setAppPath(conf.get(OozieClient.APP_PATH));
           workflow.setConf(XmlUtils.prettyPrint(conf).toString());
           workflow.setProtoActionConf(XmlUtils.prettyPrint(protoActionConf).toString());
           workflow.setCreatedTime(new Date());
           workflow.setLogToken(conf.get(OozieClient.LOG_TOKEN, ""));
           workflow.setStatus(WorkflowJob.Status.PREP);
           workflow.setRun(0);
           workflow.setUser(conf.get(OozieClient.USER_NAME));
           workflow.setGroup(conf.get(OozieClient.GROUP_NAME));
           workflow.setAuthToken(authToken);
           workflow.setWorkflowInstance(wfInstance);
           return workflow;
       }*/

}
