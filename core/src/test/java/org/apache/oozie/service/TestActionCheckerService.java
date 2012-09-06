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
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.workflow.WorkflowInstance;

/**
 * Test cases for the Action Checker Service.
 */
public class TestActionCheckerService extends XDataTestCase {

    private Services services;
    private String[] excludedServices = {"org.apache.oozie.service.StatusTransitService",
            "org.apache.oozie.service.PauseTransitService",
            "org.apache.oozie.service.CoordMaterializeTriggerService", "org.apache.oozie.service.RecoveryService",
            "org.apache.oozie.service.ActionCheckerService"};

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        setSystemProperty(SchemaService.WF_CONF_EXT_SCHEMAS, "wf-ext-schema.xsd");
        services = new Services();
        setClassesToBeExcluded(services.getConf(), excludedServices);
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
        conf.set(OozieClient.APP_PATH, "file://" + getTestCaseDir() + File.separator + "workflow.xml");
        conf.set(WorkflowAppService.HADOOP_USER, getTestUser());

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
        sleep(2000);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        WorkflowActionsGetForJobJPAExecutor actionsGetExecutor = new WorkflowActionsGetForJobJPAExecutor(jobId);
        List<WorkflowActionBean> actions = jpaService.execute(actionsGetExecutor);
        WorkflowActionBean action = null;
        for (WorkflowActionBean bean : actions) {
            if (bean.getType().equals("test")) {
                action = bean;
                break;
            }
        }
        assertNotNull(action);
        assertEquals(WorkflowActionBean.Status.RUNNING, action.getStatus());

        sleep(2000);
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
        conf.set(OozieClient.APP_PATH, "file://" + getTestCaseDir() + File.separator + "workflow.xml");
        conf.setStrings(WorkflowAppService.HADOOP_USER, getTestUser());
        conf.setStrings(OozieClient.GROUP_NAME, getTestGroup());

        conf.set(OozieClient.LOG_TOKEN, "t");

        conf.set("external-status", "ok");
        conf.set("signal-value", "based_on_action_status");
        conf.set("running-mode", "async");

        final String jobId = engine.submitJob(conf, true);
        sleep(200);

        waitFor(5000, new Predicate() {
            public boolean evaluate() throws Exception {
                return (engine.getJob(jobId).getStatus() == WorkflowJob.Status.RUNNING);
            }
        });

        sleep(100);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        WorkflowActionsGetForJobJPAExecutor actionsGetExecutor = new WorkflowActionsGetForJobJPAExecutor(jobId);
        List<WorkflowActionBean> actions = jpaService.execute(actionsGetExecutor);
        WorkflowActionBean action = null;
        for (WorkflowActionBean bean : actions) {
            if (bean.getType().equals("test")) {
                action = bean;
                break;
            }
        }
        assertNotNull(action);
        assertEquals(WorkflowActionBean.Status.RUNNING, action.getStatus());

        action.setLastCheckTime(new Date());
        jpaService.execute(new WorkflowActionUpdateJPAExecutor(action));

        int actionCheckDelay = 20;

        Runnable actionCheckRunnable = new ActionCheckRunnable(actionCheckDelay);
        actionCheckRunnable.run();

        sleep(3000);

        List<WorkflowActionBean> actions2 = jpaService.execute(actionsGetExecutor);
        WorkflowActionBean action2 = null;
        for (WorkflowActionBean bean : actions2) {
            if (bean.getType().equals("test")) {
                action2 = bean;
                break;
            }
        }
        assertNotNull(action);
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
        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        final CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, false, false, 0);
        final WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED,
                WorkflowInstance.Status.SUCCEEDED);
        final CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), actionNum,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", wfJob.getId(), "RUNNING", 0);

        sleep(3000);
        Runnable actionCheckRunnable = new ActionCheckRunnable(1);
        actionCheckRunnable.run();
        sleep(3000);

        waitFor(200000, new Predicate() {
            public boolean evaluate() throws Exception {
                return (ce.getCoordAction(action.getId()).getStatus() == CoordinatorAction.Status.SUCCEEDED);
            }
        });

        JPAService jpaService = Services.get().get(JPAService.class);
        CoordinatorActionBean recoveredAction = jpaService.execute(new CoordActionGetJPAExecutor(action.getId()));
        assertEquals(CoordinatorAction.Status.SUCCEEDED, recoveredAction.getStatus());
    }


}
