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
import java.io.FileWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.DagEngine;
import org.apache.oozie.ForTestingActionExecutor;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.executor.jpa.CoordActionGetForExternalIdJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionsGetForJobJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.LiteWorkflowStoreService;
import org.apache.oozie.service.SchemaService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowStoreService;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.workflow.WorkflowInstance;

/**
 * Test cases for checking correct functionality in case of errors while executing Actions.
 */
public class TestActionErrors extends XDataTestCase {

    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        setSystemProperty(SchemaService.WF_CONF_EXT_SCHEMAS, "wf-ext-schema.xsd");
        setSystemProperty(LiteWorkflowStoreService.CONF_USER_RETRY_ERROR_CODE_EXT, ForTestingActionExecutor.TEST_ERROR);
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
     * Tests for correct functionality when a {@link org.apache.oozie.action.ActionExecutorException.ErrorType#NON_TRANSIENT}
     * error is generated while attempting to start an action. </p> It first generates a {@link
     * org.apache.oozie.action.ActionExecutorException.ErrorType#NON_TRANSIENT} error and checks for the job to go into
     * {@link org.apache.oozie.client.WorkflowJob.Status#SUSPENDED} state. The state of the single action in the job is
     * checked to be at {@link org.apache.oozie.WorkflowActionBean.Status#START_MANUAL} and it's error code and error
     * message are verified. </p> The job is subsequently fixed to not generate any errors, and is resumed. The job
     * state and the action state are verified to be {@link org.apache.oozie.client.WorkflowJob.Status#SUCCEEDED} and
     * {@link org.apache.oozie.WorkflowActionBean.Status#OK} respectively. The action error code and error message are
     * checked to be emtpy.
     *
     * @throws Exception
     */
    public void testStartNonTransient() throws Exception {
        _testNonTransient("start.non-transient", WorkflowActionBean.Status.START_MANUAL, "start");
        assertTrue(true);
    }

    /**
     * Tests for correct functionality when a {@link org.apache.oozie.action.ActionExecutorException.ErrorType#NON_TRANSIENT}
     * error is generated while attempting to start an action. </p> It first generates a {@link
     * org.apache.oozie.action.ActionExecutorException.ErrorType#NON_TRANSIENT} error and checks for the job to go into
     * {@link org.apache.oozie.client.WorkflowJob.Status#SUSPENDED} state. Then checks for the coordinator action to go into
     * {@link org.apache.oozie.client.CoordinatorAction.Status#SUSPENDED} state.
     *
     * @throws Exception
     */
    public void testStartNonTransientWithCoordActionUpdate() throws Exception {
        _testNonTransientWithCoordActionUpdate("start.non-transient", WorkflowActionBean.Status.START_MANUAL, "start");
        assertTrue(true);
    }

    /**
     * Tests for correct functionality when a {@link org.apache.oozie.action.ActionExecutorException.ErrorType#NON_TRANSIENT}
     * error is generated while attempting to end an action. </p> It first generates a {@link
     * org.apache.oozie.action.ActionExecutorException.ErrorType#NON_TRANSIENT} error and checks for the job to go into
     * {@link org.apache.oozie.client.WorkflowJob.Status#SUSPENDED} state. The state of the single action in the job is
     * checked to be at {@link org.apache.oozie.WorkflowActionBean.Status#END_MANUAL} and it's error code and error
     * message are verified. </p> The job is subsequently fixed to not generate any errors, and is resumed. The job
     * state and the action state are verified to be {@link org.apache.oozie.client.WorkflowJob.Status#SUCCEEDED} and
     * {@link org.apache.oozie.WorkflowActionBean.Status#OK} respectively. The action error code and error message are
     * checked to be emtpy.
     *
     * @throws Exception
     */
    public void testEndNonTransient() throws Exception {
        _testNonTransient("end.non-transient", WorkflowActionBean.Status.END_MANUAL, "end");
        assertTrue(true);
    }

    /**
     * Tests for correct functionality when a {@link org.apache.oozie.action.ActionExecutorException.ErrorType#NON_TRANSIENT}
     * error is generated while attempting to end an action. </p> It first generates a {@link
     * org.apache.oozie.action.ActionExecutorException.ErrorType#NON_TRANSIENT} error and checks for the job to go into
     * {@link org.apache.oozie.client.WorkflowJob.Status#SUSPENDED} state. Then checks for the coordinator action to go into
     * {@link org.apache.oozie.client.CoordinatorAction.Status#SUSPENDED} state.
     *
     * @throws Exception
     */
    public void testEndNonTransientWithCoordActionUpdate() throws Exception {
        _testNonTransientWithCoordActionUpdate("end.non-transient", WorkflowActionBean.Status.END_MANUAL, "end");
        assertTrue(true);
    }

    /**
     * Tests for correct functionality when a {@link org.apache.oozie.action.ActionExecutorException.ErrorType#TRANSIENT}
     * error is generated when trying to start an action. </p> It first generates a {@link
     * org.apache.oozie.action.ActionExecutorException.ErrorType#TRANSIENT} error. 2 retries with an interval of 10
     * seconds between them are allowed. The state of the action is checked after each attempt to be at {@link
     * org.apache.oozie.WorkflowActionBean.Status#START_RETRY}. Error message and Error code for the action are
     * verified. </p> After the configured number of retry attempts, the job and actions status are checked to be {@link
     * org.apache.oozie.client.WorkflowJob.Status#SUSPENDED} and {@link org.apache.oozie.WorkflowActionBean.Status#END_MANUAL}
     * respectively. The error message and code are verified again.
     *
     * @throws Exception
     */
    public void testStartTransient() throws Exception {
        _testTransient("start.transient", WorkflowActionBean.Status.START_RETRY, WorkflowActionBean.Status.START_MANUAL, "start");
        assertTrue(true);
    }

    /**
     * Tests for correct functionality when a {@link org.apache.oozie.action.ActionExecutorException.ErrorType#TRANSIENT}
     * error is generated when trying to end an action. </p> It first generates a {@link
     * org.apache.oozie.action.ActionExecutorException.ErrorType#TRANSIENT} error. 2 retries with an interval of 10
     * seconds between them are allowed. The state of the action is checked after each attempt to be at {@link
     * org.apache.oozie.WorkflowActionBean.Status#END_RETRY}. Error message and Error code for the action are verified.
     * </p> After the configured number of retry attempts, the job and actions status are checked to be {@link
     * org.apache.oozie.client.WorkflowJob.Status#SUSPENDED} and {@link org.apache.oozie.WorkflowActionBean.Status#START_MANUAL}
     * respectively. The error message and code are verified again.
     *
     * @throws Exception
     */
    public void testEndTransient() throws Exception {
        _testTransient("end.transient", WorkflowActionBean.Status.END_RETRY, WorkflowActionBean.Status.END_MANUAL, "end");
        assertTrue(true);
    }

    /**
     * Tests for correct functionality when a {@link org.apache.oozie.action.ActionExecutorException.ErrorType#ERROR} is
     * generated when executing start. </p> Checks for the job to go into {@link org.apache.oozie.client.WorkflowJob.Status#KILLED}
     * state.
     *
     * @throws Exception
     */
    public void testStartError() throws Exception {
        _testError("start.error", "error", "based_on_action_status");
        assertTrue(true);
    }

    /**
     * Tests for correct functionality when a {@link org.apache.oozie.action.ActionExecutorException.ErrorType#ERROR} is
     * generated when executing end. </p> Checks for the job to go into {@link org.apache.oozie.client.WorkflowJob.Status#KILLED}
     * state.
     *
     * @throws Exception
     */
    public void testEndError() throws Exception {
        _testError("end.error", "ok", "OK");
        assertTrue(true);
    }
    
    /**
     * Tests for correct functionality when a {@link org.apache.oozie.action.ActionExecutorException.ErrorType#ERROR} is
     * generated when executing start. </p> Checks for user retry is applied to actions for specified retry-max=2.
     *
     * @throws Exception
     */
    public void testStartErrorWithUserRetry() throws Exception {
    	_testErrorWithUserRetry("start.error", "error", "based_on_action_status");
        assertTrue(true);
    }
    
    /**
     * Tests for correct functionality when a {@link org.apache.oozie.action.ActionExecutorException.ErrorType#ERROR} is
     * generated when executing end. </p> Checks for user retry is applied to actions for specified retry-max=2.
     *
     * @throws Exception
     */
    public void testEndErrorWithUserRetry() throws Exception {
    	_testErrorWithUserRetry("end.error", "ok", "OK");
        assertTrue(true);
    }
    
    /**
     * Tests for the job to be KILLED and status set to FAILED in case an Action Handler does not call setExecutionData
     * in it's start() implementation.
     *
     * @throws Exception
     */
    public void testExecutionDataNotSet() throws Exception {
        _testDataNotSet("avoid-set-execution-data", ActionStartXCommand.START_DATA_MISSING);
    }

    /**
     * Tests for the job to be KILLED and status set to FAILED in case an Action Handler does not call setEndData in
     * it's end() implementation.
     *
     * @throws Exception
     */
    public void testEndDataNotSet() throws Exception {
        _testDataNotSet("avoid-set-end-data", ActionEndXCommand.END_DATA_MISSING);
    }

    /**
     * Provides functionality to test kill node message
     *
     * @throws Exception
     */
    public void testKillNodeErrorMessage() throws Exception {
        Reader reader = IOUtils.getResourceAsReader("wf-test-kill-node-message.xml", -1);
        Writer writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
        IOUtils.copyCharStream(reader, writer);

        final DagEngine engine = new DagEngine("u", "a");
        Configuration conf = new XConfiguration();
        conf.set(OozieClient.APP_PATH, "file://" + getTestCaseDir() + File.separator + "workflow.xml");
        conf.set(OozieClient.USER_NAME, getTestUser());

        conf.set(OozieClient.LOG_TOKEN, "t");
        conf.set("error", "end.error");
        conf.set("external-status", "FAILED/KILLED");
        conf.set("signal-value", "fail");

        final String jobId = engine.submitJob(conf, true);

        final JPAService jpaService = Services.get().get(JPAService.class);
        final WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(jobId);

        waitFor(50000, new Predicate() {
            public boolean evaluate() throws Exception {
                WorkflowJobBean job = jpaService.execute(wfJobGetCmd);
                return (job.getWorkflowInstance().getStatus() == WorkflowInstance.Status.KILLED);
            }
        });

        WorkflowJobBean job = jpaService.execute(wfJobGetCmd);
        assertEquals(WorkflowJob.Status.KILLED, job.getStatus());

        WorkflowActionsGetForJobJPAExecutor wfActionsGetCmd = new WorkflowActionsGetForJobJPAExecutor(jobId);
        List<WorkflowActionBean> actions = jpaService.execute(wfActionsGetCmd);

        int n = actions.size();
        WorkflowActionBean action = actions.get(n - 1);
        assertEquals("TEST_ERROR", action.getErrorCode());
        assertEquals("[end]", action.getErrorMessage());
        assertEquals(WorkflowAction.Status.ERROR, action.getStatus());
    }

    /**
     * Provides functionality to test non transient failures.
     *
     * @param errorType the error type. (start.non-transient, end.non-transient)
     * @param expStatus1 expected status. (START_MANUAL, END_MANUAL)
     * @param expErrorMsg expected error message.
     * @throws Exception
     */
    private void _testNonTransient(String errorType, WorkflowActionBean.Status expStatus1, String expErrorMsg) throws Exception {
        Reader reader = IOUtils.getResourceAsReader("wf-ext-schema-valid.xml", -1);
        Writer writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
        IOUtils.copyCharStream(reader, writer);

        final DagEngine engine = new DagEngine("u", "a");
        Configuration conf = new XConfiguration();
        conf.set(OozieClient.APP_PATH, "file://" + getTestCaseDir() + File.separator + "workflow.xml");
        conf.set(OozieClient.USER_NAME, getTestUser());

        conf.set(OozieClient.LOG_TOKEN, "t");
        conf.set("signal-value", "OK");
        conf.set("external-status", "ok");
        conf.set("error", errorType);

        final String jobId = engine.submitJob(conf, true);

        waitFor(5000, new Predicate() {
            public boolean evaluate() throws Exception {
                return (engine.getJob(jobId).getStatus() == WorkflowJob.Status.SUSPENDED);
            }
        });

        final WorkflowStore store = Services.get().get(WorkflowStoreService.class).create();
        store.beginTrx();
        List<WorkflowActionBean> actions = store.getActionsForWorkflow(jobId, true);
        int n = actions.size();
        WorkflowActionBean action = actions.get(n - 1);
        assertEquals("TEST_ERROR", action.getErrorCode());
        assertEquals(expErrorMsg, action.getErrorMessage());
        assertEquals(expStatus1, action.getStatus());
        assertTrue(action.getPending() == false);

        assertTrue(engine.getJob(jobId).getStatus() == WorkflowJob.Status.SUSPENDED);

        String actionConf = action.getConf();
        String fixedActionConf = actionConf.replaceAll(errorType, "none");
        action.setConf(fixedActionConf);
        store.updateAction(action);
        store.commitTrx();
        store.closeTrx();

        engine.resume(jobId);

        waitFor(5000, new Predicate() {
            public boolean evaluate() throws Exception {
                return (engine.getJob(jobId).getStatus() == WorkflowJob.Status.SUCCEEDED);
            }
        });

        assertEquals(WorkflowJob.Status.SUCCEEDED, engine.getJob(jobId).getStatus());

        final WorkflowStore store2 = Services.get().get(WorkflowStoreService.class).create();
        store2.beginTrx();
        actions = store2.getActionsForWorkflow(jobId, false);
        action = actions.get(0);
        assertEquals(null, action.getErrorCode());
        assertEquals(null, action.getErrorMessage());
        assertEquals(WorkflowActionBean.Status.OK, action.getStatus());
        store2.commitTrx();
        store2.closeTrx();
    }

    /**
     * Provides functionality to test non transient failures and coordinator action update
     *
     * @param errorType the error type. (start.non-transient, end.non-transient)
     * @param expStatus1 expected status. (START_MANUAL, END_MANUAL)
     * @param expErrorMsg expected error message.
     * @throws Exception
     */
    private void _testNonTransientWithCoordActionUpdate(String errorType, WorkflowActionBean.Status expStatus1, String expErrorMsg) throws Exception {
        Reader reader = IOUtils.getResourceAsReader("wf-ext-schema-valid.xml", -1);
        Writer writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
        IOUtils.copyCharStream(reader, writer);

        final DagEngine engine = new DagEngine("u", "a");
        Configuration conf = new XConfiguration();
        conf.set(OozieClient.APP_PATH, "file://" + getTestCaseDir() + File.separator + "workflow.xml");
        conf.set(OozieClient.USER_NAME, getTestUser());

        conf.set(OozieClient.LOG_TOKEN, "t");
        conf.set("signal-value", "OK");
        conf.set("external-status", "ok");
        conf.set("error", errorType);

        JPAService jpaService = Services.get().get(JPAService.class);
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", "wfId", "RUNNING", 0);

        final String jobId = engine.submitJob(conf, true);

        coordAction.setExternalId(jobId);
        CoordActionUpdateJPAExecutor coordActionUpdateExecutor = new CoordActionUpdateJPAExecutor(coordAction);
        jpaService.execute(coordActionUpdateExecutor);

        waitFor(5000, new Predicate() {
            public boolean evaluate() throws Exception {
                return (engine.getJob(jobId).getStatus() == WorkflowJob.Status.SUSPENDED);
            }
        });

        assertNotNull(jpaService);
        WorkflowJobGetJPAExecutor wfGetCmd = new WorkflowJobGetJPAExecutor(jobId);
        WorkflowJobBean job = jpaService.execute(wfGetCmd);
        WorkflowActionsGetForJobJPAExecutor actionsGetExe = new WorkflowActionsGetForJobJPAExecutor(jobId);
        List<WorkflowActionBean> actionsList = jpaService.execute(actionsGetExe);

        int n = actionsList.size();
        WorkflowActionBean action = actionsList.get(n - 1);
        assertEquals("TEST_ERROR", action.getErrorCode());
        assertEquals(expErrorMsg, action.getErrorMessage());
        assertEquals(expStatus1, action.getStatus());
        assertFalse(action.getPending());

        assertEquals (WorkflowJob.Status.SUSPENDED, job.getStatus());

        coordAction = jpaService.execute(new CoordActionGetForExternalIdJPAExecutor(jobId));
        assertEquals (CoordinatorAction.Status.SUSPENDED, coordAction.getStatus());
    }

    /**
     * Provides functionality to test transient failures.
     *
     * @param errorType the error type. (start.transient, end.transient)
     * @param expStatus1 expected status after the first step (START_RETRY, END_RETRY)
     * @param expStatus2 expected status after the second step (START_MANUAL, END_MANUAL)
     * @param expErrorMsg the expected error message.
     * @throws Exception
     */
    private void _testTransient(String errorType, WorkflowActionBean.Status expStatus1,
            final WorkflowActionBean.Status expStatus2, String expErrorMsg) throws Exception {
        Reader reader = IOUtils.getResourceAsReader("wf-ext-schema-valid.xml", -1);
        Writer writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
        IOUtils.copyCharStream(reader, writer);

        final int maxRetries = 2;
        final int retryInterval = 10;

        final DagEngine engine = new DagEngine("u", "a");
        Configuration conf = new XConfiguration();
        conf.set(OozieClient.APP_PATH, "file://" + getTestCaseDir() + File.separator + "workflow.xml");
        conf.set(OozieClient.USER_NAME, getTestUser());

        conf.set(OozieClient.LOG_TOKEN, "t");
        conf.set("signal-value", "OK");
        conf.set("external-status", "ok");
        conf.set("error", errorType);
        conf.setInt(OozieClient.ACTION_MAX_RETRIES, maxRetries);
        conf.setInt(OozieClient.ACTION_RETRY_INTERVAL, retryInterval);

        final String jobId = engine.submitJob(conf, true);

        int retryCount = 1;
        WorkflowActionBean.Status expectedStatus = expStatus1;
        int expectedRetryCount = 2;

        Thread.sleep(20000);
        String aId = null;
        final WorkflowStore store = Services.get().get(WorkflowStoreService.class).create();
        store.beginTrx();
        while (retryCount <= maxRetries) {
            List<WorkflowActionBean> actions = store.getActionsForWorkflow(jobId, false);
            WorkflowActionBean action = actions.get(0);
            aId = action.getId();
            assertEquals(expectedStatus, action.getStatus());
            assertEquals(expectedRetryCount, action.getRetries());
            assertEquals("TEST_ERROR", action.getErrorCode());
            assertEquals(expErrorMsg, action.getErrorMessage());
            if (action.getRetries() == maxRetries) {
                expectedRetryCount = 0;
                expectedStatus = expStatus2;
                break;
            }
            else {
                expectedRetryCount++;
            }
            Thread.sleep(retryInterval * 1000);
            retryCount++;
        }
        store.commitTrx();
        store.closeTrx();
        Thread.sleep(5000);

        final String actionId = aId;

        waitFor(5000, new Predicate() {
            public boolean evaluate() throws Exception {
                return (engine.getWorkflowAction(actionId).getStatus() == expStatus2);
            }
        });

        final WorkflowStore store2 = Services.get().get(WorkflowStoreService.class).create();
        store2.beginTrx();
        WorkflowActionBean action = engine.getWorkflowAction(actionId);
        assertEquals("TEST_ERROR", action.getErrorCode());
        assertEquals(expErrorMsg, action.getErrorMessage());
        assertEquals(expStatus2, action.getStatus());
        assertTrue(action.getPending() == false);
        assertEquals(WorkflowJob.Status.SUSPENDED, engine.getJob(jobId).getStatus());
        store2.commitTrx();
        store2.closeTrx();
    }

    /**
     * Provides functionality to test errors
     *
     * @param errorType the error type. (start.non-transient, end.non-transient)
     * @param externalStatus the external status to set.
     * @param signalValue the signal value to set.
     * @throws Exception
     */
    private void _testError(String errorType, String externalStatus, String signalValue) throws Exception {
        Reader reader = IOUtils.getResourceAsReader("wf-ext-schema-valid.xml", -1);
        Writer writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
        IOUtils.copyCharStream(reader, writer);

        final DagEngine engine = new DagEngine("u", "a");
        Configuration conf = new XConfiguration();
        conf.set(OozieClient.APP_PATH, "file://" + getTestCaseDir() + File.separator + "workflow.xml");
        conf.set(OozieClient.USER_NAME, getTestUser());

        conf.set(OozieClient.LOG_TOKEN, "t");
        conf.set("error", errorType);
        conf.set("external-status", externalStatus);
        conf.set("signal-value", signalValue);

        final String jobId = engine.submitJob(conf, true);

        final WorkflowStore store = Services.get().get(WorkflowStoreService.class).create();
        store.beginTrx();
        waitFor(5000, new Predicate() {
            public boolean evaluate() throws Exception {
                WorkflowJobBean bean = store.getWorkflow(jobId, false);
                return (bean.getWorkflowInstance().getStatus() == WorkflowInstance.Status.KILLED);
            }
        });
        assertEquals(WorkflowJob.Status.KILLED, engine.getJob(jobId).getStatus());
        store.commitTrx();
        store.closeTrx();
    }
    
    /**
     * Provides functionality to test user retry
     *
     * @param errorType the error type. (start.non-transient, end.non-transient)
     * @param externalStatus the external status to set.
     * @param signalValue the signal value to set.
     * @throws Exception
     */
    private void _testErrorWithUserRetry(String errorType, String externalStatus, String signalValue) throws Exception {
        Reader reader = IOUtils.getResourceAsReader("wf-ext-schema-valid-user-retry.xml", -1);
        Writer writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
        IOUtils.copyCharStream(reader, writer);

        final DagEngine engine = new DagEngine("u", "a");
        Configuration conf = new XConfiguration();
        conf.set(OozieClient.APP_PATH, "file://" + getTestCaseDir() + File.separator + "workflow.xml");
        conf.set(OozieClient.USER_NAME, getTestUser());

        conf.set(OozieClient.LOG_TOKEN, "t");
        conf.set("error", errorType);
        conf.set("external-status", externalStatus);
        conf.set("signal-value", signalValue);

        final String jobId = engine.submitJob(conf, true);

        final JPAService jpaService = Services.get().get(JPAService.class);
        final WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(jobId);
    
        final WorkflowActionsGetForJobJPAExecutor actionsGetExecutor = new WorkflowActionsGetForJobJPAExecutor(jobId);
        waitFor(5000, new Predicate() {
            public boolean evaluate() throws Exception {
            	List<WorkflowActionBean> actions = jpaService.execute(actionsGetExecutor);
                WorkflowActionBean action = actions.get(0);
                return (action.getUserRetryCount() == 2);
            }
        });
        
        List<WorkflowActionBean> actions = jpaService.execute(actionsGetExecutor);
        WorkflowActionBean action = actions.get(0);
        assertEquals(2, action.getUserRetryCount());
    }

    /**
     * Provides functionality to test for set*Data calls not being made by the Action Handler.
     *
     * @param avoidParam set*Data function call to avoid.
     * @param expActionErrorCode the expected action error code.
     * @throws Exception
     */
    private void _testDataNotSet(String avoidParam, String expActionErrorCode) throws Exception {
        Reader reader = IOUtils.getResourceAsReader("wf-ext-schema-valid.xml", -1);
        Writer writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
        IOUtils.copyCharStream(reader, writer);

        final DagEngine engine = new DagEngine("u", "a");
        Configuration conf = new XConfiguration();
        conf.set(OozieClient.APP_PATH, "file://" + getTestCaseDir() + File.separator + "workflow.xml");
        conf.set(OozieClient.USER_NAME, getTestUser());

        conf.set(OozieClient.LOG_TOKEN, "t");
        conf.set("external-status", "ok");
        conf.set("signal-value", "based_on_action_status");
        conf.set(avoidParam, "true");

        final String jobId = engine.submitJob(conf, true);

        final WorkflowStore store = Services.get().get(WorkflowStoreService.class).create();
        store.beginTrx();
        Thread.sleep(2000);

        waitFor(5000, new Predicate() {
            public boolean evaluate() throws Exception {
                WorkflowJobBean bean = store.getWorkflow(jobId, false);
                return (bean.getWorkflowInstance().getStatus() == WorkflowInstance.Status.FAILED);
            }
        });
        store.commitTrx();
        store.closeTrx();

        final WorkflowStore store2 = Services.get().get(WorkflowStoreService.class).create();
        store2.beginTrx();
        assertEquals(WorkflowInstance.Status.FAILED, store2.getWorkflow(jobId, false).getWorkflowInstance().getStatus());
        assertEquals(WorkflowJob.Status.FAILED, engine.getJob(jobId).getStatus());

        List<WorkflowActionBean> actions = store2.getActionsForWorkflow(jobId, false);
        WorkflowActionBean action = actions.get(0);
        assertEquals(expActionErrorCode, action.getErrorCode());
        store2.commitTrx();
        store2.closeTrx();
    }
}
