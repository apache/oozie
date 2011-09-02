/**
 * 
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.command.wf;

import java.io.FileWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.DagEngine;
import org.apache.oozie.ForTestingActionExecutor;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.command.wf.ActionEndCommand;
import org.apache.oozie.command.wf.ActionStartCommand;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.service.WorkflowStoreService;
import org.apache.oozie.service.WorkflowSchemaService;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;

/**
 * Test cases for checking correct functionality in case of errors while
 * executing Actions.
 */
public class TestActionErrors extends XTestCase {

    private Services services;

    @Override
    protected void setUp()throws Exception {
        super.setUp();
        setSystemProperty(WorkflowSchemaService.CONF_EXT_SCHEMAS, "wf-ext-schema.xsd");
        services = new Services();
        cleanUpDB(services.getConf());
        services.init();
        services.get(ActionService.class).register(ForTestingActionExecutor.class);
    }

    protected void tearDown()throws Exception {
        services.destroy();
        super.tearDown();
    }

    /**
     * Tests for correct functionality when a
     * {@link org.apache.oozie.action.ActionExecutorException.ErrorType#NON_TRANSIENT} error is
     * generated while attempting to start an action.
     * </p>
     * It first generates a
     * {@link org.apache.oozie.action.ActionExecutorException.ErrorType#NON_TRANSIENT} error and checks
     * for the job to go into {@link org.apache.oozie.client.WorkflowJob.Status#SUSPENDED} state. The state
     * of the single action in the job is checked to be at
     * {@link org.apache.oozie.WorkflowActionBean.Status#START_MANUAL} and it's error code and error
     * message are verified.
     * </p>
     * The job is subsequently fixed to not generate any errors, and is resumed.
     * The job state and the action state are verified to be
     * {@link org.apache.oozie.client.WorkflowJob.Status#SUCCEEDED} and {@link org.apache.oozie.WorkflowActionBean.Status#OK}
     * respectively. The action error code and error message are checked to be
     * emtpy.
     * 
     * @throws Exception
     */
    public void testStartNonTransient() throws Exception {
        _testNonTransient("start.non-transient", WorkflowActionBean.Status.START_MANUAL, "start");
        assertTrue(true);
    }

    /**
     * Tests for correct functionality when a
     * {@link org.apache.oozie.action.ActionExecutorException.ErrorType#NON_TRANSIENT} error is
     * generated while attempting to end an action.
     * </p>
     * It first generates a
     * {@link org.apache.oozie.action.ActionExecutorException.ErrorType#NON_TRANSIENT} error and checks
     * for the job to go into {@link org.apache.oozie.client.WorkflowJob.Status#SUSPENDED} state. The state
     * of the single action in the job is checked to be at
     * {@link org.apache.oozie.WorkflowActionBean.Status#END_MANUAL} and it's error code and error
     * message are verified.
     * </p>
     * The job is subsequently fixed to not generate any errors, and is resumed.
     * The job state and the action state are verified to be
     * {@link org.apache.oozie.client.WorkflowJob.Status#SUCCEEDED} and {@link org.apache.oozie.WorkflowActionBean.Status#OK}
     * respectively. The action error code and error message are checked to be
     * emtpy.
     * 
     * @throws Exception
     */
    public void testEndNonTransient() throws Exception {
        _testNonTransient("end.non-transient", WorkflowActionBean.Status.END_MANUAL, "end");
        assertTrue(true);
    }

    /**
     * Tests for correct functionality when a
     * {@link org.apache.oozie.action.ActionExecutorException.ErrorType#TRANSIENT} error is generated
     * when trying to start an action.
     * </p>
     * It first generates a {@link org.apache.oozie.action.ActionExecutorException.ErrorType#TRANSIENT}
     * error. 2 retries with an interval of 10 seconds between them are allowed.
     * The state of the action is checked after each attempt to be at
     * {@link org.apache.oozie.WorkflowActionBean.Status#START_RETRY}. Error message and Error code for
     * the action are verified.
     * </p>
     * After the configured number of retry attempts, the job and actions status
     * are checked to be {@link org.apache.oozie.client.WorkflowJob.Status#SUSPENDED} and
     * {@link org.apache.oozie.WorkflowActionBean.Status#END_MANUAL} respectively. The error message and
     * code are verified again.
     * 
     * @throws Exception
     */
    public void testStartTransient() throws Exception {
        _testTransient("start.transient", WorkflowActionBean.Status.START_RETRY, WorkflowActionBean.Status.START_MANUAL, "start");
        assertTrue(true);
    }

    /**
     * Tests for correct functionality when a
     * {@link org.apache.oozie.action.ActionExecutorException.ErrorType#TRANSIENT} error is generated
     * when trying to end an action.
     * </p>
     * It first generates a {@link org.apache.oozie.action.ActionExecutorException.ErrorType#TRANSIENT}
     * error. 2 retries with an interval of 10 seconds between them are allowed.
     * The state of the action is checked after each attempt to be at
     * {@link org.apache.oozie.WorkflowActionBean.Status#END_RETRY}. Error message and Error code for
     * the action are verified.
     * </p>
     * After the configured number of retry attempts, the job and actions status
     * are checked to be {@link org.apache.oozie.client.WorkflowJob.Status#SUSPENDED} and
     * {@link org.apache.oozie.WorkflowActionBean.Status#START_MANUAL} respectively. The error message
     * and code are verified again.
     * 
     * @throws Exception
     */
    public void testEndTransient() throws Exception {
        _testTransient("end.transient", WorkflowActionBean.Status.END_RETRY, WorkflowActionBean.Status.END_MANUAL, "end");
        assertTrue(true);
    }

    /**
     * Tests for correct functionality when a
     * {@link org.apache.oozie.action.ActionExecutorException.ErrorType#ERROR} is generated when
     * executing start.
     * </p>
     * Checks for the job to go into {@link org.apache.oozie.client.WorkflowJob.Status#KILLED} state.
     *
     * @throws Exception
     */
    public void testStartError() throws Exception {
        _testError("start.error", "error", "based_on_action_status");
        assertTrue(true);
    }

    /**
     * Tests for correct functionality when a
     * {@link org.apache.oozie.action.ActionExecutorException.ErrorType#ERROR} is generated when
     * executing end.
     * </p>
     * Checks for the job to go into {@link org.apache.oozie.client.WorkflowJob.Status#KILLED} state.
     *
     * @throws Exception
     */
    public void testEndError() throws Exception {
        _testError("end.error", "ok", "OK");
        assertTrue(true);
    }

    /**
     * Tests for the job to be KILLED and status set to FAILED in case an Action
     * Handler does not call setExecutionData in it's start() implementation.
     *
     * @throws Exception
     */
    public void testExecutionDataNotSet() throws Exception {
        _testDataNotSet("avoid-set-execution-data", ActionStartCommand.START_DATA_MISSING);
    }

    /**
     * Tests for the job to be KILLED and status set to FAILED in case an Action
     * Handler does not call setEndData in it's end() implementation.
     *
     * @throws Exception
     */
    public void testEndDataNotSet() throws Exception {
        _testDataNotSet("avoid-set-end-data", ActionEndCommand.END_DATA_MISSING);
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
        conf.set(OozieClient.APP_PATH, getTestCaseDir());
        conf.set(OozieClient.USER_NAME, "u");
        conf.set(OozieClient.GROUP_NAME, "g");
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
        List<WorkflowActionBean> actions = store.getActionsForWorkflow(jobId, true);
        WorkflowActionBean action = actions.get(0);
        assertEquals("TEST_ERROR", action.getErrorCode());
        assertEquals(expErrorMsg, action.getErrorMessage());
        assertEquals(expStatus1, action.getStatus());

        assertTrue(engine.getJob(jobId).getStatus() == WorkflowJob.Status.SUSPENDED);

        String actionConf = action.getConf();
        String fixedActionConf = actionConf.replaceAll(errorType, "none");
        action.setConf(fixedActionConf);
        store.updateAction(action);
        store.commit();
        store.close();

        engine.resume(jobId);

        waitFor(5000, new Predicate() {
            public boolean evaluate() throws Exception {
                return (engine.getJob(jobId).getStatus() == WorkflowJob.Status.SUCCEEDED);
            }
        });

        assertEquals(WorkflowJob.Status.SUCCEEDED, engine.getJob(jobId).getStatus());

        final WorkflowStore store2 = Services.get().get(WorkflowStoreService.class).create();
        actions = store2.getActionsForWorkflow(jobId, false);
        action = actions.get(0);
        assertEquals(null, action.getErrorCode());
        assertEquals(null, action.getErrorMessage());
        assertEquals(WorkflowActionBean.Status.OK, action.getStatus());
        store2.close();
    }

    /**
     * Provides functionality to test non transient errors.
     *
     * @param errorType the error type. (start.non-transient, end.non-transient)
     * @param expStatus1 expected status. (START_MANUAL, END_MANUAL)
     * @param expErrorMsg the expected error message.
     * @throws Exception
     */

    /**
     * Provides functionality to test transient failures.
     *
     * @param errorType the error type. (start.transient, end.transient)
     * @param expStatus1 expected status after the first step (START_RETRY,
     *        END_RETRY)
     * @param expStatus2 expected status after the second step (START_MANUAL,
     *        END_MANUAL)
     * @param expErrorMsg the expected error message.
     * @throws Exception
     */
    private void _testTransient(String errorType, WorkflowActionBean.Status expStatus1, WorkflowActionBean.Status expStatus2,
            String expErrorMsg) throws Exception {
        Reader reader = IOUtils.getResourceAsReader("wf-ext-schema-valid.xml", -1);
        Writer writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
        IOUtils.copyCharStream(reader, writer);

        final int maxRetries = 2;
        final int retryInterval = 10;

        final DagEngine engine = new DagEngine("u", "a");
        Configuration conf = new XConfiguration();
        conf.set(OozieClient.APP_PATH, getTestCaseDir());
        conf.set(OozieClient.USER_NAME, "u");
        conf.set(OozieClient.GROUP_NAME, "g");
        conf.set(OozieClient.LOG_TOKEN, "t");
        conf.set("signal-value", "OK");
        conf.set("external-status", "ok");
        conf.set("error", errorType);
        conf.setInt(OozieClient.ACTION_MAX_RETRIES, maxRetries);
        conf.setInt(OozieClient.ACTION_RETRY_INTERVAL, retryInterval);

        final String jobId = engine.submitJob(conf, true);

        int retryCount = 1;
        WorkflowActionBean.Status expectedStatus = expStatus1;
        int expectedRetryCount = 1;

        final WorkflowStore store = Services.get().get(WorkflowStoreService.class).create();

        Thread.sleep(2000);
        while (retryCount <= maxRetries) {
            List<WorkflowActionBean> actions = store.getActionsForWorkflow(jobId, false);
            WorkflowActionBean action = actions.get(0);
            assertEquals(expectedStatus, action.getStatus());
            assertEquals(expectedRetryCount, action.getRetries());
            assertEquals("TEST_ERROR", action.getErrorCode());
            assertEquals(expErrorMsg, action.getErrorMessage());
            if (action.getRetries() == maxRetries) {
                expectedRetryCount = 0;
                expectedStatus = expStatus2;
            }
            else {
                expectedRetryCount++;
            }
            Thread.sleep(retryInterval * 1000);
            retryCount++;
        }

        List<WorkflowActionBean> actions = store.getActionsForWorkflow(jobId, false);
        WorkflowActionBean action = actions.get(0);
        assertEquals(expStatus2, action.getStatus());
        assertEquals("TEST_ERROR", action.getErrorCode());
        assertEquals(expErrorMsg, action.getErrorMessage());

        assertEquals(WorkflowJob.Status.SUSPENDED, engine.getJob(jobId).getStatus());
        store.close();
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
        conf.set(OozieClient.APP_PATH, getTestCaseDir());
        conf.set(OozieClient.USER_NAME, "u");
        conf.set(OozieClient.GROUP_NAME, "g");
        conf.set(OozieClient.LOG_TOKEN, "t");
        conf.set("error", errorType);
        conf.set("external-status", externalStatus);
        conf.set("signal-value", signalValue);

        final String jobId = engine.submitJob(conf, true);

        final WorkflowStore store = Services.get().get(WorkflowStoreService.class).create();

        waitFor(5000, new Predicate() {
            public boolean evaluate() throws Exception {
                WorkflowJobBean bean = store.getWorkflow(jobId, false);
                return (bean.getWorkflowInstance().getStatus() == WorkflowInstance.Status.KILLED);
            }
        });
        assertEquals(WorkflowJob.Status.KILLED, engine.getJob(jobId).getStatus());
        store.close();
    }

    /**
     * Provides functionality to test for set*Data calls not being made by the
     * Action Handler.
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
        conf.set(OozieClient.APP_PATH, getTestCaseDir());
        conf.set(OozieClient.USER_NAME, "u");
        conf.set(OozieClient.GROUP_NAME, "g");
        conf.set(OozieClient.LOG_TOKEN, "t");
        conf.set("external-status", "ok");
        conf.set("signal-value", "based_on_action_status");
        conf.set(avoidParam, "true");

        final String jobId = engine.submitJob(conf, true);

        final WorkflowStore store = Services.get().get(WorkflowStoreService.class).create();

        waitFor(5000, new Predicate() {
            public boolean evaluate() throws Exception {
                WorkflowJobBean bean = store.getWorkflow(jobId, false);
                return (bean.getWorkflowInstance().getStatus() == WorkflowInstance.Status.FAILED);
            }
        });
        assertEquals(WorkflowInstance.Status.FAILED, store.getWorkflow(jobId, false).getWorkflowInstance().getStatus());
        assertEquals(WorkflowJob.Status.FAILED, engine.getJob(jobId).getStatus());

        List<WorkflowActionBean> actions = store.getActionsForWorkflow(jobId, false);
        WorkflowActionBean action = actions.get(0);
        assertEquals(expActionErrorCode, action.getErrorCode());

        store.close();
    }
}
