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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobInsertJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.service.WorkflowStoreService;
import org.apache.oozie.service.UUIDService.ApplicationType;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.WorkflowLib;
import org.apache.oozie.workflow.lite.EndNodeDef;
import org.apache.oozie.workflow.lite.LiteWorkflowApp;
import org.apache.oozie.workflow.lite.LiteWorkflowInstance;
import org.apache.oozie.workflow.lite.StartNodeDef;

public class TestPurgeXCommand extends XDataTestCase {
    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        cleanUpDBTables();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    /**
     * Test : purge succeeded wf job and action successfully. Creates and runs a
     * new job to completion. Attempts to purge jobs older than a day. Verifies
     * the presence of the job in the system. </p> Sets the end date for the
     * same job to make it qualify for the purge criteria. Calls the purge
     * command, and ensure the job does not exist in the system.
     *
     * @throws Exception
     */
    public void testSucJobPurgeXCommand() throws Exception {
        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED,
                WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean action = this.addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.OK);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(job.getId());
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(action.getId());

        job = jpaService.execute(wfJobGetCmd);
        action = jpaService.execute(wfActionGetCmd);
        assertEquals(job.getStatus(), WorkflowJob.Status.SUCCEEDED);
        assertEquals(action.getStatus(), WorkflowAction.Status.OK);
        WorkflowInstance wfInstance = job.getWorkflowInstance();
        assertEquals(wfInstance.getStatus(), WorkflowInstance.Status.SUCCEEDED);

        new PurgeXCommand(7, 10).call();

        try {
            job = jpaService.execute(wfJobGetCmd);
            fail("Job should be purged. Should fail.");
        }
        catch (JPAExecutorException ce) {
            // Job doesn't exist. Exception is expected.
        }

        try {
            jpaService.execute(wfActionGetCmd);
            fail("Action should be purged. Should fail.");
        }
        catch (JPAExecutorException ce) {
            // Job doesn't exist. Exception is expected.
        }

    }

    /**
     * Test : purge failed wf job and action successfully
     *
     * @throws Exception
     */
    public void testFailJobPurgeXCommand() throws Exception {
        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.FAILED, WorkflowInstance.Status.FAILED);
        WorkflowActionBean action = this.addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.FAILED);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(job.getId());
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(action.getId());

        job = jpaService.execute(wfJobGetCmd);
        action = jpaService.execute(wfActionGetCmd);
        assertEquals(job.getStatus(), WorkflowJob.Status.FAILED);
        assertEquals(action.getStatus(), WorkflowAction.Status.FAILED);
        WorkflowInstance wfInstance = job.getWorkflowInstance();
        assertEquals(wfInstance.getStatus(), WorkflowInstance.Status.FAILED);

        new PurgeXCommand(7, 10).call();

        try {
            job = jpaService.execute(wfJobGetCmd);
            fail("Job should be purged. Should fail.");
        }
        catch (JPAExecutorException ce) {
            // Job doesn't exist. Exception is expected.
        }

        try {
            jpaService.execute(wfActionGetCmd);
            fail("Action should be purged. Should fail.");
        }
        catch (JPAExecutorException ce) {
            // Job doesn't exist. Exception is expected.
        }

    }

    /**
     * Test : purge killed wf job and action successfully
     *
     * @throws Exception
     */
    public void testKillJobPurgeXCommand() throws Exception {
        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.KILLED, WorkflowInstance.Status.KILLED);
        WorkflowActionBean action = this.addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.KILLED);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(job.getId());
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(action.getId());

        job = jpaService.execute(wfJobGetCmd);
        action = jpaService.execute(wfActionGetCmd);
        assertEquals(job.getStatus(), WorkflowJob.Status.KILLED);
        assertEquals(action.getStatus(), WorkflowAction.Status.KILLED);
        WorkflowInstance wfInstance = job.getWorkflowInstance();
        assertEquals(wfInstance.getStatus(), WorkflowInstance.Status.KILLED);

        new PurgeXCommand(7, 10).call();

        try {
            job = jpaService.execute(wfJobGetCmd);
            fail("Job should be purged. Should fail.");
        }
        catch (JPAExecutorException ce) {
            // Job doesn't exist. Exception is expected.
        }

        try {
            jpaService.execute(wfActionGetCmd);
            fail("Action should be purged. Should fail.");
        }
        catch (JPAExecutorException ce) {
            // Job doesn't exist. Exception is expected.
        }

    }

    /**
     * Test : purge wf job and action failed
     *
     * @throws Exception
     */
    public void testPurgeXCommandFailed() throws Exception {
        WorkflowJobBean job = this.addRecordToWfJobTableForNegCase(WorkflowJob.Status.RUNNING,
                WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action = this.addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.RUNNING);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(job.getId());
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(action.getId());

        job = jpaService.execute(wfJobGetCmd);
        action = jpaService.execute(wfActionGetCmd);
        assertEquals(job.getStatus(), WorkflowJob.Status.RUNNING);
        assertEquals(action.getStatus(), WorkflowAction.Status.RUNNING);
        WorkflowInstance wfInstance = job.getWorkflowInstance();
        assertEquals(wfInstance.getStatus(), WorkflowInstance.Status.RUNNING);

        new PurgeXCommand(7, 10).call();

        try {
            job = jpaService.execute(wfJobGetCmd);
        }
        catch (JPAExecutorException ce) {
            fail("Job should not be purged. Should fail.");
        }

        try {
            jpaService.execute(wfActionGetCmd);
        }
        catch (JPAExecutorException ce) {
            fail("Action should not be purged. Should fail.");
        }

    }

    protected WorkflowJobBean addRecordToWfJobTableForNegCase(WorkflowJob.Status jobStatus,
            WorkflowInstance.Status instanceStatus) throws Exception {
        WorkflowApp app = new LiteWorkflowApp("testApp", "<workflow-app/>", new StartNodeDef("end"))
                .addNode(new EndNodeDef("end"));
        Configuration conf = new Configuration();
        Path appUri = new Path(getAppPath(), "workflow.xml");
        conf.set(OozieClient.APP_PATH, appUri.toString());
        conf.set(OozieClient.LOG_TOKEN, "testToken");
        conf.set(OozieClient.USER_NAME, getTestUser());

        WorkflowJobBean wfBean = createWorkflow(app, conf, "auth", jobStatus, instanceStatus);
        wfBean.setStartTime(DateUtils.parseDateUTC("2015-12-18T01:00Z"));
        wfBean.setEndTime(DateUtils.parseDateUTC("2015-12-18T03:00Z"));

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            WorkflowJobInsertJPAExecutor wfInsertCmd = new WorkflowJobInsertJPAExecutor(wfBean);
            jpaService.execute(wfInsertCmd);
        }
        catch (JPAExecutorException ce) {
            ce.printStackTrace();
            fail("Unable to insert the test wf job record to table");
            throw ce;
        }
        return wfBean;
    }

    @Override
    protected WorkflowJobBean createWorkflow(WorkflowApp app, Configuration conf, String authToken,
            WorkflowJob.Status jobStatus, WorkflowInstance.Status instanceStatus) throws Exception {
        WorkflowAppService wps = Services.get().get(WorkflowAppService.class);
        Configuration protoActionConf = wps.createProtoActionConf(conf, authToken, true);
        WorkflowLib workflowLib = Services.get().get(WorkflowStoreService.class).getWorkflowLibWithNoDB();
        WorkflowInstance wfInstance = workflowLib.createInstance(app, conf);
        ((LiteWorkflowInstance) wfInstance).setStatus(instanceStatus);
        WorkflowJobBean workflow = new WorkflowJobBean();
        workflow.setId(Services.get().get(UUIDService.class).generateId(ApplicationType.WORKFLOW));
        workflow.setAppName(app.getName());
        workflow.setAppPath(conf.get(OozieClient.APP_PATH));
        workflow.setConf(XmlUtils.prettyPrint(conf).toString());
        workflow.setProtoActionConf(XmlUtils.prettyPrint(protoActionConf).toString());
        workflow.setCreatedTime(new Date());
        workflow.setLogToken(conf.get(OozieClient.LOG_TOKEN, ""));
        workflow.setStatus(jobStatus);
        workflow.setRun(0);
        workflow.setUser(conf.get(OozieClient.USER_NAME));
        workflow.setGroup(conf.get(OozieClient.GROUP_NAME));
        workflow.setAuthToken(authToken);
        workflow.setWorkflowInstance(wfInstance);
        workflow.setStartTime(DateUtils.parseDateUTC("2009-12-18T01:00Z"));
        workflow.setEndTime(DateUtils.parseDateUTC("2009-12-18T03:00Z"));
        return workflow;
    }

}
