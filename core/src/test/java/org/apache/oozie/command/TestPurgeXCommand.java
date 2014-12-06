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

package org.apache.oozie.command;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.executor.jpa.BundleActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor.WorkflowJobQuery;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.LiteWorkflowStoreService;
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
    private String[] excludedServices = { "org.apache.oozie.service.StatusTransitService",
            "org.apache.oozie.service.PauseTransitService", "org.apache.oozie.service.PurgeService",
            "org.apache.oozie.service.CoordMaterializeTriggerService", "org.apache.oozie.service.RecoveryService" };

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        setClassesToBeExcluded(services.getConf(), excludedServices);
        services.init();
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
        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
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

        new PurgeXCommand(7, 1, 1, 10).call();

        try {
            jpaService.execute(wfJobGetCmd);
            fail("Workflow Job should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(wfActionGetCmd);
            fail("Workflow Action should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
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

        new PurgeXCommand(7, 1, 1, 10).call();

        try {
            jpaService.execute(wfJobGetCmd);
            fail("Workflow Job should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(wfActionGetCmd);
            fail("Workflow Action should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
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

        new PurgeXCommand(7, 1, 1, 10).call();

        try {
            jpaService.execute(wfJobGetCmd);
            fail("Workflow Job should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(wfActionGetCmd);
            fail("Workflow Action should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

    }

    /**
     * Test : purge wf job and action failed
     *
     * @throws Exception
     */
    public void testPurgeXCommandFailed() throws Exception {
        WorkflowJobBean job = this.addRecordToWfJobTableForNegCase(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
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

        new PurgeXCommand(7, 1, 1, 10).call();

        try {
            jpaService.execute(wfJobGetCmd);
        }
        catch (JPAExecutorException ce) {
            fail("Workflow Job should not have been purged");
        }

        try {
            jpaService.execute(wfActionGetCmd);
        }
        catch (JPAExecutorException ce) {
            fail("Workflow Action should have been purged");
        }

    }

    /**
     * Test : purge succeeded coord job and action successfully
     *
     * @throws Exception
     */
    public void testSucCoordPurgeXCommand() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetJPAExecutor coordJobGetExecutor = new CoordJobGetJPAExecutor(job.getId());
        CoordActionGetJPAExecutor coordActionGetExecutor = new CoordActionGetJPAExecutor(action.getId());

        job = jpaService.execute(coordJobGetExecutor);
        action = jpaService.execute(coordActionGetExecutor);
        assertEquals(job.getStatus(), CoordinatorJob.Status.SUCCEEDED);
        assertEquals(action.getStatus(), CoordinatorAction.Status.SUCCEEDED);

        new PurgeXCommand(1, 7, 1, 10).call();

        try {
            jpaService.execute(coordJobGetExecutor);
            fail("Coordinator Job should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(coordActionGetExecutor);
            fail("Coordinator Action should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

    }

    /**
     * Test : purge failed coord job and action successfully
     *
     * @throws Exception
     */
    public void testFailCoordPurgeXCommand() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.FAILED, false, false);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.FAILED,
                "coord-action-get.xml", 0);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetJPAExecutor coordJobGetExecutor = new CoordJobGetJPAExecutor(job.getId());
        CoordActionGetJPAExecutor coordActionGetExecutor = new CoordActionGetJPAExecutor(action.getId());

        job = jpaService.execute(coordJobGetExecutor);
        action = jpaService.execute(coordActionGetExecutor);
        assertEquals(job.getStatus(), CoordinatorJob.Status.FAILED);
        assertEquals(action.getStatus(), CoordinatorAction.Status.FAILED);

        new PurgeXCommand(1, 7, 1, 10).call();

        try {
            jpaService.execute(coordJobGetExecutor);
            fail("Coordinator Job should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(coordActionGetExecutor);
            fail("Coordinator Action should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

    }

    /**
     * Test : purge killed coord job and action successfully
     *
     * @throws Exception
     */
    public void testKillCoordPurgeXCommand() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.KILLED, false, false);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.KILLED,
                "coord-action-get.xml", 0);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetJPAExecutor coordJobGetExecutor = new CoordJobGetJPAExecutor(job.getId());
        CoordActionGetJPAExecutor coordActionGetExecutor = new CoordActionGetJPAExecutor(action.getId());

        job = jpaService.execute(coordJobGetExecutor);
        action = jpaService.execute(coordActionGetExecutor);
        assertEquals(job.getStatus(), CoordinatorJob.Status.KILLED);
        assertEquals(action.getStatus(), CoordinatorAction.Status.KILLED);

        new PurgeXCommand(1, 7, 1, 10).call();

        try {
            jpaService.execute(coordJobGetExecutor);
            fail("Coordinator Job should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(coordActionGetExecutor);
            fail("Coordinator Action should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

    }

    /**
     * Test : purge coord job and action failed
     *
     * @throws Exception
     */
    public void testCoordPurgeXCommandFailed() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetJPAExecutor coordJobGetExecutor = new CoordJobGetJPAExecutor(job.getId());
        CoordActionGetJPAExecutor coordActionGetExecutor = new CoordActionGetJPAExecutor(action.getId());

        job = jpaService.execute(coordJobGetExecutor);
        action = jpaService.execute(coordActionGetExecutor);
        assertEquals(job.getStatus(), CoordinatorJob.Status.RUNNING);
        assertEquals(action.getStatus(), CoordinatorAction.Status.SUCCEEDED);

        new PurgeXCommand(1, 7, 1, 10).call();

        try {
            jpaService.execute(coordJobGetExecutor);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Job should not have been purged");
        }

        try {
            jpaService.execute(coordActionGetExecutor);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action should not have been purged");
        }
    }

    /**
     * Test : purge succeeded bundle job and action successfully
     *
     * @throws Exception
     */
    public void testSucBundlePurgeXCommand() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateOozieTZ(
            "2011-01-01T01:00Z"));
        this.addRecordToBundleActionTable(job.getId(), "action1", 0, Job.Status.SUCCEEDED);
        this.addRecordToBundleActionTable(job.getId(), "action2", 0, Job.Status.SUCCEEDED);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetExecutor = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.SUCCEEDED, job.getStatus());

        BundleActionGetJPAExecutor bundleActionGetExecutor1 = new BundleActionGetJPAExecutor(job.getId(), "action1");
        BundleActionBean action1 = jpaService.execute(bundleActionGetExecutor1);
        assertEquals(Job.Status.SUCCEEDED, action1.getStatus());

        BundleActionGetJPAExecutor bundleActionGetExecutor2 = new BundleActionGetJPAExecutor(job.getId(), "action2");
        BundleActionBean action2 = jpaService.execute(bundleActionGetExecutor2);
        assertEquals(Job.Status.SUCCEEDED, action2.getStatus());

        new PurgeXCommand(1, 1, 7, 10).call();

        try {
            jpaService.execute(bundleJobGetExecutor);
            fail("Bundle Job should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(bundleActionGetExecutor1);
            fail("Bundle Action should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(bundleActionGetExecutor2);
            fail("Bundle Action should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

    }

    /**
     * Test : purge failed bundle job and action successfully
     *
     * @throws Exception
     */
    public void testFailBundlePurgeXCommand() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.DONEWITHERROR, DateUtils.parseDateOozieTZ(
            "2011-01-01T01:00Z"));
        this.addRecordToBundleActionTable(job.getId(), "action1", 0, Job.Status.FAILED);
        this.addRecordToBundleActionTable(job.getId(), "action2", 0, Job.Status.SUCCEEDED);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetExecutor = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.DONEWITHERROR, job.getStatus());

        BundleActionGetJPAExecutor bundleActionGetExecutor1 = new BundleActionGetJPAExecutor(job.getId(), "action1");
        BundleActionBean action1 = jpaService.execute(bundleActionGetExecutor1);
        assertEquals(Job.Status.FAILED, action1.getStatus());

        BundleActionGetJPAExecutor bundleActionGetExecutor2 = new BundleActionGetJPAExecutor(job.getId(), "action2");
        BundleActionBean action2 = jpaService.execute(bundleActionGetExecutor2);
        assertEquals(Job.Status.SUCCEEDED, action2.getStatus());

        new PurgeXCommand(1, 1, 7, 10).call();

        try {
            jpaService.execute(bundleJobGetExecutor);
            fail("Bundle Job should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(bundleActionGetExecutor1);
            fail("Bundle Action should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(bundleActionGetExecutor2);
            fail("Bundle Action should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

    }

    /**
     * Test : purge killed bundle job and action successfully
     *
     * @throws Exception
     */
    public void testKillBundlePurgeXCommand() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.KILLED, DateUtils.parseDateOozieTZ(
            "2011-01-01T01:00Z"));
        this.addRecordToBundleActionTable(job.getId(), "action1", 0, Job.Status.KILLED);
        this.addRecordToBundleActionTable(job.getId(), "action2", 0, Job.Status.KILLED);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetExecutor = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.KILLED, job.getStatus());

        BundleActionGetJPAExecutor bundleActionGetExecutor1 = new BundleActionGetJPAExecutor(job.getId(), "action1");
        BundleActionBean action1 = jpaService.execute(bundleActionGetExecutor1);
        assertEquals(Job.Status.KILLED, action1.getStatus());

        BundleActionGetJPAExecutor bundleActionGetExecutor2 = new BundleActionGetJPAExecutor(job.getId(), "action2");
        BundleActionBean action2 = jpaService.execute(bundleActionGetExecutor2);
        assertEquals(Job.Status.KILLED, action2.getStatus());

        new PurgeXCommand(1, 1, 7, 10).call();

        try {
            jpaService.execute(bundleJobGetExecutor);
            fail("Bundle Job should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(bundleActionGetExecutor1);
            fail("Bundle Action should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(bundleActionGetExecutor2);
            fail("Bundle Action should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

    }

    /**
     * Test : purge bundle job and action failed
     *
     * @throws Exception
     */
    public void testBundlePurgeXCommandFailed() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.RUNNING, DateUtils.parseDateOozieTZ(
            "2011-01-01T01:00Z"));
        this.addRecordToBundleActionTable(job.getId(), "action1", 0, Job.Status.RUNNING);
        this.addRecordToBundleActionTable(job.getId(), "action2", 0, Job.Status.SUCCEEDED);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetExecutor = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.RUNNING, job.getStatus());

        BundleActionGetJPAExecutor bundleActionGetExecutor1 = new BundleActionGetJPAExecutor(job.getId(), "action1");
        BundleActionBean action1 = jpaService.execute(bundleActionGetExecutor1);
        assertEquals(Job.Status.RUNNING, action1.getStatus());

        BundleActionGetJPAExecutor bundleActionGetExecutor2 = new BundleActionGetJPAExecutor(job.getId(), "action2");
        BundleActionBean action2 = jpaService.execute(bundleActionGetExecutor2);
        assertEquals(Job.Status.SUCCEEDED, action2.getStatus());

        new PurgeXCommand(1, 1, 7, 10).call();

        try {
            jpaService.execute(bundleJobGetExecutor);
        }
        catch (JPAExecutorException je) {
            fail("Bundle Job should not have been purged");
        }

        try {
            jpaService.execute(bundleActionGetExecutor1);
        }
        catch (JPAExecutorException je) {
            fail("Bundle Action should not have been purged");
        }

        try {
            jpaService.execute(bundleActionGetExecutor2);
        }
        catch (JPAExecutorException je) {
            fail("Bundle Action should not have been purged");
        }
    }

    /**
     * Test : The workflow should get purged, but the coordinator parent shouldn't get purged --> neither will get purged
     *
     * @throws Exception
     */
    public void testPurgeCoordWithWFChild1() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob.getId(), "SUCCEEDED", 0);

        WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(wfJob.getId());
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(wfAction.getId());
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(coordJob.getId());
        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(coordAction.getId());

        wfJob = jpaService.execute(wfJobGetCmd);
        wfAction = jpaService.execute(wfActionGetCmd);
        coordJob = jpaService.execute(coordJobGetCmd);
        coordAction = jpaService.execute(coordActionGetCmd);
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction.getStatus());
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction.getStatus());

        new PurgeXCommand(7, getNumDaysToNotBePurged(coordJob.getLastModifiedTime()), 1, 10).call();

        try {
            jpaService.execute(coordJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Job should not have been purged");
        }

        try {
            jpaService.execute(coordActionGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action should not have been purged");
        }

        try {
            jpaService.execute(wfJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Job should not have been purged");
        }

        try {
            jpaService.execute(wfActionGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action should not have been purged");
        }
    }

    /**
     * Test : The workflow should get purged, but the coordinator parent shouldn't get purged -->
     * the workflow and corresponding coord actions will get purged after we turn the purge.old.coord.action on
     * Coordinator itself will not be purged
     *
     * @throws Exception
     */
    public void testPurgeLongRunningCoordWithWFChild() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob.getId(), "SUCCEEDED", 0);

        WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(wfJob.getId());
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(wfAction.getId());
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(coordJob.getId());
        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(coordAction.getId());

        wfJob = jpaService.execute(wfJobGetCmd);
        wfAction = jpaService.execute(wfActionGetCmd);
        coordJob = jpaService.execute(coordJobGetCmd);
        coordAction = jpaService.execute(coordActionGetCmd);
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction.getStatus());
        assertEquals(CoordinatorJob.Status.RUNNING, coordJob.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction.getStatus());

        new PurgeXCommand(7, getNumDaysToNotBePurged(coordJob.getLastModifiedTime()), 1, 10, true).call();

        try {
            jpaService.execute(coordJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Job should not have been purged");
        }

        try {
            jpaService.execute(coordActionGetCmd);
            fail("Coordinator Action should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(wfJobGetCmd);
            fail("Workflow Job should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(wfActionGetCmd);
            fail("Workflow Action should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }
    }

    /**
     * Test : The workflow should get purged, but the coordinator parent shouldn't get purged -->
     * the workflow and corresponding coord actions will NOT get purged after we turn the purge.old.coord.action off
     * Neither will be purged
     *
     * @throws Exception
     */
    public void testPurgeLongRunningCoordWithWFChildNegative() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob.getId(), "SUCCEEDED", 0);

        WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(wfJob.getId());
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(wfAction.getId());
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(coordJob.getId());
        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(coordAction.getId());

        wfJob = jpaService.execute(wfJobGetCmd);
        wfAction = jpaService.execute(wfActionGetCmd);
        coordJob = jpaService.execute(coordJobGetCmd);
        coordAction = jpaService.execute(coordActionGetCmd);
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction.getStatus());
        assertEquals(CoordinatorJob.Status.RUNNING, coordJob.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction.getStatus());

        new PurgeXCommand(7, getNumDaysToNotBePurged(coordJob.getLastModifiedTime()), 1, 10, false).call();

        try {
            jpaService.execute(coordJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Job should not have been purged");
        }

        try {
            jpaService.execute(coordActionGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action should not have been purged");
        }

        try {
            jpaService.execute(wfJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Job should not have been purged");
        }

        try {
            jpaService.execute(wfActionGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action should not have been purged");
        }
    }

    /**
     * Test : The workflow should not get purged, but the coordinator parent should get purged --> neither will get purged
     *
     * @throws Exception
     */
    public void testPurgeCoordWithWFChild2() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob.getId(), "SUCCEEDED", 0);

        WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(wfJob.getId());
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(wfAction.getId());
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(coordJob.getId());
        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(coordAction.getId());

        wfJob = jpaService.execute(wfJobGetCmd);
        wfAction = jpaService.execute(wfActionGetCmd);
        coordJob = jpaService.execute(coordJobGetCmd);
        coordAction = jpaService.execute(coordActionGetCmd);
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction.getStatus());
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction.getStatus());

        new PurgeXCommand(getNumDaysToNotBePurged(wfJob.getEndTime()), 7, 1, 10).call();

        try {
            jpaService.execute(coordJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Job should not have been purged");
        }

        try {
            jpaService.execute(coordActionGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action should not have been purged");
        }

        try {
            jpaService.execute(wfJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Job should not have been purged");
        }

        try {
            jpaService.execute(wfActionGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action should not have been purged");
        }
    }

    /**
     * Test : The workflows should not get purged, but the coordinator parent should get purged --> none will get purged
     * There are more workflow children than the limit
     *
     * @throws Exception
     */
    public void testPurgeCoordWithWFChild2MoreThanLimit() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        WorkflowJobBean wfJob1 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob2 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob3 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob4 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob5 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction1 = addRecordToWfActionTable(wfJob1.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction2 = addRecordToWfActionTable(wfJob2.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction3 = addRecordToWfActionTable(wfJob3.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction4 = addRecordToWfActionTable(wfJob4.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction5 = addRecordToWfActionTable(wfJob5.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction1 = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob1.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction2 = addRecordToCoordActionTable(coordJob.getId(), 2, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob2.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction3 = addRecordToCoordActionTable(coordJob.getId(), 3, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob3.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction4 = addRecordToCoordActionTable(coordJob.getId(), 4, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob4.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction5 = addRecordToCoordActionTable(coordJob.getId(), 5, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob5.getId(), "SUCCEEDED", 0);

        WorkflowJobGetJPAExecutor wfJob1GetCmd = new WorkflowJobGetJPAExecutor(wfJob1.getId());
        WorkflowJobGetJPAExecutor wfJob2GetCmd = new WorkflowJobGetJPAExecutor(wfJob2.getId());
        WorkflowJobGetJPAExecutor wfJob3GetCmd = new WorkflowJobGetJPAExecutor(wfJob3.getId());
        WorkflowJobGetJPAExecutor wfJob4GetCmd = new WorkflowJobGetJPAExecutor(wfJob4.getId());
        WorkflowJobGetJPAExecutor wfJob5GetCmd = new WorkflowJobGetJPAExecutor(wfJob5.getId());
        WorkflowActionGetJPAExecutor wfAction1GetCmd = new WorkflowActionGetJPAExecutor(wfAction1.getId());
        WorkflowActionGetJPAExecutor wfAction2GetCmd = new WorkflowActionGetJPAExecutor(wfAction2.getId());
        WorkflowActionGetJPAExecutor wfAction3GetCmd = new WorkflowActionGetJPAExecutor(wfAction3.getId());
        WorkflowActionGetJPAExecutor wfAction4GetCmd = new WorkflowActionGetJPAExecutor(wfAction4.getId());
        WorkflowActionGetJPAExecutor wfAction5GetCmd = new WorkflowActionGetJPAExecutor(wfAction5.getId());
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(coordJob.getId());
        CoordActionGetJPAExecutor coordAction1GetCmd = new CoordActionGetJPAExecutor(coordAction1.getId());
        CoordActionGetJPAExecutor coordAction2GetCmd = new CoordActionGetJPAExecutor(coordAction2.getId());
        CoordActionGetJPAExecutor coordAction3GetCmd = new CoordActionGetJPAExecutor(coordAction3.getId());
        CoordActionGetJPAExecutor coordAction4GetCmd = new CoordActionGetJPAExecutor(coordAction4.getId());
        CoordActionGetJPAExecutor coordAction5GetCmd = new CoordActionGetJPAExecutor(coordAction5.getId());

        wfJob1 = jpaService.execute(wfJob1GetCmd);
        wfJob2 = jpaService.execute(wfJob2GetCmd);
        wfJob3 = jpaService.execute(wfJob3GetCmd);
        wfJob4 = jpaService.execute(wfJob4GetCmd);
        wfJob5 = jpaService.execute(wfJob5GetCmd);
        wfAction1 = jpaService.execute(wfAction1GetCmd);
        wfAction2 = jpaService.execute(wfAction2GetCmd);
        wfAction3 = jpaService.execute(wfAction3GetCmd);
        wfAction4 = jpaService.execute(wfAction4GetCmd);
        wfAction5 = jpaService.execute(wfAction5GetCmd);
        coordJob = jpaService.execute(coordJobGetCmd);
        coordAction1 = jpaService.execute(coordAction1GetCmd);
        coordAction2 = jpaService.execute(coordAction2GetCmd);
        coordAction3 = jpaService.execute(coordAction3GetCmd);
        coordAction4 = jpaService.execute(coordAction4GetCmd);
        coordAction5 = jpaService.execute(coordAction5GetCmd);
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob1.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob2.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob3.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob4.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob5.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction1.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction2.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction3.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction4.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction5.getStatus());
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction1.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction2.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction3.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction4.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction5.getStatus());

        new PurgeXCommand(getNumDaysToNotBePurged(wfJob1.getEndTime()), 7, 1, 3).call();

        try {
            jpaService.execute(coordJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Job should not have been purged");
        }

        try {
            jpaService.execute(coordAction1GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action 1 should not have been purged");
        }

        try {
            jpaService.execute(coordAction2GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action 2 should not have been purged");
        }

        try {
            jpaService.execute(coordAction3GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action 3 should not have been purged");
        }

        try {
            jpaService.execute(coordAction4GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action 4 should not have been purged");
        }

        try {
            jpaService.execute(coordAction5GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action 5 should not have been purged");
        }

        try {
            jpaService.execute(wfJob1GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Job 1 should not have been purged");
        }

        try {
            jpaService.execute(wfJob2GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Job 2 should not have been purged");
        }

        try {
            jpaService.execute(wfJob3GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Job 3 should not have been purged");
        }

        try {
            jpaService.execute(wfJob4GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Job 4 should not have been purged");
        }

        try {
            jpaService.execute(wfJob5GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Job 5 should not have been purged");
        }

        try {
            jpaService.execute(wfAction1GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action 1 should not have been purged");
        }

        try {
            jpaService.execute(wfAction2GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action 2 should not have been purged");
        }

        try {
            jpaService.execute(wfAction3GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action 3 should not have been purged");
        }

        try {
            jpaService.execute(wfAction4GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action 4 should not have been purged");
        }

        try {
            jpaService.execute(wfAction5GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action 5 should not have been purged");
        }
    }

    /**
     * Test : The workflow should get purged, and the coordinator parent should get purged --> both will get purged
     *
     * @throws Exception
     */
    public void testPurgeCoordWithWFChild3() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob.getId(), "SUCCEEDED", 0);

        WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(wfJob.getId());
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(wfAction.getId());
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(coordJob.getId());
        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(coordAction.getId());

        wfJob = jpaService.execute(wfJobGetCmd);
        wfAction = jpaService.execute(wfActionGetCmd);
        coordJob = jpaService.execute(coordJobGetCmd);
        coordAction = jpaService.execute(coordActionGetCmd);
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction.getStatus());
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction.getStatus());

        new PurgeXCommand(7, 7, 1, 10).call();

        try {
            jpaService.execute(coordJobGetCmd);
            fail("Coordinator Job should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(coordActionGetCmd);
            fail("Coordinator Action should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(wfJobGetCmd);
            fail("Workflow Job should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(wfActionGetCmd);
            fail("Workflow Action should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }
    }

    /**
     * Test : The workflow should get purged, and the coordinator parent should get purged --> both will get purged
     *
     * @throws Exception
     */
    public void testPurgeCoordWithWFChild3MoreThanLimit() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        WorkflowJobBean wfJob1 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob2 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob3 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob4 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob5 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction1 = addRecordToWfActionTable(wfJob1.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction2 = addRecordToWfActionTable(wfJob2.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction3 = addRecordToWfActionTable(wfJob3.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction4 = addRecordToWfActionTable(wfJob4.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction5 = addRecordToWfActionTable(wfJob5.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction1 = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob1.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction2 = addRecordToCoordActionTable(coordJob.getId(), 2, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob2.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction3 = addRecordToCoordActionTable(coordJob.getId(), 3, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob3.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction4 = addRecordToCoordActionTable(coordJob.getId(), 4, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob4.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction5 = addRecordToCoordActionTable(coordJob.getId(), 5, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob5.getId(), "SUCCEEDED", 0);

        WorkflowJobGetJPAExecutor wfJob1GetCmd = new WorkflowJobGetJPAExecutor(wfJob1.getId());
        WorkflowJobGetJPAExecutor wfJob2GetCmd = new WorkflowJobGetJPAExecutor(wfJob2.getId());
        WorkflowJobGetJPAExecutor wfJob3GetCmd = new WorkflowJobGetJPAExecutor(wfJob3.getId());
        WorkflowJobGetJPAExecutor wfJob4GetCmd = new WorkflowJobGetJPAExecutor(wfJob4.getId());
        WorkflowJobGetJPAExecutor wfJob5GetCmd = new WorkflowJobGetJPAExecutor(wfJob5.getId());
        WorkflowActionGetJPAExecutor wfAction1GetCmd = new WorkflowActionGetJPAExecutor(wfAction1.getId());
        WorkflowActionGetJPAExecutor wfAction2GetCmd = new WorkflowActionGetJPAExecutor(wfAction2.getId());
        WorkflowActionGetJPAExecutor wfAction3GetCmd = new WorkflowActionGetJPAExecutor(wfAction3.getId());
        WorkflowActionGetJPAExecutor wfAction4GetCmd = new WorkflowActionGetJPAExecutor(wfAction4.getId());
        WorkflowActionGetJPAExecutor wfAction5GetCmd = new WorkflowActionGetJPAExecutor(wfAction5.getId());
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(coordJob.getId());
        CoordActionGetJPAExecutor coordAction1GetCmd = new CoordActionGetJPAExecutor(coordAction1.getId());
        CoordActionGetJPAExecutor coordAction2GetCmd = new CoordActionGetJPAExecutor(coordAction2.getId());
        CoordActionGetJPAExecutor coordAction3GetCmd = new CoordActionGetJPAExecutor(coordAction3.getId());
        CoordActionGetJPAExecutor coordAction4GetCmd = new CoordActionGetJPAExecutor(coordAction4.getId());
        CoordActionGetJPAExecutor coordAction5GetCmd = new CoordActionGetJPAExecutor(coordAction5.getId());

        wfJob1 = jpaService.execute(wfJob1GetCmd);
        wfJob2 = jpaService.execute(wfJob2GetCmd);
        wfJob3 = jpaService.execute(wfJob3GetCmd);
        wfJob4 = jpaService.execute(wfJob4GetCmd);
        wfJob5 = jpaService.execute(wfJob5GetCmd);
        wfAction1 = jpaService.execute(wfAction1GetCmd);
        wfAction2 = jpaService.execute(wfAction2GetCmd);
        wfAction3 = jpaService.execute(wfAction3GetCmd);
        wfAction4 = jpaService.execute(wfAction4GetCmd);
        wfAction5 = jpaService.execute(wfAction5GetCmd);
        coordJob = jpaService.execute(coordJobGetCmd);
        coordAction1 = jpaService.execute(coordAction1GetCmd);
        coordAction2 = jpaService.execute(coordAction2GetCmd);
        coordAction3 = jpaService.execute(coordAction3GetCmd);
        coordAction4 = jpaService.execute(coordAction4GetCmd);
        coordAction5 = jpaService.execute(coordAction5GetCmd);
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob1.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob2.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob3.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob4.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob5.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction1.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction2.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction3.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction4.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction5.getStatus());
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction1.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction2.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction3.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction4.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction5.getStatus());

        new PurgeXCommand(7, 7, 1, 10).call();

        try {
            jpaService.execute(coordJobGetCmd);
            fail("Coordinator Job should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(coordAction1GetCmd);
            fail("Coordinator Action 1 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(coordAction2GetCmd);
            fail("Coordinator Action 2 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(coordAction3GetCmd);
            fail("Coordinator Action 3 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(coordAction4GetCmd);
            fail("Coordinator Action 4 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(coordAction5GetCmd);
            fail("Coordinator Action 5 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(wfJob1GetCmd);
            fail("Workflow Job 1 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(wfJob2GetCmd);
            fail("Workflow Job 2 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(wfJob3GetCmd);
            fail("Workflow Job 3 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(wfJob4GetCmd);
            fail("Workflow Job 4 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(wfJob5GetCmd);
            fail("Workflow Job 5 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(wfAction1GetCmd);
            fail("Workflow Action 1 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(wfAction2GetCmd);
            fail("Workflow Action 2 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(wfAction3GetCmd);
            fail("Workflow Action 3 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(wfAction4GetCmd);
            fail("Workflow Action 4 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(wfAction5GetCmd);
            fail("Workflow Action 5 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }
    }

    /**
     * Test : The workflow and coordinator should get purged, but the bundle parent shouldn't get purged --> none will get purged
     *
     * @throws Exception
     */
    public void testPurgeBundleWithCoordChildWithWFChild1() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        BundleJobBean bundleJob = addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-01-01T01:00Z"));
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob.getId(), "SUCCEEDED", 0);
        BundleActionBean bundleAction = addRecordToBundleActionTable(bundleJob.getId(), coordJob.getId(), coordJob.getAppName(), 0,
                Job.Status.SUCCEEDED);

        WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(wfJob.getId());
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(wfAction.getId());
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(coordJob.getId());
        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(coordAction.getId());
        BundleJobGetJPAExecutor bundleJobGetCmd = new BundleJobGetJPAExecutor(bundleJob.getId());
        BundleActionGetJPAExecutor bundleActionGetCmd = new BundleActionGetJPAExecutor(bundleJob.getId(), coordJob.getAppName());

        wfJob = jpaService.execute(wfJobGetCmd);
        wfAction = jpaService.execute(wfActionGetCmd);
        coordJob = jpaService.execute(coordJobGetCmd);
        coordAction = jpaService.execute(coordActionGetCmd);
        bundleJob = jpaService.execute(bundleJobGetCmd);
        bundleAction = jpaService.execute(bundleActionGetCmd);
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction.getStatus());
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction.getStatus());
        assertEquals(BundleJobBean.Status.SUCCEEDED, bundleJob.getStatus());
        assertEquals(BundleJobBean.Status.SUCCEEDED, bundleAction.getStatus());

        new PurgeXCommand(7, 7, getNumDaysToNotBePurged(bundleJob.getLastModifiedTime()), 10).call();

        try {
            jpaService.execute(bundleJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Bundle Job should not have been purged");
        }

        try {
            jpaService.execute(bundleActionGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Bundle Action should not have been purged");
        }

        try {
            jpaService.execute(coordJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Job should not have been purged");
        }

        try {
            jpaService.execute(coordActionGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action should not have been purged");
        }

        try {
            jpaService.execute(wfJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Job should not have been purged");
        }

        try {
            jpaService.execute(wfActionGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action should not have been purged");
        }
    }

    /**
     * Test : The workflow and coordinator should not get purged, but the bundle parent should get purged --> none will get purged
     *
     * @throws Exception
     */
    public void testPurgeBundleWithCoordChildWithWFChild2() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        BundleJobBean bundleJob = addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-01-01T01:00Z"));
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob.getId(), "SUCCEEDED", 0);
        BundleActionBean bundleAction = addRecordToBundleActionTable(bundleJob.getId(), coordJob.getId(), coordJob.getAppName(), 0,
                Job.Status.SUCCEEDED);

        WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(wfJob.getId());
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(wfAction.getId());
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(coordJob.getId());
        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(coordAction.getId());
        BundleJobGetJPAExecutor bundleJobGetCmd = new BundleJobGetJPAExecutor(bundleJob.getId());
        BundleActionGetJPAExecutor bundleActionGetCmd = new BundleActionGetJPAExecutor(bundleJob.getId(), coordJob.getAppName());

        wfJob = jpaService.execute(wfJobGetCmd);
        wfAction = jpaService.execute(wfActionGetCmd);
        coordJob = jpaService.execute(coordJobGetCmd);
        coordAction = jpaService.execute(coordActionGetCmd);
        bundleJob = jpaService.execute(bundleJobGetCmd);
        bundleAction = jpaService.execute(bundleActionGetCmd);
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction.getStatus());
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction.getStatus());
        assertEquals(BundleJobBean.Status.SUCCEEDED, bundleJob.getStatus());
        assertEquals(BundleJobBean.Status.SUCCEEDED, bundleAction.getStatus());

        new PurgeXCommand(getNumDaysToNotBePurged(wfJob.getEndTime()),
                getNumDaysToNotBePurged(coordJob.getLastModifiedTime()), 7, 10).call();

        try {
            jpaService.execute(bundleJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Bundle Job should not have been purged");
        }

        try {
            jpaService.execute(bundleActionGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Bundle Action should not have been purged");
        }

        try {
            jpaService.execute(coordJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Job should not have been purged");
        }

        try {
            jpaService.execute(coordActionGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action should not have been purged");
        }

        try {
            jpaService.execute(wfJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Job should not have been purged");
        }

        try {
            jpaService.execute(wfActionGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action should not have been purged");
        }
    }

    /**
     * Test : The workflow and coordinator should not get purged, but the bundle parent should get purged --> none will get purged
     * There are more coordinator children than the limit
     *
     * @throws Exception
     */
    public void testPurgeBundleWithCoordChildWithWFChild2MoreThanLimit() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        BundleJobBean bundleJob = addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-01-01T01:00Z"));
        CoordinatorJobBean coordJob1 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        CoordinatorJobBean coordJob2 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        coordJob2.setAppName("coord2");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob2);
        CoordinatorJobBean coordJob3 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        coordJob3.setAppName("coord3");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob3);
        CoordinatorJobBean coordJob4 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        coordJob4.setAppName("coord4");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob4);
        CoordinatorJobBean coordJob5 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        coordJob5.setAppName("coord5");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob5);
        WorkflowJobBean wfJob1 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob2 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob3 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob4 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob5 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction1 = addRecordToWfActionTable(wfJob1.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction2 = addRecordToWfActionTable(wfJob2.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction3 = addRecordToWfActionTable(wfJob3.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction4 = addRecordToWfActionTable(wfJob4.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction5 = addRecordToWfActionTable(wfJob5.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction1 = addRecordToCoordActionTable(coordJob1.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob1.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction2 = addRecordToCoordActionTable(coordJob2.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob2.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction3 = addRecordToCoordActionTable(coordJob3.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob3.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction4 = addRecordToCoordActionTable(coordJob4.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob4.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction5 = addRecordToCoordActionTable(coordJob5.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob5.getId(), "SUCCEEDED", 0);
        BundleActionBean bundleAction1 = addRecordToBundleActionTable(bundleJob.getId(), coordJob1.getId(), coordJob1.getAppName(),
                0, Job.Status.SUCCEEDED);
        BundleActionBean bundleAction2 = addRecordToBundleActionTable(bundleJob.getId(), coordJob2.getId(), coordJob2.getAppName(),
                0, Job.Status.SUCCEEDED);
        BundleActionBean bundleAction3 = addRecordToBundleActionTable(bundleJob.getId(), coordJob3.getId(), coordJob3.getAppName(),
                0, Job.Status.SUCCEEDED);
        BundleActionBean bundleAction4 = addRecordToBundleActionTable(bundleJob.getId(), coordJob4.getId(), coordJob4.getAppName(),
                0, Job.Status.SUCCEEDED);
        BundleActionBean bundleAction5 = addRecordToBundleActionTable(bundleJob.getId(), coordJob5.getId(), coordJob5.getAppName(),
                0, Job.Status.SUCCEEDED);

        WorkflowJobGetJPAExecutor wfJob1GetCmd = new WorkflowJobGetJPAExecutor(wfJob1.getId());
        WorkflowJobGetJPAExecutor wfJob2GetCmd = new WorkflowJobGetJPAExecutor(wfJob2.getId());
        WorkflowJobGetJPAExecutor wfJob3GetCmd = new WorkflowJobGetJPAExecutor(wfJob3.getId());
        WorkflowJobGetJPAExecutor wfJob4GetCmd = new WorkflowJobGetJPAExecutor(wfJob4.getId());
        WorkflowJobGetJPAExecutor wfJob5GetCmd = new WorkflowJobGetJPAExecutor(wfJob5.getId());
        WorkflowActionGetJPAExecutor wfAction1GetCmd = new WorkflowActionGetJPAExecutor(wfAction1.getId());
        WorkflowActionGetJPAExecutor wfAction2GetCmd = new WorkflowActionGetJPAExecutor(wfAction2.getId());
        WorkflowActionGetJPAExecutor wfAction3GetCmd = new WorkflowActionGetJPAExecutor(wfAction3.getId());
        WorkflowActionGetJPAExecutor wfAction4GetCmd = new WorkflowActionGetJPAExecutor(wfAction4.getId());
        WorkflowActionGetJPAExecutor wfAction5GetCmd = new WorkflowActionGetJPAExecutor(wfAction5.getId());
        CoordJobGetJPAExecutor coordJob1GetCmd = new CoordJobGetJPAExecutor(coordJob1.getId());
        CoordJobGetJPAExecutor coordJob2GetCmd = new CoordJobGetJPAExecutor(coordJob2.getId());
        CoordJobGetJPAExecutor coordJob3GetCmd = new CoordJobGetJPAExecutor(coordJob3.getId());
        CoordJobGetJPAExecutor coordJob4GetCmd = new CoordJobGetJPAExecutor(coordJob4.getId());
        CoordJobGetJPAExecutor coordJob5GetCmd = new CoordJobGetJPAExecutor(coordJob5.getId());
        CoordActionGetJPAExecutor coordAction1GetCmd = new CoordActionGetJPAExecutor(coordAction1.getId());
        CoordActionGetJPAExecutor coordAction2GetCmd = new CoordActionGetJPAExecutor(coordAction2.getId());
        CoordActionGetJPAExecutor coordAction3GetCmd = new CoordActionGetJPAExecutor(coordAction3.getId());
        CoordActionGetJPAExecutor coordAction4GetCmd = new CoordActionGetJPAExecutor(coordAction4.getId());
        CoordActionGetJPAExecutor coordAction5GetCmd = new CoordActionGetJPAExecutor(coordAction5.getId());
        BundleJobGetJPAExecutor bundleJobGetCmd = new BundleJobGetJPAExecutor(bundleJob.getId());
        BundleActionGetJPAExecutor bundleAction1GetCmd = new BundleActionGetJPAExecutor(bundleJob.getId(), coordJob1.getAppName());
        BundleActionGetJPAExecutor bundleAction2GetCmd = new BundleActionGetJPAExecutor(bundleJob.getId(), coordJob2.getAppName());
        BundleActionGetJPAExecutor bundleAction3GetCmd = new BundleActionGetJPAExecutor(bundleJob.getId(), coordJob3.getAppName());
        BundleActionGetJPAExecutor bundleAction4GetCmd = new BundleActionGetJPAExecutor(bundleJob.getId(), coordJob4.getAppName());
        BundleActionGetJPAExecutor bundleAction5GetCmd = new BundleActionGetJPAExecutor(bundleJob.getId(), coordJob5.getAppName());

        wfJob1 = jpaService.execute(wfJob1GetCmd);
        wfJob2 = jpaService.execute(wfJob2GetCmd);
        wfJob3 = jpaService.execute(wfJob3GetCmd);
        wfJob4 = jpaService.execute(wfJob4GetCmd);
        wfJob5 = jpaService.execute(wfJob5GetCmd);
        wfAction1 = jpaService.execute(wfAction1GetCmd);
        wfAction2 = jpaService.execute(wfAction2GetCmd);
        wfAction3 = jpaService.execute(wfAction3GetCmd);
        wfAction4 = jpaService.execute(wfAction4GetCmd);
        wfAction5 = jpaService.execute(wfAction5GetCmd);
        coordJob1 = jpaService.execute(coordJob1GetCmd);
        coordJob2 = jpaService.execute(coordJob2GetCmd);
        coordJob3 = jpaService.execute(coordJob3GetCmd);
        coordJob4 = jpaService.execute(coordJob4GetCmd);
        coordJob5 = jpaService.execute(coordJob5GetCmd);
        coordAction1 = jpaService.execute(coordAction1GetCmd);
        coordAction2 = jpaService.execute(coordAction2GetCmd);
        coordAction3 = jpaService.execute(coordAction3GetCmd);
        coordAction4 = jpaService.execute(coordAction4GetCmd);
        coordAction5 = jpaService.execute(coordAction5GetCmd);
        bundleJob = jpaService.execute(bundleJobGetCmd);
        bundleAction1 = jpaService.execute(bundleAction1GetCmd);
        bundleAction2 = jpaService.execute(bundleAction2GetCmd);
        bundleAction3 = jpaService.execute(bundleAction3GetCmd);
        bundleAction4 = jpaService.execute(bundleAction4GetCmd);
        bundleAction5 = jpaService.execute(bundleAction5GetCmd);
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob1.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob2.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob3.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob4.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob5.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction1.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction2.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction3.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction4.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction5.getStatus());
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob1.getStatus());
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob2.getStatus());
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob3.getStatus());
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob4.getStatus());
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob5.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction1.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction2.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction3.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction4.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction5.getStatus());
        assertEquals(BundleJobBean.Status.SUCCEEDED, bundleJob.getStatus());
        assertEquals(BundleJobBean.Status.SUCCEEDED, bundleAction1.getStatus());
        assertEquals(BundleJobBean.Status.SUCCEEDED, bundleAction2.getStatus());
        assertEquals(BundleJobBean.Status.SUCCEEDED, bundleAction3.getStatus());
        assertEquals(BundleJobBean.Status.SUCCEEDED, bundleAction4.getStatus());
        assertEquals(BundleJobBean.Status.SUCCEEDED, bundleAction5.getStatus());

        new PurgeXCommand(getNumDaysToNotBePurged(wfJob1.getEndTime()),
                getNumDaysToNotBePurged(coordJob1.getLastModifiedTime()), 7, 3).call();

        try {
            jpaService.execute(bundleJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Bundle Job should not have been purged");
        }

        try {
            jpaService.execute(bundleAction1GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Bundle Action 1 should not have been purged");
        }

        try {
            jpaService.execute(bundleAction2GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Bundle Action 2 should not have been purged");
        }

        try {
            jpaService.execute(bundleAction3GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Bundle Action 3 should not have been purged");
        }

        try {
            jpaService.execute(bundleAction4GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Bundle Action 4 should not have been purged");
        }

        try {
            jpaService.execute(bundleAction5GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Bundle Action 5 should not have been purged");
        }

        try {
            jpaService.execute(coordJob1GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Job 1 should not have been purged");
        }

        try {
            jpaService.execute(coordJob2GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Job 2 should not have been purged");
        }

        try {
            jpaService.execute(coordJob3GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Job 3 should not have been purged");
        }

        try {
            jpaService.execute(coordJob4GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Job 4 should not have been purged");
        }

        try {
            jpaService.execute(coordJob5GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Job 5 should not have been purged");
        }

        try {
            jpaService.execute(coordAction1GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action 1 should not have been purged");
        }

        try {
            jpaService.execute(coordAction2GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action 2 should not have been purged");
        }

        try {
            jpaService.execute(coordAction3GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action 3 should not have been purged");
        }

        try {
            jpaService.execute(coordAction4GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action 4 should not have been purged");
        }

        try {
            jpaService.execute(coordAction5GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action 5 should not have been purged");
        }

        try {
            jpaService.execute(wfJob1GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Job 1 should not have been purged");
        }

        try {
            jpaService.execute(wfJob2GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Job 2 should not have been purged");
        }

        try {
            jpaService.execute(wfJob3GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Job 3 should not have been purged");
        }

        try {
            jpaService.execute(wfJob4GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Job 4 should not have been purged");
        }

        try {
            jpaService.execute(wfJob5GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Job 5 should not have been purged");
        }

        try {
            jpaService.execute(wfAction1GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action 1 should not have been purged");
        }

        try {
            jpaService.execute(wfAction2GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action 2 should not have been purged");
        }

        try {
            jpaService.execute(wfAction3GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action 3 should not have been purged");
        }

        try {
            jpaService.execute(wfAction4GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action 4 should not have been purged");
        }

        try {
            jpaService.execute(wfAction5GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action 5 should not have been purged");
        }
    }

    /**
     * Test : The workflow and coordinator should get purged, and the bundle parent should get purged --> all will get purged
     *
     * @throws Exception
     */
    public void testPurgeBundleWithCoordChildWithWFChild3() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        BundleJobBean bundleJob = addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-01-01T01:00Z"));
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob.getId(), "SUCCEEDED", 0);
        BundleActionBean bundleAction = addRecordToBundleActionTable(bundleJob.getId(), coordJob.getId(), coordJob.getAppName(), 0,
                Job.Status.SUCCEEDED);

        WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(wfJob.getId());
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(wfAction.getId());
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(coordJob.getId());
        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(coordAction.getId());
        BundleJobGetJPAExecutor bundleJobGetCmd = new BundleJobGetJPAExecutor(bundleJob.getId());
        BundleActionGetJPAExecutor bundleActionGetCmd = new BundleActionGetJPAExecutor(bundleJob.getId(), coordJob.getAppName());

        wfJob = jpaService.execute(wfJobGetCmd);
        wfAction = jpaService.execute(wfActionGetCmd);
        coordJob = jpaService.execute(coordJobGetCmd);
        coordAction = jpaService.execute(coordActionGetCmd);
        bundleJob = jpaService.execute(bundleJobGetCmd);
        bundleAction = jpaService.execute(bundleActionGetCmd);
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction.getStatus());
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction.getStatus());
        assertEquals(BundleJobBean.Status.SUCCEEDED, bundleJob.getStatus());
        assertEquals(BundleJobBean.Status.SUCCEEDED, bundleAction.getStatus());

        new PurgeXCommand(7, 7, 7, 10).call();

        try {
            jpaService.execute(bundleJobGetCmd);
            fail("Bundle Job should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(bundleActionGetCmd);
            fail("Bundle Action should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(coordJobGetCmd);
            fail("Coordinator Job should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(coordActionGetCmd);
            fail("Coordinator Action should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(wfJobGetCmd);
            fail("Workflow Job should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(wfActionGetCmd);
            fail("Workflow Action should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }
    }

    /**
     * Test : The workflow and coordinator should get purged, and the bundle parent should get purged --> all will get purged
     * There are more coordinator children than the limit
     *
     * @throws Exception
     */
    public void testPurgeBundleWithCoordChildWithWFChild3MoreThanLimit() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        BundleJobBean bundleJob = addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-01-01T01:00Z"));
        CoordinatorJobBean coordJob1 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        CoordinatorJobBean coordJob2 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        coordJob2.setAppName("coord2");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob2);
        CoordinatorJobBean coordJob3 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        coordJob3.setAppName("coord3");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob3);
        CoordinatorJobBean coordJob4 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        coordJob4.setAppName("coord4");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob4);
        CoordinatorJobBean coordJob5 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        coordJob5.setAppName("coord5");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob5);
        WorkflowJobBean wfJob1 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob2 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob3 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob4 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob5 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction1 = addRecordToWfActionTable(wfJob1.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction2 = addRecordToWfActionTable(wfJob2.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction3 = addRecordToWfActionTable(wfJob3.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction4 = addRecordToWfActionTable(wfJob4.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction5 = addRecordToWfActionTable(wfJob5.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction1 = addRecordToCoordActionTable(coordJob1.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob1.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction2 = addRecordToCoordActionTable(coordJob2.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob2.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction3 = addRecordToCoordActionTable(coordJob3.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob3.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction4 = addRecordToCoordActionTable(coordJob4.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob4.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction5 = addRecordToCoordActionTable(coordJob5.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob5.getId(), "SUCCEEDED", 0);
        BundleActionBean bundleAction1 = addRecordToBundleActionTable(bundleJob.getId(), coordJob1.getId(), coordJob1.getAppName(),
                0, Job.Status.SUCCEEDED);
        BundleActionBean bundleAction2 = addRecordToBundleActionTable(bundleJob.getId(), coordJob2.getId(), coordJob2.getAppName(),
                0, Job.Status.SUCCEEDED);
        BundleActionBean bundleAction3 = addRecordToBundleActionTable(bundleJob.getId(), coordJob3.getId(), coordJob3.getAppName(),
                0, Job.Status.SUCCEEDED);
        BundleActionBean bundleAction4 = addRecordToBundleActionTable(bundleJob.getId(), coordJob4.getId(), coordJob4.getAppName(),
                0, Job.Status.SUCCEEDED);
        BundleActionBean bundleAction5 = addRecordToBundleActionTable(bundleJob.getId(), coordJob5.getId(), coordJob5.getAppName(),
                0, Job.Status.SUCCEEDED);

        WorkflowJobGetJPAExecutor wfJob1GetCmd = new WorkflowJobGetJPAExecutor(wfJob1.getId());
        WorkflowJobGetJPAExecutor wfJob2GetCmd = new WorkflowJobGetJPAExecutor(wfJob2.getId());
        WorkflowJobGetJPAExecutor wfJob3GetCmd = new WorkflowJobGetJPAExecutor(wfJob3.getId());
        WorkflowJobGetJPAExecutor wfJob4GetCmd = new WorkflowJobGetJPAExecutor(wfJob4.getId());
        WorkflowJobGetJPAExecutor wfJob5GetCmd = new WorkflowJobGetJPAExecutor(wfJob5.getId());
        WorkflowActionGetJPAExecutor wfAction1GetCmd = new WorkflowActionGetJPAExecutor(wfAction1.getId());
        WorkflowActionGetJPAExecutor wfAction2GetCmd = new WorkflowActionGetJPAExecutor(wfAction2.getId());
        WorkflowActionGetJPAExecutor wfAction3GetCmd = new WorkflowActionGetJPAExecutor(wfAction3.getId());
        WorkflowActionGetJPAExecutor wfAction4GetCmd = new WorkflowActionGetJPAExecutor(wfAction4.getId());
        WorkflowActionGetJPAExecutor wfAction5GetCmd = new WorkflowActionGetJPAExecutor(wfAction5.getId());
        CoordJobGetJPAExecutor coordJob1GetCmd = new CoordJobGetJPAExecutor(coordJob1.getId());
        CoordJobGetJPAExecutor coordJob2GetCmd = new CoordJobGetJPAExecutor(coordJob2.getId());
        CoordJobGetJPAExecutor coordJob3GetCmd = new CoordJobGetJPAExecutor(coordJob3.getId());
        CoordJobGetJPAExecutor coordJob4GetCmd = new CoordJobGetJPAExecutor(coordJob4.getId());
        CoordJobGetJPAExecutor coordJob5GetCmd = new CoordJobGetJPAExecutor(coordJob5.getId());
        CoordActionGetJPAExecutor coordAction1GetCmd = new CoordActionGetJPAExecutor(coordAction1.getId());
        CoordActionGetJPAExecutor coordAction2GetCmd = new CoordActionGetJPAExecutor(coordAction2.getId());
        CoordActionGetJPAExecutor coordAction3GetCmd = new CoordActionGetJPAExecutor(coordAction3.getId());
        CoordActionGetJPAExecutor coordAction4GetCmd = new CoordActionGetJPAExecutor(coordAction4.getId());
        CoordActionGetJPAExecutor coordAction5GetCmd = new CoordActionGetJPAExecutor(coordAction5.getId());
        BundleJobGetJPAExecutor bundleJobGetCmd = new BundleJobGetJPAExecutor(bundleJob.getId());
        BundleActionGetJPAExecutor bundleAction1GetCmd = new BundleActionGetJPAExecutor(bundleJob.getId(), coordJob1.getAppName());
        BundleActionGetJPAExecutor bundleAction2GetCmd = new BundleActionGetJPAExecutor(bundleJob.getId(), coordJob2.getAppName());
        BundleActionGetJPAExecutor bundleAction3GetCmd = new BundleActionGetJPAExecutor(bundleJob.getId(), coordJob3.getAppName());
        BundleActionGetJPAExecutor bundleAction4GetCmd = new BundleActionGetJPAExecutor(bundleJob.getId(), coordJob4.getAppName());
        BundleActionGetJPAExecutor bundleAction5GetCmd = new BundleActionGetJPAExecutor(bundleJob.getId(), coordJob5.getAppName());

        wfJob1 = jpaService.execute(wfJob1GetCmd);
        wfJob2 = jpaService.execute(wfJob2GetCmd);
        wfJob3 = jpaService.execute(wfJob3GetCmd);
        wfJob4 = jpaService.execute(wfJob4GetCmd);
        wfJob5 = jpaService.execute(wfJob5GetCmd);
        wfAction1 = jpaService.execute(wfAction1GetCmd);
        wfAction2 = jpaService.execute(wfAction2GetCmd);
        wfAction3 = jpaService.execute(wfAction3GetCmd);
        wfAction4 = jpaService.execute(wfAction4GetCmd);
        wfAction5 = jpaService.execute(wfAction5GetCmd);
        coordJob1 = jpaService.execute(coordJob1GetCmd);
        coordJob2 = jpaService.execute(coordJob2GetCmd);
        coordJob3 = jpaService.execute(coordJob3GetCmd);
        coordJob4 = jpaService.execute(coordJob4GetCmd);
        coordJob5 = jpaService.execute(coordJob5GetCmd);
        coordAction1 = jpaService.execute(coordAction1GetCmd);
        coordAction2 = jpaService.execute(coordAction2GetCmd);
        coordAction3 = jpaService.execute(coordAction3GetCmd);
        coordAction4 = jpaService.execute(coordAction4GetCmd);
        coordAction5 = jpaService.execute(coordAction5GetCmd);
        bundleJob = jpaService.execute(bundleJobGetCmd);
        bundleAction1 = jpaService.execute(bundleAction1GetCmd);
        bundleAction2 = jpaService.execute(bundleAction2GetCmd);
        bundleAction3 = jpaService.execute(bundleAction3GetCmd);
        bundleAction4 = jpaService.execute(bundleAction4GetCmd);
        bundleAction5 = jpaService.execute(bundleAction5GetCmd);
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob1.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob2.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob3.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob4.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob5.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction1.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction2.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction3.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction4.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction5.getStatus());
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob1.getStatus());
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob2.getStatus());
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob3.getStatus());
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob4.getStatus());
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob5.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction1.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction2.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction3.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction4.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction5.getStatus());
        assertEquals(BundleJobBean.Status.SUCCEEDED, bundleJob.getStatus());
        assertEquals(BundleJobBean.Status.SUCCEEDED, bundleAction1.getStatus());
        assertEquals(BundleJobBean.Status.SUCCEEDED, bundleAction2.getStatus());
        assertEquals(BundleJobBean.Status.SUCCEEDED, bundleAction3.getStatus());
        assertEquals(BundleJobBean.Status.SUCCEEDED, bundleAction4.getStatus());
        assertEquals(BundleJobBean.Status.SUCCEEDED, bundleAction5.getStatus());

        new PurgeXCommand(7, 7, 7, 10).call();

        try {
            jpaService.execute(bundleJobGetCmd);
            fail("Bundle Job should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(bundleAction1GetCmd);
            fail("Bundle Action 1 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(bundleAction2GetCmd);
            fail("Bundle Action 2 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(bundleAction3GetCmd);
            fail("Bundle Action 3 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(bundleAction4GetCmd);
            fail("Bundle Action 4 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(bundleAction5GetCmd);
            fail("Bundle Action 5 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(coordJob1GetCmd);
            fail("Coordinator Job 1 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(coordJob2GetCmd);
            fail("Coordinator Job 2 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(coordJob3GetCmd);
            fail("Coordinator Job 3 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(coordJob4GetCmd);
            fail("Coordinator Job 4 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(coordJob5GetCmd);
            fail("Coordinator Job 5 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(coordAction1GetCmd);
            fail("Coordinator Action 1 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(coordAction2GetCmd);
            fail("Coordinator Action 2 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(coordAction3GetCmd);
            fail("Coordinator Action 3 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(coordAction4GetCmd);
            fail("Coordinator Action 4 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(coordAction5GetCmd);
            fail("Coordinator Action 5 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(wfJob1GetCmd);
            fail("Workflow Job 1 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(wfJob2GetCmd);
            fail("Workflow Job 2 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(wfJob3GetCmd);
            fail("Workflow Job 3 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(wfJob4GetCmd);
            fail("Workflow Job 4 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(wfJob5GetCmd);
            fail("Workflow Job 5 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(wfAction1GetCmd);
            fail("Workflow Action 1 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(wfAction2GetCmd);
            fail("Workflow Action 2 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(wfAction3GetCmd);
            fail("Workflow Action 3 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(wfAction4GetCmd);
            fail("Workflow Action 4 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(wfAction5GetCmd);
            fail("Workflow Action 5 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }
    }

    /**
     * Test : The subworkflow should get purged, but the workflow parent shouldn't get purged --> neither will get purged
     *
     * @throws Exception
     */
    public void testPurgeWFWithSubWF1() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        WorkflowJobBean wfJob = addRecordToWfJobTableForNegCase(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowJobBean subwfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        WorkflowActionBean subwfAction = addRecordToWfActionTable(subwfJob.getId(), "1", WorkflowAction.Status.OK);

        WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(wfJob.getId());
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(wfAction.getId());
        WorkflowJobGetJPAExecutor subwfJobGetCmd = new WorkflowJobGetJPAExecutor(subwfJob.getId());
        WorkflowActionGetJPAExecutor subwfActionGetCmd = new WorkflowActionGetJPAExecutor(subwfAction.getId());

        wfJob = jpaService.execute(wfJobGetCmd);
        wfAction = jpaService.execute(wfActionGetCmd);
        subwfJob = jpaService.execute(subwfJobGetCmd);
        subwfAction = jpaService.execute(subwfActionGetCmd);
        assertEquals(WorkflowJob.Status.RUNNING, wfJob.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, subwfJob.getStatus());
        assertEquals(WorkflowAction.Status.OK, subwfAction.getStatus());

        new PurgeXCommand(7, 1, 1, 10).call();

        try {
            jpaService.execute(wfJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Job should not have been purged");
        }

        try {
            jpaService.execute(wfActionGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action should not have been purged");
        }

        try {
            jpaService.execute(subwfJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("SubWorkflow Job should not have been purged");
        }

        try {
            jpaService.execute(subwfActionGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("SubWorkflow Action should not have been purged");
        }
    }

    /**
     * Test : The subworkflow shouldn't get purged, but the workflow parent should get purged --> neither will get purged
     *
     * @throws Exception
     */
    public void testPurgeWFWithSubWF2() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        WorkflowJobBean wfJob = addRecordToWfJobTableForNegCase(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowJobBean subwfJob = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING,
                wfJob.getId());
        WorkflowActionBean subwfAction = addRecordToWfActionTable(subwfJob.getId(), "1", WorkflowAction.Status.RUNNING);

        WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(wfJob.getId());
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(wfAction.getId());
        WorkflowJobGetJPAExecutor subwfJobGetCmd = new WorkflowJobGetJPAExecutor(subwfJob.getId());
        WorkflowActionGetJPAExecutor subwfActionGetCmd = new WorkflowActionGetJPAExecutor(subwfAction.getId());

        wfJob = jpaService.execute(wfJobGetCmd);
        wfAction = jpaService.execute(wfActionGetCmd);
        subwfJob = jpaService.execute(subwfJobGetCmd);
        subwfAction = jpaService.execute(subwfActionGetCmd);
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction.getStatus());
        assertEquals(WorkflowJob.Status.RUNNING, subwfJob.getStatus());
        assertEquals(WorkflowAction.Status.RUNNING, subwfAction.getStatus());

        new PurgeXCommand(7, 1, 1, 10).call();

        try {
            jpaService.execute(wfJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Job should not have been purged");
        }

        try {
            jpaService.execute(wfActionGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action should not have been purged");
        }

        try {
            jpaService.execute(subwfJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("SubWorkflow Job should not have been purged");
        }

        try {
            jpaService.execute(subwfActionGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("SubWorkflow Action should not have been purged");
        }
    }

    /**
     * Test : The subworkflows shouldn't get purged, but the workflow parent should get purged --> none will get purged
     * There are more subworkflow children than the limit
     *
     * @throws Exception
     */
    public void testPurgeWFWithSubWF2MoreThanLimit() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        WorkflowJobBean wfJob = addRecordToWfJobTableForNegCase(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction1 = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction2 = addRecordToWfActionTable(wfJob.getId(), "2", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction3 = addRecordToWfActionTable(wfJob.getId(), "3", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction4 = addRecordToWfActionTable(wfJob.getId(), "4", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction5 = addRecordToWfActionTable(wfJob.getId(), "5", WorkflowAction.Status.OK);
        WorkflowJobBean subwfJob1 = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING,
                wfJob.getId());
        WorkflowJobBean subwfJob2 = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING,
                wfJob.getId());
        WorkflowJobBean subwfJob3 = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING,
                wfJob.getId());
        WorkflowJobBean subwfJob4 = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING,
                wfJob.getId());
        WorkflowJobBean subwfJob5 = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING,
                wfJob.getId());
        WorkflowActionBean subwfAction1 = addRecordToWfActionTable(subwfJob1.getId(), "1", WorkflowAction.Status.RUNNING);
        WorkflowActionBean subwfAction2 = addRecordToWfActionTable(subwfJob2.getId(), "1", WorkflowAction.Status.RUNNING);
        WorkflowActionBean subwfAction3 = addRecordToWfActionTable(subwfJob3.getId(), "1", WorkflowAction.Status.RUNNING);
        WorkflowActionBean subwfAction4 = addRecordToWfActionTable(subwfJob4.getId(), "1", WorkflowAction.Status.RUNNING);
        WorkflowActionBean subwfAction5 = addRecordToWfActionTable(subwfJob5.getId(), "1", WorkflowAction.Status.RUNNING);

        WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(wfJob.getId());
        WorkflowActionGetJPAExecutor wfAction1GetCmd = new WorkflowActionGetJPAExecutor(wfAction1.getId());
        WorkflowActionGetJPAExecutor wfAction2GetCmd = new WorkflowActionGetJPAExecutor(wfAction2.getId());
        WorkflowActionGetJPAExecutor wfAction3GetCmd = new WorkflowActionGetJPAExecutor(wfAction3.getId());
        WorkflowActionGetJPAExecutor wfAction4GetCmd = new WorkflowActionGetJPAExecutor(wfAction4.getId());
        WorkflowActionGetJPAExecutor wfAction5GetCmd = new WorkflowActionGetJPAExecutor(wfAction5.getId());
        WorkflowJobGetJPAExecutor subwfJob1GetCmd = new WorkflowJobGetJPAExecutor(subwfJob1.getId());
        WorkflowJobGetJPAExecutor subwfJob2GetCmd = new WorkflowJobGetJPAExecutor(subwfJob2.getId());
        WorkflowJobGetJPAExecutor subwfJob3GetCmd = new WorkflowJobGetJPAExecutor(subwfJob3.getId());
        WorkflowJobGetJPAExecutor subwfJob4GetCmd = new WorkflowJobGetJPAExecutor(subwfJob4.getId());
        WorkflowJobGetJPAExecutor subwfJob5GetCmd = new WorkflowJobGetJPAExecutor(subwfJob5.getId());
        WorkflowActionGetJPAExecutor subwfAction1GetCmd = new WorkflowActionGetJPAExecutor(subwfAction1.getId());
        WorkflowActionGetJPAExecutor subwfAction2GetCmd = new WorkflowActionGetJPAExecutor(subwfAction2.getId());
        WorkflowActionGetJPAExecutor subwfAction3GetCmd = new WorkflowActionGetJPAExecutor(subwfAction3.getId());
        WorkflowActionGetJPAExecutor subwfAction4GetCmd = new WorkflowActionGetJPAExecutor(subwfAction4.getId());
        WorkflowActionGetJPAExecutor subwfAction5GetCmd = new WorkflowActionGetJPAExecutor(subwfAction5.getId());

        wfJob = jpaService.execute(wfJobGetCmd);
        wfAction1 = jpaService.execute(wfAction1GetCmd);
        wfAction2 = jpaService.execute(wfAction2GetCmd);
        wfAction3 = jpaService.execute(wfAction3GetCmd);
        wfAction4 = jpaService.execute(wfAction4GetCmd);
        wfAction5 = jpaService.execute(wfAction5GetCmd);
        subwfJob1 = jpaService.execute(subwfJob1GetCmd);
        subwfJob2 = jpaService.execute(subwfJob2GetCmd);
        subwfJob3 = jpaService.execute(subwfJob3GetCmd);
        subwfJob4 = jpaService.execute(subwfJob4GetCmd);
        subwfJob5 = jpaService.execute(subwfJob5GetCmd);
        subwfAction1 = jpaService.execute(subwfAction1GetCmd);
        subwfAction2 = jpaService.execute(subwfAction2GetCmd);
        subwfAction3 = jpaService.execute(subwfAction3GetCmd);
        subwfAction4 = jpaService.execute(subwfAction4GetCmd);
        subwfAction5 = jpaService.execute(subwfAction5GetCmd);
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction1.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction2.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction3.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction4.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction5.getStatus());
        assertEquals(WorkflowJob.Status.RUNNING, subwfJob1.getStatus());
        assertEquals(WorkflowJob.Status.RUNNING, subwfJob2.getStatus());
        assertEquals(WorkflowJob.Status.RUNNING, subwfJob3.getStatus());
        assertEquals(WorkflowJob.Status.RUNNING, subwfJob4.getStatus());
        assertEquals(WorkflowJob.Status.RUNNING, subwfJob5.getStatus());
        assertEquals(WorkflowAction.Status.RUNNING, subwfAction1.getStatus());
        assertEquals(WorkflowAction.Status.RUNNING, subwfAction2.getStatus());
        assertEquals(WorkflowAction.Status.RUNNING, subwfAction3.getStatus());
        assertEquals(WorkflowAction.Status.RUNNING, subwfAction4.getStatus());
        assertEquals(WorkflowAction.Status.RUNNING, subwfAction5.getStatus());

        new PurgeXCommand(7, 1, 1, 10).call();

        try {
            jpaService.execute(wfJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Job should not have been purged");
        }

        try {
            jpaService.execute(wfAction1GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action 1 should not have been purged");
        }

        try {
            jpaService.execute(wfAction2GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action 2 should not have been purged");
        }

        try {
            jpaService.execute(wfAction3GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action 3 should not have been purged");
        }

        try {
            jpaService.execute(wfAction4GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action 4 should not have been purged");
        }

        try {
            jpaService.execute(wfAction5GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action 5 should not have been purged");
        }

        try {
            jpaService.execute(subwfJob1GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("SubWorkflow Job 1 should not have been purged");
        }

        try {
            jpaService.execute(subwfJob2GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("SubWorkflow Job 2 should not have been purged");
        }

        try {
            jpaService.execute(subwfJob3GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("SubWorkflow Job 3 should not have been purged");
        }

        try {
            jpaService.execute(subwfJob4GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("SubWorkflow Job 4 should not have been purged");
        }

        try {
            jpaService.execute(subwfJob5GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("SubWorkflow Job 5 should not have been purged");
        }

        try {
            jpaService.execute(subwfAction1GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("SubWorkflow Action 1 should not have been purged");
        }

        try {
            jpaService.execute(subwfAction2GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("SubWorkflow Action 2 should not have been purged");
        }

        try {
            jpaService.execute(subwfAction3GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("SubWorkflow Action 3 should not have been purged");
        }

        try {
            jpaService.execute(subwfAction4GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("SubWorkflow Action 4 should not have been purged");
        }

        try {
            jpaService.execute(subwfAction5GetCmd);
        }
        catch (JPAExecutorException je) {
            fail("SubWorkflow Action 5 should not have been purged");
        }
    }

    /**
     * Test : The subworkflow should get purged, and the workflow parent should get purged --> both will get purged
     *
     * @throws Exception
     */
    public void testPurgeWFWithSubWF3() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowJobBean subwfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        WorkflowActionBean subwfAction = addRecordToWfActionTable(subwfJob.getId(), "1", WorkflowAction.Status.OK);

        WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(wfJob.getId());
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(wfAction.getId());
        WorkflowJobGetJPAExecutor subwfJobGetCmd = new WorkflowJobGetJPAExecutor(subwfJob.getId());
        WorkflowActionGetJPAExecutor subwfActionGetCmd = new WorkflowActionGetJPAExecutor(subwfAction.getId());

        wfJob = jpaService.execute(wfJobGetCmd);
        wfAction = jpaService.execute(wfActionGetCmd);
        subwfJob = jpaService.execute(subwfJobGetCmd);
        subwfAction = jpaService.execute(subwfActionGetCmd);
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, subwfJob.getStatus());
        assertEquals(WorkflowAction.Status.OK, subwfAction.getStatus());

        new PurgeXCommand(7, 1, 1, 10).call();

        try {
            jpaService.execute(wfJobGetCmd);
            fail("Workflow Job should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(wfActionGetCmd);
            fail("Workflow Action should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(subwfJobGetCmd);
            fail("SubWorkflow Job should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(subwfActionGetCmd);
            fail("SubWorkflow Action should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }
    }

    /**
     * Test : The subworkflow should get purged, and the workflow parent should get purged --> both will get purged
     * There are more subworkflow children than the limit
     *
     * @throws Exception
     */
    public void testPurgeWFWithSubWF3MoreThanLimit() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction1 = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction2 = addRecordToWfActionTable(wfJob.getId(), "2", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction3 = addRecordToWfActionTable(wfJob.getId(), "3", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction4 = addRecordToWfActionTable(wfJob.getId(), "4", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction5 = addRecordToWfActionTable(wfJob.getId(), "5", WorkflowAction.Status.OK);
        WorkflowJobBean subwfJob1 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        WorkflowJobBean subwfJob2 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        WorkflowJobBean subwfJob3 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        WorkflowJobBean subwfJob4 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        WorkflowJobBean subwfJob5 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        WorkflowActionBean subwfAction1 = addRecordToWfActionTable(subwfJob1.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean subwfAction2 = addRecordToWfActionTable(subwfJob2.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean subwfAction3 = addRecordToWfActionTable(subwfJob3.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean subwfAction4 = addRecordToWfActionTable(subwfJob4.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean subwfAction5 = addRecordToWfActionTable(subwfJob5.getId(), "1", WorkflowAction.Status.OK);

        WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(wfJob.getId());
        WorkflowActionGetJPAExecutor wfAction1GetCmd = new WorkflowActionGetJPAExecutor(wfAction1.getId());
        WorkflowActionGetJPAExecutor wfAction2GetCmd = new WorkflowActionGetJPAExecutor(wfAction2.getId());
        WorkflowActionGetJPAExecutor wfAction3GetCmd = new WorkflowActionGetJPAExecutor(wfAction3.getId());
        WorkflowActionGetJPAExecutor wfAction4GetCmd = new WorkflowActionGetJPAExecutor(wfAction4.getId());
        WorkflowActionGetJPAExecutor wfAction5GetCmd = new WorkflowActionGetJPAExecutor(wfAction5.getId());
        WorkflowJobGetJPAExecutor subwfJob1GetCmd = new WorkflowJobGetJPAExecutor(subwfJob1.getId());
        WorkflowJobGetJPAExecutor subwfJob2GetCmd = new WorkflowJobGetJPAExecutor(subwfJob2.getId());
        WorkflowJobGetJPAExecutor subwfJob3GetCmd = new WorkflowJobGetJPAExecutor(subwfJob3.getId());
        WorkflowJobGetJPAExecutor subwfJob4GetCmd = new WorkflowJobGetJPAExecutor(subwfJob4.getId());
        WorkflowJobGetJPAExecutor subwfJob5GetCmd = new WorkflowJobGetJPAExecutor(subwfJob5.getId());
        WorkflowActionGetJPAExecutor subwfAction1GetCmd = new WorkflowActionGetJPAExecutor(subwfAction1.getId());
        WorkflowActionGetJPAExecutor subwfAction2GetCmd = new WorkflowActionGetJPAExecutor(subwfAction2.getId());
        WorkflowActionGetJPAExecutor subwfAction3GetCmd = new WorkflowActionGetJPAExecutor(subwfAction3.getId());
        WorkflowActionGetJPAExecutor subwfAction4GetCmd = new WorkflowActionGetJPAExecutor(subwfAction4.getId());
        WorkflowActionGetJPAExecutor subwfAction5GetCmd = new WorkflowActionGetJPAExecutor(subwfAction5.getId());

        wfJob = jpaService.execute(wfJobGetCmd);
        wfAction1 = jpaService.execute(wfAction1GetCmd);
        wfAction2 = jpaService.execute(wfAction2GetCmd);
        wfAction3 = jpaService.execute(wfAction3GetCmd);
        wfAction4 = jpaService.execute(wfAction4GetCmd);
        wfAction5 = jpaService.execute(wfAction5GetCmd);
        subwfJob1 = jpaService.execute(subwfJob1GetCmd);
        subwfJob2 = jpaService.execute(subwfJob2GetCmd);
        subwfJob3 = jpaService.execute(subwfJob3GetCmd);
        subwfJob4 = jpaService.execute(subwfJob4GetCmd);
        subwfJob5 = jpaService.execute(subwfJob5GetCmd);
        subwfAction1 = jpaService.execute(subwfAction1GetCmd);
        subwfAction2 = jpaService.execute(subwfAction2GetCmd);
        subwfAction3 = jpaService.execute(subwfAction3GetCmd);
        subwfAction4 = jpaService.execute(subwfAction4GetCmd);
        subwfAction5 = jpaService.execute(subwfAction5GetCmd);
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction1.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction2.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction3.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction4.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction5.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, subwfJob1.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, subwfJob2.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, subwfJob3.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, subwfJob4.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, subwfJob5.getStatus());
        assertEquals(WorkflowAction.Status.OK, subwfAction1.getStatus());
        assertEquals(WorkflowAction.Status.OK, subwfAction2.getStatus());
        assertEquals(WorkflowAction.Status.OK, subwfAction3.getStatus());
        assertEquals(WorkflowAction.Status.OK, subwfAction4.getStatus());
        assertEquals(WorkflowAction.Status.OK, subwfAction5.getStatus());

        new PurgeXCommand(7, 1, 1, 3).call();

        try {
            jpaService.execute(wfJobGetCmd);
            fail("Workflow Job should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(wfAction1GetCmd);
            fail("Workflow Action 1 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(wfAction2GetCmd);
            fail("Workflow Action 2 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(wfAction3GetCmd);
            fail("Workflow Action 3 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(wfAction4GetCmd);
            fail("Workflow Action 4 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(wfAction5GetCmd);
            fail("Workflow Action 5 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(subwfJob1GetCmd);
            fail("SubWorkflow Job 1 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(subwfJob2GetCmd);
            fail("SubWorkflow Job 2 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(subwfJob3GetCmd);
            fail("SubWorkflow Job 3 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(subwfJob4GetCmd);
            fail("SubWorkflow Job 4 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(subwfJob5GetCmd);
            fail("SubWorkflow Job 5 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(subwfAction1GetCmd);
            fail("SubWorkflow Action 1 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(subwfAction2GetCmd);
            fail("SubWorkflow Action 2 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(subwfAction3GetCmd);
            fail("SubWorkflow Action 3 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(subwfAction4GetCmd);
            fail("SubWorkflow Action 4 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(subwfAction5GetCmd);
            fail("SubWorkflow Action 5 should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }
    }

    /**
     * Test : The subworkflow and workflow should get purged, but the coordinator parent shouldn't get purged --> none will get
     * purged
     *
     * @throws Exception
     */
    public void testPurgeCoordWithWFChildWithSubWF1() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowJobBean subwfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        WorkflowActionBean subwfAction = addRecordToWfActionTable(subwfJob.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob.getId(), "SUCCEEDED", 0);

        WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(wfJob.getId());
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(wfAction.getId());
        WorkflowJobGetJPAExecutor subwfJobGetCmd = new WorkflowJobGetJPAExecutor(subwfJob.getId());
        WorkflowActionGetJPAExecutor subwfActionGetCmd = new WorkflowActionGetJPAExecutor(subwfAction.getId());
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(coordJob.getId());
        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(coordAction.getId());

        wfJob = jpaService.execute(wfJobGetCmd);
        wfAction = jpaService.execute(wfActionGetCmd);
        subwfJob = jpaService.execute(subwfJobGetCmd);
        subwfAction = jpaService.execute(subwfActionGetCmd);
        coordJob = jpaService.execute(coordJobGetCmd);
        coordAction = jpaService.execute(coordActionGetCmd);
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, subwfJob.getStatus());
        assertEquals(WorkflowAction.Status.OK, subwfAction.getStatus());
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction.getStatus());

        new PurgeXCommand(7, getNumDaysToNotBePurged(coordJob.getLastModifiedTime()), 1, 10).call();

        try {
            jpaService.execute(coordJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Job should not have been purged");
        }

        try {
            jpaService.execute(coordActionGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action should not have been purged");
        }

        try {
            jpaService.execute(wfJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Job should not have been purged");
        }

        try {
            jpaService.execute(wfActionGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action should not have been purged");
        }

        try {
            jpaService.execute(subwfJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("SubWorkflow Job should not have been purged");
        }

        try {
            jpaService.execute(subwfActionGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("SubWorkflow Action should not have been purged");
        }
    }

    /**
     * Test : The subworkflow and workflow should not get purged, but the coordinator parent should get purged --> none will get
     * purged
     *
     * @throws Exception
     */
    public void testPurgeCoordWithWFChildWithSubWF2() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowJobBean subwfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        WorkflowActionBean subwfAction = addRecordToWfActionTable(subwfJob.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob.getId(), "SUCCEEDED", 0);

        WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(wfJob.getId());
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(wfAction.getId());
        WorkflowJobGetJPAExecutor subwfJobGetCmd = new WorkflowJobGetJPAExecutor(subwfJob.getId());
        WorkflowActionGetJPAExecutor subwfActionGetCmd = new WorkflowActionGetJPAExecutor(subwfAction.getId());
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(coordJob.getId());
        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(coordAction.getId());

        wfJob = jpaService.execute(wfJobGetCmd);
        wfAction = jpaService.execute(wfActionGetCmd);
        subwfJob = jpaService.execute(subwfJobGetCmd);
        subwfAction = jpaService.execute(subwfActionGetCmd);
        coordJob = jpaService.execute(coordJobGetCmd);
        coordAction = jpaService.execute(coordActionGetCmd);
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, subwfJob.getStatus());
        assertEquals(WorkflowAction.Status.OK, subwfAction.getStatus());
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction.getStatus());

        new PurgeXCommand(getNumDaysToNotBePurged(wfJob.getEndTime()), 7, 1, 10).call();

        try {
            jpaService.execute(coordJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Job should not have been purged");
        }

        try {
            jpaService.execute(coordActionGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action should not have been purged");
        }

        try {
            jpaService.execute(wfJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Job should not have been purged");
        }

        try {
            jpaService.execute(wfActionGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action should not have been purged");
        }

        try {
            jpaService.execute(subwfJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("SubWorkflow Job should not have been purged");
        }

        try {
            jpaService.execute(subwfActionGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("SubWorkflow Action should not have been purged");
        }
    }

    /**
     * Test : The subworkflow and workflow should get purged, and the coordinator parent should get purged --> all will get
     * purged
     *
     * @throws Exception
     */
    public void testPurgeCoordWithWFChildWithSubWF3() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowJobBean subwfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        WorkflowActionBean subwfAction = addRecordToWfActionTable(subwfJob.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob.getId(), "SUCCEEDED", 0);

        WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(wfJob.getId());
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(wfAction.getId());
        WorkflowJobGetJPAExecutor subwfJobGetCmd = new WorkflowJobGetJPAExecutor(subwfJob.getId());
        WorkflowActionGetJPAExecutor subwfActionGetCmd = new WorkflowActionGetJPAExecutor(subwfAction.getId());
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(coordJob.getId());
        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(coordAction.getId());

        wfJob = jpaService.execute(wfJobGetCmd);
        wfAction = jpaService.execute(wfActionGetCmd);
        subwfJob = jpaService.execute(subwfJobGetCmd);
        subwfAction = jpaService.execute(subwfActionGetCmd);
        coordJob = jpaService.execute(coordJobGetCmd);
        coordAction = jpaService.execute(coordActionGetCmd);
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, subwfJob.getStatus());
        assertEquals(WorkflowAction.Status.OK, subwfAction.getStatus());
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction.getStatus());

        new PurgeXCommand(7, 7, 1, 10).call();

        try {
            jpaService.execute(coordJobGetCmd);
            fail("Coordinator Job should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(coordActionGetCmd);
            fail("Coordinator Action should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(wfJobGetCmd);
            fail("Workflow Job should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(wfActionGetCmd);
            fail("Workflow Action should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(subwfJobGetCmd);
            fail("SubWorkflow Job should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(subwfActionGetCmd);
            fail("SubWorkflow Action should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }
    }

    /**
     * Test : The subworkflow, workflow, and coordinator should get purged, but the bundle parent shouldn't get purged --> none will
     * get purged
     *
     * @throws Exception
     */
    public void testPurgeBundleWithCoordChildWithWFChildWithSubWF1() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        BundleJobBean bundleJob = addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-01-01T01:00Z"));
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowJobBean subwfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        WorkflowActionBean subwfAction = addRecordToWfActionTable(subwfJob.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob.getId(), "SUCCEEDED", 0);
        BundleActionBean bundleAction = addRecordToBundleActionTable(bundleJob.getId(), coordJob.getId(), coordJob.getAppName(), 0,
                Job.Status.SUCCEEDED);

        WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(wfJob.getId());
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(wfAction.getId());
        WorkflowJobGetJPAExecutor subwfJobGetCmd = new WorkflowJobGetJPAExecutor(subwfJob.getId());
        WorkflowActionGetJPAExecutor subwfActionGetCmd = new WorkflowActionGetJPAExecutor(subwfAction.getId());
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(coordJob.getId());
        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(coordAction.getId());
        BundleJobGetJPAExecutor bundleJobGetCmd = new BundleJobGetJPAExecutor(bundleJob.getId());
        BundleActionGetJPAExecutor bundleActionGetCmd = new BundleActionGetJPAExecutor(bundleJob.getId(), coordJob.getAppName());

        wfJob = jpaService.execute(wfJobGetCmd);
        wfAction = jpaService.execute(wfActionGetCmd);
        subwfJob = jpaService.execute(subwfJobGetCmd);
        subwfAction = jpaService.execute(subwfActionGetCmd);
        coordJob = jpaService.execute(coordJobGetCmd);
        coordAction = jpaService.execute(coordActionGetCmd);
        bundleJob = jpaService.execute(bundleJobGetCmd);
        bundleAction = jpaService.execute(bundleActionGetCmd);
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, subwfJob.getStatus());
        assertEquals(WorkflowAction.Status.OK, subwfAction.getStatus());
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction.getStatus());
        assertEquals(BundleJobBean.Status.SUCCEEDED, bundleJob.getStatus());
        assertEquals(BundleJobBean.Status.SUCCEEDED, bundleAction.getStatus());

        new PurgeXCommand(7, 7, getNumDaysToNotBePurged(bundleJob.getLastModifiedTime()), 10).call();

        try {
            jpaService.execute(bundleJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Bundle Job should not have been purged");
        }

        try {
            jpaService.execute(bundleActionGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Bundle Action should not have been purged");
        }

        try {
            jpaService.execute(coordJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Job should not have been purged");
        }

        try {
            jpaService.execute(coordActionGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action should not have been purged");
        }

        try {
            jpaService.execute(wfJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Job should not have been purged");
        }

        try {
            jpaService.execute(wfActionGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action should not have been purged");
        }

        try {
            jpaService.execute(subwfJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("SubWorkflow Job should not have been purged");
        }

        try {
            jpaService.execute(subwfActionGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("SubWorkflow Action should not have been purged");
        }
    }

    /**
     * Test : The subworkflow, workflow, and coordinator should not get purged, but the bundle parent should get purged --> none
     * will get purged
     *
     * @throws Exception
     */
    public void testPurgeBundleWithCoordChildWithWFChildWithSubWF2() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        BundleJobBean bundleJob = addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-01-01T01:00Z"));
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowJobBean subwfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        WorkflowActionBean subwfAction = addRecordToWfActionTable(subwfJob.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob.getId(), "SUCCEEDED", 0);
        BundleActionBean bundleAction = addRecordToBundleActionTable(bundleJob.getId(), coordJob.getId(), coordJob.getAppName(), 0,
                Job.Status.SUCCEEDED);

        WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(wfJob.getId());
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(wfAction.getId());
        WorkflowJobGetJPAExecutor subwfJobGetCmd = new WorkflowJobGetJPAExecutor(subwfJob.getId());
        WorkflowActionGetJPAExecutor subwfActionGetCmd = new WorkflowActionGetJPAExecutor(subwfAction.getId());
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(coordJob.getId());
        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(coordAction.getId());
        BundleJobGetJPAExecutor bundleJobGetCmd = new BundleJobGetJPAExecutor(bundleJob.getId());
        BundleActionGetJPAExecutor bundleActionGetCmd = new BundleActionGetJPAExecutor(bundleJob.getId(), coordJob.getAppName());

        wfJob = jpaService.execute(wfJobGetCmd);
        wfAction = jpaService.execute(wfActionGetCmd);
        subwfJob = jpaService.execute(subwfJobGetCmd);
        subwfAction = jpaService.execute(subwfActionGetCmd);
        coordJob = jpaService.execute(coordJobGetCmd);
        coordAction = jpaService.execute(coordActionGetCmd);
        bundleJob = jpaService.execute(bundleJobGetCmd);
        bundleAction = jpaService.execute(bundleActionGetCmd);
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, subwfJob.getStatus());
        assertEquals(WorkflowAction.Status.OK, subwfAction.getStatus());
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction.getStatus());
        assertEquals(BundleJobBean.Status.SUCCEEDED, bundleJob.getStatus());
        assertEquals(BundleJobBean.Status.SUCCEEDED, bundleAction.getStatus());

        new PurgeXCommand(getNumDaysToNotBePurged(wfJob.getEndTime()),
                getNumDaysToNotBePurged(coordJob.getLastModifiedTime()), 7, 10).call();

        try {
            jpaService.execute(bundleJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Bundle Job should not have been purged");
        }

        try {
            jpaService.execute(bundleActionGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Bundle Action should not have been purged");
        }

        try {
            jpaService.execute(coordJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Job should not have been purged");
        }

        try {
            jpaService.execute(coordActionGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action should not have been purged");
        }

        try {
            jpaService.execute(wfJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Job should not have been purged");
        }

        try {
            jpaService.execute(wfActionGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action should not have been purged");
        }

        try {
            jpaService.execute(subwfJobGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("SubWorkflow Job should not have been purged");
        }

        try {
            jpaService.execute(subwfActionGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("SubWorkflow Action should not have been purged");
        }
    }

    /**
     * Test : The subworkflow, workflow, and coordinator should get purged, and the bundle parent should get purged --> all
     * will get purged
     *
     * @throws Exception
     */
    public void testPurgeBundleWithCoordChildWithWFChildWithSubWF3() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        BundleJobBean bundleJob = addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-01-01T01:00Z"));
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowJobBean subwfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        WorkflowActionBean subwfAction = addRecordToWfActionTable(subwfJob.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob.getId(), "SUCCEEDED", 0);
        BundleActionBean bundleAction = addRecordToBundleActionTable(bundleJob.getId(), coordJob.getId(), coordJob.getAppName(), 0,
                Job.Status.SUCCEEDED);

        WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(wfJob.getId());
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(wfAction.getId());
        WorkflowJobGetJPAExecutor subwfJobGetCmd = new WorkflowJobGetJPAExecutor(subwfJob.getId());
        WorkflowActionGetJPAExecutor subwfActionGetCmd = new WorkflowActionGetJPAExecutor(subwfAction.getId());
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(coordJob.getId());
        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(coordAction.getId());
        BundleJobGetJPAExecutor bundleJobGetCmd = new BundleJobGetJPAExecutor(bundleJob.getId());
        BundleActionGetJPAExecutor bundleActionGetCmd = new BundleActionGetJPAExecutor(bundleJob.getId(), coordJob.getAppName());

        wfJob = jpaService.execute(wfJobGetCmd);
        wfAction = jpaService.execute(wfActionGetCmd);
        subwfJob = jpaService.execute(subwfJobGetCmd);
        subwfAction = jpaService.execute(subwfActionGetCmd);
        coordJob = jpaService.execute(coordJobGetCmd);
        coordAction = jpaService.execute(coordActionGetCmd);
        bundleJob = jpaService.execute(bundleJobGetCmd);
        bundleAction = jpaService.execute(bundleActionGetCmd);
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, subwfJob.getStatus());
        assertEquals(WorkflowAction.Status.OK, subwfAction.getStatus());
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordAction.getStatus());
        assertEquals(BundleJobBean.Status.SUCCEEDED, bundleJob.getStatus());
        assertEquals(BundleJobBean.Status.SUCCEEDED, bundleAction.getStatus());

        new PurgeXCommand(7, 7, 7, 10).call();

        try {
            jpaService.execute(bundleJobGetCmd);
            fail("Bundle Job should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(bundleActionGetCmd);
            fail("Bundle Action should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(coordJobGetCmd);
            fail("Coordinator Job should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(coordActionGetCmd);
            fail("Coordinator Action should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(wfJobGetCmd);
            fail("Workflow Job should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(wfActionGetCmd);
            fail("Workflow Action should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(subwfJobGetCmd);
            fail("SubWorkflow Job should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(subwfActionGetCmd);
            fail("SubWorkflow Action should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }
    }

    /**
     * Test : Test purging a lot of jobs (with different parent-child relationships and purge-eligibility) in one go
     *
     * @throws Exception
     */
    public void testPurgeLotsOfJobs() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        /* Job relationships:
        bundleJobA              yes     yes
            coordJobA           yes     ^
                wfJobA          yes     ^
                    subwfJobA   yes     ^
        bundleJobB              no      no
            coordJobB           yes     ^
                wfJobB          no      ^
        coordJobC               no      no
            wfJobC              no      ^
                subwfJobC       yes     ^
        coordJobD               yes     no
            wfJobD              no      ^
        wfJobE                  yes     yes
        wfJobF                  yes     no
            subwfJobF           no      ^
        */
        BundleJobBean bundleJobA = addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-05-01T01:00Z"));
        BundleJobBean bundleJobB = addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-06-01T01:00Z"));
        CoordinatorJobBean coordJobA = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        setLastModifiedTime(coordJobA, "2011-03-01T01:00Z");
        CoordinatorJobBean coordJobB = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        setLastModifiedTime(coordJobB, "2011-03-01T01:00Z");
        CoordinatorJobBean coordJobC = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        setLastModifiedTime(coordJobC, "2011-07-01T01:00Z");
        CoordinatorJobBean coordJobD = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        setLastModifiedTime(coordJobD, "2011-06-01T01:00Z");
        WorkflowJobBean wfJobA = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        setEndTime(wfJobA, "2011-02-01T01:00Z");
        WorkflowJobBean wfJobB = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        setEndTime(wfJobB, "2011-03-01T01:00Z");
        WorkflowJobBean wfJobC = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        setEndTime(wfJobC, "2011-06-01T01:00Z");
        WorkflowJobBean wfJobD = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        setEndTime(wfJobD, "2011-06-01T01:00Z");
        WorkflowJobBean wfJobE = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        setEndTime(wfJobE, "2011-01-01T01:00Z");
        WorkflowJobBean wfJobF = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        setEndTime(wfJobF, "2011-01-01T01:00Z");
        WorkflowActionBean wfActionA = addRecordToWfActionTable(wfJobA.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfActionB = addRecordToWfActionTable(wfJobB.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfActionC = addRecordToWfActionTable(wfJobC.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfActionD = addRecordToWfActionTable(wfJobD.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfActionE = addRecordToWfActionTable(wfJobE.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfActionF = addRecordToWfActionTable(wfJobF.getId(), "1", WorkflowAction.Status.OK);
        WorkflowJobBean subwfJobA = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJobA.getId());
        setEndTime(subwfJobA, "2011-01-01T01:00Z");
        WorkflowJobBean subwfJobC = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJobC.getId());
        setEndTime(subwfJobC, "2011-01-01T01:00Z");
        WorkflowJobBean subwfJobF = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJobF.getId());
        setEndTime(subwfJobF, "2011-04-01T01:00Z");
        WorkflowActionBean subwfActionA = addRecordToWfActionTable(subwfJobA.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean subwfActionC = addRecordToWfActionTable(subwfJobC.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean subwfActionF = addRecordToWfActionTable(subwfJobF.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordActionA = addRecordToCoordActionTable(coordJobA.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJobA.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordActionB = addRecordToCoordActionTable(coordJobB.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJobB.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordActionC = addRecordToCoordActionTable(coordJobC.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJobC.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordActionD = addRecordToCoordActionTable(coordJobD.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJobD.getId(), "SUCCEEDED", 0);
        BundleActionBean bundleActionA = addRecordToBundleActionTable(bundleJobA.getId(), coordJobA.getId(), coordJobA.getAppName(),
                0,Job.Status.SUCCEEDED);
        BundleActionBean bundleActionB = addRecordToBundleActionTable(bundleJobB.getId(), coordJobB.getId(), coordJobB.getAppName(),
                0, Job.Status.SUCCEEDED);

        WorkflowJobGetJPAExecutor wfJobAGetCmd = new WorkflowJobGetJPAExecutor(wfJobA.getId());
        WorkflowJobGetJPAExecutor wfJobBGetCmd = new WorkflowJobGetJPAExecutor(wfJobB.getId());
        WorkflowJobGetJPAExecutor wfJobCGetCmd = new WorkflowJobGetJPAExecutor(wfJobC.getId());
        WorkflowJobGetJPAExecutor wfJobDGetCmd = new WorkflowJobGetJPAExecutor(wfJobD.getId());
        WorkflowJobGetJPAExecutor wfJobEGetCmd = new WorkflowJobGetJPAExecutor(wfJobE.getId());
        WorkflowJobGetJPAExecutor wfJobFGetCmd = new WorkflowJobGetJPAExecutor(wfJobF.getId());
        WorkflowActionGetJPAExecutor wfActionAGetCmd = new WorkflowActionGetJPAExecutor(wfActionA.getId());
        WorkflowActionGetJPAExecutor wfActionBGetCmd = new WorkflowActionGetJPAExecutor(wfActionB.getId());
        WorkflowActionGetJPAExecutor wfActionCGetCmd = new WorkflowActionGetJPAExecutor(wfActionC.getId());
        WorkflowActionGetJPAExecutor wfActionDGetCmd = new WorkflowActionGetJPAExecutor(wfActionD.getId());
        WorkflowActionGetJPAExecutor wfActionEGetCmd = new WorkflowActionGetJPAExecutor(wfActionE.getId());
        WorkflowActionGetJPAExecutor wfActionFGetCmd = new WorkflowActionGetJPAExecutor(wfActionF.getId());
        WorkflowJobGetJPAExecutor subwfJobAGetCmd = new WorkflowJobGetJPAExecutor(subwfJobA.getId());
        WorkflowJobGetJPAExecutor subwfJobCGetCmd = new WorkflowJobGetJPAExecutor(subwfJobC.getId());
        WorkflowJobGetJPAExecutor subwfJobFGetCmd = new WorkflowJobGetJPAExecutor(subwfJobF.getId());
        WorkflowActionGetJPAExecutor subwfActionAGetCmd = new WorkflowActionGetJPAExecutor(subwfActionA.getId());
        WorkflowActionGetJPAExecutor subwfActionCGetCmd = new WorkflowActionGetJPAExecutor(subwfActionC.getId());
        WorkflowActionGetJPAExecutor subwfActionFGetCmd = new WorkflowActionGetJPAExecutor(subwfActionF.getId());
        CoordJobGetJPAExecutor coordJobAGetCmd = new CoordJobGetJPAExecutor(coordJobA.getId());
        CoordJobGetJPAExecutor coordJobBGetCmd = new CoordJobGetJPAExecutor(coordJobB.getId());
        CoordJobGetJPAExecutor coordJobCGetCmd = new CoordJobGetJPAExecutor(coordJobC.getId());
        CoordJobGetJPAExecutor coordJobDGetCmd = new CoordJobGetJPAExecutor(coordJobD.getId());
        CoordActionGetJPAExecutor coordActionAGetCmd = new CoordActionGetJPAExecutor(coordActionA.getId());
        CoordActionGetJPAExecutor coordActionBGetCmd = new CoordActionGetJPAExecutor(coordActionB.getId());
        CoordActionGetJPAExecutor coordActionCGetCmd = new CoordActionGetJPAExecutor(coordActionC.getId());
        CoordActionGetJPAExecutor coordActionDGetCmd = new CoordActionGetJPAExecutor(coordActionD.getId());
        BundleJobGetJPAExecutor bundleJobAGetCmd = new BundleJobGetJPAExecutor(bundleJobA.getId());
        BundleJobGetJPAExecutor bundleJobBGetCmd = new BundleJobGetJPAExecutor(bundleJobB.getId());
        BundleActionGetJPAExecutor bundleActionAGetCmd = new BundleActionGetJPAExecutor(bundleJobA.getId(), coordJobA.getAppName());
        BundleActionGetJPAExecutor bundleActionBGetCmd = new BundleActionGetJPAExecutor(bundleJobB.getId(), coordJobB.getAppName());

        wfJobA = jpaService.execute(wfJobAGetCmd);
        wfJobB = jpaService.execute(wfJobBGetCmd);
        wfJobC = jpaService.execute(wfJobCGetCmd);
        wfJobD = jpaService.execute(wfJobDGetCmd);
        wfJobE = jpaService.execute(wfJobEGetCmd);
        wfJobF = jpaService.execute(wfJobFGetCmd);
        wfActionA = jpaService.execute(wfActionAGetCmd);
        wfActionB = jpaService.execute(wfActionBGetCmd);
        wfActionC = jpaService.execute(wfActionCGetCmd);
        wfActionD = jpaService.execute(wfActionDGetCmd);
        wfActionE = jpaService.execute(wfActionEGetCmd);
        wfActionF = jpaService.execute(wfActionFGetCmd);
        subwfJobA = jpaService.execute(subwfJobAGetCmd);
        subwfJobC = jpaService.execute(subwfJobCGetCmd);
        subwfJobF = jpaService.execute(subwfJobFGetCmd);
        subwfActionA = jpaService.execute(subwfActionAGetCmd);
        subwfActionC = jpaService.execute(subwfActionCGetCmd);
        subwfActionF = jpaService.execute(subwfActionFGetCmd);
        coordJobA = jpaService.execute(coordJobAGetCmd);
        coordJobB = jpaService.execute(coordJobBGetCmd);
        coordJobC = jpaService.execute(coordJobCGetCmd);
        coordJobD = jpaService.execute(coordJobDGetCmd);
        coordActionA = jpaService.execute(coordActionAGetCmd);
        coordActionB = jpaService.execute(coordActionBGetCmd);
        coordActionC = jpaService.execute(coordActionCGetCmd);
        coordActionD = jpaService.execute(coordActionDGetCmd);
        bundleJobA = jpaService.execute(bundleJobAGetCmd);
        bundleJobB = jpaService.execute(bundleJobBGetCmd);
        bundleActionA = jpaService.execute(bundleActionAGetCmd);
        bundleActionB = jpaService.execute(bundleActionBGetCmd);
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJobA.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJobB.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJobC.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJobD.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJobE.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfJobF.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfActionA.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfActionB.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfActionC.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfActionD.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfActionE.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfActionF.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, subwfJobA.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, subwfJobC.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, subwfJobF.getStatus());
        assertEquals(WorkflowAction.Status.OK, subwfActionA.getStatus());
        assertEquals(WorkflowAction.Status.OK, subwfActionC.getStatus());
        assertEquals(WorkflowAction.Status.OK, subwfActionF.getStatus());
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJobA.getStatus());
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJobB.getStatus());
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJobC.getStatus());
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJobD.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordActionA.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordActionB.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordActionC.getStatus());
        assertEquals(CoordinatorAction.Status.SUCCEEDED, coordActionD.getStatus());
        assertEquals(BundleJobBean.Status.SUCCEEDED, bundleJobA.getStatus());
        assertEquals(BundleJobBean.Status.SUCCEEDED, bundleJobB.getStatus());
        assertEquals(BundleJobBean.Status.SUCCEEDED, bundleActionA.getStatus());
        assertEquals(BundleJobBean.Status.SUCCEEDED, bundleActionB.getStatus());

        new PurgeXCommand(getNumDaysToNotBePurged(wfJobB.getEndTime()),
                          getNumDaysToNotBePurged(coordJobC.getLastModifiedTime()),
                          getNumDaysToNotBePurged(bundleJobB.getLastModifiedTime()),
                          10).call();

        try {
            jpaService.execute(bundleJobAGetCmd);
            fail("Bundle Job A should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(bundleActionAGetCmd);
            fail("Bundle Action A should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(bundleJobBGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Bundle Job B should not have been purged");
        }

        try {
            jpaService.execute(bundleActionBGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Bundle Action B should not have been purged");
        }

        try {
            jpaService.execute(coordJobAGetCmd);
            fail("Coordinator Job A should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(coordActionAGetCmd);
            fail("Coordinator Action A should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(coordJobBGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Job B should not have been purged");
        }

        try {
            jpaService.execute(coordActionBGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action B should not have been purged");
        }

        try {
            jpaService.execute(coordJobCGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Job C should not have been purged");
        }

        try {
            jpaService.execute(coordActionCGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action C should not have been purged");
        }

        try {
            jpaService.execute(coordJobDGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Job D should not have been purged");
        }

        try {
            jpaService.execute(coordActionDGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action D should not have been purged");
        }

        try {
            jpaService.execute(wfJobAGetCmd);
            fail("Workflow Job A should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(wfActionAGetCmd);
            fail("Workflow Action A should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(wfJobBGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Job B should not have been purged");
        }

        try {
            jpaService.execute(wfActionBGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action B should not have been purged");
        }

        try {
            jpaService.execute(wfJobCGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Job C should not have been purged");
        }

        try {
            jpaService.execute(wfActionCGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action C should not have been purged");
        }

        try {
            jpaService.execute(wfJobDGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Job D should not have been purged");
        }

        try {
            jpaService.execute(wfActionDGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action D should not have been purged");
        }

        try {
            jpaService.execute(wfJobEGetCmd);
            fail("Workflow Job E should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(wfActionEGetCmd);
            fail("Workflow Action E should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(wfJobFGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Job F should not have been purged");
        }

        try {
            jpaService.execute(wfActionFGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action F should not have been purged");
        }

        try {
            jpaService.execute(subwfJobAGetCmd);
            fail("SubWorkflow Job A should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(subwfActionAGetCmd);
            fail("SubWorkflow Action A should have been purged");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(subwfJobCGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("SubWorkflow Job C should not have been purged");
        }

        try {
            jpaService.execute(subwfActionCGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("SubWorkflow Action C should not have been purged");
        }

        try {
            jpaService.execute(subwfJobFGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("SubWorkflow Job F should not have been purged");
        }

        try {
            jpaService.execute(subwfActionFGetCmd);
        }
        catch (JPAExecutorException je) {
            fail("SubWorkflow Action F should not have been purged");
        }
    }

    protected WorkflowJobBean addRecordToWfJobTableForNegCase(WorkflowJob.Status jobStatus,
            WorkflowInstance.Status instanceStatus) throws Exception {
        WorkflowApp app =
            new LiteWorkflowApp("testApp", "<workflow-app/>",
                new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "end")).
                    addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));
        Configuration conf = new Configuration();
        Path appUri = new Path(getAppPath(), "workflow.xml");
        conf.set(OozieClient.APP_PATH, appUri.toString());
        conf.set(OozieClient.LOG_TOKEN, "testToken");
        conf.set(OozieClient.USER_NAME, getTestUser());

        WorkflowJobBean wfBean = createWorkflow(app, conf, jobStatus, instanceStatus);
        wfBean.setStartTime(DateUtils.parseDateOozieTZ("2015-12-18T01:00Z"));
        wfBean.setEndTime(DateUtils.parseDateOozieTZ("2015-12-18T03:00Z"));

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
    protected WorkflowJobBean createWorkflow(WorkflowApp app, Configuration conf,
            WorkflowJob.Status jobStatus, WorkflowInstance.Status instanceStatus) throws Exception {
        WorkflowAppService wps = Services.get().get(WorkflowAppService.class);
        Configuration protoActionConf = wps.createProtoActionConf(conf, true);
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
        workflow.setWorkflowInstance(wfInstance);
        workflow.setStartTime(DateUtils.parseDateOozieTZ("2009-12-18T01:00Z"));
        workflow.setEndTime(DateUtils.parseDateOozieTZ("2009-12-18T03:00Z"));
        return workflow;
    }

    @Override
    protected CoordinatorJobBean addRecordToCoordJobTable(CoordinatorJob.Status status, boolean pending, boolean doneMatd)
            throws Exception {
        CoordinatorJobBean coordJob = createCoordJob(status, pending, doneMatd);
        coordJob.setLastModifiedTime(DateUtils.parseDateOozieTZ("2009-12-18T01:00Z"));
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordJobInsertJPAExecutor coordInsertCmd = new CoordJobInsertJPAExecutor(coordJob);
            jpaService.execute(coordInsertCmd);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test coord job record to table");
            throw je;
        }

        return coordJob;
    }

    protected BundleJobBean addRecordToBundleJobTable(Job.Status jobStatus, Date lastModifiedTime) throws Exception {
        BundleJobBean bundle = createBundleJob(jobStatus, false);
        bundle.setLastModifiedTime(lastModifiedTime);
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            BundleJobInsertJPAExecutor bundleInsertjpa = new BundleJobInsertJPAExecutor(bundle);
            jpaService.execute(bundleInsertjpa);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test bundle job record to table");
            throw je;
        }
        return bundle;
    }

    /**
     * Return a number of days from today such that the job (workflow, coord, or bundle) ending at jobDate will NOT be purged
     * @param jobDate The date that the job was last modified
     * @return 5 + the number of days from today to jobDate
     */
    public static int getNumDaysToNotBePurged(Date jobDate) {
        int days = (int) (((new Date()).getTime() - jobDate.getTime()) / 1000 / 60 / 60 / 24);
        return days + 5;
    }

    public static CoordinatorJobBean setLastModifiedTime(CoordinatorJobBean job, String date) throws Exception {
        job.setLastModifiedTime(DateUtils.parseDateOozieTZ(date));
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB_LAST_MODIFIED_TIME, job);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to update coord job last modified time");
            throw je;
        }
        return job;
    }

    public static WorkflowJobBean setEndTime(WorkflowJobBean job, String date) throws Exception {
        job.setEndTime(DateUtils.parseDateOozieTZ(date));
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            WorkflowJobQueryExecutor.getInstance().executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_INSTANCE_MOD_END, job);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to update workflow job last modified time");
            throw je;
        }
        return job;
    }
}
