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

package org.apache.oozie.executor.jpa;

import java.util.ArrayList;
import java.util.List;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.FaultInjection;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.SkipCommitFaultInjection;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.workflow.WorkflowInstance;

public class TestWorkflowJobsDeleteJPAExecutor extends XDataTestCase {
    Services services;
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

    public void testDeleteWorkflows() throws Exception {
        WorkflowJobBean jobA = this.addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean actionA1 = this.addRecordToWfActionTable(jobA.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean actionA2 = this.addRecordToWfActionTable(jobA.getId(), "2", WorkflowAction.Status.OK);

        WorkflowJobBean jobB = this.addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean actionB1 = this.addRecordToWfActionTable(jobB.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean actionB2 = this.addRecordToWfActionTable(jobB.getId(), "2", WorkflowAction.Status.OK);

        WorkflowJobBean jobC = this.addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean actionC1 = this.addRecordToWfActionTable(jobC.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean actionC2 = this.addRecordToWfActionTable(jobC.getId(), "2", WorkflowAction.Status.OK);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        List<String> deleteList = new ArrayList<String>();
        deleteList.add(jobA.getId());
        deleteList.add(jobB.getId());
        deleteList.add(jobC.getId());
        jpaService.execute(new WorkflowJobsDeleteJPAExecutor(deleteList));

        try {
            jpaService.execute(new WorkflowJobGetJPAExecutor(jobA.getId()));
            fail("Workflow Job A should have been deleted");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(new WorkflowActionGetJPAExecutor(actionA1.getId()));
            fail("Workflow Action A1 should have been deleted");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(new WorkflowActionGetJPAExecutor(actionA2.getId()));
            fail("Workflow Action A2 should have been deleted");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(new WorkflowJobGetJPAExecutor(jobB.getId()));
            fail("Workflow Job B should have been deleted");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(new WorkflowActionGetJPAExecutor(actionB1.getId()));
            fail("Workflow Action B1 should have been deleted");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(new WorkflowActionGetJPAExecutor(actionB2.getId()));
            fail("Workflow Action B2 should have been deleted");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(new WorkflowJobGetJPAExecutor(jobC.getId()));
            fail("Workflow Job C should have been deleted");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(new WorkflowActionGetJPAExecutor(actionC1.getId()));
            fail("Workflow Action C1 should have been deleted");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(new WorkflowActionGetJPAExecutor(actionC2.getId()));
            fail("Workflow Action C2 should have been deleted");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }
    }

    public void testDeleteWorkflowsRollback() throws Exception{
        WorkflowJobBean jobA = this.addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean actionA1 = this.addRecordToWfActionTable(jobA.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean actionA2 = this.addRecordToWfActionTable(jobA.getId(), "2", WorkflowAction.Status.OK);

        WorkflowJobBean jobB = this.addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean actionB1 = this.addRecordToWfActionTable(jobB.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean actionB2 = this.addRecordToWfActionTable(jobB.getId(), "2", WorkflowAction.Status.OK);

        WorkflowJobBean jobC = this.addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean actionC1 = this.addRecordToWfActionTable(jobC.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean actionC2 = this.addRecordToWfActionTable(jobC.getId(), "2", WorkflowAction.Status.OK);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        try {
            // set fault injection to true, so transaction is roll backed
            setSystemProperty(FaultInjection.FAULT_INJECTION, "true");
            setSystemProperty(SkipCommitFaultInjection.ACTION_FAILOVER_FAULT_INJECTION, "true");

            List<String> deleteList = new ArrayList<String>();
            deleteList.add(jobA.getId());
            deleteList.add(jobB.getId());
            deleteList.add(jobC.getId());

            try {
                jpaService.execute(new WorkflowJobsDeleteJPAExecutor(deleteList));
                fail("Should have skipped commit for failover testing");
            }
            catch (RuntimeException re) {
                assertEquals("Skipping Commit for Failover Testing", re.getMessage());
            }
        }
        finally {
            // Remove fault injection
            FaultInjection.deactivate("org.apache.oozie.command.SkipCommitFaultInjection");
        }

        try {
            jpaService.execute(new WorkflowJobGetJPAExecutor(jobA.getId()));
        }
        catch (JPAExecutorException je) {
            fail("Workflow Job A should not have been deleted");
        }

        try {
            jpaService.execute(new WorkflowActionGetJPAExecutor(actionA1.getId()));
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action A1 should not have been deleted");
        }

        try {
            jpaService.execute(new WorkflowActionGetJPAExecutor(actionA2.getId()));
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action A2 should not have been deleted");
        }

        try {
            jpaService.execute(new WorkflowJobGetJPAExecutor(jobB.getId()));
        }
        catch (JPAExecutorException je) {
            fail("Workflow Job B should not have been deleted");
        }

        try {
            jpaService.execute(new WorkflowActionGetJPAExecutor(actionB1.getId()));
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action B1 should not have been deleted");
        }

        try {
            jpaService.execute(new WorkflowActionGetJPAExecutor(actionB2.getId()));
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action B2 should not have been deleted");
        }

        try {
            jpaService.execute(new WorkflowJobGetJPAExecutor(jobC.getId()));
        }
        catch (JPAExecutorException je) {
            fail("Workflow Job C should not have been deleted");
        }

        try {
            jpaService.execute(new WorkflowActionGetJPAExecutor(actionC1.getId()));
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action C1 should not have been deleted");
        }

        try {
            jpaService.execute(new WorkflowActionGetJPAExecutor(actionC2.getId()));
        }
        catch (JPAExecutorException je) {
            fail("Workflow Action C2 should not have been deleted");
        }
    }
}
