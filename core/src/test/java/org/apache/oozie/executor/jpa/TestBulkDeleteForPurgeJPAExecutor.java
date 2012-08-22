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

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.FaultInjection;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.command.SkipCommitFaultInjection;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.workflow.WorkflowInstance;

/**
 * Testcases for bulk JPA writes - delete operations for Purge commands
 */
public class TestBulkDeleteForPurgeJPAExecutor extends XDataTestCase {
    Services services;

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
     * Test bulk deletes with bundle job and its 1 bundle action
     * @throws Exception
     */
    public void testDeleteBundle() throws Exception {
        BundleJobBean bundleJob = addRecordToBundleJobTable(BundleJob.Status.PREP, true);
        addRecordToBundleActionTable(bundleJob.getId(), "COORD_NAME", 1, Job.Status.SUCCEEDED);
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        List<JsonBean> deleteList = new ArrayList<JsonBean>();
        // add to list for doing bulk delete by bundle job
        deleteList.add(bundleJob);
        BulkDeleteForPurgeJPAExecutor bulkPurgeDelCmd = new BulkDeleteForPurgeJPAExecutor();
        bulkPurgeDelCmd.setDeleteList(deleteList);
        jpaService.execute(bulkPurgeDelCmd);

        // check for non existence after running bulkDeleteJPA
        try {
            jpaService.execute(new BundleJobGetJPAExecutor(bundleJob.getId()));
            fail(); //should not be found
        }
        catch(JPAExecutorException jex) {
            assertEquals(ErrorCode.E0604, jex.getErrorCode());
        }
        try {
            jpaService.execute(new BundleActionGetJPAExecutor(bundleJob.getId(), "COORD_NAME"));
            fail(); //should not be found
        }
        catch(JPAExecutorException jex) {
            assertEquals(ErrorCode.E0605, jex.getErrorCode());
        }
    }

    /**
     * Test bulk deletes with coord job with its 2 coord actions - only 1 in terminal state
     * @throws Exception
     */
    public void testDeleteCoord() throws Exception {
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, true, true);
        CoordinatorActionBean action1 = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.KILLED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(coordJob.getId(), 2, CoordinatorAction.Status.SUSPENDED, "coord-action-get.xml", 0);
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        List<JsonBean> deleteList = new ArrayList<JsonBean>();
        // add to list for doing bulk delete by bundle job
        deleteList.add(coordJob);
        BulkDeleteForPurgeJPAExecutor bulkPurgeDelCmd = new BulkDeleteForPurgeJPAExecutor();
        bulkPurgeDelCmd.setDeleteList(deleteList);
        assertEquals(1, jpaService.execute(bulkPurgeDelCmd).intValue());

        // check for non existence after running bulkDeleteJPA
        try {
            jpaService.execute(new CoordJobGetJPAExecutor(coordJob.getId()));
            fail(); //should not be found
        }
        catch(JPAExecutorException jex) {
            assertEquals(ErrorCode.E0604, jex.getErrorCode());
        }
        try {
            jpaService.execute(new CoordActionGetJPAExecutor(action1.getId()));
            fail(); //should not be found
        }
        catch(JPAExecutorException jex) {
            assertEquals(ErrorCode.E0605, jex.getErrorCode());
        }
    }

    /**
     * Test bulk deletes with workflow job and its 2 actions
     * @throws Exception
     */
    public void testDeleteWorkflow() throws Exception {
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action1 = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.KILLED);
        WorkflowActionBean action2 = addRecordToWfActionTable(wfJob.getId(), "2", WorkflowAction.Status.START_RETRY);
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        List<JsonBean> deleteList = new ArrayList<JsonBean>();
        // add to list for doing bulk delete by bundle job
        deleteList.add(wfJob);
        BulkDeleteForPurgeJPAExecutor bulkPurgeDelCmd = new BulkDeleteForPurgeJPAExecutor();
        bulkPurgeDelCmd.setDeleteList(deleteList);
        jpaService.execute(bulkPurgeDelCmd);

        // check for non existence after running bulkDeleteJPA
        try {
            jpaService.execute(new WorkflowJobGetJPAExecutor(wfJob.getId()));
            fail(); //should not be found
        }
        catch(JPAExecutorException jex) {
            assertEquals(ErrorCode.E0604, jex.getErrorCode());
        }
        try {
            jpaService.execute(new WorkflowActionGetJPAExecutor(action1.getId()));
            fail(); //should not be found
        }
        catch(JPAExecutorException jex) {
            assertEquals(ErrorCode.E0605, jex.getErrorCode());
        }
        try {
            jpaService.execute(new WorkflowActionGetJPAExecutor(action2.getId()));
            fail(); //should not be found
        }
        catch(JPAExecutorException jex) {
            assertEquals(ErrorCode.E0605, jex.getErrorCode());
        }
    }

    /**
     * Test bulk deletes rollback
     *
     * @throws Exception
     */
    public void testBulkDeletesRollback() throws Exception{
        WorkflowJobBean job = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action1 = addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.KILLED);
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        List<JsonBean> deleteList = new ArrayList<JsonBean>();
        deleteList.add(job);
        BulkDeleteForPurgeJPAExecutor bulkPurgeDelCmd = new BulkDeleteForPurgeJPAExecutor(deleteList);

        // set fault injection to true, so transaction is roll backed
        setSystemProperty(FaultInjection.FAULT_INJECTION, "true");
        setSystemProperty(SkipCommitFaultInjection.ACTION_FAILOVER_FAULT_INJECTION, "true");
        try {
            jpaService.execute(bulkPurgeDelCmd);
            fail("Expected exception due to commit failure but didn't get any");
        }
        catch (Exception e) {
        }
        FaultInjection.deactivate("org.apache.oozie.command.SkipCommitFaultInjection");

        // Check whether transactions are rolled back or not
        try {
            jpaService.execute(new WorkflowJobGetJPAExecutor(job.getId()));
        }
        catch (JPAExecutorException je) {
            fail("WF job should not be removed due to transaction rollback but was not found");
        }
        try {
            jpaService.execute(new WorkflowActionGetJPAExecutor(action1.getId()));
        }
        catch (JPAExecutorException je) {
            fail("WF action should not be removed due to transaction rollback but was not found");
        }
    }

}
