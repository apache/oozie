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

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.FaultInjection;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
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
 * Testcases for bulk JPA writes - update and delete operations
 */
public class TestBulkUpdateDeleteJPAExecutor extends XDataTestCase {
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
     * Test bulk updates by updating coordinator job, workflow job and workflow action
     * @throws Exception
     */
    public void testUpdates() throws Exception {
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, true, true);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);
        WorkflowActionBean action = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.PREP);
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        // update the status
        coordJob.setStatus(CoordinatorJob.Status.RUNNING);
        wfJob.setStatus(WorkflowJob.Status.SUCCEEDED);
        action.setStatus(WorkflowAction.Status.RUNNING);

        List<JsonBean> updateList = new ArrayList<JsonBean>();
        // update the list for doing bulk writes
        updateList.add(coordJob);
        updateList.add(wfJob);
        updateList.add(action);
        BulkUpdateDeleteJPAExecutor bulkUpdateCmd = new BulkUpdateDeleteJPAExecutor();
        bulkUpdateCmd.setUpdateList(updateList);
        jpaService.execute(bulkUpdateCmd);

        // check for expected status after running bulkUpdateJPA
        coordJob = jpaService.execute(new CoordJobGetJPAExecutor(coordJob.getId()));
        assertEquals("RUNNING", coordJob.getStatusStr());

        wfJob = jpaService.execute(new WorkflowJobGetJPAExecutor(wfJob.getId()));
        assertEquals("SUCCEEDED", wfJob.getStatusStr());

        WorkflowActionBean action2 = jpaService.execute(new WorkflowActionGetJPAExecutor(action.getId()));
        assertEquals(WorkflowAction.Status.RUNNING, action2.getStatus());

    }

    /**
     * Test bulk deletes by deleting a coord action and a wf action
     * @throws Exception
     */
    public void testDeletes() throws Exception{
        CoordinatorActionBean action1 = addRecordToCoordActionTable("000-123-C", 1, CoordinatorAction.Status.KILLED, "coord-action-get.xml", 0);
        WorkflowActionBean action2 = addRecordToWfActionTable("000-123-W", "2", WorkflowAction.Status.PREP);
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        List<JsonBean> deleteList = new ArrayList<JsonBean>();
        // insert one workflow job and two actions
        deleteList.add(action1);
        deleteList.add(action2);

        BulkUpdateDeleteJPAExecutor bulkDelRerunCmd = new BulkUpdateDeleteJPAExecutor();
        bulkDelRerunCmd.setDeleteList(deleteList);
        jpaService.execute(bulkDelRerunCmd);

        // check for non existence after running bulkDeleteJPA
        try {
            jpaService.execute(new CoordActionGetJPAExecutor(action1.getId()));
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
     * Test bulk updates and deletes
     * workflow job and action
     *
     * @throws Exception
     */
    public void testBulkUpdatesDeletes() throws Exception{
        WorkflowJobBean job = addRecordToWfJobTable(WorkflowJob.Status.KILLED, WorkflowInstance.Status.KILLED);
        WorkflowActionBean action = addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.PREP);
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        job.setStatus(WorkflowJob.Status.RUNNING);
        List<JsonBean> updateList = new ArrayList<JsonBean>();
        // Add job to update
        updateList.add(job);

        List<JsonBean> deleteList = new ArrayList<JsonBean>();
        //Add action to delete
        deleteList.add(action);

        BulkUpdateDeleteJPAExecutor bulkDelRerunCmd = new BulkUpdateDeleteJPAExecutor();
        bulkDelRerunCmd.setUpdateList(updateList);
        bulkDelRerunCmd.setDeleteList(deleteList);
        jpaService.execute(bulkDelRerunCmd);

        // check for update after running bulkJPA. job should be updated from KILLED -> RUNING
        job = jpaService.execute(new WorkflowJobGetJPAExecutor(job.getId()));
        assertEquals("RUNNING", job.getStatusStr());

        // check for non existence after running bulkJPA
        try {
            jpaService.execute(new WorkflowActionGetJPAExecutor(action.getId()));
            fail(); //should not be found
        }
        catch(JPAExecutorException jex) {
            assertEquals(ErrorCode.E0605, jex.getErrorCode());
        }
    }

    /**
     * Test bulk updates and deletes rollback
     *
     * @throws Exception
     */
    public void testBulkUpdatesDeletesRollback() throws Exception{
        WorkflowJobBean job = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);
        WorkflowActionBean action1 = addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.PREP);
        WorkflowActionBean action2 = addRecordToWfActionTable(job.getId(), "2", WorkflowAction.Status.PREP);

        job.setStatus(WorkflowJob.Status.RUNNING);
        List<JsonBean> deleteList = new ArrayList<JsonBean>();
        // Add two actions to delete list
        deleteList.add(action1);
        deleteList.add(action2);

        List<JsonBean> updateList = new ArrayList<JsonBean>();
        // Add to update list
        updateList.add(job);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        BulkUpdateDeleteJPAExecutor wfUpdateCmd1 = new BulkUpdateDeleteJPAExecutor(updateList, deleteList, true);

        // set fault injection to true, so transaction is roll backed
        setSystemProperty(FaultInjection.FAULT_INJECTION, "true");
        setSystemProperty(SkipCommitFaultInjection.ACTION_FAILOVER_FAULT_INJECTION, "true");
        try {
            jpaService.execute(wfUpdateCmd1);
            fail("Expected exception due to commit failure but didn't get any");
        }
        catch (Exception e) {
        }
        FaultInjection.deactivate("org.apache.oozie.command.SkipCommitFaultInjection");

        // Check whether transactions are rolled back or not
        WorkflowJobGetJPAExecutor wfGetCmd = new WorkflowJobGetJPAExecutor(job.getId());
        WorkflowJobBean wfBean = jpaService.execute(wfGetCmd);
        // status should NOT be RUNNING
        assertEquals("PREP", wfBean.getStatusStr());

        try {
            jpaService.execute(new WorkflowActionGetJPAExecutor(action1.getId()));
        }
        catch (JPAExecutorException je) {
            fail("WF action should not be removed due to rollback but was not found");
        }

        try {
            jpaService.execute(new WorkflowActionGetJPAExecutor(action2.getId()));
        }
        catch (JPAExecutorException je) {
            fail("WF action should not be removed due to rollback but was not found");
        }
    }

}
