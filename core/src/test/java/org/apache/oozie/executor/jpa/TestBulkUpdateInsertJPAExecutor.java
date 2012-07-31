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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.FaultInjection;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.command.SkipCommitFaultInjection;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.lite.EndNodeDef;
import org.apache.oozie.workflow.lite.LiteWorkflowApp;
import org.apache.oozie.workflow.lite.StartNodeDef;

public class TestBulkUpdateInsertJPAExecutor extends XDataTestCase {
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
        BulkUpdateInsertJPAExecutor bulkUpdateCmd = new BulkUpdateInsertJPAExecutor();
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
     * Test bulk inserts by inserting a workflow job and two workflow actions
     * @throws Exception
     */
    public void testInserts() throws Exception{
        WorkflowApp app = new LiteWorkflowApp("testApp", "<workflow-app/>", new StartNodeDef("end"))
                .addNode(new EndNodeDef("end"));
        Configuration conf = new Configuration();
        Path appUri = new Path(getAppPath(), "workflow.xml");
        conf.set(OozieClient.APP_PATH, appUri.toString());
        conf.set(OozieClient.LOG_TOKEN, "testToken");
        conf.set(OozieClient.USER_NAME, getTestUser());

        WorkflowJobBean job = createWorkflow(app, conf, "auth", WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);
        WorkflowActionBean action1 = createWorkflowAction(job.getId(), "1", WorkflowAction.Status.PREP);
        WorkflowActionBean action2 = createWorkflowAction(job.getId(), "2", WorkflowAction.Status.PREP);

        List<JsonBean> insertList = new ArrayList<JsonBean>();
        // insert one workflow job and two actions
        insertList.add(action1);
        insertList.add(action2);
        insertList.add(job);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BulkUpdateInsertJPAExecutor bulkInsertCmd = new BulkUpdateInsertJPAExecutor();
        bulkInsertCmd.setInsertList(insertList);
        jpaService.execute(bulkInsertCmd);

        // check for expected status after running bulkUpdateJPA
        WorkflowActionGetJPAExecutor actionGetCmd = new WorkflowActionGetJPAExecutor(action1.getId());
        action1 = jpaService.execute(actionGetCmd);
        assertEquals("PREP", action1.getStatusStr());

        actionGetCmd = new WorkflowActionGetJPAExecutor(action2.getId());
        action2 = jpaService.execute(actionGetCmd);
        assertEquals("PREP", action2.getStatusStr());

        WorkflowJobGetJPAExecutor wfGetCmd = new WorkflowJobGetJPAExecutor(job.getId());
        job = jpaService.execute(wfGetCmd);
        assertEquals("PREP", job.getStatusStr());

    }

    /**
     * Test bulk inserts and updates by inserting wf actions and updating
     * coordinator and workflow jobs
     *
     * @throws Exception
     */
    public void testBulkInsertUpdates() throws Exception{
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, true, true);
        WorkflowJobBean job = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);
        WorkflowActionBean action1 = createWorkflowAction(job.getId(), "1", WorkflowAction.Status.PREP);
        WorkflowActionBean action2 = createWorkflowAction(job.getId(), "2", WorkflowAction.Status.PREP);

        job.setStatus(WorkflowJob.Status.RUNNING);
        coordJob.setStatus(Job.Status.SUCCEEDED);
        List<JsonBean> insertList = new ArrayList<JsonBean>();
        // Add two actions to insert list
        insertList.add(action1);
        insertList.add(action2);

        List<JsonBean> updateList = new ArrayList<JsonBean>();
        //Add two jobs to update list
        updateList.add(coordJob);
        updateList.add(job);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        BulkUpdateInsertJPAExecutor bulkUpdateCmd = new BulkUpdateInsertJPAExecutor(updateList, insertList);
        jpaService.execute(bulkUpdateCmd);

        coordJob = jpaService.execute(new CoordJobGetJPAExecutor(coordJob.getId()));
        assertEquals("SUCCEEDED", coordJob.getStatusStr());

        WorkflowJobGetJPAExecutor wfGetCmd = new WorkflowJobGetJPAExecutor(job.getId());
        WorkflowJobBean wfBean = jpaService.execute(wfGetCmd);
        assertEquals("RUNNING", wfBean.getStatusStr());

        WorkflowActionGetJPAExecutor actionGetCmd = new WorkflowActionGetJPAExecutor(action1.getId());
        action1 = jpaService.execute(actionGetCmd);
        assertEquals("PREP", action1.getStatusStr());

        actionGetCmd = new WorkflowActionGetJPAExecutor(action2.getId());
        action2 = jpaService.execute(actionGetCmd);
        assertEquals("PREP", action2.getStatusStr());
    }

    /**
     * Test bulk inserts and updates rollback
     *
     * @throws Exception
     */
    public void testBulkInsertUpdatesRollback() throws Exception{
        WorkflowJobBean job = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);
        WorkflowActionBean action1 = createWorkflowAction(job.getId(), "1", WorkflowAction.Status.PREP);
        WorkflowActionBean action2 = createWorkflowAction(job.getId(), "2", WorkflowAction.Status.PREP);

        job.setStatus(WorkflowJob.Status.RUNNING);
        List<JsonBean> insertList = new ArrayList<JsonBean>();
        // Add two actions to insert list
        insertList.add(action1);
        insertList.add(action2);

        List<JsonBean> updateList = new ArrayList<JsonBean>();
        // Add to update list
        updateList.add(job);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        BulkUpdateInsertJPAExecutor wfUpdateCmd1 = new BulkUpdateInsertJPAExecutor(updateList, insertList);

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
        // status should not be RUNNING
        assertEquals("PREP", wfBean.getStatusStr());

        WorkflowActionGetJPAExecutor actionGetCmd = new WorkflowActionGetJPAExecutor(action1.getId());
        try {
            action1 = jpaService.execute(actionGetCmd);
            fail("Expected exception but didnt get any");
        }
        catch (JPAExecutorException jpaee) {
            assertEquals(ErrorCode.E0605, jpaee.getErrorCode());
        }


        actionGetCmd = new WorkflowActionGetJPAExecutor(action2.getId());
        try {
            action2 = jpaService.execute(actionGetCmd);
            fail("Expected exception but didnt get any");
        }
        catch (JPAExecutorException jpaee) {
            assertEquals(ErrorCode.E0605, jpaee.getErrorCode());
        }

    }

}
