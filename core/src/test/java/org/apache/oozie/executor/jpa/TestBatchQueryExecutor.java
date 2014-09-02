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
import java.util.Collection;
import java.util.List;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.FaultInjection;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.command.SkipCommitFaultInjection;
import org.apache.oozie.executor.jpa.BatchQueryExecutor.UpdateEntry;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor.WorkflowActionQuery;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor.WorkflowJobQuery;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.workflow.WorkflowInstance;

public class TestBatchQueryExecutor extends XDataTestCase {
    Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testExecuteBatchUpdateInsertDelete() throws Exception {
        BatchQueryExecutor executor = BatchQueryExecutor.getInstance();
        // for update
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, true, true);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.PREP);
        // for insert
        CoordinatorActionBean coordAction = new CoordinatorActionBean();
        coordAction.setId("testCoordAction1");
        JPAService jpaService = Services.get().get(JPAService.class);

        // update the status
        coordJob.setStatus(CoordinatorJob.Status.RUNNING);
        wfJob.setStatus(WorkflowJob.Status.SUCCEEDED);

        // update the list for doing bulk writes
        List<UpdateEntry> updateList = new ArrayList<UpdateEntry>();
        updateList.add(new UpdateEntry<CoordJobQuery>(CoordJobQuery.UPDATE_COORD_JOB_STATUS_MODTIME, coordJob));
        updateList.add(new UpdateEntry<WorkflowJobQuery>(WorkflowJobQuery.UPDATE_WORKFLOW, wfJob));

        // insert beans
        Collection<JsonBean> insertList = new ArrayList<JsonBean>();
        insertList.add(coordAction);

        // delete beans
        Collection<JsonBean> deleteList = new ArrayList<JsonBean>();
        deleteList.add(wfAction);

        BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(insertList, updateList, deleteList);

        // check update after running ExecuteBatchUpdateInsertDelete
        coordJob = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB, coordJob.getId());
        assertEquals("RUNNING", coordJob.getStatusStr());
        wfJob = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW, wfJob.getId());
        assertEquals("SUCCEEDED", wfJob.getStatusStr());
        coordAction = CoordActionQueryExecutor.getInstance()
                .get(CoordActionQuery.GET_COORD_ACTION, coordAction.getId());
        assertEquals("testCoordAction1", coordAction.getId());
        try {
            wfAction = WorkflowActionQueryExecutor.getInstance().get(WorkflowActionQuery.GET_ACTION, wfJob.getId());
            fail();
        }
        catch (JPAExecutorException ex) {
            assertEquals(ex.getErrorCode().toString(), "E0605");
        }
    }

    public void testExecuteBatchUpdateInsertDeleteRollBack() throws Exception {
        BatchQueryExecutor executor = BatchQueryExecutor.getInstance();
        WorkflowJobBean job = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);
        WorkflowActionBean action1 = createWorkflowAction(job.getId(), "1", WorkflowAction.Status.PREP);
        WorkflowActionBean action2 = createWorkflowAction(job.getId(), "2", WorkflowAction.Status.PREP);
        job.setStatus(WorkflowJob.Status.RUNNING);

        Collection<JsonBean> insertList = new ArrayList<JsonBean>();
        insertList.add(action1);
        insertList.add(action2);

        List<UpdateEntry> updateList = new ArrayList<UpdateEntry>();
        // Add two actions to insert list
        updateList.add(new UpdateEntry<WorkflowJobQuery>(WorkflowJobQuery.UPDATE_WORKFLOW, job));

        // set fault injection to true, so transaction is roll backed
        setSystemProperty(FaultInjection.FAULT_INJECTION, "true");
        setSystemProperty(SkipCommitFaultInjection.ACTION_FAILOVER_FAULT_INJECTION, "true");
        FaultInjection.activate("org.apache.oozie.command.SkipCommitFaultInjection");
        try {
            executor.executeBatchInsertUpdateDelete(insertList, updateList, null);
            fail("Expected exception due to commit failure but didn't get any");
        }
        catch (Exception e) {
        }
        FaultInjection.deactivate("org.apache.oozie.command.SkipCommitFaultInjection");

        // Check whether transactions are rolled back or not

        WorkflowJobBean wfBean = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW, job.getId());
        // status should not be RUNNING
        assertEquals("PREP", wfBean.getStatusStr());

        WorkflowActionBean waBean;
        try {
            waBean = WorkflowActionQueryExecutor.getInstance().get(WorkflowActionQuery.GET_ACTION, action1.getId());
            fail("Expected exception but didnt get any");
        }
        catch (JPAExecutorException jpaee) {
            assertEquals(ErrorCode.E0605, jpaee.getErrorCode());
        }

        try {
            waBean = WorkflowActionQueryExecutor.getInstance().get(WorkflowActionQuery.GET_ACTION, action2.getId());
            fail("Expected exception but didnt get any");
        }
        catch (JPAExecutorException jpaee) {
            assertEquals(ErrorCode.E0605, jpaee.getErrorCode());
        }

    }
}
