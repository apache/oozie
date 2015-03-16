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

import org.apache.oozie.command.OperationType;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.workflow.WorkflowInstance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestBulkWorkflowXCommand extends XDataTestCase {
    private Services services;

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

    public void testbulkWfKillSuspendResumeSuccess() throws Exception {
        WorkflowJobBean job1 = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action1 = this.addRecordToWfActionTable(job1.getId(), "1", WorkflowAction.Status.PREP);

        WorkflowJobBean job2 = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action2 = this.addRecordToWfActionTable(job2.getId(), "1", WorkflowAction.Status.PREP);

        Map<String, List<String>> map = new HashMap<String, List<String>>();
        List<String> names = new ArrayList<String>();
        names.add("testApp");
        map.put("name", names);

        new BulkWorkflowXCommand(map, 1, 10, OperationType.Suspend).call();
        verifyJobStatus(job1.getId(), WorkflowJob.Status.SUSPENDED);
        verifyJobStatus(job2.getId(), WorkflowJob.Status.SUSPENDED);

        new BulkWorkflowXCommand(map, 1, 10, OperationType.Resume).call();
        verifyJobStatus(job1.getId(), WorkflowJob.Status.RUNNING);
        verifyJobStatus(job2.getId(), WorkflowJob.Status.RUNNING);

        new BulkWorkflowXCommand(map, 1, 10, OperationType.Kill).call();
        verifyJobStatus(job1.getId(), WorkflowJob.Status.KILLED);
        verifyJobStatus(job2.getId(), WorkflowJob.Status.KILLED);
        verifyActionStatus(action1.getId(), WorkflowAction.Status.KILLED);
        verifyActionStatus(action2.getId(), WorkflowAction.Status.KILLED);
    }

    public void testbulkWfKillSuccess() throws Exception {
        WorkflowJobBean job1 = this.addRecordToWfJobTable(WorkflowJob.Status.SUSPENDED, WorkflowInstance.Status.SUSPENDED);
        WorkflowActionBean action1 = this.addRecordToWfActionTable(job1.getId(), "1", WorkflowAction.Status.RUNNING);

        WorkflowJobBean job2 = this.addRecordToWfJobTable(WorkflowJob.Status.SUSPENDED, WorkflowInstance.Status.SUSPENDED);
        WorkflowActionBean action2 = this.addRecordToWfActionTable(job2.getId(), "1", WorkflowAction.Status.RUNNING);

        Map<String, List<String>> map = new HashMap<String, List<String>>();
        List<String> names = new ArrayList<String>();
        names.add("testApp");
        map.put("name", names);

        new BulkWorkflowXCommand(map, 1, 50, OperationType.Kill).call();
        verifyJobStatus(job1.getId(), WorkflowJob.Status.KILLED);
        verifyJobStatus(job2.getId(), WorkflowJob.Status.KILLED);
        verifyActionStatus(action1.getId(), WorkflowAction.Status.KILLED);
        verifyActionStatus(action2.getId(), WorkflowAction.Status.KILLED);
    }

    public void testbulkWfKillNoOp() throws Exception {
        WorkflowJobBean job1 = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action1 = this.addRecordToWfActionTable(job1.getId(), "1", WorkflowAction.Status.RUNNING);

        WorkflowJobBean job2 = this.addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean action2 = this.addRecordToWfActionTable(job2.getId(), "1", WorkflowAction.Status.DONE);

        Map<String, List<String>> map = new HashMap<String, List<String>>();
        List<String> names = new ArrayList<String>();
        names.add("testApp");
        map.put("name", names);

        new BulkWorkflowXCommand(map, 1, 50, OperationType.Kill).call();
        verifyJobStatus(job1.getId(), WorkflowJob.Status.KILLED);
        verifyJobStatus(job2.getId(), WorkflowJob.Status.SUCCEEDED);
        verifyActionStatus(action1.getId(), WorkflowAction.Status.KILLED);
        verifyActionStatus(action2.getId(), WorkflowAction.Status.DONE);
    }

    public void testbulkWfKillNegative() throws Exception {
        WorkflowJobBean job1 = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action1 = this.addRecordToWfActionTable(job1.getId(), "1", WorkflowAction.Status.RUNNING);

        Map<String, List<String>> map = new HashMap<String, List<String>>();
        List<String> names = new ArrayList<String>();
        names.add("testApp-new");
        map.put("name", names);

        new BulkWorkflowXCommand(map, 1, 50, OperationType.Kill).call();
        verifyJobStatus(job1.getId(), WorkflowJob.Status.RUNNING);
        verifyActionStatus(action1.getId(), WorkflowAction.Status.RUNNING);
    }

    public void testBulkSuspendNoOp() throws Exception {
        WorkflowJobBean job1 = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action1 = this.addRecordToWfActionTable(job1.getId(), "1", WorkflowAction.Status.RUNNING);

        WorkflowJobBean job2 = this.addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean action2 = this.addRecordToWfActionTable(job2.getId(), "1", WorkflowAction.Status.DONE);

        Map<String, List<String>> map = new HashMap<String, List<String>>();
        List<String> names = new ArrayList<String>();
        names.add("testApp");
        map.put("name", names);

        new BulkWorkflowXCommand(map, 1, 50, OperationType.Suspend).call();
        verifyJobStatus(job1.getId(), WorkflowJob.Status.SUSPENDED);
        verifyActionStatus(action1.getId(), WorkflowAction.Status.RUNNING);
        verifyJobStatus(job2.getId(), WorkflowJob.Status.SUCCEEDED);
        verifyActionStatus(action2.getId(), WorkflowAction.Status.DONE);
    }

    public void testBulkSuspendNegative() throws Exception {
        WorkflowJobBean job1 = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action1 = this.addRecordToWfActionTable(job1.getId(), "1", WorkflowAction.Status.RUNNING);

        Map<String, List<String>> map = new HashMap<String, List<String>>();
        List<String> names = new ArrayList<String>();
        names.add("testApp-new");
        map.put("name", names);

        new BulkWorkflowXCommand(map, 1, 50, OperationType.Suspend).call();
        verifyJobStatus(job1.getId(), WorkflowJob.Status.RUNNING);
        verifyActionStatus(action1.getId(), WorkflowAction.Status.RUNNING);
    }

    public void testBulkResumeNegative() throws Exception {
        WorkflowJobBean job1 = this.addRecordToWfJobTable(WorkflowJob.Status.SUSPENDED, WorkflowInstance.Status.SUSPENDED);
        WorkflowActionBean action1 = this.addRecordToWfActionTable(job1.getId(), "1", WorkflowAction.Status.RUNNING);

        Map<String, List<String>> map = new HashMap<String, List<String>>();
        List<String> names = new ArrayList<String>();
        names.add("testApp-new");
        map.put("name", names);

        new BulkWorkflowXCommand(map, 1, 50, OperationType.Resume).call();
        verifyJobStatus(job1.getId(), WorkflowJob.Status.SUSPENDED);
        verifyActionStatus(action1.getId(), WorkflowAction.Status.RUNNING);
    }

    public void testBulkResumeNoOp() throws Exception {
        WorkflowJobBean job1 = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action1 = this.addRecordToWfActionTable(job1.getId(), "1", WorkflowAction.Status.RUNNING);

        Map<String, List<String>> map = new HashMap<String, List<String>>();
        List<String> names = new ArrayList<String>();
        names.add("testApp");
        map.put("name", names);

        new BulkWorkflowXCommand(map, 1, 50, OperationType.Resume).call();
        verifyJobStatus(job1.getId(), WorkflowJob.Status.RUNNING);
        verifyActionStatus(action1.getId(), WorkflowAction.Status.RUNNING);
    }

    private void verifyJobStatus(String jobId, WorkflowJob.Status status) throws Exception {
        WorkflowJobBean job = WorkflowJobQueryExecutor.getInstance().get(
                WorkflowJobQueryExecutor.WorkflowJobQuery.GET_WORKFLOW, jobId);
        assertEquals(status, job.getStatus());
    }

    private void verifyActionStatus(String actionId, WorkflowAction.Status status) throws Exception {
        WorkflowActionBean action = WorkflowActionQueryExecutor.getInstance().get(
                WorkflowActionQueryExecutor.WorkflowActionQuery.GET_ACTION, actionId);
        assertEquals(status, action.getStatus());
    }
}
