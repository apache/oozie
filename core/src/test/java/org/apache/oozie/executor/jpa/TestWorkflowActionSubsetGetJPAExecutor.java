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

import java.util.List;

import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.workflow.WorkflowInstance;

public class TestWorkflowActionSubsetGetJPAExecutor extends XDataTestCase {
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

    public void testWfActionSubsetGet() throws Exception {
        WorkflowJobBean job = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);
        addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.RUNNING);
        addRecordToWfActionTable(job.getId(), "2", WorkflowAction.Status.RUNNING);
        _testWfActionSubsetGet(job.getId());
        System.out.println("testWfActionSubsetGet Successful");
    }

    private void _testWfActionSubsetGet(String wfId) throws JPAExecutorException {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        WorkflowActionSubsetGetJPAExecutor wfActionCmd = new WorkflowActionSubsetGetJPAExecutor(wfId, 1, 1);
        List<WorkflowActionBean> actions = jpaService.execute(wfActionCmd);
        assertEquals(1, actions.size());
        wfActionCmd = new WorkflowActionSubsetGetJPAExecutor(wfId, 1, 2);
        actions = jpaService.execute(wfActionCmd);
        assertEquals(2, actions.size());
    }
}
