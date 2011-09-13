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

import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobUpdateJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.workflow.WorkflowInstance;

public class TestWorkflowJobUpdateJPAExecutor extends XDataTestCase {
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

    public void testWorkflowJobUpdate() throws Exception {
        WorkflowJobBean job = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        WorkflowJobGetJPAExecutor wfGetCmd = new WorkflowJobGetJPAExecutor(job.getId());
        WorkflowJobBean wfBean = jpaService.execute(wfGetCmd);

        // first update;
        wfBean.setStatus(WorkflowJob.Status.SUCCEEDED);
        WorkflowJobUpdateJPAExecutor wfUpdateCmd1 = new WorkflowJobUpdateJPAExecutor(wfBean);
        jpaService.execute(wfUpdateCmd1);
        WorkflowJobBean wfBean1 = jpaService.execute(wfGetCmd);
        assertEquals(wfBean1.getId(), job.getId());
        assertEquals(wfBean1.getStatusStr(), "SUCCEEDED");

        // second update;
        wfBean.setAppName("test");
        wfBean.setStatus(WorkflowJob.Status.RUNNING);
        WorkflowJobUpdateJPAExecutor wfUpdateCmd2 = new WorkflowJobUpdateJPAExecutor(wfBean);
        jpaService.execute(wfUpdateCmd2);
        WorkflowJobBean wfBean2 = jpaService.execute(wfGetCmd);
        assertEquals(wfBean2.getId(), job.getId());
        assertEquals(wfBean2.getStatusStr(), "RUNNING");
    }

}
