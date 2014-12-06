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
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.workflow.WorkflowInstance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TestWorkflowJobsBasicInfoFromWorkflowParentIdJPAExecutor extends XDataTestCase {
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

    public void testGetWorkflowParent() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        HashMap<String,WorkflowJobBean> wflist = new HashMap<String, WorkflowJobBean>();

        WorkflowJobBean wfJobA = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJobB = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean subwfJobA1 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJobA.getId());
        WorkflowJobBean subwfJobA2 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJobA.getId());
        WorkflowJobBean subwfJobB = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJobB.getId());

        List<WorkflowJobBean> children = new ArrayList<WorkflowJobBean>();
        children.addAll(jpaService.execute(new WorkflowJobsBasicInfoFromWorkflowParentIdJPAExecutor(wfJobA.getId(), 10)));
        wflist.put(subwfJobA1.getId(), subwfJobA1);
        wflist.put(subwfJobA2.getId(),subwfJobA2);
        checkChildren(children, wflist);

        children = new ArrayList<WorkflowJobBean>();
        children.addAll(jpaService.execute(new WorkflowJobsBasicInfoFromWorkflowParentIdJPAExecutor(wfJobB.getId(), 10)));
        wflist.clear();
        wflist.put(subwfJobB.getId(),subwfJobB);
        checkChildren(children, wflist);
    }

    public void testGetCoordinatorParentTooMany() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        HashMap<String,WorkflowJobBean> wflist = new HashMap<String, WorkflowJobBean>();
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
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

        List<WorkflowJobBean> children = new ArrayList<WorkflowJobBean>();
        // Get the first 3
        children.addAll(jpaService.execute(new WorkflowJobsBasicInfoFromWorkflowParentIdJPAExecutor(wfJob.getId(), 3)));
        assertEquals(3, children.size());
        // Get the next 3 (though there's only 2 more)
        children.addAll(jpaService.execute(new WorkflowJobsBasicInfoFromWorkflowParentIdJPAExecutor(wfJob.getId(), 3, 3)));
        assertEquals(5, children.size());
        wflist.put(subwfJob1.getId(), subwfJob1);
        wflist.put(subwfJob2.getId(), subwfJob2);
        wflist.put(subwfJob3.getId(), subwfJob3);
        wflist.put(subwfJob4.getId(), subwfJob4);
        wflist.put(subwfJob5.getId(), subwfJob5);

        checkChildren(children, wflist);
    }

    private void checkChildren(List<WorkflowJobBean> children, HashMap<String,WorkflowJobBean> wfJobBaselist) {
        assertEquals(wfJobBaselist.size(), children.size());
        for (int i = 0; i < children.size(); i++) {
            WorkflowJobBean wfJobBase = wfJobBaselist.get(children.get(i).getId());
            assertNotNull(wfJobBase);
            assertEquals(wfJobBase.getStatus(),children.get(i).getStatus());
        }
    }
}
