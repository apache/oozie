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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.workflow.WorkflowInstance;

public class TestWorkflowJobsGetFromParentIdJPAExecutor extends XDataTestCase {
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
        cleanUpDBTables();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testGetCoordinatorParent() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        CoordinatorJobBean coordJobA = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        CoordinatorJobBean coordJobB = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        WorkflowJobBean wfJobA1 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJobA2 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJobB = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfActionA1 = addRecordToWfActionTable(wfJobA1.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfActionA2 = addRecordToWfActionTable(wfJobA2.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfActionB = addRecordToWfActionTable(wfJobB.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordActionA1 = addRecordToCoordActionTable(coordJobA.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJobA1.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordActionA2 = addRecordToCoordActionTable(coordJobA.getId(), 2, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJobA2.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordActionB = addRecordToCoordActionTable(coordJobB.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJobB.getId(), "SUCCEEDED", 0);

        List<String> children = new ArrayList<String>();
        children.addAll(jpaService.execute(new WorkflowJobsGetFromParentIdJPAExecutor(coordJobA.getId(), 10)));
        checkChildren(children, wfJobA1.getId(), wfJobA2.getId());

        children = new ArrayList<String>();
        children.addAll(jpaService.execute(new WorkflowJobsGetFromParentIdJPAExecutor(coordJobB.getId(), 10)));
        checkChildren(children, wfJobB.getId());
    }

    public void testGetWorkflowParent() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        WorkflowJobBean wfJobA = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJobB = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean subwfJobA1 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJobA.getId());
        WorkflowJobBean subwfJobA2 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJobA.getId());
        WorkflowJobBean subwfJobB = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJobB.getId());
        WorkflowActionBean wfActionA = addRecordToWfActionTable(wfJobA.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfActionB = addRecordToWfActionTable(wfJobB.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean subwfActionA1 = addRecordToWfActionTable(subwfJobA1.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean subwfActionA2 = addRecordToWfActionTable(subwfJobA2.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean subwfActionB = addRecordToWfActionTable(subwfJobB.getId(), "1", WorkflowAction.Status.OK);

        List<String> children = new ArrayList<String>();
        children.addAll(jpaService.execute(new WorkflowJobsGetFromParentIdJPAExecutor(wfJobA.getId(), 10)));
        checkChildren(children, subwfJobA1.getId(), subwfJobA2.getId());

        children = new ArrayList<String>();
        children.addAll(jpaService.execute(new WorkflowJobsGetFromParentIdJPAExecutor(wfJobB.getId(), 10)));
        checkChildren(children, subwfJobB.getId());
    }

    public void testGetWorkflowParentTooMany() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        WorkflowJobBean wfJob1 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                coordJob.getId());
        WorkflowJobBean wfJob2 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                coordJob.getId());
        WorkflowJobBean wfJob3 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                coordJob.getId());
        WorkflowJobBean wfJob4 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                coordJob.getId());
        WorkflowJobBean wfJob5 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                coordJob.getId());
        WorkflowActionBean wfAction1 = addRecordToWfActionTable(wfJob1.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction2 = addRecordToWfActionTable(wfJob2.getId(), "2", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction3 = addRecordToWfActionTable(wfJob3.getId(), "2", WorkflowAction.Status.OK);
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

        List<String> children = new ArrayList<String>();
        // Get the first 3
        children.addAll(jpaService.execute(new WorkflowJobsGetFromParentIdJPAExecutor(coordJob.getId(), 3)));
        assertEquals(3, children.size());
        // Get the next 3 (though there's only 2 more)
        children.addAll(jpaService.execute(new WorkflowJobsGetFromParentIdJPAExecutor(coordJob.getId(), 3, 3)));
        assertEquals(5, children.size());
        checkChildren(children, wfJob1.getId(), wfJob2.getId(), wfJob3.getId(), wfJob4.getId(), wfJob5.getId());
    }

    public void testGetCoordinatorParentTooMany() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

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
        WorkflowActionBean wfAction1 = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction2 = addRecordToWfActionTable(wfJob.getId(), "2", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction3 = addRecordToWfActionTable(wfJob.getId(), "3", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction4 = addRecordToWfActionTable(wfJob.getId(), "4", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction5 = addRecordToWfActionTable(wfJob.getId(), "5", WorkflowAction.Status.OK);
        WorkflowActionBean subwfAction1 = addRecordToWfActionTable(subwfJob1.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean subwfAction2 = addRecordToWfActionTable(subwfJob2.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean subwfAction3 = addRecordToWfActionTable(subwfJob3.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean subwfAction4 = addRecordToWfActionTable(subwfJob4.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean subwfAction5 = addRecordToWfActionTable(subwfJob5.getId(), "1", WorkflowAction.Status.OK);

        List<String> children = new ArrayList<String>();
        // Get the first 3
        children.addAll(jpaService.execute(new WorkflowJobsGetFromParentIdJPAExecutor(wfJob.getId(), 3)));
        assertEquals(3, children.size());
        // Get the next 3 (though there's only 2 more)
        children.addAll(jpaService.execute(new WorkflowJobsGetFromParentIdJPAExecutor(wfJob.getId(), 3, 3)));
        assertEquals(5, children.size());
        checkChildren(children, subwfJob1.getId(), subwfJob2.getId(), subwfJob3.getId(), subwfJob4.getId(), subwfJob5.getId());
    }

    private void checkChildren(List<String> children, String... wfJobIDs) {
        assertEquals(wfJobIDs.length, children.size());
        Arrays.sort(wfJobIDs);
        Collections.sort(children);

        for (int i = 0; i < wfJobIDs.length; i++) {
            assertEquals(wfJobIDs[i], children.get(i));
        }
    }
}
