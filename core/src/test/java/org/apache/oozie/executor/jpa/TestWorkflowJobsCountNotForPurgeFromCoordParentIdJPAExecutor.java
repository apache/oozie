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

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.TestPurgeXCommand;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.workflow.WorkflowInstance;

public class TestWorkflowJobsCountNotForPurgeFromCoordParentIdJPAExecutor extends XDataTestCase {
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

    public void testCount() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        String coordJobId = coordJob.getId();
        int days = 1;
        assertEquals(0, (long) jpaService.execute(new WorkflowJobsCountNotForPurgeFromCoordParentIdJPAExecutor(days, coordJobId)));

        WorkflowJobBean wfJob1 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED, coordJobId);
        wfJob1 = TestPurgeXCommand.setEndTime(wfJob1, "2009-12-01T01:00Z");
        CoordinatorActionBean coordAction1 = addRecordToCoordActionTable(coordJobId, 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob1.getId(), wfJob1.getStatusStr(), 0);
        days = 1;
        assertEquals(0, (long) jpaService.execute(new WorkflowJobsCountNotForPurgeFromCoordParentIdJPAExecutor(days, coordJobId)));
        days = TestPurgeXCommand.getNumDaysToNotBePurged(wfJob1.getEndTime());
        assertEquals(1, (long) jpaService.execute(new WorkflowJobsCountNotForPurgeFromCoordParentIdJPAExecutor(days, coordJobId)));

        WorkflowJobBean wfJob2 = addRecordToWfJobTable(WorkflowJob.Status.FAILED, WorkflowInstance.Status.FAILED, coordJobId);
        wfJob2 = TestPurgeXCommand.setEndTime(wfJob2, "2009-11-01T01:00Z");
        CoordinatorActionBean coordAction2 = addRecordToCoordActionTable(coordJobId, 2, CoordinatorAction.Status.FAILED,
                "coord-action-get.xml", wfJob2.getId(), wfJob2.getStatusStr(), 0);
        days = TestPurgeXCommand.getNumDaysToNotBePurged(wfJob1.getEndTime());
        assertEquals(1, (long) jpaService.execute(new WorkflowJobsCountNotForPurgeFromCoordParentIdJPAExecutor(days, coordJobId)));
        days = TestPurgeXCommand.getNumDaysToNotBePurged(wfJob2.getEndTime());
        assertEquals(2, (long) jpaService.execute(new WorkflowJobsCountNotForPurgeFromCoordParentIdJPAExecutor(days, coordJobId)));

        WorkflowJobBean wfJob3 = addRecordToWfJobTable(WorkflowJob.Status.KILLED, WorkflowInstance.Status.KILLED, coordJobId);
        wfJob3 = TestPurgeXCommand.setEndTime(wfJob3, "2009-10-01T01:00Z");
        CoordinatorActionBean coordAction3 = addRecordToCoordActionTable(coordJobId, 3, CoordinatorAction.Status.KILLED,
                "coord-action-get.xml", wfJob3.getId(), wfJob3.getStatusStr(), 0);
        days = TestPurgeXCommand.getNumDaysToNotBePurged(wfJob2.getEndTime());
        assertEquals(2, (long) jpaService.execute(new WorkflowJobsCountNotForPurgeFromCoordParentIdJPAExecutor(days, coordJobId)));
        days = TestPurgeXCommand.getNumDaysToNotBePurged(wfJob3.getEndTime());
        assertEquals(3, (long) jpaService.execute(new WorkflowJobsCountNotForPurgeFromCoordParentIdJPAExecutor(days, coordJobId)));

        WorkflowJobBean wfJob4 = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP, coordJobId);
        wfJob4 = TestPurgeXCommand.setEndTime(wfJob4, "2009-09-01T01:00Z");
        CoordinatorActionBean coordAction4 = addRecordToCoordActionTable(coordJobId, 4, CoordinatorAction.Status.RUNNING,
                "coord-action-get.xml", wfJob4.getId(), wfJob4.getStatusStr(), 0);
        days = TestPurgeXCommand.getNumDaysToNotBePurged(wfJob3.getEndTime());
        assertEquals(4, (long) jpaService.execute(new WorkflowJobsCountNotForPurgeFromCoordParentIdJPAExecutor(days, coordJobId)));
        days = TestPurgeXCommand.getNumDaysToNotBePurged(wfJob4.getEndTime());
        assertEquals(4, (long) jpaService.execute(new WorkflowJobsCountNotForPurgeFromCoordParentIdJPAExecutor(days, coordJobId)));

        WorkflowJobBean wfJob5 = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING, coordJobId);
        wfJob5 = TestPurgeXCommand.setEndTime(wfJob5, "2009-08-01T01:00Z");
        CoordinatorActionBean coordAction5 = addRecordToCoordActionTable(coordJobId, 5, CoordinatorAction.Status.RUNNING,
                "coord-action-get.xml", wfJob5.getId(), wfJob5.getStatusStr(), 0);
        days = TestPurgeXCommand.getNumDaysToNotBePurged(wfJob4.getEndTime());
        assertEquals(5, (long) jpaService.execute(new WorkflowJobsCountNotForPurgeFromCoordParentIdJPAExecutor(days, coordJobId)));
        days = TestPurgeXCommand.getNumDaysToNotBePurged(wfJob5.getEndTime());
        assertEquals(5, (long) jpaService.execute(new WorkflowJobsCountNotForPurgeFromCoordParentIdJPAExecutor(days, coordJobId)));

        WorkflowJobBean wfJob6 = addRecordToWfJobTable(WorkflowJob.Status.SUSPENDED, WorkflowInstance.Status.SUSPENDED, coordJobId);
        wfJob6 = TestPurgeXCommand.setEndTime(wfJob6, "2009-07-01T01:00Z");
        CoordinatorActionBean coordAction6 = addRecordToCoordActionTable(coordJobId, 6, CoordinatorAction.Status.SUSPENDED,
                "coord-action-get.xml", wfJob6.getId(), wfJob6.getStatusStr(), 0);
        days = TestPurgeXCommand.getNumDaysToNotBePurged(wfJob5.getEndTime());
        assertEquals(6, (long) jpaService.execute(new WorkflowJobsCountNotForPurgeFromCoordParentIdJPAExecutor(days, coordJobId)));
        days = TestPurgeXCommand.getNumDaysToNotBePurged(wfJob6.getEndTime());
        assertEquals(6, (long) jpaService.execute(new WorkflowJobsCountNotForPurgeFromCoordParentIdJPAExecutor(days, coordJobId)));
    }
}
