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
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.TestPurgeXCommand;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.workflow.WorkflowInstance;

public class TestWorkflowJobsCountNotForPurgeFromWorkflowParentIdJPAExecutor extends XDataTestCase {
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

    public void testCount() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        String wfJobId = wfJob.getId();
        int days = 1;
        assertEquals(0, (long) jpaService.execute(new WorkflowJobsCountNotForPurgeFromWorkflowParentIdJPAExecutor(days, wfJobId)));

        WorkflowJobBean subwfJob1 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED, wfJobId);
        subwfJob1 = TestPurgeXCommand.setEndTime(subwfJob1, "2009-12-01T01:00Z");
        days = 1;
        assertEquals(0, (long) jpaService.execute(new WorkflowJobsCountNotForPurgeFromWorkflowParentIdJPAExecutor(days, wfJobId)));
        days = TestPurgeXCommand.getNumDaysToNotBePurged(subwfJob1.getEndTime());
        System.out.println("Debug: days " + days);
        assertEquals(1, (long) jpaService.execute(new WorkflowJobsCountNotForPurgeFromWorkflowParentIdJPAExecutor(days, wfJobId)));

        WorkflowJobBean subwfJob2 = addRecordToWfJobTable(WorkflowJob.Status.FAILED, WorkflowInstance.Status.FAILED, wfJobId);
        subwfJob2 = TestPurgeXCommand.setEndTime(subwfJob2, "2009-11-01T01:00Z");
        days = TestPurgeXCommand.getNumDaysToNotBePurged(subwfJob1.getEndTime());
        assertEquals(1, (long) jpaService.execute(new WorkflowJobsCountNotForPurgeFromWorkflowParentIdJPAExecutor(days, wfJobId)));
        days = TestPurgeXCommand.getNumDaysToNotBePurged(subwfJob2.getEndTime());
        assertEquals(2, (long) jpaService.execute(new WorkflowJobsCountNotForPurgeFromWorkflowParentIdJPAExecutor(days, wfJobId)));

        WorkflowJobBean subwfJob3 = addRecordToWfJobTable(WorkflowJob.Status.KILLED, WorkflowInstance.Status.KILLED, wfJobId);
        subwfJob3 = TestPurgeXCommand.setEndTime(subwfJob3, "2009-10-01T01:00Z");
        days = TestPurgeXCommand.getNumDaysToNotBePurged(subwfJob2.getEndTime());
        assertEquals(2, (long) jpaService.execute(new WorkflowJobsCountNotForPurgeFromWorkflowParentIdJPAExecutor(days, wfJobId)));
        days = TestPurgeXCommand.getNumDaysToNotBePurged(subwfJob3.getEndTime());
        assertEquals(3, (long) jpaService.execute(new WorkflowJobsCountNotForPurgeFromWorkflowParentIdJPAExecutor(days, wfJobId)));

        WorkflowJobBean subwfJob4 = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP, wfJobId);
        subwfJob4 = TestPurgeXCommand.setEndTime(subwfJob4, "2009-09-01T01:00Z");
        days = TestPurgeXCommand.getNumDaysToNotBePurged(subwfJob3.getEndTime());
        assertEquals(4, (long) jpaService.execute(new WorkflowJobsCountNotForPurgeFromWorkflowParentIdJPAExecutor(days, wfJobId)));
        days = TestPurgeXCommand.getNumDaysToNotBePurged(subwfJob4.getEndTime());
        assertEquals(4, (long) jpaService.execute(new WorkflowJobsCountNotForPurgeFromWorkflowParentIdJPAExecutor(days, wfJobId)));

        WorkflowJobBean subwfJob5 = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING, wfJobId);
        subwfJob5 = TestPurgeXCommand.setEndTime(subwfJob5, "2009-08-01T01:00Z");
        days = TestPurgeXCommand.getNumDaysToNotBePurged(subwfJob4.getEndTime());
        assertEquals(5, (long) jpaService.execute(new WorkflowJobsCountNotForPurgeFromWorkflowParentIdJPAExecutor(days, wfJobId)));
        days = TestPurgeXCommand.getNumDaysToNotBePurged(subwfJob5.getEndTime());
        assertEquals(5, (long) jpaService.execute(new WorkflowJobsCountNotForPurgeFromWorkflowParentIdJPAExecutor(days, wfJobId)));

        WorkflowJobBean subwfJob6 = addRecordToWfJobTable(WorkflowJob.Status.SUSPENDED, WorkflowInstance.Status.SUSPENDED, wfJobId);
        subwfJob6 = TestPurgeXCommand.setEndTime(subwfJob6, "2009-07-01T01:00Z");
        days = TestPurgeXCommand.getNumDaysToNotBePurged(subwfJob5.getEndTime());
        assertEquals(6, (long) jpaService.execute(new WorkflowJobsCountNotForPurgeFromWorkflowParentIdJPAExecutor(days, wfJobId)));
        days = TestPurgeXCommand.getNumDaysToNotBePurged(subwfJob6.getEndTime());
        assertEquals(6, (long) jpaService.execute(new WorkflowJobsCountNotForPurgeFromWorkflowParentIdJPAExecutor(days, wfJobId)));
    }
}
