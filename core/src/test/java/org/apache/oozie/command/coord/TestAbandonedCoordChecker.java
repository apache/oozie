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

package org.apache.oozie.command.coord;

import java.util.Date;

import org.apache.commons.lang.time.DateUtils;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.service.AbandonedCoordCheckerService.AbandonedCoordCheckerRunnable;

public class TestAbandonedCoordChecker extends XDataTestCase {
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

    public void tesAbandonedFailed() throws Exception {
        Date start = DateUtils.addMonths(new Date(), -1);
        Date end = new Date(start.getTime() + (4 * 60 * 60 * 1000)); // 4 hrs
        Date createdTime = start;

        final CoordinatorJobBean job1 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end,
                createdTime, true, false, 5);
        addRecordToCoordActionTable(job1.getId(), 5, CoordinatorAction.Status.FAILED);

        final CoordinatorJobBean job2 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end,
                createdTime, true, false, 4);
        addRecordToCoordActionTable(job2.getId(), 4, CoordinatorAction.Status.FAILED);

        AbandonedCoordCheckerRunnable coordChecked = new AbandonedCoordCheckerRunnable(5);
        coordChecked.run();
        String msg = coordChecked.getMessage();
        assertTrue(msg.contains(job1.getId()));
        assertFalse(msg.contains(job2.getId()));

    }

    public void testAbandoned_notAbandoned() throws Exception {
        Date start = DateUtils.addMonths(new Date(), -1);
        Date end = new Date(start.getTime() + (4 * 60 * 60 * 1000)); // 4 hrs

        Date createdTime = start;

        final CoordinatorJobBean job1 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end,
                createdTime, true, false, 6);

        addRecordToCoordActionTable(job1.getId(), 6, CoordinatorAction.Status.SUCCEEDED,
                CoordinatorAction.Status.FAILED);

        final CoordinatorJobBean job2 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, start, end,
                createdTime, true, false, 6);

        addRecordToCoordActionTable(job2.getId(), 6, CoordinatorAction.Status.SUCCEEDED,
                CoordinatorAction.Status.FAILED);

        AbandonedCoordCheckerRunnable coordChecked = new AbandonedCoordCheckerRunnable(5);
        coordChecked.run();
        String msg = coordChecked.getMessage();
        assertFalse(msg.contains(job1.getId()));
        assertFalse(msg.contains(job2.getId()));
    }

    public void testMessage_withTimedout() throws Exception {
        Date start = DateUtils.addMonths(new Date(), -1);
        Date end = new Date(start.getTime() + (4 * 60 * 60 * 1000)); // 4 hrs
        Date createdTime = start;

        final CoordinatorJobBean job1 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end,
                createdTime, true, false, 12);

        addRecordToCoordActionTable(job1.getId(), 12, CoordinatorAction.Status.TIMEDOUT);

        final CoordinatorJobBean job2 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end,
                createdTime, true, false, 4);
        addRecordToCoordActionTable(job2.getId(), 4, CoordinatorAction.Status.TIMEDOUT);

        AbandonedCoordCheckerRunnable coordChecked = new AbandonedCoordCheckerRunnable(10);
        coordChecked.run();
        String msg = coordChecked.getMessage();
        assertTrue(msg.contains(job1.getId()));
        assertFalse(msg.contains(job2.getId()));

    }

    public void testMessage_withMixedStatus() throws Exception {
        Date start = DateUtils.addMonths(new Date(), -1);
        Date end = new Date(start.getTime() + (4 * 60 * 60 * 1000)); // 4 hrs
        Date createdTime = start;

        final CoordinatorJobBean job1 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end,
                createdTime, true, false, 5);

        addRecordToCoordActionTable(job1.getId(), 5, CoordinatorAction.Status.FAILED,
                CoordinatorAction.Status.SUSPENDED, CoordinatorAction.Status.TIMEDOUT);

        final CoordinatorJobBean job2 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end,
                createdTime, true, false, 5);

        addRecordToCoordActionTable(job2.getId(), 5, CoordinatorAction.Status.FAILED,
                CoordinatorAction.Status.SUSPENDED, CoordinatorAction.Status.TIMEDOUT);

        final CoordinatorJobBean job3 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, start, end,
                createdTime, true, false, 5);
        addRecordToCoordActionTable(job3.getId(), 5, CoordinatorAction.Status.FAILED,
                CoordinatorAction.Status.SUSPENDED, CoordinatorAction.Status.TIMEDOUT);

        AbandonedCoordCheckerRunnable coordChecked = new AbandonedCoordCheckerRunnable(5);
        coordChecked.run();
        String msg = coordChecked.getMessage();
        assertTrue(msg.contains(job1.getId()));
        assertTrue(msg.contains(job2.getId()));
        assertFalse(msg.contains(job3.getId()));
    }

    public void testKill() throws Exception {
        Date start = DateUtils.addMonths(new Date(), -1);
        Date end = new Date(start.getTime() + (4 * 60 * 60 * 1000)); // 4 hrs
        Date createdTime = start;

        CoordinatorJobBean job1 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, createdTime,
                true, false, 6);
        addRecordToCoordActionTable(job1.getId(), 6, CoordinatorAction.Status.FAILED);
        CoordinatorJobBean job2 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, createdTime,
                true, false, 4);
        addRecordToCoordActionTable(job2.getId(), 4, CoordinatorAction.Status.FAILED);
        new AbandonedCoordCheckerRunnable(5, true).run();
        assertEquals(CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB, job1.getId()).getStatus(),
                CoordinatorJob.Status.KILLED);
        assertEquals(CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB, job2.getId()).getStatus(),
                CoordinatorJob.Status.RUNNING);
    }

    public void testStartTime() throws Exception {
        Date start = DateUtils.addDays(new Date(), 1);
        Date end = DateUtils.addDays(new Date(), 6);
        Date createdTime = new Date();

        CoordinatorJobBean job1 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, createdTime,
                true, false, 6);
        addRecordToCoordActionTable(job1.getId(), 6, CoordinatorAction.Status.FAILED);

        start = DateUtils.addDays(new Date(), -3);
        createdTime = DateUtils.addDays(new Date(), -4);
        CoordinatorJobBean job2 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, createdTime,
                true, false, 4);
        addRecordToCoordActionTable(job2.getId(), 10, CoordinatorAction.Status.FAILED);
        new AbandonedCoordCheckerRunnable(5, true).run();

        // job1 should be RUNNING as starttime > 2 days buffer
        assertEquals(CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB, job1.getId()).getStatus(),
                CoordinatorJob.Status.RUNNING);
        assertEquals(CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB, job2.getId()).getStatus(),
                CoordinatorJob.Status.KILLED);
    }

    public void testCatchupJob() throws Exception {
        Date start = DateUtils.addMonths(new Date(), -1);
        Date end = new Date(start.getTime() + (4 * 60 * 60 * 1000)); // 4 hrs
        Date createdTime = DateUtils.addDays(new Date(), -1);

        CoordinatorJobBean job1 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, createdTime,
                true, false, 6);
        addRecordToCoordActionTable(job1.getId(), 6, CoordinatorAction.Status.FAILED);

        createdTime = DateUtils.addDays(new Date(), -3);

        CoordinatorJobBean job2 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, createdTime,
                true, false, 4);
        addRecordToCoordActionTable(job2.getId(), 10, CoordinatorAction.Status.FAILED);
        new AbandonedCoordCheckerRunnable(5, true).run();

        // Only one job should be running.
        assertEquals(CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB, job1.getId()).getStatus(),
                CoordinatorJob.Status.RUNNING);
        assertEquals(CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB, job2.getId()).getStatus(),
                CoordinatorJob.Status.KILLED);
    }

    private void addRecordToCoordActionTable(String jobId, int count, CoordinatorAction.Status... status)
            throws Exception {

        for (int i = 1; i <= count; i++) {
            CoordinatorAction.Status jobStatus = status[status.length - 1];
            if (i <= status.length) {
                jobStatus = status[i - 1];
            }
            addRecordToCoordActionTable(jobId, i, jobStatus, "coord-action-get.xml", 0);
        }
    }
}
