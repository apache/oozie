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

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.command.TestPurgeXCommand;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestCoordJobsCountNotForPurgeFromParentIdJPAExecutor extends XDataTestCase {
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

        BundleJobBean bundleJob = addRecordToBundleJobTable(Job.Status.SUCCEEDED, false);
        String bundleJobId = bundleJob.getId();
        int days = 1;
        assertEquals(0, (long) jpaService.execute(new CoordJobsCountNotForPurgeFromParentIdJPAExecutor(days, bundleJobId)));

        CoordinatorJobBean coordJob1 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        coordJob1 = TestPurgeXCommand.setLastModifiedTime(coordJob1, "2009-12-01T01:00Z");
        coordJob1.setAppName("coord1");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob1);
        addRecordToBundleActionTable(bundleJobId, coordJob1.getId(), coordJob1.getAppName(), 0, Job.Status.SUCCEEDED);
        days = 1;
        assertEquals(0, (long) jpaService.execute(new CoordJobsCountNotForPurgeFromParentIdJPAExecutor(days, bundleJobId)));
        days = TestPurgeXCommand.getNumDaysToNotBePurged(coordJob1.getLastModifiedTime());
        assertEquals(1, (long) jpaService.execute(new CoordJobsCountNotForPurgeFromParentIdJPAExecutor(days, bundleJobId)));

        CoordinatorJobBean coordJob2 = addRecordToCoordJobTable(CoordinatorJob.Status.DONEWITHERROR, false, false);
        coordJob2 = TestPurgeXCommand.setLastModifiedTime(coordJob2, "2009-11-01T01:00Z");
        coordJob2.setAppName("coord2");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob2);
        addRecordToBundleActionTable(bundleJobId, coordJob2.getId(), coordJob2.getAppName(), 0, Job.Status.DONEWITHERROR);
        days = TestPurgeXCommand.getNumDaysToNotBePurged(coordJob1.getLastModifiedTime());
        assertEquals(1, (long) jpaService.execute(new CoordJobsCountNotForPurgeFromParentIdJPAExecutor(days, bundleJobId)));
        days = TestPurgeXCommand.getNumDaysToNotBePurged(coordJob2.getLastModifiedTime());
        assertEquals(2, (long) jpaService.execute(new CoordJobsCountNotForPurgeFromParentIdJPAExecutor(days, bundleJobId)));

        CoordinatorJobBean coordJob3 = addRecordToCoordJobTable(CoordinatorJob.Status.FAILED, false, false);
        coordJob3 = TestPurgeXCommand.setLastModifiedTime(coordJob3, "2009-10-01T01:00Z");
        coordJob3.setAppName("coord3");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob3);
        addRecordToBundleActionTable(bundleJobId, coordJob3.getId(), coordJob3.getAppName(), 0, Job.Status.FAILED);
        days = TestPurgeXCommand.getNumDaysToNotBePurged(coordJob2.getLastModifiedTime());
        assertEquals(2, (long) jpaService.execute(new CoordJobsCountNotForPurgeFromParentIdJPAExecutor(days, bundleJobId)));
        days = TestPurgeXCommand.getNumDaysToNotBePurged(coordJob3.getLastModifiedTime());
        assertEquals(3, (long) jpaService.execute(new CoordJobsCountNotForPurgeFromParentIdJPAExecutor(days, bundleJobId)));

        CoordinatorJobBean coordJob4 = addRecordToCoordJobTable(CoordinatorJob.Status.KILLED, false, false);
        coordJob4 = TestPurgeXCommand.setLastModifiedTime(coordJob4, "2009-09-01T01:00Z");
        coordJob4.setAppName("coord4");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob4);
        addRecordToBundleActionTable(bundleJobId, coordJob4.getId(), coordJob4.getAppName(), 0, Job.Status.KILLED);
        days = TestPurgeXCommand.getNumDaysToNotBePurged(coordJob3.getLastModifiedTime());
        assertEquals(3, (long) jpaService.execute(new CoordJobsCountNotForPurgeFromParentIdJPAExecutor(days, bundleJobId)));
        days = TestPurgeXCommand.getNumDaysToNotBePurged(coordJob4.getLastModifiedTime());
        assertEquals(4, (long) jpaService.execute(new CoordJobsCountNotForPurgeFromParentIdJPAExecutor(days, bundleJobId)));

        CoordinatorJobBean coordJob5 = addRecordToCoordJobTable(CoordinatorJob.Status.PAUSED, false, false);
        coordJob5 = TestPurgeXCommand.setLastModifiedTime(coordJob5, "2009-08-01T01:00Z");
        coordJob5.setAppName("coord5");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob5);
        addRecordToBundleActionTable(bundleJobId, coordJob5.getId(), coordJob5.getAppName(), 0, Job.Status.PAUSED);
        days = TestPurgeXCommand.getNumDaysToNotBePurged(coordJob4.getLastModifiedTime());
        assertEquals(5, (long) jpaService.execute(new CoordJobsCountNotForPurgeFromParentIdJPAExecutor(days, bundleJobId)));
        days = TestPurgeXCommand.getNumDaysToNotBePurged(coordJob5.getLastModifiedTime());
        assertEquals(5, (long) jpaService.execute(new CoordJobsCountNotForPurgeFromParentIdJPAExecutor(days, bundleJobId)));

        CoordinatorJobBean coordJob6 = addRecordToCoordJobTable(CoordinatorJob.Status.PAUSEDWITHERROR, false, false);
        coordJob5 = TestPurgeXCommand.setLastModifiedTime(coordJob6, "2009-07-01T01:00Z");
        coordJob5.setAppName("coord6");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob6);
        addRecordToBundleActionTable(bundleJobId, coordJob6.getId(), coordJob6.getAppName(), 0, Job.Status.PAUSEDWITHERROR);
        days = TestPurgeXCommand.getNumDaysToNotBePurged(coordJob5.getLastModifiedTime());
        assertEquals(6, (long) jpaService.execute(new CoordJobsCountNotForPurgeFromParentIdJPAExecutor(days, bundleJobId)));
        days = TestPurgeXCommand.getNumDaysToNotBePurged(coordJob6.getLastModifiedTime());
        assertEquals(6, (long) jpaService.execute(new CoordJobsCountNotForPurgeFromParentIdJPAExecutor(days, bundleJobId)));

        CoordinatorJobBean coordJob7 = addRecordToCoordJobTable(CoordinatorJob.Status.PREMATER, false, false);
        coordJob5 = TestPurgeXCommand.setLastModifiedTime(coordJob7, "2009-06-01T01:00Z");
        coordJob5.setAppName("coord7");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob7);
        addRecordToBundleActionTable(bundleJobId, coordJob7.getId(), coordJob7.getAppName(), 0, Job.Status.PREMATER);
        days = TestPurgeXCommand.getNumDaysToNotBePurged(coordJob6.getLastModifiedTime());
        assertEquals(7, (long) jpaService.execute(new CoordJobsCountNotForPurgeFromParentIdJPAExecutor(days, bundleJobId)));
        days = TestPurgeXCommand.getNumDaysToNotBePurged(coordJob7.getLastModifiedTime());
        assertEquals(7, (long) jpaService.execute(new CoordJobsCountNotForPurgeFromParentIdJPAExecutor(days, bundleJobId)));

        CoordinatorJobBean coordJob8 = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, false, false);
        coordJob8 = TestPurgeXCommand.setLastModifiedTime(coordJob8, "2009-05-01T01:00Z");
        coordJob8.setAppName("coord8");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob8);
        addRecordToBundleActionTable(bundleJobId, coordJob8.getId(), coordJob8.getAppName(), 0, Job.Status.PREP);
        days = TestPurgeXCommand.getNumDaysToNotBePurged(coordJob7.getLastModifiedTime());
        assertEquals(8, (long) jpaService.execute(new CoordJobsCountNotForPurgeFromParentIdJPAExecutor(days, bundleJobId)));
        days = TestPurgeXCommand.getNumDaysToNotBePurged(coordJob8.getLastModifiedTime());
        assertEquals(8, (long) jpaService.execute(new CoordJobsCountNotForPurgeFromParentIdJPAExecutor(days, bundleJobId)));

        CoordinatorJobBean coordJob9 = addRecordToCoordJobTable(CoordinatorJob.Status.PREPPAUSED, false, false);
        coordJob9 = TestPurgeXCommand.setLastModifiedTime(coordJob9, "2009-04-01T01:00Z");
        coordJob9.setAppName("coord9");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob9);
        addRecordToBundleActionTable(bundleJobId, coordJob9.getId(), coordJob9.getAppName(), 0, Job.Status.PREPPAUSED);
        days = TestPurgeXCommand.getNumDaysToNotBePurged(coordJob8.getLastModifiedTime());
        assertEquals(9, (long) jpaService.execute(new CoordJobsCountNotForPurgeFromParentIdJPAExecutor(days, bundleJobId)));
        days = TestPurgeXCommand.getNumDaysToNotBePurged(coordJob9.getLastModifiedTime());
        assertEquals(9, (long) jpaService.execute(new CoordJobsCountNotForPurgeFromParentIdJPAExecutor(days, bundleJobId)));

        CoordinatorJobBean coordJob10 = addRecordToCoordJobTable(CoordinatorJob.Status.PREPSUSPENDED, false, false);
        coordJob10 = TestPurgeXCommand.setLastModifiedTime(coordJob10, "2009-03-01T01:00Z");
        coordJob10.setAppName("coord10");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob10);
        addRecordToBundleActionTable(bundleJobId, coordJob10.getId(), coordJob10.getAppName(), 0, Job.Status.PREPSUSPENDED);
        days = TestPurgeXCommand.getNumDaysToNotBePurged(coordJob9.getLastModifiedTime());
        assertEquals(10, (long) jpaService.execute(new CoordJobsCountNotForPurgeFromParentIdJPAExecutor(days, bundleJobId)));
        days = TestPurgeXCommand.getNumDaysToNotBePurged(coordJob10.getLastModifiedTime());
        assertEquals(10, (long) jpaService.execute(new CoordJobsCountNotForPurgeFromParentIdJPAExecutor(days, bundleJobId)));

        CoordinatorJobBean coordJob11 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        coordJob11 = TestPurgeXCommand.setLastModifiedTime(coordJob11, "2009-02-01T01:00Z");
        coordJob11.setAppName("coord11");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob11);
        addRecordToBundleActionTable(bundleJobId, coordJob11.getId(), coordJob11.getAppName(), 0, Job.Status.RUNNING);
        days = TestPurgeXCommand.getNumDaysToNotBePurged(coordJob10.getLastModifiedTime());
        assertEquals(11, (long) jpaService.execute(new CoordJobsCountNotForPurgeFromParentIdJPAExecutor(days, bundleJobId)));
        days = TestPurgeXCommand.getNumDaysToNotBePurged(coordJob11.getLastModifiedTime());
        assertEquals(11, (long) jpaService.execute(new CoordJobsCountNotForPurgeFromParentIdJPAExecutor(days, bundleJobId)));

        CoordinatorJobBean coordJob12 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNINGWITHERROR, false, false);
        coordJob12 = TestPurgeXCommand.setLastModifiedTime(coordJob12, "2009-01-01T01:00Z");
        coordJob12.setAppName("coord12");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob12);
        addRecordToBundleActionTable(bundleJobId, coordJob12.getId(), coordJob12.getAppName(), 0, Job.Status.RUNNINGWITHERROR);
        days = TestPurgeXCommand.getNumDaysToNotBePurged(coordJob11.getLastModifiedTime());
        assertEquals(12, (long) jpaService.execute(new CoordJobsCountNotForPurgeFromParentIdJPAExecutor(days, bundleJobId)));
        days = TestPurgeXCommand.getNumDaysToNotBePurged(coordJob12.getLastModifiedTime());
        assertEquals(12, (long) jpaService.execute(new CoordJobsCountNotForPurgeFromParentIdJPAExecutor(days, bundleJobId)));

        CoordinatorJobBean coordJob13 = addRecordToCoordJobTable(CoordinatorJob.Status.SUSPENDED, false, false);
        coordJob13 = TestPurgeXCommand.setLastModifiedTime(coordJob13, "2008-12-01T01:00Z");
        coordJob13.setAppName("coord13");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob13);
        addRecordToBundleActionTable(bundleJobId, coordJob13.getId(), coordJob13.getAppName(), 0, Job.Status.SUSPENDED);
        days = TestPurgeXCommand.getNumDaysToNotBePurged(coordJob12.getLastModifiedTime());
        assertEquals(13, (long) jpaService.execute(new CoordJobsCountNotForPurgeFromParentIdJPAExecutor(days, bundleJobId)));
        days = TestPurgeXCommand.getNumDaysToNotBePurged(coordJob13.getLastModifiedTime());
        assertEquals(13, (long) jpaService.execute(new CoordJobsCountNotForPurgeFromParentIdJPAExecutor(days, bundleJobId)));

        CoordinatorJobBean coordJob14 = addRecordToCoordJobTable(CoordinatorJob.Status.SUSPENDEDWITHERROR, false, false);
        coordJob14 = TestPurgeXCommand.setLastModifiedTime(coordJob14, "2008-11-01T01:00Z");
        coordJob14.setAppName("coord14");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob14);
        addRecordToBundleActionTable(bundleJobId, coordJob14.getId(), coordJob14.getAppName(), 0, Job.Status.SUSPENDEDWITHERROR);
        days = TestPurgeXCommand.getNumDaysToNotBePurged(coordJob13.getLastModifiedTime());
        assertEquals(14, (long) jpaService.execute(new CoordJobsCountNotForPurgeFromParentIdJPAExecutor(days, bundleJobId)));
        days = TestPurgeXCommand.getNumDaysToNotBePurged(coordJob14.getLastModifiedTime());
        assertEquals(14, (long) jpaService.execute(new CoordJobsCountNotForPurgeFromParentIdJPAExecutor(days, bundleJobId)));
    }
}
