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

import java.io.File;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.SLAEventBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.CoordinatorJob.Timeunit;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.coord.TimeUnit;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetActionsJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetRunningActionsCountJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.CoordJobGetActionsSubsetJPAExecutor;
import org.apache.oozie.executor.jpa.SLAEventsGetForSeqIdJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.SchedulerService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;

@SuppressWarnings("deprecation")
public class TestCoordMaterializeTransitionXCommand extends XDataTestCase {

    private int oneHourInSeconds = hoursToSeconds(1);

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        new Services().init();
        Services.get().setService(FakeCallableQueueService.class);
        Services.get().get(SchedulerService.class).destroy();
    }

    @Override
    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    public void testActionMater() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2009-03-06T010:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2009-03-11T10:00Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, false, false, 0);
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        checkCoordAction(job.getId() + "@1");
    }

    public void testActionMaterForHcatalog() throws Exception {
        Services.get().destroy();
        Services services = super.setupServicesForHCatalog();
        services.init();
        Date startTime = DateUtils.parseDateOozieTZ("2009-03-06T010:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2009-03-11T10:00Z");
        CoordinatorJobBean job = addRecordToCoordJobTableForWaiting("coord-job-for-matd-hcat.xml",
                CoordinatorJob.Status.RUNNING, startTime, endTime, false, false, 0);
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        CoordinatorActionBean actionBean = getCoordAction(job.getId() + "@1");
        assertEquals("file://dummyhdfs/2009/05/_SUCCESS" + CoordCommandUtils.RESOLVED_UNRESOLVED_SEPARATOR
                + "${coord:latestRange(-1,0)}", actionBean.getMissingDependencies());

        assertEquals("hcat://dummyhcat:1000/db1/table1/ds=2009-12" + CoordELFunctions.INSTANCE_SEPARATOR
                + "hcat://dummyhcat:1000/db3/table3/ds=2009-05" + CoordELFunctions.INSTANCE_SEPARATOR
                + "hcat://dummyhcat:1000/db3/table3/ds=2009-26", actionBean.getPushMissingDependencies());
    }

    public void testActionMaterForHcatalogIncorrectURI() throws Exception {
        Services.get().destroy();
        Services services = super.setupServicesForHCatalog();
        services.init();
        Date startTime = DateUtils.parseDateOozieTZ("2009-03-06T010:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2009-03-11T10:00Z");
        CoordinatorJobBean job = addRecordToCoordJobTableForWaiting("coord-job-for-matd-neg-hcat.xml",
                CoordinatorJob.Status.RUNNING, startTime, endTime, false, false, 0);
        try {
            new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
            fail("Expected Command exception but didn't catch any");
        }
        catch (CommandException e) {
            e.printStackTrace();
            job = services.get(JPAService.class).execute(new CoordJobGetJPAExecutor(job.getId()));
            assertEquals(CoordinatorJob.Status.FAILED, job.getStatus());
            assertEquals(ErrorCode.E1012, e.getErrorCode());
        }
        catch (Exception e) {
            fail("Unexpected exception " + e.getMessage());
        }
    }

    public void testActionMaterForHcatalogRelativePath() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2009-03-06T010:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2009-03-11T10:00Z");
        CoordinatorJobBean job = addRecordToCoordJobTableForWaiting("coord-job-for-matd-relative.xml",
                CoordinatorJob.Status.RUNNING, startTime, endTime, false, false, 0);
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
    }

    public void testActionMaterWithCronFrequency1() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2013-07-18T00:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2013-07-18T01:00Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, null,
                "10,20 * * * *");
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        Date[] nominalTimes = new Date[] {DateUtils.parseDateOozieTZ("2013-07-18T00:10Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:20Z")};
        final int expectedNominalTimeCount = 2;
        checkCoordActionsNominalTime(job.getId(), expectedNominalTimeCount, nominalTimes);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            job =  jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
            assertTrue(job.isDoneMaterialization());
            assertEquals(job.getLastActionNumber(), expectedNominalTimeCount);
            assertEquals(job.getNextMaterializedTime(), DateUtils.parseDateOozieTZ("2013-07-18T01:10Z"));
        }
        catch (JPAExecutorException se) {
            se.printStackTrace();
            fail("Job ID " + job.getId() + " was not stored properly in db");
        }
    }

    public void testActionMaterWithCronFrequency2() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2013-07-18T00:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2013-07-18T01:00Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, null,
                "10-20 * * * *");
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        Date[] nominalTimes = new Date[] {DateUtils.parseDateOozieTZ("2013-07-18T00:10Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:11Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:12Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:13Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:14Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:15Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:16Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:17Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:18Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:19Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:20Z"),};
        final int expectedNominalTimeCount = 11;
        checkCoordActionsNominalTime(job.getId(), expectedNominalTimeCount, nominalTimes);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            job =  jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
            assertTrue(job.isDoneMaterialization());
            assertEquals(job.getLastActionNumber(), expectedNominalTimeCount);
            assertEquals(job.getNextMaterializedTime(), DateUtils.parseDateOozieTZ("2013-07-18T01:10Z"));
        }
        catch (JPAExecutorException se) {
            se.printStackTrace();
            fail("Job ID " + job.getId() + " was not stored properly in db");
        }
    }

    public void testActionMaterWithCronFrequency3() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2013-07-18T00:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2013-07-18T01:00Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, null,
                "0/15 2 * 5-7 4,5");
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        final int expectedNominalTimeCount = 0;
        checkCoordActionsNominalTime(job.getId(), expectedNominalTimeCount, new Date[]{});

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            job =  jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
            assertTrue(job.isDoneMaterialization());
            assertEquals(job.getLastActionNumber(), expectedNominalTimeCount);
            assertEquals(job.getNextMaterializedTime(), DateUtils.parseDateOozieTZ("2013-07-18T02:00Z"));
        }
        catch (JPAExecutorException se) {
            se.printStackTrace();
            fail("Job ID " + job.getId() + " was not stored properly in db");
        }
    }

    public void testActionMaterWithCronFrequency4() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2013-07-18T00:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2013-07-18T01:00Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, null,
                "0/15 * * 5-7 4,5");
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        Date[] nominalTimes = new Date[] {DateUtils.parseDateOozieTZ("2013-07-18T00:00Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:15Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:30Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:45Z"),};
        final int expectedNominalTimeCount = 4;
        checkCoordActionsNominalTime(job.getId(), expectedNominalTimeCount, nominalTimes);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            job =  jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
            assertTrue(job.isDoneMaterialization());
            assertEquals(job.getLastActionNumber(), expectedNominalTimeCount);
            assertEquals(job.getNextMaterializedTime(), DateUtils.parseDateOozieTZ("2013-07-18T01:00Z"));
        }
        catch (JPAExecutorException se) {
            se.printStackTrace();
            fail("Job ID " + job.getId() + " was not stored properly in db");
        }
    }

    public void testActionMaterWithCronFrequency5() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2013-07-18T00:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2013-07-18T01:00Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, null,
                "20/15 * * 5-7 4,5");
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        Date[] nominalTimes = new Date[] {DateUtils.parseDateOozieTZ("2013-07-18T00:20Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:35Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:50Z"),};
        final int expectedNominalTimeCount = 3;
        checkCoordActionsNominalTime(job.getId(), expectedNominalTimeCount, nominalTimes);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            job =  jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
            assertTrue(job.isDoneMaterialization());
            assertEquals(job.getLastActionNumber(), expectedNominalTimeCount);
            assertEquals(job.getNextMaterializedTime(), DateUtils.parseDateOozieTZ("2013-07-18T01:20Z"));
        }
        catch (JPAExecutorException se) {
            se.printStackTrace();
            fail("Job ID " + job.getId() + " was not stored properly in db");
        }
    }

    public void testActionMaterWithCronFrequency6() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2013-07-18T00:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2013-07-18T01:00Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, null,
                "20");
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        Date[] nominalTimes = new Date[] {DateUtils.parseDateOozieTZ("2013-07-18T00:00Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:20Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:40Z"),};
        final int expectedNominalTimeCount = 3;
        checkCoordActionsNominalTime(job.getId(), expectedNominalTimeCount, nominalTimes);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            job =  jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
            assertTrue(job.isDoneMaterialization());
            assertEquals(job.getLastActionNumber(), expectedNominalTimeCount);
            assertEquals(job.getNextMaterializedTime(), DateUtils.parseDateOozieTZ("2013-07-18T01:00Z"));
        }
        catch (JPAExecutorException se) {
            se.printStackTrace();
            fail("Job ID " + job.getId() + " was not stored properly in db");
        }
    }

    public void testActionMaterWithCronFrequency7() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2013-07-18T00:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2013-07-18T01:00Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, null,
                "20/15 * * 7,10 THU");
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        Date[] nominalTimes = new Date[] {DateUtils.parseDateOozieTZ("2013-07-18T00:20Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:35Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:50Z"),};
        final int expectedNominalTimeCount = 3;
        checkCoordActionsNominalTime(job.getId(), expectedNominalTimeCount, nominalTimes);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            job =  jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
            assertTrue(job.isDoneMaterialization());
            assertEquals(expectedNominalTimeCount, job.getLastActionNumber());
            assertEquals(job.getNextMaterializedTime(), DateUtils.parseDateOozieTZ("2013-07-18T01:20Z"));
        }
        catch (JPAExecutorException se) {
            se.printStackTrace();
            fail("Job ID " + job.getId() + " was not stored properly in db");
        }
    }

    public void testActionMaterwithCronFrequencyWithThrottle() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2013-07-18T00:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2013-07-18T01:00Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, null,
                "0/10 * * * *");
        job.setMatThrottling(3);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, job);

        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        Date[] nominalTimes = new Date[] {DateUtils.parseDateOozieTZ("2013-07-18T00:00Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:10Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:20Z")};
        final int expectedNominalTimeCount = 3;
        checkCoordActionsNominalTime(job.getId(), expectedNominalTimeCount, nominalTimes);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            job =  jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
            assertFalse(job.isDoneMaterialization());
            assertEquals(expectedNominalTimeCount, job.getLastActionNumber());
            assertEquals(DateUtils.parseDateOozieTZ("2013-07-18T00:30Z"), job.getNextMaterializedTime());
        }
        catch (JPAExecutorException se) {
            se.printStackTrace();
            fail("Job ID " + job.getId() + " was not stored properly in db");
        }
    }

    public void testCronFrequencyCatchupThrottleLessThanDuration() throws Exception {
        final String startInThePast = "2013-03-10T08:00Z";
        final String startPlusOneDay = "2013-03-11T08:00Z";
        final Date startTime = DateUtils.parseDateOozieTZ(startInThePast);
        final Date endTime = DateUtils.parseDateOozieTZ(startPlusOneDay);
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, startTime, endTime, false, false, 0);
        job.setNextMaterializedTime(startTime);
        job.setMatThrottling(3);
        final String everyHour = "0 * * * *";
        job.setFrequency(everyHour);
        job.setTimeUnit(Timeunit.CRON);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, job);

        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        final String startPlusOneHour = "2013-03-10T09:00Z";
        final String startPlusTwoHours = "2013-03-10T10:00Z";
        final Date[] nominalTimes = new Date[] {DateUtils.parseDateOozieTZ(startInThePast),
                DateUtils.parseDateOozieTZ(startPlusOneHour),
                DateUtils.parseDateOozieTZ(startPlusTwoHours)};
        final int expectedNominalTimeCount = 3;
        checkCoordActionsNominalTime(job.getId(), expectedNominalTimeCount, nominalTimes);

        try {
            final JPAService jpaService = Services.get().get(JPAService.class);
            job =  jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
            assertFalse("coordinator job shouldn't have yet been materialized", job.isDoneMaterialization());
            assertEquals("coordinator action count mismatch", expectedNominalTimeCount, job.getLastActionNumber());
            final String startPlusThreeHours = "2013-03-10T11:00Z";
            assertEquals("coordinator next materialization time mismatch",
                    DateUtils.parseDateOozieTZ(startPlusThreeHours), job.getNextMaterializedTime());
        }
        catch (final JPAExecutorException se) {
            se.printStackTrace();
            fail("Job ID " + job.getId() + " was not stored properly in db");
        }
    }

    public void testCronFrequencyCatchupThrottleEqualsDurationDSTChange() throws Exception {
        final String startInThePast = "2013-03-10T08:00Z";
        final Date startTimeBeforeDSTChange = DateUtils.parseDateOozieTZ(startInThePast);
        final String startPlusTwoHoursAndSome = "2013-03-10T10:01Z";
        final Date endTimeAfterDSTChange = DateUtils.parseDateOozieTZ(startPlusTwoHoursAndSome);
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.PREP,
                startTimeBeforeDSTChange,
                endTimeAfterDSTChange,
                false,
                false,
                0);
        job.setNextMaterializedTime(startTimeBeforeDSTChange);
        job.setMatThrottling(3);
        final String everyHour = "0 * * * *";
        job.setFrequency(everyHour);
        job.setTimeUnit(Timeunit.CRON);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, job);

        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();

        final String startPlusOneHour = "2013-03-10T09:00Z";
        final String startPlusTwoHours = "2013-03-10T10:00Z";
        final Date[] nominalTimesWithDSTChange = new Date[] {DateUtils.parseDateOozieTZ(startInThePast),
                DateUtils.parseDateOozieTZ(startPlusOneHour),
                DateUtils.parseDateOozieTZ(startPlusTwoHours)
        };
        final int expectedNominalTimeCount = 3;
        checkCoordActionsNominalTime(job.getId(), expectedNominalTimeCount, nominalTimesWithDSTChange);

        checkTwoActionsAfterCatchup(job, expectedNominalTimeCount, "2013-03-10T11:00Z");
    }

    private void checkTwoActionsAfterCatchup(CoordinatorJobBean job, int expectedJobCount, String nextMaterialization)
            throws ParseException {
        try {
            final JPAService jpaService = Services.get().get(JPAService.class);
            job =  jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
            assertTrue("coordinator job should have already been materialized", job.isDoneMaterialization());
            assertEquals("coordinator action count mismatch", expectedJobCount, job.getLastActionNumber());
            assertEquals("coordinator next materialization time mismatch",
                    DateUtils.parseDateOozieTZ(nextMaterialization), job.getNextMaterializedTime());
        }
        catch (final JPAExecutorException se) {
            se.printStackTrace();
            fail("Job ID " + job.getId() + " was not stored properly in db");
        }
    }

    public void testCronFrequencyCatchupThrottleMoreThanDurationNoDSTChange() throws Exception {
        final String startInThePast = "2013-03-10T08:00Z";
        final Date startTime = DateUtils.parseDateOozieTZ(startInThePast);
        final String startPlusOneHourAndSome = "2013-03-10T09:01Z";
        final Date endTime = DateUtils.parseDateOozieTZ(startPlusOneHourAndSome);
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, startTime, endTime, false, false, 0);
        job.setNextMaterializedTime(startTime);
        job.setMatThrottling(5);
        final String everyHour = "0 * * * *";
        job.setFrequency(everyHour);
        job.setTimeUnit(Timeunit.CRON);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, job);

        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();

        final String startPlusOneHour = "2013-03-10T09:00Z";
        final Date[] nominalTimesWithoutDSTChange = new Date[] {DateUtils.parseDateOozieTZ(startInThePast),
                DateUtils.parseDateOozieTZ(startPlusOneHour)};
        final int expectedNominalTimeCount = 2;
        checkCoordActionsNominalTime(job.getId(), expectedNominalTimeCount, nominalTimesWithoutDSTChange);

        checkTwoActionsAfterCatchup(job, expectedNominalTimeCount, "2013-03-10T10:00Z");
    }

    public void testActionMaterWithDST1() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2013-03-10T08:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2013-03-10T12:00Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, null,
                "0 * * * *");
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(4)).call();
        Date[] nominalTimes = new Date[] {DateUtils.parseDateOozieTZ("2013-03-10T08:00Z"),
                DateUtils.parseDateOozieTZ("2013-03-10T09:00Z"),
                DateUtils.parseDateOozieTZ("2013-03-10T10:00Z"),
                DateUtils.parseDateOozieTZ("2013-03-10T11:00Z"),
        };
        final int expectedNominalTimeCount = 4;
        checkCoordActionsNominalTime(job.getId(), expectedNominalTimeCount, nominalTimes);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            job =  jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
            assertTrue(job.isDoneMaterialization());
            assertEquals(expectedNominalTimeCount, job.getLastActionNumber());
            assertEquals(DateUtils.parseDateOozieTZ("2013-03-10T12:00Z"), job.getNextMaterializedTime());
        }
        catch (JPAExecutorException se) {
            se.printStackTrace();
            fail("Job ID " + job.getId() + " was not stored properly in db");
        }
    }

    public void testActionMaterWithDST2() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2012-11-04T07:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2012-11-04T11:00Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, null,
                "0 * * * *");
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(4)).call();
        Date[] nominalTimes = new Date[] {DateUtils.parseDateOozieTZ("2012-11-04T07:00Z"),
                DateUtils.parseDateOozieTZ("2012-11-04T08:00Z"),
                DateUtils.parseDateOozieTZ("2012-11-04T09:00Z"),
                DateUtils.parseDateOozieTZ("2012-11-04T10:00Z"),
        };
        final int expectedNominalTimeCount = 4;
        checkCoordActionsNominalTime(job.getId(), expectedNominalTimeCount, nominalTimes);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            job =  jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
            assertTrue(job.isDoneMaterialization());
            assertEquals(job.getLastActionNumber(), expectedNominalTimeCount);
            assertEquals(job.getNextMaterializedTime(), DateUtils.parseDateOozieTZ("2012-11-04T11:00Z"));
        }
        catch (JPAExecutorException se) {
            se.printStackTrace();
            fail("Job ID " + job.getId() + " was not stored properly in db");
        }
    }
    public void testActionMaterWithPauseTime1() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2009-03-06T10:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2009-03-06T10:14Z");
        Date pauseTime = DateUtils.parseDateOozieTZ("2009-03-06T10:04Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, pauseTime, "5");
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        Date[] nominalTimes = new Date[] {DateUtils.parseDateOozieTZ("2009-03-06T10:00Z")};
        checkCoordActionsNominalTime(job.getId(), 1, nominalTimes);
    }

    public void testActionMaterWithPauseTime2() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2009-03-06T10:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2009-03-06T10:14Z");
        Date pauseTime = DateUtils.parseDateOozieTZ("2009-03-06T10:08Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, pauseTime, "5");
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        Date[] nominalTimes = new Date[] {DateUtils.parseDateOozieTZ("2009-03-06T10:00Z"),
                DateUtils.parseDateOozieTZ("2009-03-06T10:05Z")};
        checkCoordActionsNominalTime(job.getId(), 2, nominalTimes);
    }

    public void testGetDryrun() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2009-03-06T10:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2009-03-06T10:14Z");
        CoordinatorJobBean job = createCoordJob(CoordinatorJob.Status.RUNNING, startTime, endTime, false, false, 0);
        job.setFrequency("5");
        job.setTimeUnit(Timeunit.MINUTE);
        job.setMatThrottling(20);
        String dryRunOutput = new CoordMaterializeTransitionXCommand(job, hoursToSeconds(1), startTime, endTime)
                .materializeActions(true);
        String[] actions = dryRunOutput.split("action for new instance");
        assertEquals(3, actions.length -1);
        for(int i = 1; i < actions.length; i++) {
            assertTrue(actions[i].contains("action-nominal-time"));
        }
    }

    public void testTimeout() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2009-03-06T10:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2009-03-06T10:14Z");
        Date pauseTime = null;
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime,
                pauseTime, 300, "5", Timeunit.MINUTE);
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        checkCoordActionsTimeout(job.getId() + "@1", 300);
    }

    public void testMatLookupCommand1() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2009-02-01T01:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2009-02-03T23:59Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, startTime, endTime, false, false, 0);
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        checkCoordJobs(job.getId(), CoordinatorJob.Status.RUNNING);
    }

    public void testMatThrottle() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2009-02-01T01:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2009-02-03T23:59Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, startTime, endTime, false, false, 0);
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        checkCoordWaiting(job.getId(), job.getMatThrottling());
    }

    /**
     * Test a coordinator job that will run in far future,
     * materialization should not happen.
     * @throws Exception
     */
    public void testMatLookupCommand2() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2099-02-01T01:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2099-02-03T23:59Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, startTime, endTime, false, false, 0);
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        checkCoordJobs(job.getId(), CoordinatorJob.Status.PREP);
    }

    /**
     * Test a coordinator job that will run within 5 minutes from now,
     * materilization should happen.
     * @throws Exception
     */
    public void testMatLookupCommand3() throws Exception {
        Date startTime = DateUtils.toDate(new Timestamp(System.currentTimeMillis() + 180 * 1000));
        Date endTime = DateUtils.parseDateOozieTZ("2099-02-03T23:59Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, startTime, endTime, false, false, 0);
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        checkCoordJobs(job.getId(), CoordinatorJob.Status.RUNNING);
    }

    /**
     * Test a coordinator does not materialize actions upon CommandException
     * leading to FAILED state
     *
     * @throws Exception
     */
    public void testFailedJobNotMaterializeActions() throws Exception {
        String coordXml = "<coordinator-app xmlns=\"uri:oozie:coordinator:0.4\"" + " name=\"NAME\" frequency=\"5\""
                + " start=\"#start\" end=\"#end\" timezone=\"America/Los_Angeles\""
                + " freq_timeunit=\"DAY\" end_of_duration=\"NONE\">" + "<input-events>"
                + "<data-in name=\"a\" dataset=\"a\">"
                + "<dataset name=\"a\" frequency=\"7\" initial-instance=\"2010-01-01T00:00Z\" timezone=\"UTC\" "
                + "freq_timeunit=\"MINUTE\" end_of_duration=\"NONE\">"
                + "<uri-template>${hcatNode}/${db}/${table}/ds=${YEAR}-${MONTH}-${DAY};region=${region}</uri-template>"
                + "</dataset>" + "<start-instance>${coord:current(0)}</start-instance>"
                + "<end-instance>${coord:latest(0)}</end-instance>" + "</data-in>" + "</input-events>" + "<action>"
                + "<workflow>" + "<app-path>hdfs:///tmp/workflows/</app-path>" + "</workflow>" + "</action>"
                + "</coordinator-app>";
        CoordinatorJobBean job = addRecordToCoordJobTable(coordXml);
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        JPAService jpaService = Services.get().get(JPAService.class);
        job = jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
        assertEquals(CoordinatorJob.Status.FAILED, job.getStatus());
        // GetActions for coord job, should be none
        int actions = jpaService.execute(new CoordJobGetActionsJPAExecutor(job.getId()));
        assertEquals(0, actions);

    }

    /**
     * Test a coordinator job that will run beyond 5 minutes from now,
     * materilization should not happen.
     * @throws Exception
     */
    public void testMatLookupCommand4() throws Exception {
        Date startTime = DateUtils.toDate(new Timestamp(System.currentTimeMillis() + 360 * 1000));
        Date endTime = DateUtils.parseDateOozieTZ("2099-02-03T23:59Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, startTime, endTime, false, false, 0);
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        checkCoordJobs(job.getId(), CoordinatorJob.Status.PREP);
    }

    /**
     * Test lookup materialization for catchup jobs
     *
     * @throws Exception
     */
    public void testMaterializationLookup() throws Exception {
        long TIME_IN_MIN = 60 * 1000;
        long TIME_IN_HOURS = TIME_IN_MIN * 60;
        long TIME_IN_DAY = TIME_IN_HOURS * 24;
        JPAService jpaService = Services.get().get(JPAService.class);
        // test with days
        Date startTime = DateUtils.parseDateOozieTZ("2009-02-01T01:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2009-05-03T23:59Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, startTime, endTime, false, false,
                0);
        job.setNextMaterializedTime(startTime);
        job.setMatThrottling(3);
        job.setFrequency("1");
        job.setTimeUnitStr("DAY");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, job);
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        job = jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
        assertEquals(new Date(startTime.getTime() + TIME_IN_DAY * 3), job.getNextMaterializedTime());

        // test with hours
        startTime = DateUtils.parseDateOozieTZ("2009-02-01T01:00Z");
        endTime = DateUtils.parseDateOozieTZ("2009-05-03T23:59Z");
        job = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, startTime, endTime, false, false, 0);
        job.setNextMaterializedTime(startTime);
        job.setMatThrottling(10);
        job.setFrequency("1");
        job.setTimeUnitStr("HOUR");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, job);
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        job = jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
        assertEquals(new Date(startTime.getTime() + TIME_IN_HOURS * 10), job.getNextMaterializedTime());

        // test with hours, time should not pass the current time.
        startTime = new Date(new Date().getTime() - TIME_IN_DAY * 3);
        endTime = new Date(startTime.getTime() + TIME_IN_DAY * 3);
        job = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, startTime, endTime, false, false, 0);
        job.setNextMaterializedTime(startTime);
        job.setMatThrottling(10);
        job.setFrequency("1");
        job.setTimeUnitStr("DAY");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, job);
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        job = jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
        // If the startTime and endTime straddle a DST shift (the Coord is in "America/Los_Angeles"), then we need to adjust for
        // that because startTime and endTime assume GMT
        Date next = new Date(startTime.getTime() + TIME_IN_DAY * 3);
        TimeZone tz = TimeZone.getTimeZone(job.getTimeZone());
        next.setTime(next.getTime() + DaylightOffsetCalculator.getDSTOffset(tz, startTime, next));
        assertEquals(next, job.getNextMaterializedTime());

        // test with hours, time should not pass the current time.
        startTime = new Date(new Date().getTime());
        endTime = new Date(startTime.getTime() + TIME_IN_DAY * 3);
        job = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, startTime, endTime, false, false, 0);
        job.setNextMaterializedTime(startTime);
        job.setMatThrottling(10);
        job.setFrequency("1");
        job.setTimeUnitStr("DAY");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, job);
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        job = jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
        // If the startTime and endTime straddle a DST shift (the Coord is in "America/Los_Angeles"), then we need to adjust for
        // that because startTime and endTime assume GMT
        next = new Date(startTime.getTime() + TIME_IN_DAY);
        tz = TimeZone.getTimeZone(job.getTimeZone());
        next.setTime(next.getTime() + DaylightOffsetCalculator.getDSTOffset(tz, startTime, next));
        assertEquals(next, job.getNextMaterializedTime());

        // for current job in min, should not exceed hour windows
        startTime = new Date(new Date().getTime());
        endTime = new Date(startTime.getTime() + TIME_IN_HOURS * 24);
        job = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, startTime, endTime, false, false, 0);
        job.setMatThrottling(20);
        job.setFrequency("5");
        job.setTimeUnitStr("MINUTE");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, job);
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        job = jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
        // If the startTime and endTime straddle a DST shift (the Coord is in "America/Los_Angeles"), then we need to adjust for
        // that because startTime and endTime assume GMT
        next = new Date(startTime.getTime() + TIME_IN_HOURS);
        tz = TimeZone.getTimeZone(job.getTimeZone());
        next.setTime(next.getTime() + DaylightOffsetCalculator.getDSTOffset(tz, startTime, next));
        assertEquals(next, job.getNextMaterializedTime());

        // for current job in hour, should not exceed hour windows
        startTime = new Date(new Date().getTime());
        endTime = new Date(startTime.getTime() + TIME_IN_DAY * 24);
        job = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, startTime, endTime, false, false, 0);
        job.setMatThrottling(20);
        job.setFrequency("1");
        job.setTimeUnitStr("DAY");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, job);
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        job = jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
        // If the startTime and endTime straddle a DST shift (the Coord is in "America/Los_Angeles"), then we need to adjust for
        // that because startTime and endTime assume GMT
        next = new Date(startTime.getTime() + TIME_IN_DAY);
        tz = TimeZone.getTimeZone(job.getTimeZone());
        next.setTime(next.getTime() + DaylightOffsetCalculator.getDSTOffset(tz, startTime, next));
        assertEquals(next, job.getNextMaterializedTime());

        // Case: job started in Daylight time, and materialization is in
        // Standard time
        startTime = getDaylightCalendar().getTime();
        endTime = new Date(startTime.getTime() + TIME_IN_DAY * 3);
        job = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, startTime, endTime, false, false, 0);
        job.setNextMaterializedTime(startTime);
        job.setMatThrottling(10);
        job.setFrequency("1");
        job.setTimeUnitStr("DAY");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, job);
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        job = jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
        // If the startTime and endTime straddle a DST shift (the Coord is in
        // "America/Los_Angeles"), then we need to adjust for
        // that because startTime and endTime assume GMT
        next = new Date(startTime.getTime() + TIME_IN_DAY * 3);
        tz = TimeZone.getTimeZone(job.getTimeZone());
        next.setTime(next.getTime() + DaylightOffsetCalculator.getDSTOffset(tz, startTime, next));

        assertEquals(next, job.getNextMaterializedTime());

        // Case: job started in Standard time, and materialization is in
        // Daylight time
        Calendar c = getStandardCalendar();
        startTime = c.getTime();
        endTime = new Date(startTime.getTime() + TIME_IN_DAY * 3);
        job = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, startTime, endTime, false, false, 0);
        job.setNextMaterializedTime(startTime);
        job.setMatThrottling(10);
        job.setFrequency("1");
        job.setTimeUnitStr("DAY");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, job);
        new CoordMaterializeTransitionXCommand(job, hoursToSeconds(1), startTime, endTime).call();
        job = jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
        // If the startTime and endTime straddle a DST shift (the Coord is in
        // "America/Los_Angeles"), then we need to adjust for
        // that because startTime and endTime assume GMT
        next = new Date(startTime.getTime() + TIME_IN_DAY * 4);
        tz = TimeZone.getTimeZone(job.getTimeZone());
        next.setTime(next.getTime() + DaylightOffsetCalculator.getDSTOffset(tz, startTime, next));
        assertEquals(next, job.getNextMaterializedTime());

    }

    Calendar getDaylightCalendar() {
        final Calendar daylight = Calendar.getInstance();
        daylight.set(2012, 10, 2, 15, 28, 00);

        return daylight;
    }

    Calendar getStandardCalendar() {
        final Calendar standard = Calendar.getInstance();
        standard.set(2013, 2, 9, 15, 28, 00);

        return standard;
    }

    public void testWhenChangingDSTCronAndELMonthlyFrequenciesEqual() throws Exception {
        String dstAwareMonthlyCron = "10 23 1 1-12 *";
        Date startTime = DateUtils.parseDateOozieTZ("2016-03-01T23:10Z");
        Date endTime = DateUtils.parseDateOozieTZ("2016-12-03T00:00Z");;
        Date[] nominalTimesWithTwoDstChange = new Date[]{
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-01T15:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-04-01T15:10")),  // DST started
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-05-01T15:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-06-01T15:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-07-01T15:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-08-01T15:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-09-01T15:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-10-01T15:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-11-01T15:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-12-01T15:10")),  // DST ended
        };
        testELAndCronNominalTimesEqual(startTime, endTime, nominalTimesWithTwoDstChange, dstAwareMonthlyCron, "1", Timeunit.MONTH);
    }

    public void testWhenChangingDSTCronAndELDailyFrequenciesEqual() throws Exception {
        String dstAwareDailyCron = "10 23 * * *";
        Date startTime = DateUtils.parseDateOozieTZ("2016-03-11T23:10Z");
        Date endTime = DateUtils.parseDateOozieTZ("2016-03-15T02:00Z");
        Date[] nominalTimesWithTwoDstChange = new Date[]{
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-11T15:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-12T15:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-13T15:10")), // DST started
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-14T15:10")),
        };
        testELAndCronNominalTimesEqual(startTime, endTime, nominalTimesWithTwoDstChange, dstAwareDailyCron, "1", Timeunit.DAY);
    }

    public void testWhenChangingDSTELEveryTwentyFourthHour() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2016-03-11T23:10Z");
        Date endTime = DateUtils.parseDateOozieTZ("2016-03-15T02:00Z");
        Date[] nominalTimesWithTwoDstChange = new Date[]{
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-11T15:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-12T15:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-13T16:10")), // DST started
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-14T16:10")),
        };
        testELNominalTimes(startTime, endTime, nominalTimesWithTwoDstChange,"24", Timeunit.HOUR);
    }

    public void testWhenBeginningDSTCronAndELHourlyFrequenciesEqual() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2017-03-12T07:10Z");
        Date endTime = DateUtils.parseDateOozieTZ("2017-03-12T12:30Z");
        String everyHourAtTen = "10 * * * *";
        Date[] nominalTimesWithOneDstChange = new Date[]{
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2017-03-11T23:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2017-03-12T00:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2017-03-12T01:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2017-03-12T03:10")), // DST started
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2017-03-12T04:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2017-03-12T05:10")),
        };

        testELAndCronNominalTimesEqual(startTime, endTime, nominalTimesWithOneDstChange, everyHourAtTen, "1", Timeunit.HOUR);
    }

    public void testWhenEndingDSTCronAndELHourlyFrequenciesEqual() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2017-11-05T07:10Z");
        Date endTime = DateUtils.parseDateOozieTZ("2017-11-05T10:30Z");
        String everyHourAtTen = "10 * * * *";
        Date[] nominalTimesWithOneDstChange = new Date[]{
                DateUtils.parseDateOozieTZ("2017-11-05T07:10Z"), // LA time: 2017-11-05T00:10
                DateUtils.parseDateOozieTZ("2017-11-05T08:10Z"), // LA time: 2017-11-05T01:10
                DateUtils.parseDateOozieTZ("2017-11-05T09:10Z"), // LA time: 2017-11-05T01:10, DST ended
                DateUtils.parseDateOozieTZ("2017-11-05T10:10Z"), // LA time: 2017-11-05T02:10
        };

        testELAndCronNominalTimesEqual(startTime, endTime, nominalTimesWithOneDstChange, everyHourAtTen, "1", Timeunit.HOUR);
    }

    public void testWhenChangingDSTELEveryTwentiethDay() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2016-02-01T13:10Z");
        Date endTime = DateUtils.parseDateOozieTZ("2016-12-03T00:00Z");
        Date[] nominalTimesWithTwoDstChange = new Date[]{
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-02-01T05:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-02-21T05:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-12T05:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-04-01T05:10")), // DST started
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-04-21T05:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-05-11T05:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-05-31T05:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-06-20T05:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-07-10T05:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-07-30T05:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-08-19T05:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-09-08T05:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-09-28T05:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-10-18T05:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-11-07T05:10")), // DST ended
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-11-27T05:10")),
        };

        testELNominalTimes(startTime, endTime, nominalTimesWithTwoDstChange,"20", Timeunit.DAY);
    }

    public void testWhenChangingDSTCronEveryTwentiethDay() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2016-02-01T13:10Z");
        Date endTime = DateUtils.parseDateOozieTZ("2016-12-03T00:00Z");
        String everyTwentiethDayAroundDstShift = "10 13 */20 2-3,11,12 *";

        Date[] nominalTimesWithTwoDstChange = new Date[]{
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-02-01T05:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-02-21T05:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-01T05:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-21T05:10")), // DST started
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-11-01T05:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-11-21T05:10")), // DST ended
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-12-01T05:10")),
        };
        testCronNominalTimes(startTime, endTime, nominalTimesWithTwoDstChange, everyTwentiethDayAroundDstShift);
    }

    public void testWhenChangingDSTCronAndELEveryThirdMonthFrequenciesEqual() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2016-01-01T13:10Z");
        Date endTime = DateUtils.parseDateOozieTZ("2017-12-03T00:00Z");
        String everyThirdMonth = "10 13 1 */3 *";

        Date[] nominalTimesWithTwoDstChange = new Date[]{
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-01-01T05:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-04-01T05:10")), // DST started on 13th March
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-07-01T05:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-10-01T05:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2017-01-01T05:10")), // DST ended on 6th of November
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2017-04-01T05:10")), // DST started again on 12th March
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2017-07-01T05:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2017-10-01T05:10")),
        };
        testELAndCronNominalTimesEqual(startTime, endTime, nominalTimesWithTwoDstChange, everyThirdMonth, "3", Timeunit.MONTH);
    }

    public void testWhenDSTStartsCronFrequencyEveryTwentiethHour() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2016-01-01T13:10Z");
        Date endTime = DateUtils.parseDateOozieTZ("2016-12-03T00:00Z");
        String everyTwentiethHourNearDSTShift = "10 */20 12-14 3 *";
        Date[] nominalTimesWithTwoDstChange = new Date[]{
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-11T16:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-12T12:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-12T16:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-13T13:10")), // DST change
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-13T17:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-14T13:10")),
        };
        testCronNominalTimes(startTime, endTime, nominalTimesWithTwoDstChange, everyTwentiethHourNearDSTShift);
    }

    public void testWhenDSTStartsELFrequencyEveryTwentiethHour() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2016-03-12T13:10Z");
        Date endTime = DateUtils.parseDateOozieTZ("2016-03-16T00:00Z");
        Date[] nominalTimesWithTwoDstChange = new Date[]{
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-12T05:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-13T01:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-13T22:10")), // DST started
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-14T18:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-15T14:10")),
        };

        testELNominalTimes(startTime, endTime, nominalTimesWithTwoDstChange, "20", Timeunit.HOUR);
    }

    public void testWhenDSTSEndsCronFrequencyEveryTwentiethHour() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2016-01-01T13:10Z");
        Date endTime = DateUtils.parseDateOozieTZ("2016-12-03T00:00Z");
        String everyTwentiethHourNearDSTShift = "10 */20 5-7 11 *";
        Date[] nominalTimesWithTwoDstChange = new Date[]{
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-11-04T16:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-11-05T13:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-11-05T17:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-11-06T12:10")), // DST change
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-11-06T16:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-11-07T12:10")),
        };

        testCronNominalTimes(startTime, endTime, nominalTimesWithTwoDstChange, everyTwentiethHourNearDSTShift);
    }

    public void testWhenDSTEndsELFrequencyEveryTwentiethHour() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2016-11-04T23:10Z");
        Date endTime = DateUtils.parseDateOozieTZ("2016-11-08T22:00Z");
        Date[] nominalTimesWithTwoDstChange = new Date[]{
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-11-04T16:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-11-05T12:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-11-06T07:10")), // DST ended
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-11-07T03:10")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-11-07T23:10")),
        };

        testELNominalTimes(startTime, endTime, nominalTimesWithTwoDstChange, "20", Timeunit.HOUR);
    }

    public void testWhenDSTSwitchELAndCronFrequencyEveryThirtiethMinute() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2016-03-13T08:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2016-03-13T13:00Z");
        String everyThirtiethMinuteCron = "*/30 * * * *";
        Date[] nominalTimesWithTwoDstChange = new Date[]{
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-13T00:00")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-13T00:30")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-13T01:00")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-13T01:30")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-13T02:00")), // DST change
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-13T02:30")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-13T04:00")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-13T04:30")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-13T05:00")),
                DateUtils.parseDateOozieTZ(convertLATimeToUTC("2016-03-13T05:30")),
        };

        testELAndCronNominalTimesEqual(startTime, endTime, nominalTimesWithTwoDstChange,everyThirtiethMinuteCron,
                "30", Timeunit.MINUTE);
    }

    private String convertLATimeToUTC (String localTime) throws Exception {
        DateFormat LATimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");
        LATimeFormat.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        Date date = LATimeFormat.parse(localTime);

        DateFormat utcFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        utcFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        return utcFormat.format(date);

    }

    private void testELAndCronNominalTimesEqual (Date startTime, Date endTime, Date[] nominalTimes, String cronFrequency,
                                                 String elFrequency, Timeunit elTimeUnit) throws Exception {
        CoordinatorJobBean elJob = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, null,
                elFrequency, elTimeUnit);
        CoordinatorJobBean cronJob = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, null,
                cronFrequency);

        new CoordMaterializeTransitionXCommand(elJob.getId(), oneHourInSeconds).call();
        new CoordMaterializeTransitionXCommand(cronJob.getId(), oneHourInSeconds).call();


        JPAService jpaService = Services.get().get(JPAService.class);

        elJob = jpaService.execute(new CoordJobGetJPAExecutor(elJob.getId()));
        cronJob = jpaService.execute(new CoordJobGetJPAExecutor(cronJob.getId()));

        checkCoordActionsNominalTime(cronJob.getId(), nominalTimes.length, nominalTimes);
        checkCoordActionsNominalTime(elJob.getId(), nominalTimes.length, nominalTimes);

        assertTrue("Cron and EL job materialization should both be complete",
                elJob.isDoneMaterialization() && cronJob.isDoneMaterialization());
    }

    private void testCronNominalTimes (Date startTime, Date endTime, Date[] nominalTimes, String cronFrequency) throws Exception {
        CoordinatorJobBean cronJob = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, null,
                cronFrequency);
        new CoordMaterializeTransitionXCommand(cronJob.getId(), oneHourInSeconds).call();

        JPAService jpaService = Services.get().get(JPAService.class);
        cronJob = jpaService.execute(new CoordJobGetJPAExecutor(cronJob.getId()));
        checkCoordActionsNominalTime(cronJob.getId(), nominalTimes.length, nominalTimes);
        assertTrue("Cron job materialization should be complete", cronJob.isDoneMaterialization());
    }

    private void testELNominalTimes (Date startTime, Date endTime, Date[] nominalTimes, String elFrequency, Timeunit elTimeUnit)
            throws Exception {
        CoordinatorJobBean elJob = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, null,
                elFrequency, elTimeUnit);
        new CoordMaterializeTransitionXCommand(elJob.getId(), oneHourInSeconds).call();

        JPAService jpaService = Services.get().get(JPAService.class);
        elJob = jpaService.execute(new CoordJobGetJPAExecutor(elJob.getId()));
        checkCoordActionsNominalTime(elJob.getId(), nominalTimes.length, nominalTimes);
        assertTrue("EL job materialization should be complete", elJob.isDoneMaterialization());
    }

    public void testLastOnlyMaterialization() throws Exception {
        long now = System.currentTimeMillis();
        Date startTime = DateUtils.toDate(new Timestamp(now - 180 * 60 * 1000));    // 3 hours ago
        Date endTime = DateUtils.toDate(new Timestamp(now + 180 * 60 * 1000));      // 3 hours from now
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, null, -1, "10",
                CoordinatorJob.Execution.LAST_ONLY);
        // This would normally materialize the throttle amount and within a 1 hour window; however, with LAST_ONLY this should
        // ignore those parameters and materialize everything in the past
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        checkCoordJobs(job.getId(), CoordinatorJob.Status.RUNNING);
        CoordinatorActionBean.Status[] expectedStatuses = new CoordinatorActionBean.Status[19];
        Arrays.fill(expectedStatuses, CoordinatorActionBean.Status.WAITING);
        checkCoordActionsStatus(job.getId(), expectedStatuses);

        startTime = DateUtils.toDate(new Timestamp(now));                           // now
        job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, null, -1, "10",
                CoordinatorJob.Execution.LAST_ONLY);
        // We're starting from "now" this time (i.e. present/future), so it should materialize things normally
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        checkCoordJobs(job.getId(), CoordinatorJob.Status.RUNNING);
        expectedStatuses = new CoordinatorActionBean.Status[6];
        Arrays.fill(expectedStatuses, CoordinatorActionBean.Status.WAITING);
        checkCoordActionsStatus(job.getId(), expectedStatuses);
    }

    public void testCurrentTimeCheck() throws Exception {
        long now = System.currentTimeMillis();
        Date startTime = DateUtils.toDate(new Timestamp(now)); // now
        Date endTime = DateUtils.toDate(new Timestamp(now + 3 * 60 * 60 * 1000)); // 3 secondsFromHours from now
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, null, "5",
                20);
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        checkCoordJobs(job.getId(), CoordinatorJob.Status.RUNNING);

        job = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB, job.getId());
        assertEquals(job.getLastActionNumber(), 12);
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        // unfortunatily XCommand doesn't throw exception on precondition
        // assertEquals(e.getErrorCode(), ErrorCode.E1100);
        // assertTrue(e.getMessage().contains("Request is for future time. Lookup time is"));

        job = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB, job.getId());
        // getLastActionNumber should 12, last CoordMaterializeTransitionXCommand have failed
        assertEquals(job.getLastActionNumber(), 12);
    }
    public void testMaterizationEndOfMonths() throws Exception {
        Configuration conf = new XConfiguration();
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        String appXml = "<coordinator-app name=\"test\" frequency=\"${coord:endOfMonths(1)}\" start=\"2009-02-01T01:00Z\" "
                + "end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.2\"> <controls> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>"
                + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}")
                + "</uri-template>  "
                + "</dataset> "
                + "<dataset name=\"local_a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>"
                + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}")
                + "</uri-template> "
                + " </dataset> "
                + "</datasets> <input-events> "
                + "<data-in name=\"A\" dataset=\"a\"> <instance>${coord:latest(0)}</instance> </data-in>  "
                + "</input-events> "
                + "<output-events> <data-out name=\"LOCAL_A\" dataset=\"local_a\"> "
                + "<instance>${coord:current(-1)}</instance> </data-out> </output-events> <action> <workflow> "
                + "<app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> "
                + "<property> <name>inputB</name> <value>${coord:dataOut('LOCAL_A')}</value> "
                + "</property></configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPathFile);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);
        String jobId = sc.call();

        Calendar cal = Calendar.getInstance(DateUtils.getOozieProcessingTimeZone());
        cal.add(Calendar.MONTH, -3);
        Date startTime = cal.getTime();
        cal = Calendar.getInstance(DateUtils.getOozieProcessingTimeZone());
        cal.add(Calendar.MONTH, 3);
        Date endTime = cal.getTime();
        CoordinatorJobBean job = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB, jobId);
        assertEquals(job.getLastActionNumber(), 0);

        job.setStartTime(startTime);
        job.setEndTime(endTime);
        job.setMatThrottling(10);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, job);
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        job = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB, job.getId());
        assertEquals(job.getLastActionNumber(), 3);

        String jobXml = job.getJobXml();
        Element eJob = XmlUtils.parseXml(jobXml);
        TimeZone appTz = DateUtils.getTimeZone(job.getTimeZone());
        TimeUnit endOfFlag = TimeUnit.valueOf(eJob.getAttributeValue("end_of_duration"));
        TimeUnit freqTU = TimeUnit.valueOf(job.getTimeUnitStr());
        Calendar origStart = Calendar.getInstance(appTz);
        origStart.setTime(job.getStartTimestamp());
        // Move to the End of duration, if needed.
        DateUtils.moveToEnd(origStart, endOfFlag);
        origStart.add(freqTU.getCalendarUnit(), 3 * Integer.parseInt(job.getFrequency()));
        assertEquals(job.getNextMaterializedTime(), origStart.getTime());
    }

    public void testActionMaterEndOfWeeks() throws Exception {
        Configuration conf = new XConfiguration();
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        String appXml = "<coordinator-app name=\"test\" frequency=\"${coord:endOfWeeks(1)}\" start=\"2016-02-03T01:00Z\" "
                + "end=\"2016-03-03T23:59Z\" timezone=\"UTC\" " + "xmlns=\"uri:oozie:coordinator:0.2\"> <controls> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"a\" frequency=\"${coord:endOfWeeks(1)}\" initial-instance=\"2016-01-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}")
                + "</uri-template>  " + "</dataset> "
                + "<dataset name=\"local_a\" frequency=\"${coord:endOfWeeks(1)}\" initial-instance=\"2016-01-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}")
                + "</uri-template> " + " </dataset> " + "</datasets> <input-events> "
                + "<data-in name=\"A\" dataset=\"a\"> <instance>${coord:latest(0)}</instance> </data-in>  "
                + "</input-events> " + "<output-events> <data-out name=\"LOCAL_A\" dataset=\"local_a\"> "
                + "<instance>${coord:current(-1)}</instance> </data-out> </output-events> <action> <workflow> "
                + "<app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> "
                + "<property> <name>inputB</name> <value>${coord:dataOut('LOCAL_A')}</value> "
                + "</property></configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPathFile);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);
        String jobId = sc.call();

        CoordinatorJobBean job = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB, jobId);
        assertEquals(job.getLastActionNumber(), 0);

        job.setMatThrottling(10);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, job);
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        job = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB, job.getId());
        assertEquals(4, job.getLastActionNumber());

        String jobXml = job.getJobXml();
        Element eJob = XmlUtils.parseXml(jobXml);
        TimeZone appTz = DateUtils.getTimeZone(job.getTimeZone());
        TimeUnit endOfFlag = TimeUnit.valueOf(eJob.getAttributeValue("end_of_duration"));
        TimeUnit freqTU = TimeUnit.valueOf(job.getTimeUnitStr());
        Calendar origStart = Calendar.getInstance(appTz);
        origStart.setTime(job.getStartTimestamp());
        // Move to the End of duration, if needed.
        DateUtils.moveToEnd(origStart, endOfFlag);
        origStart.add(freqTU.getCalendarUnit(), 4 * Integer.parseInt(job.getFrequency()));
        assertEquals(origStart.getTime(), job.getNextMaterializedTime());
    }

    private void checkCoordJobs(String jobId, CoordinatorJob.Status expectedStatus) {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            CoordinatorJobBean job = jpaService.execute(new CoordJobGetJPAExecutor(jobId));
            if (job.getStatus() != expectedStatus) {
                fail("CoordJobMatLookupCommand didn't work because the status for job id"
                        + jobId + " is : " + job.getStatusStr() + "; however expected status is : " + expectedStatus.toString());
            }
        }
        catch (JPAExecutorException se) {
            fail("Job ID " + jobId + " was not stored properly in db");
        }
    }

    private void checkCoordWaiting(String jobId, int expectedValue) {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            int numWaitingActions = jpaService.execute(new CoordJobGetRunningActionsCountJPAExecutor(jobId));
            assert (numWaitingActions <= expectedValue);
        }
        catch (JPAExecutorException se) {
            fail("Job ID " + jobId + " was not stored properly in db");
        }
    }

    private CoordinatorActionBean checkCoordAction(String actionId) throws JPAExecutorException {
        long lastSeqId[] = new long[1];
        JPAService jpaService = Services.get().get(JPAService.class);
        List<SLAEventBean> slaEventList = jpaService.execute(new SLAEventsGetForSeqIdJPAExecutor(-1, 10, lastSeqId));

        if (slaEventList.size() == 0) {
            fail("Unable to GET any record of sequence id greater than 0");
        }


        CoordinatorActionBean actionBean;
        actionBean = jpaService.execute(new CoordActionGetJPAExecutor(actionId));

        return actionBean;
    }

    private CoordinatorActionBean getCoordAction(String actionId) throws JPAExecutorException {
        JPAService jpaService = Services.get().get(JPAService.class);
        CoordinatorActionBean actionBean;
        actionBean = jpaService.execute(new CoordActionGetJPAExecutor(actionId));
        return actionBean;
    }

    private void checkCoordActionsNominalTime(String jobId, int number, Date[] nominalTimes) {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            List<CoordinatorActionBean> actions = jpaService.execute(new CoordJobGetActionsSubsetJPAExecutor(jobId,
                    null, 1, 1000, false));

            if (actions.size() != number) {
                fail("Should have " + number + " actions created for job " + jobId + ", but has " + actions.size() + " actions.");
            }

            for (int i=0; i < nominalTimes.length; i++ ) {
                assertEquals(nominalTimes[i], actions.get(i).getNominalTime());
            }
        }

        catch (JPAExecutorException se) {
            se.printStackTrace();
            fail("Job ID " + jobId + " was not stored properly in db");
        }
    }

    private void checkCoordActionsStatus(String jobId, CoordinatorActionBean.Status[] statuses) {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            List<CoordinatorActionBean> actions = jpaService.execute(new CoordJobGetActionsSubsetJPAExecutor(jobId,
                    null, 1, 1000, false));

            if (actions.size() != statuses.length) {
                fail("Should have " + statuses.length + " actions created for job " + jobId + ", but has " + actions.size()
                        + " actions.");
            }

            for (int i=0; i < statuses.length; i++ ) {
                assertEquals(statuses[i], actions.get(i).getStatus());
            }
        }
        catch (JPAExecutorException se) {
            se.printStackTrace();
            fail("Job ID " + jobId + " was not stored properly in db");
        }
    }

    private void checkCoordActionsTimeout(String actionId, int expected) {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            CoordinatorActionBean action = jpaService.execute(new CoordActionGetJPAExecutor(actionId));
            assertEquals(action.getTimeOut(), expected);
        }
        catch (JPAExecutorException se) {
            se.printStackTrace();
            fail("Action ID " + actionId + " was not stored properly in db");
        }
    }

    /**
     * Test a coordinator SLA define EL functions as variable
     *
     * @throws Exception
     */
    public void testSuccessedJobSlaParseElFunctionVariableInMaterializeActions() throws Exception {
        Configuration conf = new XConfiguration();
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        String coordXml = "<coordinator-app name=\"NAME\" frequency=\"0 * * * *\""
                + " start=\"2017-06-12T01:00Z\" end=\"2017-06-12T02:00Z\" timezone=\"Asia/Shanghai\""
                + " xmlns=\"uri:oozie:coordinator:0.4\" xmlns:sla=\"uri:oozie:sla:0.2\">"
                + "<controls> <execution>FIFO</execution> </controls>"
                + "<action>"
                + " <workflow> <app-path>hdfs:///tmp/workflows/</app-path> </workflow> "
                + " <sla:info>"
                + "  <sla:nominal-time>${NOMINAL_TIME}</sla:nominal-time>"
                + "  <sla:should-start>${SHOULD_START}</sla:should-start>"
                + "  <sla:should-end>${SHOULD_END}</sla:should-end>"
                + " </sla:info>"
                + "</action>"
                + "</coordinator-app>";
        writeToFile(coordXml, appPathFile);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("NOMINAL_TIME", "${coord:nominalTime()}");
        conf.set("SHOULD_START", "${5 * MINUTES}");
        conf.set("SHOULD_END", "${ SLA_OFFSET * HOURS}");
        conf.set("SLA_OFFSET", "10");
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);
        String jobId = sc.call();
        new CoordMaterializeTransitionXCommand(jobId, 60).call();
    }
}
