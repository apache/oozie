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
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;

@SuppressWarnings("deprecation")
public class TestCoordMaterializeTransitionXCommand extends XDataTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        LocalOozie.start(); //LocalOozie does new Services().init();
    }

    @Override
    protected void tearDown() throws Exception {
        LocalOozie.stop();
        super.tearDown();
    }

    public void testActionMater() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2009-03-06T010:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2009-03-11T10:00Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, false, false, 0);
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
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
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
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
            new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
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
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
    }

    public void testActionMaterWithCronFrequency1() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2013-07-18T00:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2013-07-18T01:00Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, null,
                "10,20 * * * *");
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
        Date[] nominalTimes = new Date[] {DateUtils.parseDateOozieTZ("2013-07-18T00:10Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:20Z")};
        checkCoordActionsNominalTime(job.getId(), 2, nominalTimes);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            job =  jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
            assertTrue(job.isDoneMaterialization());
            assertEquals(job.getLastActionNumber(), 2);
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
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
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
        checkCoordActionsNominalTime(job.getId(), 11, nominalTimes);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            job =  jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
            assertTrue(job.isDoneMaterialization());
            assertEquals(job.getLastActionNumber(), 11);
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
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
        checkCoordActionsNominalTime(job.getId(), 0, new Date[]{});

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            job =  jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
            assertTrue(job.isDoneMaterialization());
            assertEquals(job.getLastActionNumber(), 0);
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
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
        Date[] nominalTimes = new Date[] {DateUtils.parseDateOozieTZ("2013-07-18T00:00Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:15Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:30Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:45Z"),};
        checkCoordActionsNominalTime(job.getId(), 4, nominalTimes);


        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            job =  jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
            assertTrue(job.isDoneMaterialization());
            assertEquals(job.getLastActionNumber(), 4);
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
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
        Date[] nominalTimes = new Date[] {DateUtils.parseDateOozieTZ("2013-07-18T00:20Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:35Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:50Z"),};
        checkCoordActionsNominalTime(job.getId(), 3, nominalTimes);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            job =  jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
            assertTrue(job.isDoneMaterialization());
            assertEquals(job.getLastActionNumber(), 3);
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
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
        Date[] nominalTimes = new Date[] {DateUtils.parseDateOozieTZ("2013-07-18T00:00Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:20Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:40Z"),};
        checkCoordActionsNominalTime(job.getId(), 3, nominalTimes);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            job =  jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
            assertTrue(job.isDoneMaterialization());
            assertEquals(job.getLastActionNumber(), 3);
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
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
        Date[] nominalTimes = new Date[] {DateUtils.parseDateOozieTZ("2013-07-18T00:20Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:35Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:50Z"),};
        checkCoordActionsNominalTime(job.getId(), 3, nominalTimes);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            job =  jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
            assertTrue(job.isDoneMaterialization());
            assertEquals(job.getLastActionNumber(), 3);
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

        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
        Date[] nominalTimes = new Date[] {DateUtils.parseDateOozieTZ("2013-07-18T00:00Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:10Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:20Z")};
        checkCoordActionsNominalTime(job.getId(), 3, nominalTimes);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            job =  jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
            assertFalse(job.isDoneMaterialization());
            assertEquals(3, job.getLastActionNumber());
            assertEquals(DateUtils.parseDateOozieTZ("2013-07-18T00:30Z"), job.getNextMaterializedTime());
        }
        catch (JPAExecutorException se) {
            se.printStackTrace();
            fail("Job ID " + job.getId() + " was not stored properly in db");
        }
    }
    public void testActionMaterWithDST1() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2013-03-10T08:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2013-03-10T12:00Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, null,
                "0 * * * *");
        new CoordMaterializeTransitionXCommand(job.getId(), 3600*4).call();
        Date[] nominalTimes = new Date[] {DateUtils.parseDateOozieTZ("2013-03-10T08:00Z"),
                DateUtils.parseDateOozieTZ("2013-03-10T09:00Z"),
                DateUtils.parseDateOozieTZ("2013-03-10T10:00Z"),
                DateUtils.parseDateOozieTZ("2013-03-10T11:00Z"),};
        checkCoordActionsNominalTime(job.getId(), 4, nominalTimes);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            job =  jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
            assertTrue(job.isDoneMaterialization());
            assertEquals(job.getLastActionNumber(), 4);
            assertEquals(job.getNextMaterializedTime(), DateUtils.parseDateOozieTZ("2013-03-10T12:00Z"));
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
        new CoordMaterializeTransitionXCommand(job.getId(), 3600*4).call();
        Date[] nominalTimes = new Date[] {DateUtils.parseDateOozieTZ("2012-11-04T07:00Z"),
                DateUtils.parseDateOozieTZ("2012-11-04T08:00Z"),
                DateUtils.parseDateOozieTZ("2012-11-04T09:00Z"),
                DateUtils.parseDateOozieTZ("2012-11-04T10:00Z"),};
        checkCoordActionsNominalTime(job.getId(), 4, nominalTimes);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            job =  jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
            assertTrue(job.isDoneMaterialization());
            assertEquals(job.getLastActionNumber(), 4);
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
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
        Date[] nominalTimes = new Date[] {DateUtils.parseDateOozieTZ("2009-03-06T10:00Z")};
        checkCoordActionsNominalTime(job.getId(), 1, nominalTimes);
    }

    public void testActionMaterWithPauseTime2() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2009-03-06T10:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2009-03-06T10:14Z");
        Date pauseTime = DateUtils.parseDateOozieTZ("2009-03-06T10:08Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, pauseTime, "5");
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
        Date[] nominalTimes = new Date[] {DateUtils.parseDateOozieTZ("2009-03-06T10:00Z"),
                DateUtils.parseDateOozieTZ("2009-03-06T10:05Z")};
        checkCoordActionsNominalTime(job.getId(), 2, nominalTimes);
    }

    /**
     * Job start time is after pause time, should pause the job.
     *
     * @throws Exception
     */
    public void testActionMaterWithPauseTime3() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2009-03-06T10:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2009-03-06T10:14Z");
        Date pauseTime = DateUtils.parseDateOozieTZ("2009-03-06T09:58Z");
        final CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, pauseTime, "5");
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
        waitFor(1000*60, new Predicate() {
            public boolean evaluate() throws Exception {
                return (getStatus(job.getId()) == CoordinatorJob.Status.PAUSED?true:false);
            }
        });
        checkCoordActions(job.getId(), 0, CoordinatorJob.Status.PAUSED);
    }

    public void testGetDryrun() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2009-03-06T10:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2009-03-06T10:14Z");
        CoordinatorJobBean job = createCoordJob(CoordinatorJob.Status.RUNNING, startTime, endTime, false, false, 0);
        job.setFrequency("5");
        job.setTimeUnit(Timeunit.MINUTE);
        job.setMatThrottling(20);
        String dryRunOutput = new CoordMaterializeTransitionXCommand(job, 3600, startTime, endTime).materializeActions(true);
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
                pauseTime, 300, "5");
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
        checkCoordActionsTimeout(job.getId() + "@1", 300);
    }

    public void testMatLookupCommand1() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2009-02-01T01:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2009-02-03T23:59Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, startTime, endTime, false, false, 0);
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
        checkCoordJobs(job.getId(), CoordinatorJob.Status.RUNNING);
    }

    public void testMatThrottle() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2009-02-01T01:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2009-02-03T23:59Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, startTime, endTime, false, false, 0);
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
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
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
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
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
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
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
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
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
        checkCoordJobs(job.getId(), CoordinatorJob.Status.PREP);
    }

    /**
     * Test lookup materialization for catchup jobs
     *
     * @throws Exception
     */
    public void testMaterizationLookup() throws Exception {
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
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
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
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
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
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
        job = jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
        // If the startTime and endTime straddle a DST shift (the Coord is in "America/Los_Angeles"), then we need to adjust for
        // that because startTime and endTime assume GMT
        Date next = new Date(startTime.getTime() + TIME_IN_DAY * 3);
        TimeZone tz = TimeZone.getTimeZone(job.getTimeZone());
        next.setTime(next.getTime() - getDSTOffset(tz, startTime, next));
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
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
        job = jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
        // If the startTime and endTime straddle a DST shift (the Coord is in "America/Los_Angeles"), then we need to adjust for
        // that because startTime and endTime assume GMT
        next = new Date(startTime.getTime() + TIME_IN_DAY);
        tz = TimeZone.getTimeZone(job.getTimeZone());
        next.setTime(next.getTime() - getDSTOffset(tz, startTime, next));
        assertEquals(next, job.getNextMaterializedTime());

        // for current job in min, should not exceed hour windows
        startTime = new Date(new Date().getTime());
        endTime = new Date(startTime.getTime() + TIME_IN_HOURS * 24);
        job = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, startTime, endTime, false, false, 0);
        job.setMatThrottling(20);
        job.setFrequency("5");
        job.setTimeUnitStr("MINUTE");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, job);
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
        job = jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
        // If the startTime and endTime straddle a DST shift (the Coord is in "America/Los_Angeles"), then we need to adjust for
        // that because startTime and endTime assume GMT
        next = new Date(startTime.getTime() + TIME_IN_HOURS);
        tz = TimeZone.getTimeZone(job.getTimeZone());
        next.setTime(next.getTime() - getDSTOffset(tz, startTime, next));
        assertEquals(next, job.getNextMaterializedTime());

        // for current job in hour, should not exceed hour windows
        startTime = new Date(new Date().getTime());
        endTime = new Date(startTime.getTime() + TIME_IN_DAY * 24);
        job = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, startTime, endTime, false, false, 0);
        job.setMatThrottling(20);
        job.setFrequency("1");
        job.setTimeUnitStr("DAY");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, job);
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
        job = jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
        // If the startTime and endTime straddle a DST shift (the Coord is in "America/Los_Angeles"), then we need to adjust for
        // that because startTime and endTime assume GMT
        next = new Date(startTime.getTime() + TIME_IN_DAY);
        tz = TimeZone.getTimeZone(job.getTimeZone());
        next.setTime(next.getTime() - getDSTOffset(tz, startTime, next));
        assertEquals(next, job.getNextMaterializedTime());

    }

    public void testLastOnlyMaterialization() throws Exception {

        long now = System.currentTimeMillis();
        Date startTime = DateUtils.toDate(new Timestamp(now - 180 * 60 * 1000));    // 3 hours ago
        Date endTime = DateUtils.toDate(new Timestamp(now + 180 * 60 * 1000));      // 3 hours from now
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, null, -1, "10",
                CoordinatorJob.Execution.LAST_ONLY);
        // This would normally materialize the throttle amount and within a 1 hour window; however, with LAST_ONLY this should
        // ignore those parameters and materialize everything in the past
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
        checkCoordJobs(job.getId(), CoordinatorJob.Status.RUNNING);
        CoordinatorActionBean.Status[] expectedStatuses = new CoordinatorActionBean.Status[19];
        Arrays.fill(expectedStatuses, CoordinatorActionBean.Status.WAITING);
        checkCoordActionsStatus(job.getId(), expectedStatuses);

        startTime = DateUtils.toDate(new Timestamp(now));                           // now
        job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, null, -1, "10",
                CoordinatorJob.Execution.LAST_ONLY);
        // We're starting from "now" this time (i.e. present/future), so it should materialize things normally
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
        checkCoordJobs(job.getId(), CoordinatorJob.Status.RUNNING);
        expectedStatuses = new CoordinatorActionBean.Status[6];
        Arrays.fill(expectedStatuses, CoordinatorActionBean.Status.WAITING);
        checkCoordActionsStatus(job.getId(), expectedStatuses);
    }

    public void testCurrentTimeCheck() throws Exception {
        long now = System.currentTimeMillis();
        Date startTime = DateUtils.toDate(new Timestamp(now)); // now
        Date endTime = DateUtils.toDate(new Timestamp(now + 3 * 60 * 60 * 1000)); // 3 hours from now
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, null, "5",
                20);
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
        checkCoordJobs(job.getId(), CoordinatorJob.Status.RUNNING);

        job = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB, job.getId());
        assertEquals(job.getLastActionNumber(), 12);
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
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
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
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

    protected CoordinatorJobBean addRecordToCoordJobTable(CoordinatorJob.Status status, Date startTime, Date endTime,
            Date pauseTime, String freq) throws Exception {
        return addRecordToCoordJobTable(status, startTime, endTime, pauseTime, -1, freq);
    }

    protected CoordinatorJobBean addRecordToCoordJobTable(CoordinatorJob.Status status, Date startTime, Date endTime,
            Date pauseTime, String freq, int matThrottling) throws Exception {
        return addRecordToCoordJobTable(status, startTime, endTime, pauseTime, -1, freq, CoordinatorJob.Execution.FIFO,
                matThrottling);
    }

    protected CoordinatorJobBean addRecordToCoordJobTable(CoordinatorJob.Status status, Date startTime, Date endTime,
            Date pauseTime, int timeout, String freq) throws Exception {
        return addRecordToCoordJobTable(status, startTime, endTime, pauseTime, timeout, freq,
                CoordinatorJob.Execution.FIFO, 20);
    }

    protected CoordinatorJobBean addRecordToCoordJobTable(CoordinatorJob.Status status, Date startTime, Date endTime,
            Date pauseTime, int timeout, String freq, CoordinatorJob.Execution execution) throws Exception {
        return addRecordToCoordJobTable(status, startTime, endTime, pauseTime, timeout, freq, execution, 20);
    }

    protected CoordinatorJobBean addRecordToCoordJobTable(CoordinatorJob.Status status, Date startTime, Date endTime,
            Date pauseTime, int timeout, String freq, CoordinatorJob.Execution execution, int matThrottling)
            throws Exception {
        CoordinatorJobBean coordJob = createCoordJob(status, startTime, endTime, false, false, 0);
        coordJob.setStartTime(startTime);
        coordJob.setEndTime(endTime);
        coordJob.setPauseTime(pauseTime);
        coordJob.setFrequency(freq);
        coordJob.setTimeUnit(Timeunit.MINUTE);
        coordJob.setTimeout(timeout);
        coordJob.setConcurrency(3);
        coordJob.setMatThrottling(matThrottling);
        coordJob.setExecutionOrder(execution);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordJobInsertJPAExecutor coordInsertCmd = new CoordJobInsertJPAExecutor(coordJob);
            jpaService.execute(coordInsertCmd);
        }
        catch (JPAExecutorException ex) {
            ex.printStackTrace();
            fail("Unable to insert the test coord job record to table");
            throw ex;
        }

        return coordJob;
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

    private CoordinatorJob.Status getStatus(String jobId){
        CoordinatorJob job = null;
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            job = jpaService.execute(new CoordJobGetJPAExecutor(jobId));
        }
        catch (JPAExecutorException se) {
            se.printStackTrace();
        }
        return job.getStatus();
    }

    private void checkCoordActions(String jobId, int number, CoordinatorJob.Status status) {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            Integer actionsSize = jpaService.execute(new CoordJobGetActionsJPAExecutor(jobId));
            if (actionsSize != number) {
                fail("Should have " + number + " actions created for job " + jobId + ", but has " + actionsSize + " actions.");
            }

            if (status != null) {
                CoordinatorJob job = jpaService.execute(new CoordJobGetJPAExecutor(jobId));
                if (job.getStatus() != status) {
                    fail("Job status " + job.getStatus() + " should be " + status);
                }
            }
        }
        catch (JPAExecutorException se) {
            se.printStackTrace();
            fail("Job ID " + jobId + " was not stored properly in db");
        }
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

    private CoordinatorJobBean addRecordToCoordJobTableForWaiting(String testFileName, CoordinatorJob.Status status,
            Date start, Date end, boolean pending, boolean doneMatd, int lastActionNum) throws Exception {

        String testDir = getTestCaseDir();
        CoordinatorJobBean coordJob = createCoordJob(testFileName, status, start, end, pending, doneMatd, lastActionNum);
        String appXml = getCoordJobXmlForWaiting(testFileName, testDir);
        coordJob.setJobXml(appXml);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordJobInsertJPAExecutor coordInsertCmd = new CoordJobInsertJPAExecutor(coordJob);
            jpaService.execute(coordInsertCmd);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test coord job record to table");
            throw je;
        }

        return coordJob;
    }

    private long getDSTOffset(TimeZone tz, Date d1, Date d2) {
        if (tz.inDaylightTime(d1) && !tz.inDaylightTime(d2)) {
            Calendar cal = Calendar.getInstance(tz);
            cal.setTime(d1);
            return cal.get(Calendar.DST_OFFSET);
        }
        if (!tz.inDaylightTime(d1) && tz.inDaylightTime(d2)) {
            Calendar cal = Calendar.getInstance(tz);
            cal.setTime(d2);
            return cal.get(Calendar.DST_OFFSET);
        }
        return 0;
    }
}
