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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.CoordinatorJob.Execution;
import org.apache.oozie.client.CoordinatorJob.Timeunit;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.client.Job;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetActionByActionNumberJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.SLARegistrationQueryExecutor;
import org.apache.oozie.executor.jpa.SLASummaryQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.executor.jpa.SLARegistrationQueryExecutor.SLARegQuery;
import org.apache.oozie.executor.jpa.SLASummaryQueryExecutor.SLASummaryQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.StatusTransitService;
import org.apache.oozie.sla.SLARegistrationBean;
import org.apache.oozie.sla.SLASummaryBean;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;

public class TestCoordChangeXCommand extends XDataTestCase {
    private Services services;

    /**
     * Return the UTC date and time in W3C format down to second
     * (yyyy-MM-ddTHH:mmZ). i.e.: 1997-07-16T19:20Z The input date is a
     * long (Unix Time Stamp)
     *
     * @return the formatted time string.
     */
    public static String convertDateToString(long timeStamp) {
        return DateUtils.formatDateOozieTZ(new Date(timeStamp));
    }

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

    /**
     * Testing the Coordinator Change Commands with different combination of
     * "Change Value"
     * Changing the end time to be 20 minutes after the current time
     * Changing the pause time to be 10 minutes after the current time
     *
     * @throws Exception
     */

    public void testCoordChangeXCommand() throws StoreException, CommandException {
        System.out.println("Running Test");
        String jobId = "0000000-" + new Date().getTime() + "-testCoordChangeXCommand-C";

        try {
            addRecordToJobTable(jobId);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Exception thrown " + ex);
        }

        String pauseTime = convertDateToString(new Date().getTime() + 10 * 60 * 1000);
        String endTime = convertDateToString(new Date().getTime() + 20 * 60 * 1000);

        new CoordChangeXCommand(jobId, "endtime=" + endTime + ";concurrency=200").call();
        try {
            checkCoordJobs(jobId, DateUtils.parseDateOozieTZ(endTime), 200, null, false);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Invalid date" + ex);
        }

        String changeValue = "endtime=" + endTime + ";concurrency=200;pausetime=" + pauseTime;
        new CoordChangeXCommand(jobId, changeValue).call();
        try {
            checkCoordJobs(jobId, DateUtils.parseDateOozieTZ(endTime), 200, DateUtils.parseDateOozieTZ(pauseTime), true);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Invalid date" + ex);
        }

        new CoordChangeXCommand(jobId, "endtime=" + endTime + ";concurrency=200;pausetime=").call();
        try {
            checkCoordJobs(jobId, DateUtils.parseDateOozieTZ(endTime), 200, null, true);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Invalid date" + ex);
        }

        new CoordChangeXCommand(jobId, "endtime=" + endTime + ";pausetime=;concurrency=200").call();
        try {
            checkCoordJobs(jobId, DateUtils.parseDateOozieTZ(endTime), 200, null, true);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Invalid date" + ex);
        }

        new CoordChangeXCommand(jobId, "endtime=2012-12-20T05:00Z;concurrency=-200").call();
        try {
            checkCoordJobs(jobId, DateUtils.parseDateOozieTZ("2012-12-20T05:00Z"), -200, null, false);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Invalid date" + ex);
        }

        new CoordChangeXCommand(jobId, "endtime=2012-12-20T05:00Z").call();
        try {
            checkCoordJobs(jobId, DateUtils.parseDateOozieTZ("2012-12-20T05:00Z"), null, null, false);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Invalid date" + ex);
        }

        new CoordChangeXCommand(jobId, "concurrency=-1").call();
        try {
            checkCoordJobs(jobId, null, -1, null, false);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Invalid date" + ex);
        }

        try {
            new CoordChangeXCommand(jobId, "a=1;b=-200").call();
            fail("Should not reach here.");
        }
        catch (CommandException ex) {
            if (ex.getErrorCode() != ErrorCode.E1015) {
                fail("Error code should be E1015.");
            }
        }

        try {
            new CoordChangeXCommand(jobId, "endtime=2012-12-20T05:00;concurrency=-200").call();
            fail("Should not reach here.");
        }
        catch (CommandException ex) {
            if (ex.getErrorCode() != ErrorCode.E1015) {
                fail("Error code should be E1015.");
            }
        }

        try {
            new CoordChangeXCommand(jobId, "endtime=2012-12-20T05:00Z;concurrency=2ac").call();
            fail("Should not reach here.");
        }
        catch (CommandException ex) {
            if (ex.getErrorCode() != ErrorCode.E1015) {
                fail("Error code should be E1015.");
            }
        }

        try {
            new CoordChangeXCommand(jobId, "endtime=1900-12-20T05:00Z").call();
        }
        catch (CommandException ex) {
            fail("Should not throw exception");
        }

        try {
            new CoordChangeXCommand(jobId, "pausetime=null").call();
            fail("Should not reach here.");
        }
        catch (CommandException ex) {
            if (ex.getErrorCode() != ErrorCode.E1015) {
                fail("Error code should be E1015.");
            }
        }

        try {
            new CoordChangeXCommand(jobId, "pausetime=" + pauseTime).call();
        }
        catch (CommandException ex) {
            fail("Should not throw exception");
        }
    }

    /**
     * test end time change : pending should mark false if job is running with
     * pending true
     * Changing the end time to be 40 minutes after the current time
     * Changing the pause time to be 10 minutes after the current time
     *
     * @throws Exception
     */
    public void testCoordChangeEndTime() throws Exception {
        Date startTime = new Date();
        Date endTime = new Date(startTime.getTime() + (20 * 60 * 1000));

        final CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime,
                true, true, 0);

        String pauseTime = convertDateToString(startTime.getTime() + 10 * 60 * 1000);
        String newEndTime = convertDateToString(startTime.getTime() + 40 * 60 * 1000);

        new CoordChangeXCommand(job.getId(), "endtime=" + newEndTime + ";pausetime=" + pauseTime).call();
        try {
            checkCoordJobs(job.getId(), DateUtils.parseDateOozieTZ(newEndTime), null, DateUtils.parseDateOozieTZ(
                pauseTime),
                    true);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Invalid date" + ex);
        }

        JPAService jpaService = Services.get().get(JPAService.class);
        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
        CoordinatorJobBean coordJob = jpaService.execute(coordGetCmd);
        assertEquals(Job.Status.RUNNING, coordJob.getStatus());
        assertFalse(coordJob.isDoneMaterialization());
    }

    /**
     * Testcase when changing end-time == nextMaterializedTime
     * reflects correct job status via StatusTransit
     *
     * @throws Exception
     */
    public void testCoordChangeEndTime1() throws Exception {

        JPAService jpaService = Services.get().get(JPAService.class);

        Date startTime = new Date();
        Date endTime = new Date(startTime.getTime() + (50 * 60 * 1000));
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, true, true, 1);
        coordJob.setNextMaterializedTime(new Date(startTime.getTime() + (30 * 60 * 1000)));
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob);
        addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);

        Runnable runnable = new StatusTransitService.StatusTransitRunnable();
        runnable.run(); // dummy run so we get to the interval check following coord job change
        sleep(1000);

        assertEquals(endTime.getTime(), coordJob.getEndTime().getTime()); // checking before change

        String newEndTime = convertDateToString(startTime.getTime() + 30 * 60 * 1000);

        new CoordChangeXCommand(coordJob.getId(), "endtime=" + newEndTime).call();
        try {
            checkCoordJobs(coordJob.getId(), DateUtils.parseDateOozieTZ(newEndTime), null, null, false);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Invalid date" + ex);
        }

        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(coordJob.getId());
        coordJob = jpaService.execute(coordGetCmd);
        assertEquals(Job.Status.RUNNING, coordJob.getStatus());
        assertEquals(newEndTime, convertDateToString(coordJob.getEndTime().getTime())); // checking after change
        assertTrue(coordJob.isPending());
        assertTrue(coordJob.isDoneMaterialization());

        runnable.run();
        sleep(1000);
        coordJob = jpaService.execute(coordGetCmd);
        assertEquals(Job.Status.SUCCEEDED, coordJob.getStatus());
        assertFalse(coordJob.isPending());
        assertTrue(coordJob.isDoneMaterialization());
    }

    /**
     * Testcase when changing end-time > nextMaterializedTime, but < original end
     * reflects correct job state and values
     *
     * @throws Exception
     */
    public void testCoordChangeEndTime2() throws Exception {

        JPAService jpaService = Services.get().get(JPAService.class);

        Date startTime = new Date();
        Date endTime = new Date(startTime.getTime() + (50 * 60 * 1000));
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, true, true, 1);
        coordJob.setNextMaterializedTime(new Date(startTime.getTime() + (30 * 60 * 1000)));
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob);
        addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);

        assertTrue(coordJob.isDoneMaterialization()); // checking initial condition before change

        Runnable runnable = new StatusTransitService.StatusTransitRunnable();
        runnable.run(); // dummy run so we get to the interval check following coord job change
        sleep(1000);

        String newEndTime = convertDateToString(startTime.getTime() + 40 * 60 * 1000);

        new CoordChangeXCommand(coordJob.getId(), "endtime=" + newEndTime).call();
        try {
            checkCoordJobs(coordJob.getId(), DateUtils.parseDateOozieTZ(newEndTime), null, null, false);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Invalid date" + ex);
        }

        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(coordJob.getId());
        coordJob = jpaService.execute(coordGetCmd);
        assertEquals(Job.Status.RUNNING, coordJob.getStatus());
        assertTrue(coordJob.isPending());
        assertFalse(coordJob.isDoneMaterialization()); // <-- changed
        assertEquals(newEndTime, convertDateToString(coordJob.getEndTime().getTime()));

    }

    /**
     * Testcase when changing end-time to after original end-time
     * but before nextMaterializedTime should not cause unnecessary changes
     *
     * @throws Exception
     */
    public void testCoordChangeEndTime3() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        Date startTime = new Date();
        Date endTime = new Date(startTime.getTime() + (10 * 60 * 1000));
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, true, true, 1);
        coordJob.setNextMaterializedTime(new Date(startTime.getTime() + (40 * 60 * 1000)));
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob);
        addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);

        Runnable runnable = new StatusTransitService.StatusTransitRunnable();
        runnable.run();

        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(coordJob.getId());
        coordJob = jpaService.execute(coordGetCmd);
        assertEquals(Job.Status.SUCCEEDED, coordJob.getStatus());
        assertFalse(coordJob.isPending());
        assertTrue(coordJob.isDoneMaterialization());

        String newEndTime = convertDateToString(startTime.getTime() + 20 * 60 * 1000);
        try{
        new CoordChangeXCommand(coordJob.getId(), "endtime=" + newEndTime).call();
        } catch(Exception e){
            assertTrue(e.getMessage().contains(
                    "Didn't change endtime. Endtime is in between coord end time and next materialization time"));
        }
        coordJob = jpaService.execute(coordGetCmd);
        assertFalse(Job.Status.RUNNING == coordJob.getStatus());
        assertFalse(coordJob.isPending());
        assertTrue(coordJob.isDoneMaterialization());
    }

    // Testcase to test deletion of lookahead action in case of end-date change
    public void testCoordChangeEndTimeDeleteAction() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2013-08-01T00:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2013-08-01T05:00Z");
        Date changeEndTime = DateUtils.parseDateOozieTZ("2013-08-01T02:00Z");
        String endTimeChangeStr = "endtime=" + DateUtils.formatDateOozieTZ(changeEndTime);
        final CoordinatorJobBean job = addRecordToCoordJobTableForPauseTimeTest(CoordinatorJob.Status.RUNNING,
                startTime, endTime, endTime, true, false, 4);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0,
                DateUtils.parseDateOozieTZ("2013-08-01T00:00Z"));
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0,
                DateUtils.parseDateOozieTZ("2013-08-01T01:00Z"));
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0,
                DateUtils.parseDateOozieTZ("2013-08-01T02:00Z"));
        addRecordToCoordActionTable(job.getId(), 4, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0,
                DateUtils.parseDateOozieTZ("2013-08-01T03:00Z"));
        JPAService jpaService = Services.get().get(JPAService.class);
        new CoordChangeXCommand(job.getId(), endTimeChangeStr).call();
        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
        CoordinatorJobBean coordJob = jpaService.execute(coordGetCmd);
        assertEquals(coordJob.getEndTime(), changeEndTime);
        assertEquals(Job.Status.RUNNING, coordJob.getStatus());
        assertEquals(2, coordJob.getLastActionNumber());
        try {
            jpaService.execute(new CoordJobGetActionByActionNumberJPAExecutor(job.getId(), 2));
        }
        catch (JPAExecutorException jpae) {
            fail(" Action should be there");
        }
        try {
            jpaService.execute(new CoordJobGetActionByActionNumberJPAExecutor(job.getId(), 3));
            fail("Expected to fail as action 3 should have been deleted");
        }
        catch (JPAExecutorException jpae) {
            assertEquals(ErrorCode.E0603, jpae.getErrorCode());
        }
        assertEquals(DateUtils.parseDateOozieTZ("2013-08-01T02:00Z"), coordJob.getNextMaterializedTime());
        assertEquals(DateUtils.parseDateOozieTZ("2013-08-01T02:00Z"), coordJob.getLastActionTime());
    }

    // Testcase to test deletion of lookahead action in case of end-date change
    // Added one more test case to test processLookaheadActions with day frequency and SUSPENDED status.
    public void testProcessLookaheadActions() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2013-08-01T00:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2013-08-29T00:00Z");

        Date changeEndTime = DateUtils.parseDateOozieTZ("2013-08-05T00:00Z");
        String endTimeChangeStr = "endtime=" + DateUtils.formatDateOozieTZ(changeEndTime);
        final CoordinatorJobBean job = addRecordToCoordJobTableForPauseTimeTest(CoordinatorJob.Status.SUSPENDED,
                startTime, endTime, endTime, true, false, 6);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0,
                DateUtils.parseDateOozieTZ("2013-08-01T00:00Z"));
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0,
                DateUtils.parseDateOozieTZ("2013-08-02T00:00Z"));
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0,
                DateUtils.parseDateOozieTZ("2013-08-03T00:00Z"));
        addRecordToCoordActionTable(job.getId(), 4, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0,
                DateUtils.parseDateOozieTZ("2013-08-04T00:00Z"));
        addRecordToCoordActionTable(job.getId(), 5, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0,
                DateUtils.parseDateOozieTZ("2013-08-05T00:00Z"));
        addRecordToCoordActionTable(job.getId(), 6, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0,
                DateUtils.parseDateOozieTZ("2013-08-06T00:00Z"));

        job.setFrequency("1");
        job.setTimeUnit(Timeunit.DAY);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, job);

        JPAService jpaService = Services.get().get(JPAService.class);
        new CoordChangeXCommand(job.getId(), endTimeChangeStr).call();

        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
        CoordinatorJobBean coordJob = jpaService.execute(coordGetCmd);
        assertEquals(coordJob.getEndTime(), changeEndTime);
        assertEquals(Job.Status.SUSPENDED, coordJob.getStatus());
        assertEquals(4, coordJob.getLastActionNumber());
        assertEquals(DateUtils.parseDateOozieTZ("2013-08-05T00:00Z"), coordJob.getNextMaterializedTime());
        assertEquals(DateUtils.parseDateOozieTZ("2013-08-05T00:00Z"), coordJob.getLastActionTime());
        assertEquals(changeEndTime, coordJob.getEndTime());
    }

    // Testcase to check status for coord whose enddate is set before startdate.
    public void testCoordChangeEndTimeBeforeStart() throws Exception {
        Date start = new Date();
        Date end = new Date(start.getTime() + (4 * 60 * 60 * 1000)); // 4 hrs
        Date endTime = new Date(start.getTime() - 3000);

        String endTimeChangeStr = "endtime=" + DateUtils.formatDateOozieTZ(endTime);
        final CoordinatorJobBean job = addRecordToCoordJobTableForPauseTimeTest(CoordinatorJob.Status.PREP, start, end,
                end, true, false, 0);

        JPAService jpaService = Services.get().get(JPAService.class);
        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
        CoordinatorJobBean coordJob = jpaService.execute(coordGetCmd);

        assertEquals(Job.Status.PREP, coordJob.getStatus());
        assertEquals(0, coordJob.getLastActionNumber());
        new CoordChangeXCommand(job.getId(), endTimeChangeStr).call();
        coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
        coordJob = jpaService.execute(coordGetCmd);
        assertEquals(DateUtils.formatDateOozieTZ(coordJob.getEndTime()), DateUtils.formatDateOozieTZ(endTime));
        assertEquals(Job.Status.SUCCEEDED, coordJob.getStatus());

        Date newEndTime = new Date(start.getTime() - 2000);
        endTimeChangeStr = "endtime=" + DateUtils.formatDateOozieTZ(newEndTime);
        new CoordChangeXCommand(job.getId(), endTimeChangeStr).call();
        coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
        coordJob = jpaService.execute(coordGetCmd);
        assertEquals(DateUtils.formatDateOozieTZ(coordJob.getEndTime()), DateUtils.formatDateOozieTZ(newEndTime));
        assertEquals(Job.Status.SUCCEEDED, coordJob.getStatus());

        // setting end date after startdate should make coord in running state
        newEndTime = new Date(start.getTime() + (4 * 60 * 60 * 1000));
        endTimeChangeStr = "endtime=" + DateUtils.formatDateOozieTZ(newEndTime);
        new CoordChangeXCommand(job.getId(), endTimeChangeStr).call();
        coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
        coordJob = jpaService.execute(coordGetCmd);
        assertEquals(DateUtils.formatDateOozieTZ(coordJob.getEndTime()), DateUtils.formatDateOozieTZ(newEndTime));
        assertEquals(Job.Status.RUNNING, coordJob.getStatus());

        // setting end date before startdate should make coord in SUCCEEDED state
        newEndTime = new Date(start.getTime() - 1000);
        endTimeChangeStr = "endtime=" + DateUtils.formatDateOozieTZ(newEndTime);
        new CoordChangeXCommand(job.getId(), endTimeChangeStr).call();
        coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
        coordJob = jpaService.execute(coordGetCmd);
        assertEquals(DateUtils.formatDateOozieTZ(coordJob.getEndTime()), DateUtils.formatDateOozieTZ(newEndTime));
        assertEquals(Job.Status.SUCCEEDED, coordJob.getStatus());


    }

    /**
     * Change the pause time and end time of a failed coordinator job. Check whether the status changes
     * to RUNNINGWITHERROR
     * @throws Exception
     */
    public void testCoordChangeStatus() throws Exception {
        Services.get().destroy();
        setSystemProperty(StatusTransitService.CONF_BACKWARD_SUPPORT_FOR_STATES_WITHOUT_ERROR, "false");
        services = new Services();
        services.init();
        Date startTime = new Date();
        Date endTime = new Date(startTime.getTime() + (20 * 60 * 1000));

        final CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.FAILED, startTime, endTime,
                true, true, 0);

        String pauseTime = convertDateToString(startTime.getTime() + 10 * 60 * 1000);
        String newEndTime = convertDateToString(startTime.getTime() + 40 * 60 * 1000);

        new CoordChangeXCommand(job.getId(), "endtime=" + newEndTime + ";pausetime=" + pauseTime).call();

        JPAService jpaService = Services.get().get(JPAService.class);
        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
        CoordinatorJobBean coordJob = jpaService.execute(coordGetCmd);
        assertEquals(Job.Status.RUNNINGWITHERROR, coordJob.getStatus());
    }

    /**
     * test pause time change : pending should mark false if job is running with
     * pending true. two actions should be removed for pause time changes.
     *
     * @throws Exception
     */
    public void testCoordChangePauseTime() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2013-08-01T00:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2013-08-01T04:59Z");

        Date pauseTime = new Date(startTime.getTime() + (3 * 60 * 60 * 1000));  //2 hrs
        String pauseTimeChangeStr = "pausetime=" + DateUtils.formatDateOozieTZ(pauseTime);
        final CoordinatorJobBean job = addRecordToCoordJobTableForPauseTimeTest(CoordinatorJob.Status.RUNNING, startTime,
                endTime, endTime, true, false, 4);
        CoordinatorActionBean ca1 = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);
        CoordinatorActionBean ca2 = addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0, DateUtils.parseDateOozieTZ("2013-08-01T02:00Z"));
        CoordinatorActionBean ca3 = addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.WAITING,
                "coord-action-get.xml", 0, DateUtils.parseDateOozieTZ("2013-08-01T03:00Z"));
        CoordinatorActionBean ca4 = addRecordToCoordActionTable(job.getId(), 4, CoordinatorAction.Status.WAITING,
                "coord-action-get.xml", 0, DateUtils.parseDateOozieTZ("2013-08-01T04:00Z"));

        SLARegistrationBean slaRegBean1 = new SLARegistrationBean();
        slaRegBean1.setId(ca1.getId());
        SLARegistrationBean slaRegBean2 = new SLARegistrationBean();
        slaRegBean2.setId(ca2.getId());
        SLARegistrationBean slaRegBean3 = new SLARegistrationBean();
        slaRegBean3.setId(ca3.getId());
        SLARegistrationBean slaRegBean4 = new SLARegistrationBean();
        slaRegBean4.setId(ca4.getId());
        SLASummaryBean slaSummaryBean1 = new SLASummaryBean();
        slaSummaryBean1.setId(ca1.getId());
        SLASummaryBean slaSummaryBean3 = new SLASummaryBean();
        slaSummaryBean3.setId(ca3.getId());
        List<JsonBean> insertList = new ArrayList<JsonBean>();
        insertList.add(slaRegBean1);
        insertList.add(slaRegBean2);
        insertList.add(slaRegBean3);
        insertList.add(slaRegBean4);
        insertList.add(slaSummaryBean1);
        insertList.add(slaSummaryBean3);

        JPAService jpaService = Services.get().get(JPAService.class);
        BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(insertList, null, null);

        new CoordChangeXCommand(job.getId(), pauseTimeChangeStr).call();

        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
        CoordinatorJobBean coordJob = jpaService.execute(coordGetCmd);
        assertEquals(DateUtils.formatDateOozieTZ(coordJob.getPauseTime()), DateUtils.formatDateOozieTZ(pauseTime));
        assertEquals(Job.Status.RUNNING, coordJob.getStatus());
        assertEquals(2, coordJob.getLastActionNumber());
        try {
            jpaService.execute(new CoordJobGetActionByActionNumberJPAExecutor(job.getId(), 3));
            fail("Expected to fail as action 3 should have been deleted");
        }
        catch (JPAExecutorException jpae) {
            assertEquals(ErrorCode.E0603, jpae.getErrorCode());
        }

        try {
            jpaService.execute(new CoordJobGetActionByActionNumberJPAExecutor(job.getId(), 4));
            fail("Expected to fail as action 4 should have been deleted");
        }
        catch (JPAExecutorException jpae) {
            assertEquals(ErrorCode.E0603, jpae.getErrorCode());
        }

        slaRegBean1 = SLARegistrationQueryExecutor.getInstance().get(SLARegQuery.GET_SLA_REG_ALL, slaRegBean1.getId());
        assertNotNull(slaRegBean1);
        slaRegBean2 = SLARegistrationQueryExecutor.getInstance().get(SLARegQuery.GET_SLA_REG_ALL, slaRegBean2.getId());
        assertNotNull(slaRegBean2);
        slaRegBean3 = SLARegistrationQueryExecutor.getInstance().get(SLARegQuery.GET_SLA_REG_ALL, slaRegBean3.getId());
        assertNull(slaRegBean3);
        slaRegBean4 = SLARegistrationQueryExecutor.getInstance().get(SLARegQuery.GET_SLA_REG_ALL, slaRegBean4.getId());
        assertNull(slaRegBean4);
        slaSummaryBean3 = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, slaSummaryBean3.getId());
        assertNull(slaSummaryBean3);
        slaSummaryBean1 = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, slaSummaryBean1.getId());
        assertNotNull(slaSummaryBean1);
    }

    // Checks that RUNNING coord action is not deleted
    public void testChangeTimeDeleteRunning() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2013-08-01T00:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2013-08-01T04:59Z");
        Date pauseTime = new Date(startTime.getTime() + (2 * 60 * 60 * 1000)); // 2 hrs
        String pauseTimeChangeStr = "pausetime=" + DateUtils.formatDateOozieTZ(pauseTime);
        final CoordinatorJobBean job = addRecordToCoordJobTableForPauseTimeTest(CoordinatorJob.Status.RUNNING,
                startTime, endTime, endTime, true, false, 4);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0,
                DateUtils.parseDateOozieTZ("2013-08-01T02:00Z"));
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0,
                DateUtils.parseDateOozieTZ("2013-08-01T03:00Z"));
        addRecordToCoordActionTable(job.getId(), 4, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0,
                DateUtils.parseDateOozieTZ("2013-08-01T04:00Z"));
        try {
            new CoordChangeXCommand(job.getId(), pauseTimeChangeStr).call();
            fail("Should not reach here.");
        }
        catch (CommandException e) {
            if (e.getErrorCode() != ErrorCode.E1022) {
                fail("Error code should be E1022");
            }
        }
        String endTimeChangeStr = "endtime=" + DateUtils.formatDateOozieTZ(pauseTime);
        try {
            new CoordChangeXCommand(job.getId(), endTimeChangeStr).call();
            fail("Should not reach here.");
        }
        catch (CommandException e) {
            if (e.getErrorCode() != ErrorCode.E1022) {
                fail("Error code should be E1022");
            }
        }
    }

    public void testCoordStatus_Ignored() throws Exception {
        Date start = new Date();
        Date end = new Date(start.getTime() + (5 * 60 * 60 * 1000)); // 5 hrs
        String statusToRUNNING = "status=RUNNING";
        String statusToIGNORED = "status=IGNORED";
        final CoordinatorJobBean job1 = addRecordToCoordJobTableForPauseTimeTest(CoordinatorJob.Status.IGNORED, start,
                end, end, false, false, 4);
        final CoordinatorJobBean job2 = addRecordToCoordJobTableForPauseTimeTest(CoordinatorJob.Status.KILLED, start,
                end, end, false, false, 4);
        final CoordinatorJobBean job3 = addRecordToCoordJobTableForPauseTimeTest(CoordinatorJob.Status.RUNNING, start,
                end, end, false, false, 4);
        final CoordinatorJobBean job4 = addRecordToCoordJobTableForPauseTimeTest(CoordinatorJob.Status.FAILED, start,
                end, end, true, false, 4);

        // Status change from IGNORED to RUNNING
        new CoordChangeXCommand(job1.getId(), statusToRUNNING).call();
        CoordinatorJobBean coordJob = CoordJobQueryExecutor.getInstance()
                .get(CoordJobQuery.GET_COORD_JOB, job1.getId());
        assertEquals(coordJob.getStatus(), Job.Status.RUNNING);

        // Status change from KILLED -> IGNORED
        new CoordChangeXCommand(job2.getId(), statusToIGNORED).call();
        coordJob = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB, job2.getId());
        assertEquals(coordJob.getStatus(), Job.Status.IGNORED);

        // Status change from RUNNING -> IGNORED
        try {
            new CoordChangeXCommand(job3.getId(), statusToIGNORED).call();
        }
        catch (CommandException ex) {
            assertEquals(ErrorCode.E1015, ex.getErrorCode());
            assertTrue(ex.getMessage().indexOf(
                    "Only FAILED or KILLED non-pending job can be changed to IGNORED") > -1);
        }
        // Status change from FAILED -> IGNORED when coord job is pending
        try {
            new CoordChangeXCommand(job4.getId(), statusToIGNORED).call();
        }
        catch (CommandException ex) {
            assertEquals(ErrorCode.E1015, ex.getErrorCode());
            assertTrue(ex.getMessage().indexOf(
                    "Only FAILED or KILLED non-pending job can be changed to IGNORED") > -1);
        }
    }
    // Status change from failed- successful
    public void testCoordStatus_Failed() throws Exception {
        Date start = new Date();
        Date end = new Date(start.getTime() + (5 * 60 * 60 * 1000)); // 5 hrs
        String status = "status=RUNNING";
        final CoordinatorJobBean job = addRecordToCoordJobTableForPauseTimeTest(CoordinatorJob.Status.FAILED, start,
                end, end, true, false, 4);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 4, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0);

        try {
            new CoordChangeXCommand(job.getId(), status).call();
            JPAService jpaService = Services.get().get(JPAService.class);

            CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
            CoordinatorJobBean coordJob = jpaService.execute(coordGetCmd);

            assertEquals(coordJob.getStatusStr(), "RUNNING");
        }
        catch (CommandException e) {
            e.printStackTrace();
            if (e.getErrorCode() != ErrorCode.E1022) {
                fail("Error code should be E1022");
            }
        }
    }

    //  Status change from Killed- successful
    public void testCoordStatus_Killed() throws Exception {
        Date start = new Date();
        Date end = new Date(start.getTime() + (5 * 60 * 60 * 1000)); // 5 hrs
        String status = "status=RUNNING";
        final CoordinatorJobBean job = addRecordToCoordJobTableForPauseTimeTest(CoordinatorJob.Status.KILLED, start,
                end, end, true, false, 4);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 4, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0);

        try {
            new CoordChangeXCommand(job.getId(), status).call();
            JPAService jpaService = Services.get().get(JPAService.class);

            CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
            CoordinatorJobBean coordJob = jpaService.execute(coordGetCmd);

            assertEquals(coordJob.getStatusStr(), "RUNNING");
        }
        catch (CommandException e) {
            e.printStackTrace();
            if (e.getErrorCode() != ErrorCode.E1022) {
                fail("Error code should be E1022");
            }
        }
    }

    // Check status change from Succeeded-  exception
    public void testCoordStatus_Changefailed() throws Exception {
        Date start = new Date();
        Date end = new Date(start.getTime() + (4 * 60 * 60 * 1000)); // 5 hrs
        String status = "status=RUNNING";
        final CoordinatorJobBean job = addRecordToCoordJobTableForPauseTimeTest(CoordinatorJob.Status.SUCCEEDED, start,
                end, end, true, false, 4);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 4, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0);

        try {
            new CoordChangeXCommand(job.getId(), status).call();
        }
        catch (CommandException e) {
            assertTrue(e.getMessage().contains("Invalid coordinator job change value RUNNING"));
        }
    }

    // Check status change - with multiple option. Pause can't be applied to killed job, old behavior.
    public void testCoord_throwException() throws Exception {
        Date start = new Date();
        Date end = new Date(start.getTime() + (4 * 60 * 60 * 1000)); // 4 hrs
        String status = "status=RUNNING;pausetime=" + DateUtils.formatDateOozieTZ(end);
        final CoordinatorJobBean job = addRecordToCoordJobTableForPauseTimeTest(CoordinatorJob.Status.KILLED, start,
                end, end, true, false, 4);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 4, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);

        try {
            new CoordChangeXCommand(job.getId(), status).call();
            fail("should throw Exception");
        }
        catch (CommandException e) {
            assertTrue(e.getMessage().contains("Cannot change a killed coordinator job"));
        }
    }
    protected CoordinatorJobBean addRecordToCoordJobTableForPauseTimeTest(CoordinatorJob.Status status, Date start,
            Date end, Date lastActionTime, boolean pending, boolean doneMatd, int lastActionNum) throws Exception {
        CoordinatorJobBean coordJob = createCoordJob(status, start, end, pending, doneMatd, lastActionNum);
        coordJob.setFrequency("1");
        coordJob.setTimeUnit(Timeunit.HOUR);
        coordJob.setLastActionNumber(lastActionNum);
        coordJob.setLastActionTime(lastActionTime);
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

    private void addRecordToJobTable(String jobId) throws Exception {
        // CoordinatorStore store = new CoordinatorStore(false);
        CoordinatorJobBean coordJob = new CoordinatorJobBean();
        coordJob.setId(jobId);
        coordJob.setAppName("testApp");
        coordJob.setAppPath("testAppPath");
        coordJob.setStatus(CoordinatorJob.Status.SUCCEEDED);
        coordJob.setCreatedTime(new Date());
        coordJob.setLastModifiedTime(DateUtils.parseDateOozieTZ("2009-01-02T23:59Z"));
        coordJob.setTimeZone("UTC");
        coordJob.setTimeUnit(Timeunit.MINUTE);
        coordJob.setUser("testUser");
        coordJob.setGroup("testGroup");

        String confStr = "<configuration></configuration>";
        coordJob.setConf(confStr);
        String appXml = "<coordinator-app xmlns='uri:oozie:coordinator:0.2' name='NAME' frequency=\"5\" start='2009-02-01T01:00Z' end='2009-02-01T01:09Z' timezone='UTC' freq_timeunit='MINUTE' end_of_duration='NONE'>";
        appXml += "<controls>";
        appXml += "<timeout>10</timeout>";
        appXml += "<concurrency>2</concurrency>";
        appXml += "<execution>LIFO</execution>";
        appXml += "</controls>";
        appXml += "<input-events>";
        appXml += "<data-in name='A' dataset='a'>";
        appXml += "<dataset name='a' frequency='5' initial-instance='2009-02-01T01:00Z' timezone='UTC' freq_timeunit='MINUTE' end_of_duration='NONE'>";
        appXml += "<uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template>";
        appXml += "</dataset>";
        appXml += "<instance>${coord:latest(0)}</instance>";
        appXml += "</data-in>";
        appXml += "</input-events>";
        appXml += "<output-events>";
        appXml += "<data-out name='LOCAL_A' dataset='local_a'>";
        appXml += "<dataset name='local_a' frequency='5' initial-instance='2009-02-01T01:00Z' timezone='UTC' freq_timeunit='MINUTE' end_of_duration='NONE'>";
        appXml += "<uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template>";
        appXml += "</dataset>";
        appXml += "<instance>${coord:current(-1)}</instance>";
        appXml += "</data-out>";
        appXml += "</output-events>";
        appXml += "<action>";
        appXml += "<workflow>";
        appXml += "<app-path>hdfs:///tmp/workflows/</app-path>";
        appXml += "<configuration>";
        appXml += "<property>";
        appXml += "<name>inputA</name>";
        appXml += "<value>${coord:dataIn('A')}</value>";
        appXml += "</property>";
        appXml += "<property>";
        appXml += "<name>inputB</name>";
        appXml += "<value>${coord:dataOut('LOCAL_A')}</value>";
        appXml += "</property>";
        appXml += "</configuration>";
        appXml += "</workflow>";
        appXml += "</action>";
        appXml += "</coordinator-app>";
        coordJob.setJobXml(appXml);
        coordJob.setLastActionNumber(0);
        coordJob.setFrequency("5");
        coordJob.setExecutionOrder(Execution.FIFO);
        coordJob.setConcurrency(1);
        try {
            coordJob.setStartTime(DateUtils.parseDateOozieTZ("2009-02-01T01:00Z"));
            coordJob.setEndTime(DateUtils.parseDateOozieTZ("2009-02-01T01:09Z"));
            coordJob.setLastActionTime(DateUtils.parseDateOozieTZ("2009-02-01T01:10Z"));
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not set Date/time");
        }

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
    }

    private void checkCoordJobs(String jobId, Date endTime, Integer concurrency, Date pauseTime, boolean checkPauseTime)
            throws StoreException {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(jobId);
            CoordinatorJobBean job = null;

            job = jpaService.execute(coordGetCmd);

            if (endTime != null) {
                Date d = job.getEndTime();
                if (d.compareTo(endTime) != 0) {
                    fail("Endtime is not updated properly job_end_time=" + d + ", expected_end_time=" + endTime);
                }

                CoordinatorJob.Status status = job.getStatus();
                if (status != CoordinatorJob.Status.RUNNING) {
                    fail("Coordinator job's status is not updated properly");
                }
            }

            if (concurrency != null) {
                int c = job.getConcurrency();

                if (c != concurrency) {
                    fail("Concurrency is not updated properly");
                }
            }

            if (checkPauseTime) {
                Date d = job.getPauseTime();
                if (pauseTime == null) {
                    if (d != null) {
                        fail("Pausetime is not updated properly job_pause_time=" + d + ", expected_pause_time="
                                + pauseTime);
                    }
                }
                else {
                    if (d.compareTo(pauseTime) != 0) {
                        fail("Pausetime is not updated properly job_pause_time=" + d + ", expected_pause_time="
                                + pauseTime);
                    }
                }
            }
        }
        catch (JPAExecutorException e) {
            e.printStackTrace();
        }

    }
}
