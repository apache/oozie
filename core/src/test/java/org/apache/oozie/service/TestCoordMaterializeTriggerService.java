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

package org.apache.oozie.service;

import java.io.IOException;
import java.io.Reader;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.CoordinatorJob.Execution;
import org.apache.oozie.client.CoordinatorJob.Timeunit;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetActionsJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetRunningActionsCountJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.service.CoordMaterializeTriggerService.CoordMaterializeTriggerRunnable;
import org.apache.oozie.service.UUIDService.ApplicationType;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;

public class TestCoordMaterializeTriggerService extends XDataTestCase {
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

    /**
     * Tests functionality of the CoordMaterializeTriggerService Runnable
     * command. </p> Insert a coordinator job with PREP. Then, runs the
     * CoordMaterializeTriggerService runnable and ensures the job status
     * changes to RUNNING.
     *
     * @throws Exception
     */
    public void testCoordMaterializeTriggerService1() throws Exception {

        Date start = DateUtils.parseDateOozieTZ("2009-02-01T01:00Z");
        Date end = DateUtils.parseDateOozieTZ("2009-02-20T23:59Z");
        final CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, start, end, false, false, 0);

        sleep(3000);
        Runnable runnable = new CoordMaterializeTriggerRunnable(3600, 300);
        runnable.run();
        sleep(1000);

        JPAService jpaService = Services.get().get(JPAService.class);
        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
        CoordinatorJobBean coordJob = jpaService.execute(coordGetCmd);
        assertEquals(CoordinatorJob.Status.RUNNING, coordJob.getStatus());

        int numWaitingActions = jpaService.execute(new CoordJobGetRunningActionsCountJPAExecutor(coordJob.getId()));
        assert (numWaitingActions <= coordJob.getMatThrottling());
    }

    /**
     * Test current mode. The job should be picked up for materialization.
     *
     * @throws Exception
     */
    public void testCoordMaterializeTriggerService2() throws Exception {
        Date start = new Date();
        Date end = new Date(start.getTime() + 3600 * 48 * 1000);
        final CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, start, end, false, false, 0);

        sleep(3000);
        Runnable runnable = new CoordMaterializeTriggerRunnable(3600, 300);
        runnable.run();
        sleep(1000);

        JPAService jpaService = Services.get().get(JPAService.class);
        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
        CoordinatorJobBean coordJob = jpaService.execute(coordGetCmd);
        assertEquals(CoordinatorJob.Status.RUNNING, coordJob.getStatus());
    }

    public void testCoordMaterializeTriggerService3() throws Exception {
        Services.get().destroy();
        setSystemProperty(CoordMaterializeTriggerService.CONF_MATERIALIZATION_SYSTEM_LIMIT, "1");
        services = new Services();
        services.init();

        Date start = new Date();
        Date end = new Date(start.getTime() + 3600 * 5 * 1000);
        CoordinatorJobBean job1 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, false, false, 1);
        addRecordToCoordActionTable(job1.getId(), 2, CoordinatorAction.Status.WAITING,
                "coord-action-get.xml", 0);
        job1.setMatThrottling(1);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, job1);

        CoordinatorJobBean job2 = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, start, end, false, false, 0);
        CoordinatorJobBean job3 = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, start, end, false, false, 0);

        Runnable runnable = new CoordMaterializeTriggerRunnable(3600, 300);
        runnable.run();
        sleep(1000);

        JPAService jpaService = Services.get().get(JPAService.class);
        // second job is beyond limit but still should be picked up
        job2 = jpaService.execute(new CoordJobGetJPAExecutor(job2.getId()));
        assertEquals(CoordinatorJob.Status.RUNNING, job2.getStatus());
        // third job not picked up because limit iteration only twice
        job3 = jpaService.execute(new CoordJobGetJPAExecutor(job3.getId()));
        assertEquals(CoordinatorJob.Status.PREP, job3.getStatus());
    }

    public void testMaxMatThrottleNotPicked() throws Exception {
        Services.get().destroy();
        setSystemProperty(CoordMaterializeTriggerService.CONF_MATERIALIZATION_SYSTEM_LIMIT, "10");
        services = new Services();
        services.init();

        Date start = new Date();
        Date end = new Date(start.getTime() + 3600 * 5 * 1000);
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, false, false, 1);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0);
        job.setMatThrottling(3);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, job);
        JPAService jpaService = Services.get().get(JPAService.class);
        job = jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
        Date lastModifiedDate = job.getLastModifiedTime();
        Runnable runnable = new CoordMaterializeTriggerRunnable(3600, 300);
        runnable.run();
        sleep(1000);
        job = jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
        assertNotSame(lastModifiedDate, job.getLastModifiedTime());

        job.setMatThrottling(2);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, job);
        job = jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
        lastModifiedDate = job.getLastModifiedTime();
        runnable.run();
        sleep(1000);
        job = jpaService.execute(new CoordJobGetJPAExecutor(job.getId()));
        assertEquals(lastModifiedDate, job.getLastModifiedTime());
    }

    public void testMaxMatThrottleNotPickedMultipleJobs() throws Exception {
        Services.get().destroy();
        setSystemProperty(CoordMaterializeTriggerService.CONF_MATERIALIZATION_SYSTEM_LIMIT, "3");
        services = new Services();
        services.init();
        Date start = new Date();
        Date end = new Date(start.getTime() + 3600 * 5 * 1000);
        CoordinatorJobBean job1 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, false, false, 1);
        addRecordToCoordActionTable(job1.getId(), 1, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job1.getId(), 2, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0);
        job1.setMatThrottling(3);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, job1);

        CoordinatorJobBean job2 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, false, false, 1);
        addRecordToCoordActionTable(job2.getId(), 1, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job2.getId(), 2, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0);
        job2.setMatThrottling(3);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, job2);

        CoordinatorJobBean job3 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, false, false, 1);
        addRecordToCoordActionTable(job3.getId(), 1, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job3.getId(), 2, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0);
        job3.setMatThrottling(2);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, job3);

        JPAService jpaService = Services.get().get(JPAService.class);
        job1 = jpaService.execute(new CoordJobGetJPAExecutor(job1.getId()));
        Date lastModifiedDate1 = job1.getLastModifiedTime();
        job2 = jpaService.execute(new CoordJobGetJPAExecutor(job2.getId()));
        Date lastModifiedDate2 = job2.getLastModifiedTime();
        job3 = jpaService.execute(new CoordJobGetJPAExecutor(job3.getId()));
        Date lastModifiedDate3 = job3.getLastModifiedTime();


        Runnable runnable = new CoordMaterializeTriggerRunnable(3600, 300);
        runnable.run();
        sleep(1000);

        job1 = jpaService.execute(new CoordJobGetJPAExecutor(job1.getId()));
        assertNotSame(lastModifiedDate1, job1.getLastModifiedTime());
        job2 = jpaService.execute(new CoordJobGetJPAExecutor(job2.getId()));
        assertNotSame(lastModifiedDate2, job2.getLastModifiedTime());
        job3 = jpaService.execute(new CoordJobGetJPAExecutor(job3.getId()));
        assertEquals(lastModifiedDate3, job3.getLastModifiedTime());
    }

    @Override
    protected CoordinatorJobBean createCoordJob(CoordinatorJob.Status status, Date start, Date end, boolean pending, boolean doneMatd, int lastActionNum) throws Exception {
        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String appXml = writeCoordXml(appPath);

        String startDateStr = null, endDateStr = null;
        try {
            startDateStr = DateUtils.formatDateOozieTZ(start);
            endDateStr = DateUtils.formatDateOozieTZ(end);

            appXml = appXml.replaceAll("#start", startDateStr);
            appXml = appXml.replaceAll("#end", endDateStr);
        }
        catch (Exception ex) {
            fail("Could not get coord-matLookup-trigger.xml" + ex.getMessage());
            throw ex;
        }

        CoordinatorJobBean coordJob = new CoordinatorJobBean();
        coordJob.setId(Services.get().get(UUIDService.class).generateId(ApplicationType.COORDINATOR));
        coordJob.setAppName("COORD-TEST");
        coordJob.setAppPath(appPath.toString());
        coordJob.setStatus(status);
        coordJob.setTimeZone("America/Los_Angeles");
        coordJob.setCreatedTime(new Date());
        coordJob.setLastModifiedTime(new Date());
        coordJob.setUser(getTestUser());
        coordJob.setGroup(getTestGroup());

        Configuration conf = getCoordConf(appPath);
        coordJob.setConf(XmlUtils.prettyPrint(conf).toString());
        coordJob.setJobXml(appXml);
        coordJob.setLastActionNumber(0);
        coordJob.setFrequency("1");
        coordJob.setTimeUnit(Timeunit.DAY);
        coordJob.setExecutionOrder(Execution.FIFO);
        coordJob.setConcurrency(1);
        coordJob.setMatThrottling(1);
        try {
            coordJob.setStartTime(start);
            coordJob.setEndTime(end);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not set Date/time");
        }
        return coordJob;
    }

    @Override
    protected String getCoordJobXml(Path appPath) {
        try {
            Reader reader = IOUtils.getResourceAsReader("coord-matLookup-trigger.xml", -1);
            String appXml = IOUtils.getReaderAsString(reader, -1);
            return appXml;
        }
        catch (IOException ioe) {
            throw new RuntimeException(XLog.format("Could not get coord-matLookup-trigger.xml", ioe));
        }
    }
}
