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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.CoordinatorJob.Execution;
import org.apache.oozie.client.CoordinatorJob.Timeunit;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetRunningActionsCountJPAExecutor;
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
        cleanUpDBTables();
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

        Date start = DateUtils.parseDateUTC("2009-02-01T01:00Z");
        Date end = DateUtils.parseDateUTC("2009-02-20T23:59Z");
        final CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, start, end, false, false, 0);

        Thread.sleep(3000);
        Runnable runnable = new CoordMaterializeTriggerRunnable(3600);
        runnable.run();
        Thread.sleep(1000);

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

        Thread.sleep(3000);
        Runnable runnable = new CoordMaterializeTriggerRunnable(3600);
        runnable.run();
        Thread.sleep(1000);

        JPAService jpaService = Services.get().get(JPAService.class);
        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
        CoordinatorJobBean coordJob = jpaService.execute(coordGetCmd);
        assertEquals(CoordinatorJob.Status.RUNNING, coordJob.getStatus());
    }

    @Override
    protected CoordinatorJobBean createCoordJob(CoordinatorJob.Status status, Date start, Date end, boolean pending, boolean doneMatd, int lastActionNum) throws Exception {
        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String appXml = writeCoordXml(appPath);

        String startDateStr = null, endDateStr = null;
        try {
            startDateStr = DateUtils.formatDateUTC(start);
            endDateStr = DateUtils.formatDateUTC(end);

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
        coordJob.setAuthToken("notoken");

        Configuration conf = getCoordConf(appPath);
        coordJob.setConf(XmlUtils.prettyPrint(conf).toString());
        coordJob.setJobXml(appXml);
        coordJob.setLastActionNumber(0);
        coordJob.setFrequency(1);
        coordJob.setTimeUnit(Timeunit.DAY);
        coordJob.setExecution(Execution.FIFO);
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
