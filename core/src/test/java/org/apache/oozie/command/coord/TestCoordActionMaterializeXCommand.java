/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.command.coord;

import java.io.IOException;
import java.io.Reader;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.SLAEventBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.CoordinatorJob.Timeunit;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetActionsJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.SLAEventsGetForSeqIdJPAExecutor;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XLog;

public class TestCoordActionMaterializeXCommand extends XDataTestCase {
    protected Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        cleanUpDBTables();
        LocalOozie.start();
    }

    @Override
    protected void tearDown() throws Exception {
        LocalOozie.stop();
        services.destroy();
        super.tearDown();
    }

    public void testActionMater() throws Exception {
        Date startTime = DateUtils.parseDateUTC("2009-03-06T010:00Z");
        Date endTime = DateUtils.parseDateUTC("2009-03-11T10:00Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.PREMATER, startTime, endTime);
        new CoordActionMaterializeXCommand(job.getId(), startTime, endTime).call();
        CoordinatorActionBean action = checkCoordAction(job.getId() + "@1");
    }

    public void testActionMaterWithPauseTime1() throws Exception {

        Date startTime = DateUtils.parseDateUTC("2009-03-06T10:00Z");
        Date endTime = DateUtils.parseDateUTC("2009-03-06T10:14Z");
        Date pauseTime = DateUtils.parseDateUTC("2009-03-06T10:04Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.PREMATER, startTime, endTime, pauseTime);
        new CoordActionMaterializeXCommand(job.getId(), startTime, endTime).call();
        checkCoordActions(job.getId(), 1, null);
    }

    public void testActionMaterWithPauseTime2() throws Exception {

        Date startTime = DateUtils.parseDateUTC("2009-03-06T10:00Z");
        Date endTime = DateUtils.parseDateUTC("2009-03-06T10:14Z");
        Date pauseTime = DateUtils.parseDateUTC("2009-03-06T10:08Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.PREMATER, startTime, endTime, pauseTime);
        new CoordActionMaterializeXCommand(job.getId(), startTime, endTime).call();
        checkCoordActions(job.getId(), 2, null);
    }

    public void testActionMaterWithPauseTime3() throws Exception {

        Date startTime = DateUtils.parseDateUTC("2009-03-06T10:00Z");
        Date endTime = DateUtils.parseDateUTC("2009-03-06T10:14Z");
        Date pauseTime = DateUtils.parseDateUTC("2009-03-06T09:58Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.PREMATER, startTime, endTime, pauseTime);
        new CoordActionMaterializeXCommand(job.getId(), startTime, endTime).call();
        checkCoordActions(job.getId(), 0, CoordinatorJob.Status.RUNNING);
    }

    public void testTimeout() throws Exception {
        Date startTime = DateUtils.parseDateUTC("2009-03-06T10:00Z");
        Date endTime = DateUtils.parseDateUTC("2009-03-06T10:14Z");
        Date pauseTime = null;
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.PREMATER, startTime, endTime,
                pauseTime, 300);
        new CoordActionMaterializeXCommand(job.getId(), startTime, endTime).call();

        JPAService jpaService = Services.get().get(JPAService.class);
        List<CoordinatorActionBean> actions = jpaService.execute(new CoordJobGetActionsJPAExecutor(job.getId()));
        checkCoordActionsTimeout(job.getId() + "@1", 300);
    }

    protected CoordinatorJobBean addRecordToCoordJobTable(CoordinatorJob.Status status, Date startTime, Date endTime)
            throws Exception {
        CoordinatorJobBean coordJob = createCoordJob(status);
        coordJob.setStartTime(startTime);
        coordJob.setEndTime(endTime);

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

    protected CoordinatorJobBean addRecordToCoordJobTable(CoordinatorJob.Status status, Date startTime, Date endTime,
            Date pauseTime) throws Exception {
        return addRecordToCoordJobTable(status, startTime, endTime, pauseTime, -1);
    }
    
    protected CoordinatorJobBean addRecordToCoordJobTable(CoordinatorJob.Status status, Date startTime, Date endTime,
            Date pauseTime, int timeout) throws Exception {
        CoordinatorJobBean coordJob = createCoordJob(status);
        coordJob.setStartTime(startTime);
        coordJob.setEndTime(endTime);
        coordJob.setPauseTime(pauseTime);
        coordJob.setFrequency(5);
        coordJob.setTimeUnit(Timeunit.MINUTE);
        coordJob.setTimeout(timeout);
        coordJob.setConcurrency(3);

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

    @Override
    protected String getCoordJobXml(Path appPath) {
        try {
            Reader reader = IOUtils.getResourceAsReader("coord-matd-job.xml", -1);
            String appXml = IOUtils.getReaderAsString(reader, -1);
            return appXml;
        }
        catch (IOException ioe) {
            throw new RuntimeException(XLog.format("Could not get coord-matd-job.xml", ioe));
        }
    }

    private CoordinatorActionBean checkCoordAction(String actionId) throws JPAExecutorException {
        long lastSeqId[] = new long[1];
        JPAService jpaService = Services.get().get(JPAService.class);
        List<SLAEventBean> slaEventList = jpaService.execute(new SLAEventsGetForSeqIdJPAExecutor(0, 10, lastSeqId));

        if (slaEventList.size() == 0) {
            fail("Unable to GET any record of sequence id greater than 0");
        }

        
        CoordinatorActionBean actionBean;
        actionBean = jpaService.execute(new CoordActionGetJPAExecutor(actionId));

        return actionBean;
    }

    private void checkCoordActions(String jobId, int number, CoordinatorJob.Status status) {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            List<CoordinatorActionBean> actions = jpaService.execute(new CoordJobGetActionsJPAExecutor(jobId));
            if (actions.size() != number) {
                fail("Should have " + number + " actions created for job " + jobId + ", but jave " + actions.size() + " actions.");
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
}
