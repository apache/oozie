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

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.SLAEventBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.CoordinatorJob.Timeunit;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetActionsJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetRunningActionsCountJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.SLAEventsGetForSeqIdJPAExecutor;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;

public class TestCoordMaterializeTransitionXCommand extends XDataTestCase {
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
        Date startTime = DateUtils.parseDateOozieTZ("2009-03-06T010:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2009-03-11T10:00Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, false, false, 0);
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
        checkCoordAction(job.getId() + "@1");
    }

    public void testActionMaterWithPauseTime1() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2009-03-06T10:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2009-03-06T10:14Z");
        Date pauseTime = DateUtils.parseDateOozieTZ("2009-03-06T10:04Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, pauseTime);
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
        checkCoordActions(job.getId(), 1, null);
    }

    public void testActionMaterWithPauseTime2() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2009-03-06T10:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2009-03-06T10:14Z");
        Date pauseTime = DateUtils.parseDateOozieTZ("2009-03-06T10:08Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, pauseTime);
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
        checkCoordActions(job.getId(), 2, null);
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
        final CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, pauseTime);
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
        waitFor(1000*60, new Predicate() {
            public boolean evaluate() throws Exception {
                return (getStatus(job.getId()) == CoordinatorJob.Status.PAUSED?true:false);
            }
        });
        checkCoordActions(job.getId(), 0, CoordinatorJob.Status.PAUSED);
    }

    public void testTimeout() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2009-03-06T10:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2009-03-06T10:14Z");
        Date pauseTime = null;
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime,
                pauseTime, 300);
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

    protected CoordinatorJobBean addRecordToCoordJobTable(CoordinatorJob.Status status, Date startTime, Date endTime,
            Date pauseTime) throws Exception {
        return addRecordToCoordJobTable(status, startTime, endTime, pauseTime, -1);
    }

    protected CoordinatorJobBean addRecordToCoordJobTable(CoordinatorJob.Status status, Date startTime, Date endTime,
            Date pauseTime, int timeout) throws Exception {
        CoordinatorJobBean coordJob = createCoordJob(status, startTime, endTime, false, false, 0);
        coordJob.setStartTime(startTime);
        coordJob.setEndTime(endTime);
        coordJob.setPauseTime(pauseTime);
        coordJob.setFrequency(5);
        coordJob.setTimeUnit(Timeunit.MINUTE);
        coordJob.setTimeout(timeout);
        coordJob.setConcurrency(3);
        coordJob.setMatThrottling(3);

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
