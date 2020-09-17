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

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.executor.jpa.CoordJobGetActionsJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;

import java.util.Date;

@SuppressWarnings("deprecation")
public class TestCoordMaterializeTransitionXCommandWithRunningServices extends XDataTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        new Services().init();
    }

    @Override
    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    /**
     * Job start time is after pause time, should pause the job.
     *
     * @throws Exception
     */
    public void testActionMaterWithPauseTimeAfterStartTime() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2009-03-06T10:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2009-03-06T10:14Z");
        Date pauseTime = DateUtils.parseDateOozieTZ("2009-03-06T09:58Z");
        final CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, pauseTime, "5");
        new CoordMaterializeTransitionXCommand(job.getId(), hoursToSeconds(1)).call();
        waitFor(60_000, new Predicate() {
            public boolean evaluate() throws Exception {
                return (getStatus(job.getId()) == CoordinatorJob.Status.PAUSED);
            }
        });
        checkCoordActions(job.getId(), 0, CoordinatorJob.Status.PAUSED);
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
}
