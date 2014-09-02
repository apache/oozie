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

import java.util.List;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestCoordJobGetActionsNotCompletedJPAExecutor extends XDataTestCase {
    Services services;

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

    /*
     * Add a Coordinator action with status WAITING and check for expected column values
     */
    public void testCoordActionsNotCompletetedForColumnValues() throws Exception {
        int actionNum = 1;
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        String jobId = job.getId();
        CoordinatorActionBean action = addRecordToCoordActionTable(jobId, actionNum++,
                CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0);

        _testCoordActionForCorrectColumnValues(jobId, action.getId(), action.getStatus(), action.getPending());

    }

    /*
     * Add 2 Coordinator actions which are not completed (status as RUNNING and WAITING) and add 2 Coordinator actions
     * which are completed (status as FAILED and KILLED). Then check for expected number of actions retrieved.
     */
    public void testCoordActionsNotCompletetedForSize() throws Exception {
        int actionNum = 1;
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        String jobId = job.getId();
        addRecordToCoordActionTable(jobId, actionNum++, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(jobId, actionNum++, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(jobId, actionNum++, CoordinatorAction.Status.FAILED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(jobId, actionNum++, CoordinatorAction.Status.KILLED, "coord-action-get.xml", 0);

        _testCoordActionsNotCompletedSize(jobId, 2);
    }


    // test sql projection operator
    private void _testCoordActionForCorrectColumnValues(String jobId, String actionId, CoordinatorAction.Status status,
            int pending) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        // Call JPAExecutor to get actions which are not completed
        CoordJobGetActionsNotCompletedJPAExecutor actionGetCmd = new CoordJobGetActionsNotCompletedJPAExecutor(jobId);
        List<CoordinatorActionBean> actionList = jpaService.execute(actionGetCmd);
        // check for expected column values
        CoordinatorActionBean action = actionList.get(0);
        assertEquals(action.getId(), actionId);
        assertEquals(action.getStatus(), status);
        assertEquals(action.getPending(), pending);
        assertEquals(jobId, action.getJobId());
    }

    // test sql selection operator
    private void _testCoordActionsNotCompletedSize(String jobId, int expectedSize) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        // Call JPAExecutor to get actions which are not completed
        CoordJobGetActionsNotCompletedJPAExecutor actionGetCmd = new CoordJobGetActionsNotCompletedJPAExecutor(jobId);
        List<CoordinatorActionBean> actionList = jpaService.execute(actionGetCmd);
        // As two actions are not completed, expected result set is of size 2
        assertEquals(actionList.size(), expectedSize);

    }

}
