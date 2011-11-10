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

public class TestCoordJobGetActionsStatusByPendingFalseJPAExecutor extends XDataTestCase {
    Services services;

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

    /*
     * Add a Coordinator action with pending false and check for expected column values
     */
    public void testCoordActionsStatusByPendingFalseForColumnValues() throws Exception {
        int actionNum = 1;
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        String jobId = job.getId();
        CoordinatorActionBean action = addRecordToCoordActionTable(jobId, actionNum++,
                CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0);

        _testCoordActionForCorrectColumnValues(jobId, action.getStatus());

    }

    /*
     * Add 3 Coordinator actions with pending false and 1 Coordinator action with pending true.
     * Then check for expected number of actions retrieved
     */
    public void testCoordActionsStatusByPendingFalseForSize() throws Exception{
        int actionNum = 1;
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        String jobId = job.getId();
        addRecordToCoordActionTable(jobId, actionNum++, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(jobId, actionNum++, CoordinatorAction.Status.FAILED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(jobId, actionNum++, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(jobId, actionNum++, CoordinatorAction.Status.KILLED, "coord-action-get.xml", 1);

        _testCoordActionsPendingSize(jobId, 3);
    }

    // test sql projection operation
    private void _testCoordActionForCorrectColumnValues(String jobId, CoordinatorAction.Status status) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        // Call JPAExecutor to get actions which are pending
        CoordJobGetActionsStatusByPendingFalseJPAExecutor actionGetCmd = new CoordJobGetActionsStatusByPendingFalseJPAExecutor(
                jobId);
        List<CoordinatorAction.Status> actionList = jpaService.execute(actionGetCmd);
        CoordinatorAction.Status cAStatus = actionList.get(0);

        assertEquals(cAStatus, status);

    }

    // test sql selection operation
    private void _testCoordActionsPendingSize(String jobId, int expectedSize) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        // Call JPAExecutor to get actions which are pending
        CoordJobGetActionsStatusByPendingFalseJPAExecutor actionGetCmd = new CoordJobGetActionsStatusByPendingFalseJPAExecutor(jobId);
        List<CoordinatorAction.Status> actionList = jpaService.execute(actionGetCmd);
        // As 3 actions are not pending, expected result set is of size 3
        assertEquals(actionList.size(), expectedSize);

    }

}
