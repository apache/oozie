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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestCoordJobGetActionsSubsetJPAExecutor extends XDataTestCase {
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

    public void testCoordActionGet() throws Exception {
        int actionNum = 1;
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), actionNum, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0);

        _testGetActionsSubset(job.getId(), action.getId(), 1, 1);
    }

    private void _testGetActionsSubset(String jobId, String actionId, int start, int len) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetActionsSubsetJPAExecutor actionGetCmd = new CoordJobGetActionsSubsetJPAExecutor(jobId, Collections.<String>emptyList(), start, len);
        List<CoordinatorActionBean> actions = jpaService.execute(actionGetCmd);
        assertEquals(actions.size(), 1);
        assertEquals(actions.get(0).getId(), actionId);
    }

    // Check the ordering of actions by nominal time
    public void testCoordActionOrderBy() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        // Add Coordinator action with nominal time: 2009-12-15T01:00Z
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0);
        // Add Coordinator action with nominal time: 2009-02-01T23:59Z
        CoordinatorActionBean action1 = addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.WAITING, "coord-action-for-action-input-check.xml", 0);
        // test for the expected action number
        _testGetActionsSubsetOrderBy(job.getId(), 2, 1, 2);
    }

   private void _testGetActionsSubsetOrderBy(String jobId, int actionNum, int start, int len) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetActionsSubsetJPAExecutor actionGetCmd = new CoordJobGetActionsSubsetJPAExecutor(jobId, Collections.<String>emptyList(), start, len);
        List<CoordinatorActionBean> actions = jpaService.execute(actionGetCmd);
        assertEquals(actions.size(), 2);
        // As actions are sorted by nominal time, the first action should be with action number 2
        assertEquals(actions.get(0).getActionNumber(), actionNum);
    }

    // Check status filters for Coordinator actions
    public void testCoordActionFilter() throws Exception{
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        // Add Coordinator action with nominal time: 2009-12-15T01:00Z
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.RUNNING,
                "coord-action-get.xml", 0);
        // Add Coordinator action with nominal time: 2009-02-01T23:59Z
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.WAITING,
                "coord-action-get.xml", 0);
        // Create lists for status filter
        List<String> filterList = new ArrayList<String>();
        filterList.add("RUNNING");
        filterList.add("KILLED");
        _testGetActionsSubsetFilter(job.getId(), 1, filterList, 1, 2);
    }

    // Check whether actions are retrieved based on the filter values for status
    private void _testGetActionsSubsetFilter(String jobId, int actionNum, List<String> filterList, int start, int len)
            throws JPAExecutorException {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetActionsSubsetJPAExecutor actionGetCmd = new CoordJobGetActionsSubsetJPAExecutor(jobId, filterList,
                start, len);
        List<CoordinatorActionBean> actions = jpaService.execute(actionGetCmd);
        // As actions are filtered by RUNNING status, only 1 action should be returned
        assertEquals(actions.size(), 1);
        assertEquals(actions.get(0).getActionNumber(), 1);
    }

}
