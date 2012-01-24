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

package org.apache.oozie.coord;

import java.util.List;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestCoordUtils extends  XDataTestCase{
    private Services services;

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

    // test retrieval of single action (action 1)
    public void testGetCoordActionsFromIds() throws Exception {
        int actionNum = 1;
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        String jobId = job.getId();
        CoordinatorActionBean action1 = addRecordToCoordActionTable(jobId, actionNum,
                CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        List<CoordinatorActionBean> coordActions = CoordUtils
                .getCoordActionsFromIds(jobId, Integer.toString(actionNum));
        //check for the expected size of actions list
        assertEquals(1, coordActions.size());
        //check for the expected action
        assertEquals(action1, coordActions.get(0));
    }

    // test retrieval of range of actions (action 1-2)
    public void testGetCoordActionsFromIdsRange() throws Exception {
        int actionNum1 = 1;
        int actionNum2 = 2;
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        String jobId = job.getId();
        addRecordToCoordActionTable(jobId, actionNum1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(jobId, actionNum2, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);

        String rerunScope = Integer.toString(actionNum1) + "-" + Integer.toString(actionNum2);
        List<CoordinatorActionBean> coordActions = CoordUtils.getCoordActionsFromIds(jobId, rerunScope);

        assertEquals(2, coordActions.size());
    }

    // test retrieval of action corresponding to single date (date1)
    public void testGetCoordActionsFromDate() throws Exception{
        int actionNum = 1;
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        String jobId = job.getId();
        CoordinatorActionBean action1 = addRecordToCoordActionTable(jobId, actionNum,
                CoordinatorAction.Status.SUCCEEDED, "coord-rerun-action1.xml", 0);
        List<CoordinatorActionBean> coordActions = CoordUtils.getCoordActionsFromDates(jobId, "2009-12-15T01:00Z");

        assertEquals(1, coordActions.size());
        assertEquals(action1, coordActions.get(0));
    }

    //test retrieval of action corresponding to range of dates (date1::date2);
    public void testGetCoordActionsFromDateRange() throws Exception{
        int actionNum1 = 1;
        int actionNum2 = 2;
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        String jobId = job.getId();
        addRecordToCoordActionTable(jobId, actionNum1, CoordinatorAction.Status.SUCCEEDED, "coord-rerun-action1.xml", 0);
        addRecordToCoordActionTable(jobId, actionNum2, CoordinatorAction.Status.SUCCEEDED, "coord-rerun-action2.xml", 0);
        String rerunScope = "2009-12-15T01:00Z" + "::" + "2009-12-16T01:00Z";
        List<CoordinatorActionBean> coordActions = CoordUtils.getCoordActionsFromDates(jobId, rerunScope);

        assertEquals(2, coordActions.size());
    }
}
