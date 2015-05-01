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
import org.apache.oozie.CoordinatorActionInfo;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;

public class TestCoordActionsIgnoreXCommand extends XDataTestCase {
    private Services services;
    List<CoordinatorJobBean> coordJobs;
    List<CoordinatorActionBean> coordActions;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
    }

    @Override
    protected void tearDown() throws Exception {
        LocalOozie.stop();
        services.destroy();
        super.tearDown();
    }

    public void testCoordActionsIgnore() throws Exception {
        createDBRecords();

        // positive test of single action - oozie job -ingore job1 -action 1
        CoordinatorActionInfo retInfo = new CoordActionsIgnoreXCommand(coordJobs.get(0).getId(), "action", "1").call();
        CoordinatorActionBean actionBean1 = CoordActionQueryExecutor.getInstance().get(
                CoordActionQuery.GET_COORD_ACTION, coordActions.get(0).getId());
        assertEquals(CoordinatorAction.Status.IGNORED, actionBean1.getStatus());
        assertEquals(1, retInfo.getCoordActions().size());
        assertEquals(actionBean1.getId(), retInfo.getCoordActions().get(0).getId());

        // positive test of action range - oozie job -ignore job1 -action 2-3
        retInfo = new CoordActionsIgnoreXCommand(coordJobs.get(0).getId(), "action", "2-3").call();
        CoordinatorActionBean actionBean2 = CoordActionQueryExecutor.getInstance().get(
                CoordActionQuery.GET_COORD_ACTION, coordActions.get(1).getId());
        CoordinatorActionBean actionBean3 = CoordActionQueryExecutor.getInstance().get(
                CoordActionQuery.GET_COORD_ACTION, coordActions.get(2).getId());
        assertEquals(CoordinatorAction.Status.IGNORED, actionBean2.getStatus());
        assertEquals(CoordinatorAction.Status.IGNORED, actionBean3.getStatus());
        assertEquals(2, retInfo.getCoordActions().size());
        String retId1 = retInfo.getCoordActions().get(0).getId();
        String retId2 = retInfo.getCoordActions().get(1).getId();
        assertTrue(actionBean2.getId().equals(retId1) || actionBean2.getId().equals(retId2));
        assertTrue(actionBean3.getId().equals(retId1) || actionBean3.getId().equals(retId2));

        // negative test when ignoring a coord action in RUNNING (@5 is running)
        try {
            new CoordActionsIgnoreXCommand(coordJobs.get(0).getId(), "action", "4-5").call();
        }
        catch (CommandException ex) {
            assertEquals(ex.getErrorCode(), ErrorCode.E1024);
            assertTrue(ex.getMessage().indexOf(
                    "part or all actions are not eligible to ignore, check state of action number(s) [5]") > -1);
        }

        // negative test when ignore command on coordinator job in PREP
        try {
            new CoordActionsIgnoreXCommand(coordJobs.get(1).getId(), "action", "1").call();
        }
        catch (CommandException ex) {
            assertEquals(ex.getErrorCode(), ErrorCode.E1024);
            assertTrue(ex.getMessage().indexOf("No actions are materialized to ignore") > -1);
        }
    }

    private void createDBRecords() throws Exception {
        JPAService jpaService = services.get(JPAService.class);
        coordJobs = new ArrayList<CoordinatorJobBean>();
        coordActions = new ArrayList<CoordinatorActionBean>();

        Date startTime = DateUtils.parseDateOozieTZ("2013-08-01T23:59Z");
        Date endTime = DateUtils.parseDateOozieTZ("2013-08-02T23:59Z");
        CoordinatorJobBean job1 = addRecordToCoordJobTable(CoordinatorJob.Status.KILLED, startTime, endTime, false,
                true, 0);
        CoordinatorJobBean job2 = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, startTime, endTime, false, true,
                0);
        coordJobs.add(job1);
        coordJobs.add(job2);

        CoordinatorActionBean action1_1 = addRecordToCoordActionTable(job1.getId(), 1, CoordinatorAction.Status.FAILED,
                "coord-action-get.xml", 0);
        CoordinatorActionBean action1_2 = addRecordToCoordActionTable(job1.getId(), 2,
                CoordinatorAction.Status.FAILED, "coord-action-get.xml", 0);
        CoordinatorActionBean action1_3 = addRecordToCoordActionTable(job1.getId(), 3, CoordinatorAction.Status.KILLED,
                "coord-action-get.xml", 0);
        CoordinatorActionBean action1_4 = addRecordToCoordActionTable(job1.getId(), 4, CoordinatorAction.Status.KILLED,
                "coord-action-get.xml", 0);
        CoordinatorActionBean action1_5 = addRecordToCoordActionTable(job1.getId(), 5,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);
        coordActions.add(action1_1);
        coordActions.add(action1_2);
        coordActions.add(action1_3);
        coordActions.add(action1_4);
        coordActions.add(action1_5);

        action1_1.setNominalTime(DateUtils.parseDateOozieTZ("2009-12-15T02:00Z"));
        action1_1.setExternalId(null);
        CoordActionQueryExecutor.getInstance().executeUpdate(CoordActionQuery.UPDATE_COORD_ACTION, action1_1);
    }
}
