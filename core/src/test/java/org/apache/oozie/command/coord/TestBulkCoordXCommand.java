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

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.command.OperationType;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestBulkCoordXCommand extends XDataTestCase {
    private Services services;
    private String[] excludedServices = { "org.apache.oozie.service.StatusTransitService",
            "org.apache.oozie.service.PauseTransitService",
            "org.apache.oozie.service.CoordMaterializeTriggerService", "org.apache.oozie.service.RecoveryService" };

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        setClassesToBeExcluded(services.getConf(), excludedServices);
        services.init();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testBulkCoordSuspendResumeKillSuccess() throws Exception {
        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);

        CoordinatorJobBean job1 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, false, false, 0);
        CoordinatorActionBean action1 = addRecordToCoordActionTable(job1.getId(), 1,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);

        CoordinatorJobBean job2 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, false, false, 0);
        CoordinatorActionBean action2 = addRecordToCoordActionTable(job2.getId(), 1,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);

        List<String> jobIds = new ArrayList<String>();
        jobIds.add(job1.getId());
        jobIds.add(job2.getId());

        List<String> actionIds = new ArrayList<String>();
        actionIds.add(action1.getId());
        actionIds.add(action2.getId());

        Map<String, List<String>> map = new HashMap<String, List<String>>();
        List<String> names = new ArrayList<String>();
        names.add("COORD-TEST");
        map.put("name", names);

        new BulkCoordXCommand(map, 1, 50, OperationType.Suspend).call();
        verifyJobsStatus(jobIds, CoordinatorJob.Status.SUSPENDED);
        verifyActionsStatus(actionIds, CoordinatorAction.Status.SUSPENDED);

        new BulkCoordXCommand(map, 1, 50, OperationType.Resume).call();
        verifyJobsStatus(jobIds, CoordinatorJob.Status.RUNNING);
        verifyActionsStatus(actionIds, CoordinatorAction.Status.RUNNING);

        new BulkCoordXCommand(map, 1, 50, OperationType.Kill).call();
        verifyJobsStatus(jobIds, CoordinatorJob.Status.KILLED);
        verifyActionsStatus(actionIds, CoordinatorAction.Status.KILLED);
    }

    public void testBulkCoordKillNegative() throws Exception {
        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);

        CoordinatorJobBean job1 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, false, false, 0);
        CoordinatorActionBean action1 = addRecordToCoordActionTable(job1.getId(), 1,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);

        Map<String, List<String>> map = new HashMap<String, List<String>>();
        List<String> names = new ArrayList<String>();
        names.add("COORD");
        map.put("name", names);

        new BulkCoordXCommand(map, 1, 50, OperationType.Kill).call();

        List<String> jobIds = new ArrayList<String>();
        jobIds.add(job1.getId());
        List<String> actionIds = new ArrayList<String>();
        actionIds.add(action1.getId());

        verifyJobsStatus(jobIds, CoordinatorJob.Status.RUNNING);
        verifyActionsStatus(actionIds, CoordinatorAction.Status.RUNNING);
    }

    public void testBulkCoordKillNoOp() throws Exception {
        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);

        CoordinatorJobBean job1 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, start, end, false, false, 0);
        CoordinatorActionBean action1 = addRecordToCoordActionTable(job1.getId(), 1,
                CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);

        Map<String, List<String>> map = new HashMap<String, List<String>>();
        List<String> names = new ArrayList<String>();
        names.add("COORD-TEST");
        map.put("name", names);

        new BulkCoordXCommand(map, 1, 50, OperationType.Kill).call();

        List<String> jobIds = new ArrayList<String>();
        jobIds.add(job1.getId());
        List<String> actionIds = new ArrayList<String>();
        actionIds.add(action1.getId());

        verifyJobsStatus(jobIds, CoordinatorJob.Status.SUCCEEDED);
        verifyActionsStatus(actionIds, CoordinatorAction.Status.SUCCEEDED);
    }

    public void testBulkCoordSuspendNoOp() throws Exception {
        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);

        CoordinatorJobBean job1 = addRecordToCoordJobTable(CoordinatorJob.Status.KILLED, start, end, false, false, 0);
        CoordinatorActionBean action1 = addRecordToCoordActionTable(job1.getId(), 1,
                CoordinatorAction.Status.KILLED, "coord-action-get.xml", 0);

        Map<String, List<String>> map = new HashMap<String, List<String>>();
        List<String> names = new ArrayList<String>();
        names.add("COORD-TEST");
        map.put("name", names);

        new BulkCoordXCommand(map, 1, 50, OperationType.Suspend).call();

        List<String> jobIds = new ArrayList<String>();
        jobIds.add(job1.getId());
        List<String> actionIds = new ArrayList<String>();
        actionIds.add(action1.getId());
        verifyJobsStatus(jobIds, CoordinatorJob.Status.KILLED);
        verifyActionsStatus(actionIds, CoordinatorAction.Status.KILLED);
    }

    public void testBulkCoordSuspendNegative() throws Exception {
        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);

        CoordinatorJobBean job1 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, false, false, 0);
        CoordinatorActionBean action1 = addRecordToCoordActionTable(job1.getId(), 1,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);

        Map<String, List<String>> map = new HashMap<String, List<String>>();
        List<String> names = new ArrayList<String>();
        names.add("COORD");
        map.put("name", names);

        new BulkCoordXCommand(map, 1, 50, OperationType.Suspend).call();

        List<String> jobIds = new ArrayList<String>();
        jobIds.add(job1.getId());
        List<String> actionIds = new ArrayList<String>();
        actionIds.add(action1.getId());
        verifyJobsStatus(jobIds, CoordinatorJob.Status.RUNNING);
        verifyActionsStatus(actionIds, CoordinatorAction.Status.RUNNING);
    }

    public void testBulkCoordResumeNoOp() throws Exception {
        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);

        CoordinatorJobBean job1 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, false, false, 0);
        CoordinatorActionBean action1 = addRecordToCoordActionTable(job1.getId(), 1,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);

        Map<String, List<String>> map = new HashMap<String, List<String>>();
        List<String> names = new ArrayList<String>();
        names.add("COORD-TEST");
        map.put("name", names);

        new BulkCoordXCommand(map, 1, 50, OperationType.Resume).call();
        List<String> jobIds = new ArrayList<String>();
        jobIds.add(job1.getId());
        List<String> actionIds = new ArrayList<String>();
        actionIds.add(action1.getId());

        verifyJobsStatus(jobIds, CoordinatorJob.Status.RUNNING);
        verifyActionsStatus(actionIds, CoordinatorAction.Status.RUNNING);
    }

    public void testBulkCoordResumeNegative() throws Exception {
        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);

        CoordinatorJobBean job1 = addRecordToCoordJobTable(CoordinatorJob.Status.SUSPENDED, start, end, false, false, 0);
        CoordinatorActionBean action1 = addRecordToCoordActionTable(job1.getId(), 1,
                CoordinatorAction.Status.SUSPENDED, "coord-action-get.xml", 0);

        Map<String, List<String>> map = new HashMap<String, List<String>>();
        List<String> names = new ArrayList<String>();
        names.add("COORD");
        map.put("name", names);

        new BulkCoordXCommand(map, 1, 50, OperationType.Resume).call();
        List<String> jobIds = new ArrayList<String>();
        jobIds.add(job1.getId());
        List<String> actionIds = new ArrayList<String>();
        actionIds.add(action1.getId());

        verifyJobsStatus(jobIds, CoordinatorJob.Status.SUSPENDED);
        verifyActionsStatus(actionIds, CoordinatorAction.Status.SUSPENDED);
    }

    private void verifyJobsStatus(List<String> jobIds, CoordinatorJob.Status status) throws Exception {
        for (String id : jobIds) {
            CoordinatorJobBean job = CoordJobQueryExecutor.getInstance().get(
                    CoordJobQueryExecutor.CoordJobQuery.GET_COORD_JOB, id);
            assertEquals(status, job.getStatus());
        }
    }

    private void verifyActionsStatus(List<String> actionIds, CoordinatorAction.Status status) throws Exception {
        for (String id : actionIds) {
            CoordinatorActionBean action = CoordActionQueryExecutor.getInstance().get(
                    CoordActionQueryExecutor.CoordActionQuery.GET_COORD_ACTION, id);
            assertEquals(status, action.getStatus());
        }
    }
}
