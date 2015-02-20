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

import java.util.Date;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;

public class TestCoordActionQueryExecutor extends XDataTestCase {

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

    public void testGetTerminatedActionForDates() throws Exception {
        int actionNum = 1;
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        addRecordToCoordActionTable(job.getId(), actionNum, CoordinatorAction.Status.FAILED, "coord-action-get.xml", 0);

        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String actionXml = getCoordActionXml(appPath, "coord-action-get.xml");
        String actionNomialTime = getActionNominalTime(actionXml);
        Date nominalTime = DateUtils.parseDateOozieTZ(actionNomialTime);

        Date d1 = new Date(nominalTime.getTime() - 1000);
        Date d2 = new Date(nominalTime.getTime() + 1000);
        _testGetTerminatedActionForDates(job.getId(), d1, d2, 1);

        d1 = new Date(nominalTime.getTime() + 1000);
        d2 = new Date(nominalTime.getTime() + 2000);
        _testGetTerminatedActionForDates(job.getId(), d1, d2, 0);

        cleanUpDBTables();
        job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        addRecordToCoordActionTable(job.getId(), actionNum, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0);
        _testGetTerminatedActionForDates(job.getId(), d1, d2, 0);
    }

    private void _testGetTerminatedActionForDates(String jobId, Date d1, Date d2, int expected) throws Exception {
        List<CoordinatorActionBean> actionIds = CoordActionQueryExecutor.getInstance().getList(
                CoordActionQuery.GET_TERMINATED_ACTIONS_FOR_DATES, jobId, d1, d2);
        assertEquals(expected, actionIds.size());
    }

    public void testGetTerminatedActionIdsForDates() throws Exception {
        int actionNum = 1;
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        addRecordToCoordActionTable(job.getId(), actionNum, CoordinatorAction.Status.FAILED, "coord-action-get.xml", 0);

        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String actionXml = getCoordActionXml(appPath, "coord-action-get.xml");
        String actionNomialTime = getActionNominalTime(actionXml);
        Date nominalTime = DateUtils.parseDateOozieTZ(actionNomialTime);

        Date d1 = new Date(nominalTime.getTime() - 1000);
        Date d2 = new Date(nominalTime.getTime() + 1000);
        _testGetTerminatedActionIdsForDates(job.getId(), d1, d2, 1);

        d1 = new Date(nominalTime.getTime() + 1000);
        d2 = new Date(nominalTime.getTime() + 2000);
        _testGetTerminatedActionIdsForDates(job.getId(), d1, d2, 0);

        cleanUpDBTables();
        job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        addRecordToCoordActionTable(job.getId(), actionNum, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0);
        _testGetTerminatedActionIdsForDates(job.getId(), d1, d2, 0);
    }

    private void _testGetTerminatedActionIdsForDates(String jobId, Date d1, Date d2, int expected) throws Exception {
        List<CoordinatorActionBean> actions = CoordActionQueryExecutor.getInstance().getList(
                CoordActionQuery.GET_TERMINATED_ACTION_IDS_FOR_DATES, jobId, d1, d2);
        assertEquals(expected, actions.size());
    }

}
