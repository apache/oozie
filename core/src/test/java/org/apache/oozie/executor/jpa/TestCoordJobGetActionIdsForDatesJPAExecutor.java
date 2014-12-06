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
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;

public class TestCoordJobGetActionIdsForDatesJPAExecutor extends XDataTestCase {
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

    public void testCoordActionGet() throws Exception {
        int actionNum = 1;
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        addRecordToCoordActionTable(job.getId(), actionNum, CoordinatorAction.Status.FAILED, "coord-action-get.xml", 0);

        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String actionXml = getCoordActionXml(appPath, "coord-action-get.xml");
        String actionNomialTime = getActionNominalTime(actionXml);
        Date nominalTime = DateUtils.parseDateOozieTZ(actionNomialTime);

        Date d1 = new Date(nominalTime.getTime() - 1000);
        Date d2 = new Date(nominalTime.getTime() + 1000);
        _testGetActionForDates(job.getId(), d1, d2, 1);

        d1 = new Date(nominalTime.getTime() + 1000);
        d2 = new Date(nominalTime.getTime() + 2000);
        _testGetActionForDates(job.getId(), d1, d2, 0);

        cleanUpDBTables();
        job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        addRecordToCoordActionTable(job.getId(), actionNum, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0);
        _testGetActionForDates(job.getId(), d1, d2, 0);
    }

    private void _testGetActionForDates(String jobId, Date d1, Date d2, int expected) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetActionIdsForDateRangeJPAExecutor actionGetCmd = new CoordJobGetActionIdsForDateRangeJPAExecutor(jobId, d1, d2);
        List<String> actionIds = jpaService.execute(actionGetCmd);
        assertEquals(expected, actionIds.size());
    }

}
