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

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestCoordJobGetReadyActionsJPAExecutor extends XDataTestCase {
    Services services;
    private final String[] excludedServices = { "org.apache.oozie.service.StatusTransitService",
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

    public void testCoordActionGetFIFO() throws Exception {
        _testGetReadyActions("FIFO", false);
    }

    public void testCoordActionGetLIFO() throws Exception {
        _testGetReadyActions("LIFO", true);
    }

    public void testCoordActionGetLAST_ONLY() throws Exception {
        _testGetReadyActions("LAST_ONLY", true);
    }

    public void testCoordActionGetNONE() throws Exception {
        _testGetReadyActions("NONE", true);
    }

    private void _testGetReadyActions(String execution, boolean reverseOrder) throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        Date nomTime = new Date();
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0, nomTime);
        nomTime.setTime(nomTime.getTime() + 1000);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0, nomTime);
        nomTime.setTime(nomTime.getTime() + 1000);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0, nomTime);
        getReadyActions(job.getId(), 0, execution);

        cleanUpDBTables();
        job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        int[] actionIndexes = reverseOrder ? new int[]{1, 0} : new int[]{0, 1};
        nomTime = new Date();
        Date[] createdTimes = new Date[2];
        Date[] nomTimes = new Date[2];
        nomTimes[actionIndexes[0]] = new Date(nomTime.getTime());
        createdTimes[actionIndexes[0]] =
                addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.READY, "coord-action-get.xml", 0, nomTime)
                        .getCreatedTime();
        nomTime.setTime(nomTime.getTime() + 1000);
        nomTimes[actionIndexes[1]] = new Date(nomTime.getTime());
        createdTimes[actionIndexes[1]] =
                addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.READY, "coord-action-get.xml", 0, nomTime)
                        .getCreatedTime();
        nomTime.setTime(nomTime.getTime() + 1000);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0, nomTime);
        List<CoordinatorActionBean> actions = getReadyActions(job.getId(), 2, execution);
        for (int i : actionIndexes) {
            assertEquals(job.getId() + "@" + (actionIndexes[i] + 1), actions.get(i).getId());
            assertEquals(actionIndexes[i] + 1, actions.get(i).getActionNumber());
            assertEquals(job.getId(), actions.get(i).getJobId());
            assertEquals(CoordinatorAction.Status.READY, actions.get(i).getStatus());
            assertEquals(nomTimes[i], actions.get(i).getNominalTime());
            assertEquals(createdTimes[i], actions.get(i).getCreatedTime());
        }
    }

    private List<CoordinatorActionBean> getReadyActions(String jobId, int expected, String execution) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetReadyActionsJPAExecutor actionGetCmd = new CoordJobGetReadyActionsJPAExecutor(jobId, execution);
        List<CoordinatorActionBean> actions = jpaService.execute(actionGetCmd);
        assertEquals(expected, actions.size());
        return actions;
    }

}
