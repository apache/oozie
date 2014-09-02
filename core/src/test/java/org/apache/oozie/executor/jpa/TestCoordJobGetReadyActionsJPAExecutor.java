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
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        Date nomTime = new Date();
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0, nomTime);
        nomTime.setTime(nomTime.getTime() + 1000);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0, nomTime);
        nomTime.setTime(nomTime.getTime() + 1000);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0, nomTime);
        _testGetReadyActions(job.getId(), 0, "FIFO");

        cleanUpDBTables();
        job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        nomTime = new Date();
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.READY, "coord-action-get.xml", 0, nomTime);
        nomTime.setTime(nomTime.getTime() + 1000);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.READY, "coord-action-get.xml", 0, nomTime);
        nomTime.setTime(nomTime.getTime() + 1000);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0, nomTime);
        List<CoordinatorActionBean> actions = _testGetReadyActions(job.getId(), 2, "FIFO");
        assertEquals((job.getId() + "@1"), actions.get(0).getId());
        assertEquals((job.getId() + "@2"), actions.get(1).getId());
    }

    public void testCoordActionGetLIFO() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        Date nomTime = new Date();
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0, nomTime);
        nomTime.setTime(nomTime.getTime() + 1000);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0, nomTime);
        nomTime.setTime(nomTime.getTime() + 1000);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0, nomTime);
        _testGetReadyActions(job.getId(), 0, "LIFO");

        cleanUpDBTables();
        job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        nomTime = new Date();
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.READY, "coord-action-get.xml", 0, nomTime);
        nomTime.setTime(nomTime.getTime() + 1000);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.READY, "coord-action-get.xml", 0, nomTime);
        nomTime.setTime(nomTime.getTime() + 1000);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0, nomTime);
        List<CoordinatorActionBean> actions = _testGetReadyActions(job.getId(), 2, "LIFO");
        assertEquals((job.getId() + "@2"), actions.get(0).getId());
        assertEquals((job.getId() + "@1"), actions.get(1).getId());
    }

    public void testCoordActionGetLAST_ONLY() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        Date nomTime = new Date();
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0, nomTime);
        nomTime.setTime(nomTime.getTime() + 1000);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0, nomTime);
        nomTime.setTime(nomTime.getTime() + 1000);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0, nomTime);
        _testGetReadyActions(job.getId(), 0, "LAST_ONLY");

        cleanUpDBTables();
        job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        nomTime = new Date();
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.READY, "coord-action-get.xml", 0, nomTime);
        nomTime.setTime(nomTime.getTime() + 1000);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.READY, "coord-action-get.xml", 0, nomTime);
        nomTime.setTime(nomTime.getTime() + 1000);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0, nomTime);
        List<CoordinatorActionBean> actions = _testGetReadyActions(job.getId(), 2, "LAST_ONLY");
        assertEquals((job.getId() + "@2"), actions.get(0).getId());
        assertEquals((job.getId() + "@1"), actions.get(1).getId());
    }

    private List<CoordinatorActionBean> _testGetReadyActions(String jobId, int expected, String execution) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetReadyActionsJPAExecutor actionGetCmd = new CoordJobGetReadyActionsJPAExecutor(jobId, 10, execution);
        List<CoordinatorActionBean> actions = jpaService.execute(actionGetCmd);
        assertEquals(expected, actions.size());
        return actions;
    }

}
