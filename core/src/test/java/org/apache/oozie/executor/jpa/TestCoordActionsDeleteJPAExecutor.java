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
import java.util.List;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.FaultInjection;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.command.SkipCommitFaultInjection;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
public class TestCoordActionsDeleteJPAExecutor extends XDataTestCase {
    Services services;
    private String[] excludedServices = { "org.apache.oozie.service.StatusTransitService",
            "org.apache.oozie.service.PauseTransitService", "org.apache.oozie.service.PurgeService",
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

    public void testDeleteCoordActions() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        CoordinatorActionBean action1 = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);
        CoordinatorActionBean action2 = addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);
        CoordinatorActionBean action3 = addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        List<String> deleteList = new ArrayList<String>();
        deleteList.add(action1.getId());
        deleteList.add(action2.getId());
        deleteList.add(action3.getId());

        jpaService.execute(new CoordActionsDeleteJPAExecutor(deleteList));

        try {
            jpaService.execute(new CoordActionGetJPAExecutor(action1.getId()));
            fail("CoordinatorAction action1 should have been deleted");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(new CoordActionGetJPAExecutor(action2.getId()));
            fail("CoordinatorAction action1 should have been deleted");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(new CoordActionGetJPAExecutor(action3.getId()));
            fail("CoordinatorAction action1 should have been deleted");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }
    }

    public void testDeleteCoordActionsRollback() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        CoordinatorActionBean action1 = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);
        CoordinatorActionBean action2 = addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);
        CoordinatorActionBean action3 = addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        List<String> deleteList = new ArrayList<String>();
        deleteList.add(action1.getId());
        deleteList.add(action2.getId());
        deleteList.add(action3.getId());

        try {
            // set fault injection to true, so transaction is roll backed
            setSystemProperty(FaultInjection.FAULT_INJECTION, "true");
            setSystemProperty(SkipCommitFaultInjection.ACTION_FAILOVER_FAULT_INJECTION, "true");

            try {
                jpaService.execute(new CoordActionsDeleteJPAExecutor(deleteList));
                fail("Should have skipped commit for failover testing");
            }
            catch (RuntimeException re) {
                assertEquals("Skipping Commit for Failover Testing", re.getMessage());
            }
        }
        finally {
            // Remove fault injection
            FaultInjection.deactivate("org.apache.oozie.command.SkipCommitFaultInjection");
        }

        try {
            jpaService.execute(new CoordActionGetJPAExecutor(action1.getId()));
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action1 should not have been deleted");
        }

        try {
            jpaService.execute(new CoordActionGetJPAExecutor(action2.getId()));
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action2 should not have been deleted");
        }

        try {
            jpaService.execute(new CoordActionGetJPAExecutor(action3.getId()));
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action3 should not have been deleted");
        }
    }
}
