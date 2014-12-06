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

import java.util.ArrayList;
import java.util.List;

public class TestCoordJobsDeleteJPAExecutor extends XDataTestCase {
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

    public void testDeleteCoords() throws Exception {
        CoordinatorJobBean jobA = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        CoordinatorActionBean actionA1 = addRecordToCoordActionTable(jobA.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);
        CoordinatorActionBean actionA2 = addRecordToCoordActionTable(jobA.getId(), 2, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);

        CoordinatorJobBean jobB = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        CoordinatorActionBean actionB1 = addRecordToCoordActionTable(jobB.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);
        CoordinatorActionBean actionB2 = addRecordToCoordActionTable(jobB.getId(), 2, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);

        CoordinatorJobBean jobC = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        CoordinatorActionBean actionC1 = addRecordToCoordActionTable(jobC.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);
        CoordinatorActionBean actionC2 = addRecordToCoordActionTable(jobC.getId(), 2, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        List<String> deleteCoordlist = new ArrayList<String>();
        deleteCoordlist.add(jobA.getId());
        deleteCoordlist.add(jobB.getId());
        deleteCoordlist.add(jobC.getId());
        jpaService.execute(new CoordJobsDeleteJPAExecutor(deleteCoordlist));

        List<String> deleteActionList = new ArrayList<String>();
        deleteActionList.add(actionA1.getId());
        deleteActionList.add(actionB1.getId());
        deleteActionList.add(actionC1.getId());
        deleteActionList.add(actionA2.getId());
        deleteActionList.add(actionB2.getId());
        deleteActionList.add(actionC2.getId());
        jpaService.execute(new CoordActionsDeleteJPAExecutor(deleteActionList));

        try {
            jpaService.execute(new CoordJobGetJPAExecutor(jobA.getId()));
            fail("Coordinator Job A should have been deleted");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(new CoordActionGetJPAExecutor(actionA1.getId()));
            fail("Coordinator Action A1 should have been deleted");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(new CoordActionGetJPAExecutor(actionA2.getId()));
            fail("Coordinator Action A2 should have been deleted");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(new CoordJobGetJPAExecutor(jobB.getId()));
            fail("Coordinator Job B should have been deleted");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(new CoordActionGetJPAExecutor(actionB1.getId()));
            fail("Coordinator Action B1 should have been deleted");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(new CoordActionGetJPAExecutor(actionB2.getId()));
            fail("Coordinator Action B2 should have been deleted");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(new CoordJobGetJPAExecutor(jobC.getId()));
            fail("Coordinator Job C should have been deleted");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(new CoordActionGetJPAExecutor(actionC1.getId()));
            fail("Coordinator Action C1 should have been deleted");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(new CoordActionGetJPAExecutor(actionC2.getId()));
            fail("Coordinator Action C2 should have been deleted");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }
    }

    public void testDeleteCoordsRollback() throws Exception{
        CoordinatorJobBean jobA = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        CoordinatorActionBean actionA1 = addRecordToCoordActionTable(jobA.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);
        CoordinatorActionBean actionA2 = addRecordToCoordActionTable(jobA.getId(), 2, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);

        CoordinatorJobBean jobB = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        CoordinatorActionBean actionB1 = addRecordToCoordActionTable(jobB.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);
        CoordinatorActionBean actionB2 = addRecordToCoordActionTable(jobB.getId(), 2, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);

        CoordinatorJobBean jobC = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        CoordinatorActionBean actionC1 = addRecordToCoordActionTable(jobC.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);
        CoordinatorActionBean actionC2 = addRecordToCoordActionTable(jobC.getId(), 2, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        try {
            // set fault injection to true, so transaction is roll backed
            setSystemProperty(FaultInjection.FAULT_INJECTION, "true");
            setSystemProperty(SkipCommitFaultInjection.ACTION_FAILOVER_FAULT_INJECTION, "true");

            List<String> deleteCoordlist = new ArrayList<String>();
            deleteCoordlist.add(jobA.getId());
            deleteCoordlist.add(jobB.getId());
            deleteCoordlist.add(jobC.getId());

            List<String> deleteActionList = new ArrayList<String>();
            deleteActionList.add(actionA1.getId());
            deleteActionList.add(actionB1.getId());
            deleteActionList.add(actionC1.getId());
            deleteActionList.add(actionA2.getId());
            deleteActionList.add(actionB2.getId());
            deleteActionList.add(actionC2.getId());

            try {
                jpaService.execute(new CoordJobsDeleteJPAExecutor(deleteCoordlist));
                jpaService.execute(new CoordActionsDeleteJPAExecutor(deleteActionList));
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
            jpaService.execute(new CoordJobGetJPAExecutor(jobA.getId()));
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Job A should not have been deleted");
        }

        try {
            jpaService.execute(new CoordActionGetJPAExecutor(actionA1.getId()));
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action A1 should not have been deleted");
        }

        try {
            jpaService.execute(new CoordActionGetJPAExecutor(actionA2.getId()));
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action A2 should not have been deleted");
        }

        try {
            jpaService.execute(new CoordJobGetJPAExecutor(jobB.getId()));
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Job B should not have been deleted");
        }

        try {
            jpaService.execute(new CoordActionGetJPAExecutor(actionB1.getId()));
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action B1 should not have been deleted");
        }

        try {
            jpaService.execute(new CoordActionGetJPAExecutor(actionB2.getId()));
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action B2 should not have been deleted");
        }

        try {
            jpaService.execute(new CoordJobGetJPAExecutor(jobC.getId()));
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Job C should not have been deleted");
        }

        try {
            jpaService.execute(new CoordActionGetJPAExecutor(actionC1.getId()));
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action C1 should not have been deleted");
        }

        try {
            jpaService.execute(new CoordActionGetJPAExecutor(actionC2.getId()));
        }
        catch (JPAExecutorException je) {
            fail("Coordinator Action C2 should not have been deleted");
        }
    }
}
