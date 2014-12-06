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
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.FaultInjection;
import org.apache.oozie.client.Job;
import org.apache.oozie.command.SkipCommitFaultInjection;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestBundleJobsDeleteJPAExecutor extends XDataTestCase {
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

    public void testDeleteBundles() throws Exception {
        BundleJobBean jobA = this.addRecordToBundleJobTable(Job.Status.SUCCEEDED, false);
        BundleActionBean actionA1 = addRecordToBundleActionTable(jobA.getId(), "actionA1", 0, Job.Status.SUCCEEDED);
        BundleActionBean actionA2 = addRecordToBundleActionTable(jobA.getId(), "actionA2", 0, Job.Status.SUCCEEDED);

        BundleJobBean jobB = this.addRecordToBundleJobTable(Job.Status.SUCCEEDED, false);
        BundleActionBean actionB1 = addRecordToBundleActionTable(jobB.getId(), "actionB1", 0, Job.Status.SUCCEEDED);
        BundleActionBean actionB2 = addRecordToBundleActionTable(jobB.getId(), "actionB2", 0, Job.Status.SUCCEEDED);

        BundleJobBean jobC = this.addRecordToBundleJobTable(Job.Status.SUCCEEDED, false);
        BundleActionBean actionC1 = addRecordToBundleActionTable(jobC.getId(), "actionC1", 0, Job.Status.SUCCEEDED);
        BundleActionBean actionC2 = addRecordToBundleActionTable(jobC.getId(), "actionC2", 0, Job.Status.SUCCEEDED);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        List<String> deleteList = new ArrayList<String>();
        deleteList.add(jobA.getId());
        deleteList.add(jobB.getId());
        deleteList.add(jobC.getId());
        jpaService.execute(new BundleJobsDeleteJPAExecutor(deleteList));

        try {
            jpaService.execute(new BundleJobGetJPAExecutor(jobA.getId()));
            fail("Bundle Job A should have been deleted");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(new BundleActionGetJPAExecutor(actionA1.getBundleId(), actionA1.getCoordName()));
            fail("Bundle Action A1 should have been deleted");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(new BundleActionGetJPAExecutor(actionA2.getBundleId(), actionA2.getCoordName()));
            fail("Bundle Action A2 should have been deleted");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(new BundleJobGetJPAExecutor(jobB.getId()));
            fail("Bundle Job B should have been deleted");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(new BundleActionGetJPAExecutor(actionB1.getBundleId(), actionB1.getCoordName()));
            fail("Bundle Action B1 should have been deleted");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(new BundleActionGetJPAExecutor(actionB2.getBundleId(), actionB2.getCoordName()));
            fail("Bundle Action B2 should have been deleted");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(new BundleJobGetJPAExecutor(jobC.getId()));
            fail("Bundle Job C should have been deleted");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }

        try {
            jpaService.execute(new BundleActionGetJPAExecutor(actionC1.getBundleId(), actionC1.getCoordName()));
            fail("Bundle Action C1 should have been deleted");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }

        try {
            jpaService.execute(new BundleActionGetJPAExecutor(actionC2.getBundleId(), actionC2.getCoordName()));
            fail("Bundle Action C2 should have been deleted");
        }
        catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }
    }

    public void testDeleteBundlesRollback() throws Exception{
        BundleJobBean jobA = this.addRecordToBundleJobTable(Job.Status.SUCCEEDED, false);
        BundleActionBean actionA1 = addRecordToBundleActionTable(jobA.getId(), "actionA1", 0, Job.Status.SUCCEEDED);
        BundleActionBean actionA2 = addRecordToBundleActionTable(jobA.getId(), "actionA2", 0, Job.Status.SUCCEEDED);

        BundleJobBean jobB = this.addRecordToBundleJobTable(Job.Status.SUCCEEDED, false);
        BundleActionBean actionB1 = addRecordToBundleActionTable(jobB.getId(), "actionB1", 0, Job.Status.SUCCEEDED);
        BundleActionBean actionB2 = addRecordToBundleActionTable(jobB.getId(), "actionB2", 0, Job.Status.SUCCEEDED);

        BundleJobBean jobC = this.addRecordToBundleJobTable(Job.Status.SUCCEEDED, false);
        BundleActionBean actionC1 = addRecordToBundleActionTable(jobC.getId(), "actionC1", 0, Job.Status.SUCCEEDED);
        BundleActionBean actionC2 = addRecordToBundleActionTable(jobC.getId(), "actionC2", 0, Job.Status.SUCCEEDED);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        try {
            // set fault injection to true, so transaction is roll backed
            setSystemProperty(FaultInjection.FAULT_INJECTION, "true");
            setSystemProperty(SkipCommitFaultInjection.ACTION_FAILOVER_FAULT_INJECTION, "true");

            List<String> deleteList = new ArrayList<String>();
            deleteList.add(jobA.getId());
            deleteList.add(jobB.getId());
            deleteList.add(jobC.getId());

            try {
                jpaService.execute(new BundleJobsDeleteJPAExecutor(deleteList));
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
            jpaService.execute(new BundleJobGetJPAExecutor(jobA.getId()));
        }
        catch (JPAExecutorException je) {
            fail("Bundle Job A should not have been deleted");
        }

        try {
            jpaService.execute(new BundleActionGetJPAExecutor(actionA1.getBundleId(), actionA1.getCoordName()));
        }
        catch (JPAExecutorException je) {
            fail("Bundle Action A1 should not have been deleted");
        }

        try {
            jpaService.execute(new BundleActionGetJPAExecutor(actionA2.getBundleId(), actionA2.getCoordName()));
        }
        catch (JPAExecutorException je) {
            fail("Bundle Action A2 should not have been deleted");
        }

        try {
            jpaService.execute(new BundleJobGetJPAExecutor(jobB.getId()));
        }
        catch (JPAExecutorException je) {
            fail("Bundle Job B should not have been deleted");
        }

        try {
            jpaService.execute(new BundleActionGetJPAExecutor(actionB1.getBundleId(), actionB1.getCoordName()));
        }
        catch (JPAExecutorException je) {
            fail("Bundle Action B1 should not have been deleted");
        }

        try {
            jpaService.execute(new BundleActionGetJPAExecutor(actionB2.getBundleId(), actionB2.getCoordName()));
        }
        catch (JPAExecutorException je) {
            fail("Bundle Action B2 should not have been deleted");
        }

        try {
            jpaService.execute(new BundleJobGetJPAExecutor(jobC.getId()));
        }
        catch (JPAExecutorException je) {
            fail("Bundle Job C should not have been deleted");
        }

        try {
            jpaService.execute(new BundleActionGetJPAExecutor(actionC1.getBundleId(), actionC1.getCoordName()));
        }
        catch (JPAExecutorException je) {
            fail("Bundle Action C1 should not have been deleted");
        }

        try {
            jpaService.execute(new BundleActionGetJPAExecutor(actionC2.getBundleId(), actionC2.getCoordName()));
        }
        catch (JPAExecutorException je) {
            fail("Bundle Action C2 should not have been deleted");
        }
    }
}
