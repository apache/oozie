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

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.service.CallableQueueService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.PartitionDependencyManagerService;
import org.apache.oozie.service.SchemaService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.StatusTransitService;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.HCatURI;
import org.apache.oozie.workflow.WorkflowInstance;

public class TestCoordKillXCommand extends XDataTestCase {
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

    /**
     * Test : kill job and action (READY) successfully
     *
     * @throws Exception
     */
    public void testCoordKillSuccess1() throws Exception {
        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);

        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, false, false, 0);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.READY, "coord-action-get.xml", 0);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(job.getId());
        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(action.getId());

        job = jpaService.execute(coordJobGetCmd);
        action = jpaService.execute(coordActionGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.RUNNING);
        assertEquals(action.getStatus(), CoordinatorAction.Status.READY);
        assertFalse(job.isDoneMaterialization());

        new CoordKillXCommand(job.getId()).call();

        job = jpaService.execute(coordJobGetCmd);
        action = jpaService.execute(coordActionGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.KILLED);
        assertTrue(job.isDoneMaterialization());
        assertNotNull(job.getLastModifiedTime());
        assertEquals(action.getStatus(), CoordinatorAction.Status.KILLED);

        // Change job status to RUNNINGWITHERROR to simulate StatusTransitService changing it to
        // RUNNINGWITHERROR if it had loaded status and had it as RUNNING in memory when CoordKill
        // executes and updates status to KILLED in database.
        job.setStatus(CoordinatorJob.Status.RUNNINGWITHERROR);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB_STATUS, job);
        job = jpaService.execute(coordJobGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.RUNNINGWITHERROR);
        final CoordMaterializeTransitionXCommand transitionCmd = new CoordMaterializeTransitionXCommand(job.getId(), 3600);
        try {
            transitionCmd.loadState();
            transitionCmd.verifyPrecondition();
            fail();
        }
        catch (PreconditionException e) {
            // Materialization should not happen as done materialization is set to true by coord kill
        }
        StatusTransitService.StatusTransitRunnable statusTransit = new StatusTransitService.StatusTransitRunnable();
        statusTransit.run();
        // StatusTransitService should change the job back to KILLED
        job = jpaService.execute(coordJobGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.KILLED);
        assertTrue(job.isDoneMaterialization());
        assertNotNull(job.getLastModifiedTime());
    }

    /**
     * Test : kill job and action (RUNNING) successfully
     *
     * @throws Exception
     */
    public void testCoordKillSuccess2() throws Exception {
        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);

        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, false, true, 0);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(job.getId());
        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(action.getId());

        job = jpaService.execute(coordJobGetCmd);
        action = jpaService.execute(coordActionGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.RUNNING);
        assertEquals(action.getStatus(), CoordinatorAction.Status.RUNNING);

        new CoordKillXCommand(job.getId()).call();

        job = jpaService.execute(coordJobGetCmd);
        action = jpaService.execute(coordActionGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.KILLED);
        assertNotNull(job.getLastModifiedTime());
        assertEquals(action.getStatus(), CoordinatorAction.Status.KILLED);
    }

    /**
     * Test : kill job successfully but failed to kill an already successful action
     *
     * @throws Exception
     */
    public void testCoordKillFailedOnAction() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, true);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(job.getId());
        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(action.getId());

        job = jpaService.execute(coordJobGetCmd);
        action = jpaService.execute(coordActionGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.SUCCEEDED);
        assertEquals(action.getStatus(), CoordinatorAction.Status.SUCCEEDED);

        new CoordKillXCommand(job.getId()).call();

        job = jpaService.execute(coordJobGetCmd);
        action = jpaService.execute(coordActionGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.SUCCEEDED);
        assertEquals(action.getStatus(), CoordinatorAction.Status.SUCCEEDED);
    }

    /**
     * Test : kill SUCCEEDED job successfully when CONF_BACKWARD_SUPPORT_FOR_COORD_STATUS is true and coordinator schema
     * is 0.1
     *
     * @throws Exception
     */
    public void testCoordKillForBackwardSupport() throws Exception {
        Services.get().destroy();
        setSystemProperty(StatusTransitService.CONF_BACKWARD_SUPPORT_FOR_COORD_STATUS, "true");
        services = new Services();
        setClassesToBeExcluded(services.getConf(), excludedServices);
        services.init();

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, true);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);

        job.setAppNamespace(SchemaService.COORDINATOR_NAMESPACE_URI_1);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB_APPNAMESPACE, job);

        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(job.getId());
        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(action.getId());

        job = jpaService.execute(coordJobGetCmd);
        action = jpaService.execute(coordActionGetCmd);
        assertEquals(CoordinatorJob.Status.SUCCEEDED, job.getStatus());
        assertEquals(CoordinatorAction.Status.RUNNING, action.getStatus());

        new CoordKillXCommand(job.getId()).call();

        job = jpaService.execute(coordJobGetCmd);
        action = jpaService.execute(coordActionGetCmd);
        assertEquals(CoordinatorJob.Status.KILLED, job.getStatus());
        assertEquals(CoordinatorAction.Status.KILLED, action.getStatus());
    }


    /**
     * Test : kill job failed. Job does not exist.
     *
     * @throws Exception
     */
    public void testCoordKillFailed() throws Exception {
        final String testJobId = "0000001-" + new Date().getTime() + "-testCoordKill-C";

        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, true);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.READY, "coord-action-get.xml", 0);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(job.getId());
        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(action.getId());

        job = jpaService.execute(coordJobGetCmd);
        action = jpaService.execute(coordActionGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.SUCCEEDED);
        assertEquals(action.getStatus(), CoordinatorAction.Status.READY);

        try {
            new CoordKillXCommand(testJobId).call();
            fail("Job doesn't exist. Should fail.");
        } catch (CommandException ce) {
            //Job doesn't exist. Exception is expected.
        }
    }


    /**
     * Test: Kill a waiting coord action
     * @throws Exception
     */
    public void testCoordKillWaiting() throws Exception {
        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, false, true, 0);
        // Create a workflow job with RUNNING status
        WorkflowJobBean wfJob1 = this
                .addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        // Create a coordinator job with RUNNING status
        CoordinatorActionBean action1 = addRecordToCoordActionTable(coordJob.getId(), 1,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", wfJob1.getId(), "RUNNING", 0);
        // Create a coordinator job with WAITING status
        CoordinatorActionBean action2 = addRecordToCoordActionTable(coordJob.getId(), 2,
                CoordinatorAction.Status.WAITING, "coord-action-get.xml", null, null, 0);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(coordJob.getId());
        CoordActionGetJPAExecutor coordActionGetCmd1 = new CoordActionGetJPAExecutor(action1.getId());
        CoordActionGetJPAExecutor coordActionGetCmd2 = new CoordActionGetJPAExecutor(action2.getId());

        coordJob = jpaService.execute(coordJobGetCmd);
        action1 = jpaService.execute(coordActionGetCmd1);
        action2 = jpaService.execute(coordActionGetCmd2);

        // Make sure the status is updated
        assertEquals(coordJob.getStatus(), CoordinatorJob.Status.RUNNING);
        assertEquals(action1.getStatus(), CoordinatorAction.Status.RUNNING);
        assertEquals(action2.getStatus(), CoordinatorAction.Status.WAITING);

        // Issue the kill command
        new CoordKillXCommand(coordJob.getId()).call();

        coordJob = jpaService.execute(coordJobGetCmd);
        action1 = jpaService.execute(coordActionGetCmd1);
        action2 = jpaService.execute(coordActionGetCmd2);

        // Check the status and pending flag after kill command is issued
        assertEquals(coordJob.getStatus(), CoordinatorJob.Status.KILLED);
        assertEquals(action1.getStatus(), CoordinatorAction.Status.KILLED);
        // The wf job is running and can be killed, so pending for coord action
        // kill should be true
        assertEquals(action1.getPending(), 1);
        assertEquals(action2.getStatus(), CoordinatorAction.Status.KILLED);
        // The coord job is waiting and no wf created yet, so pending for coord
        // action kill should be false
        assertEquals(action2.getPending(), 0);
    }

    public class MyCoordKillXCommand extends CoordKillXCommand {
        long executed = 0;
        int wait;

        public MyCoordKillXCommand(String jobId, int wait) {
            super(jobId);
            this.wait = wait;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Type:").append(getType());
            sb.append(",Priority:").append(getPriority());
            return sb.toString();
        }

        @Override
        protected Void execute() throws CommandException {
            super.execute();
            try {
                Thread.sleep(wait);
            }
            catch (InterruptedException e) {
            }
            executed = System.currentTimeMillis();
            return null;
        }

    }

    public void testCoordKillXCommandUniqueness() throws Exception {
        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, false, false, 0);

        final MyCoordKillXCommand callable1 = new MyCoordKillXCommand(job.getId(), 100);
        final MyCoordKillXCommand callable2 = new MyCoordKillXCommand(job.getId(), 100);
        final MyCoordKillXCommand callable3 = new MyCoordKillXCommand(job.getId(), 100);

        List<MyCoordKillXCommand> callables = Arrays.asList(callable1, callable2, callable3);

        CallableQueueService queueservice = services.get(CallableQueueService.class);

        for (MyCoordKillXCommand c : callables) {
            queueservice.queue(c);
        }

        waitFor(1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return callable1.executed != 0 && callable2.executed == 0 && callable3.executed == 0;
            }
        });

        assertTrue(callable1.executed != 0);
        assertTrue(callable2.executed == 0);
        assertTrue(callable3.executed == 0);
    }


    public void testCoordKillRemovePushMissingDeps() throws Exception {
        try {
            services.destroy();
            services = super.setupServicesForHCatalog();
            services.init();
            String db = "default";
            String table = "tablename";
            String server = "hcatserver";
            String newHCatDependency1 = "hcat://" + server + "/" + db + "/" + table + "/dt=20120430;country=brazil";
            String newHCatDependency2 = "hcat://" + server + "/" + db + "/" + table + "/dt=20120430;country=usa";
            String pushMissingDeps = newHCatDependency1 + CoordELFunctions.INSTANCE_SEPARATOR + newHCatDependency2;
            PartitionDependencyManagerService pdms = Services.get().get(PartitionDependencyManagerService.class);

            CoordinatorJobBean job = addRecordToCoordJobTableForWaiting("coord-job-for-action-input-check.xml",
                    CoordinatorJob.Status.RUNNING, false, true);

            CoordinatorActionBean action1 = addRecordToCoordActionTableForWaiting(job.getId(), 1,
                    CoordinatorAction.Status.WAITING, "coord-action-for-action-input-check.xml", null, pushMissingDeps,
                    "Z");

            String newHCatDependency3 = "hcat://" + server + "/" + db + "/" + table + "/dt=20120430;country=russia";
            CoordinatorActionBean action2 = addRecordToCoordActionTableForWaiting(job.getId(), 2,
                    CoordinatorAction.Status.WAITING, "coord-action-for-action-input-check.xml", null,
                    newHCatDependency3, "Z");

            HCatURI hcatURI1, hcatURI2, hcatURI3;
            hcatURI1 = new HCatURI(newHCatDependency1);
            hcatURI2 = new HCatURI(newHCatDependency2);
            hcatURI3 = new HCatURI(newHCatDependency3);

            pdms.addMissingDependency(hcatURI1, action1.getId());
            pdms.addMissingDependency(hcatURI2, action1.getId());
            pdms.addMissingDependency(hcatURI3, action2.getId());
            assertTrue(pdms.getWaitingActions(new HCatURI(newHCatDependency1)).contains(action1.getId()));
            assertTrue(pdms.getWaitingActions(new HCatURI(newHCatDependency2)).contains(action1.getId()));
            assertTrue(pdms.getWaitingActions(new HCatURI(newHCatDependency3)).contains(action2.getId()));
            new CoordKillXCommand(job.getId()).call();
            assertNull(pdms.getWaitingActions(new HCatURI(newHCatDependency1)));
            assertNull(pdms.getWaitingActions(new HCatURI(newHCatDependency2)));
            assertNull(pdms.getWaitingActions(new HCatURI(newHCatDependency3)));
        }
        catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

    }

}
