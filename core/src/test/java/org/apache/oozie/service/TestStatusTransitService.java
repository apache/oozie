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
package org.apache.oozie.service;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.bundle.BundleJobResumeXCommand;
import org.apache.oozie.command.bundle.BundleJobSuspendXCommand;
import org.apache.oozie.command.bundle.BundleKillXCommand;
import org.apache.oozie.command.coord.CoordKillXCommand;
import org.apache.oozie.command.coord.CoordResumeXCommand;
import org.apache.oozie.command.coord.CoordSuspendXCommand;
import org.apache.oozie.executor.jpa.BundleActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobInsertJPAExecutor;
import org.apache.oozie.service.StatusTransitService.StatusTransitRunnable;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.lite.EndNodeDef;
import org.apache.oozie.workflow.lite.LiteWorkflowApp;
import org.apache.oozie.workflow.lite.StartNodeDef;

public class TestStatusTransitService extends XDataTestCase {
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
        cleanUpDBTables();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    /**
     * Tests functionality of the StatusTransitService Runnable command. </p> Insert a coordinator job with RUNNING and
     * pending true and coordinator actions with pending false. Then, runs the StatusTransitService runnable and ensures
     * the job status changes to SUCCEEDED.
     *
     * @throws Exception
     */
    public void testCoordStatusTransitServiceSucceeded() throws Exception {
        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, true, true, 3);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();
        sleep(1000);

        JPAService jpaService = Services.get().get(JPAService.class);
        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
        CoordinatorJobBean coordJob = jpaService.execute(coordGetCmd);
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob.getStatus());
    }

    /**
     * Tests functionality of the StatusTransitService Runnable command. </p> Insert a coordinator job with RUNNING and
     * pending true and coordinator actions with pending false, but one of action is KILLED.
     * Then, runs the StatusTransitService runnable and ensures the job status changes to DONEWITHERROR.
     *
     * @throws Exception
     */
    public void testCoordStatusTransitServiceDoneWithError() throws Exception {
        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);

        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, true, true, 3);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.KILLED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();
        sleep(1000);

        JPAService jpaService = Services.get().get(JPAService.class);
        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
        CoordinatorJobBean coordJob = jpaService.execute(coordGetCmd);
        assertEquals(CoordinatorJob.Status.DONEWITHERROR, coordJob.getStatus());
    }

    /**
     * Tests functionality of the StatusTransitService Runnable command. </p> Insert a coordinator job with RUNNING and
     * pending true and coordinator actions with pending false, but one of action is KILLED.
     * Set oozie.service.StatusTransitService.backward.support.for.coord.status=true
     * and use uri:oozie:coordinator:0.1 namespace, then, runs the StatusTransitService runnable and
     * ensures the job status stay in RUNNING.
     *
     * @throws Exception
     */
    public void testCoordStatusTransitServiceNoDoneWithErrorForBackwardSupport() throws Exception {
        Services.get().destroy();
        setSystemProperty(StatusTransitService.CONF_BACKWARD_SUPPORT_FOR_COORD_STATUS, "true");
        services = new Services();
        setClassesToBeExcluded(services.getConf(), excludedServices);
        services.init();

        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);

        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, true, true, 3);

        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        coordJob.setAppNamespace(SchemaService.COORDINATOR_NAMESPACE_URI_1);
        jpaService.execute(new CoordJobUpdateJPAExecutor(coordJob));

        addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.KILLED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(coordJob.getId(), 2, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(coordJob.getId(), 3, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();
        sleep(1000);

        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(coordJob.getId());
        coordJob = jpaService.execute(coordGetCmd);
        assertEquals(CoordinatorJob.Status.RUNNING, coordJob.getStatus());
    }

    /**
     * Tests functionality of the StatusTransitService Runnable command. </p> Insert a coordinator job with RUNNING and
     * pending false and coordinator actions with pending false. Then, runs the CoordKillXCommand and
     * StatusTransitService runnable and ensures the job pending changes to false.
     *
     * @throws Exception
     */
    public void testCoordStatusTransitServiceKilledByUser1() throws Exception {
        final JPAService jpaService = Services.get().get(JPAService.class);
        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);

        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, false, false,
                1);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        final String wfJobId = wfJob.getId();
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", wfJobId, "RUNNING", 0);

        new CoordKillXCommand(coordJob.getId()).call();

        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                WorkflowJobGetJPAExecutor wfGetCmd = new WorkflowJobGetJPAExecutor(wfJobId);
                WorkflowJobBean wfBean = jpaService.execute(wfGetCmd);
                return wfBean.getStatusStr().equals("KILLED");
            }
        });

        assertNotNull(jpaService);
        final CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(coordJob.getId());
        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(coordAction.getId());
        WorkflowJobGetJPAExecutor wfGetCmd = new WorkflowJobGetJPAExecutor(wfJobId);

        coordJob = jpaService.execute(coordJobGetCmd);
        coordAction = jpaService.execute(coordActionGetCmd);
        wfJob = jpaService.execute(wfGetCmd);
        assertEquals(CoordinatorJob.Status.KILLED, coordJob.getStatus());
        assertEquals(CoordinatorAction.Status.KILLED, coordAction.getStatus());
        assertEquals(WorkflowJob.Status.KILLED, wfJob.getStatus());
        assertEquals(false, coordAction.isPending());

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();

        // Status of coordJobBean is being updated asynchronously.
        // Increasing wait time to atmost 10s to make sure there is
        // sufficient time for the status to get updated. Thus, resulting
        // in following assertion not failing.
        waitFor(10 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorJobBean coordJobBean = jpaService.execute(coordJobGetCmd);
                return !coordJobBean.isPending();
            }
        });

        coordJob = jpaService.execute(coordJobGetCmd);
        assertEquals(false, coordJob.isPending());
    }

    /**
     * Test : coord job killed by user - pending update to false
     *
     * @throws Exception
     */
    public void testCoordStatusTransitServiceKilledByUser2() throws Exception {
        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.KILLED, start, end, true, false, 3);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.KILLED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.KILLED, "coord-action-get.xml", 0);

        final String jobId = job.getId();
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();
        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorJobBean coordJob = jpaService.execute(new CoordJobGetJPAExecutor(jobId));
                return coordJob.isPending() == false;
            }
        });

        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
        job = jpaService.execute(coordGetCmd);
        assertFalse(job.isPending());
    }

    /**
     * Test : coord job suspended by user and all coord actions are succeeded - pending update to false
     *
     * @throws Exception
     */
    public void testCoordStatusTransitServiceSuspendedByUser() throws Exception {
        String currentDateplusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDateplusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDateplusMonth);

        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.SUSPENDED, start, end, true, true, 3);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);

        final String jobId = job.getId();
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();
        waitFor(10 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorJobBean coordJob = jpaService.execute(new CoordJobGetJPAExecutor(jobId));
                return coordJob.isPending() == false;
            }
        });

        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
        job = jpaService.execute(coordGetCmd);
        assertFalse(job.isPending());
        assertEquals(Job.Status.SUCCEEDED, job.getStatus());
    }

    /**
     * Test : coord actions suspended and 1 succeeded - check status change to SUSPENDED and pending update to false
     *
     * @throws Exception
     */
    public void testCoordStatusTransitServiceSuspendedBottomUp() throws Exception {
        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);

        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, true, true, 4);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUSPENDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.SUSPENDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.SUSPENDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 4, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);


        final String jobId = job.getId();
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();
        // Keeping wait time to 20s to ensure status is updated
        waitFor(20 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorJobBean coordJob = jpaService.execute(new CoordJobGetJPAExecutor(jobId));
                return coordJob.getStatus() == CoordinatorJob.Status.SUSPENDED;
            }
        });

        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
        job = jpaService.execute(coordGetCmd);
        assertEquals(CoordinatorJob.Status.SUSPENDED, job.getStatus());
        assertFalse(job.isPending());
    }

    /**
     * Test : all coord actions suspended except one which is killed - check status change to SUSPENDEDWITHERROR
     *
     * @throws Exception
     */
    public void testCoordStatusTransitServiceSuspendedWithError() throws Exception {
        Services.get().destroy();
        setSystemProperty(StatusTransitService.CONF_BACKWARD_SUPPORT_FOR_STATES_WITHOUT_ERROR, "false");
        services = new Services();
        setClassesToBeExcluded(services.getConf(), excludedServices);
        services.init();
        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, true, true, 4);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.KILLED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.SUSPENDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.SUSPENDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 4, CoordinatorAction.Status.SUSPENDED, "coord-action-get.xml", 0);


        final String jobId = job.getId();
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();
        // Keeping wait time to 20s to ensure status is updated
        waitFor(20 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorJobBean coordJob = jpaService.execute(new CoordJobGetJPAExecutor(jobId));
                return coordJob.getStatus() == CoordinatorJob.Status.SUSPENDEDWITHERROR;
            }
        });

        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
        job = jpaService.execute(coordGetCmd);
        assertEquals(CoordinatorJob.Status.SUSPENDEDWITHERROR, job.getStatus());
        assertFalse(job.isPending());
    }

    /**
     * Test : Suspend and resume a coordinator job which has finished materialization and all actions are succeeded.
     * </p>
     * Coordinator job changes to succeeded after resume
     *
     * @throws Exception
     */
    public void testCoordStatusTransitServiceSuspendAndResume() throws Exception {
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);

        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, false, true, 2);
        final String coordJobId = coordJob.getId();

        final CoordinatorActionBean coordAction1_1 = addRecordToCoordActionTable(coordJobId, 1,
                CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        final CoordinatorActionBean coordAction1_2 = addRecordToCoordActionTable(coordJobId, 2,
                CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);

        this.addRecordToWfJobTable(coordAction1_1.getExternalId(), WorkflowJob.Status.SUCCEEDED,
                WorkflowInstance.Status.SUCCEEDED);
        this.addRecordToWfJobTable(coordAction1_2.getExternalId(), WorkflowJob.Status.SUCCEEDED,
                WorkflowInstance.Status.SUCCEEDED);

        new CoordSuspendXCommand(coordJobId).call();

        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(coordJobId);
        coordJob = jpaService.execute(coordJobGetCmd);

        assertEquals(Job.Status.SUSPENDED, coordJob.getStatus());

        sleep(3000);

        new CoordResumeXCommand(coordJobId).call();

        coordJob = jpaService.execute(coordJobGetCmd);

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();

        waitFor(20 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorJobBean job = jpaService.execute(new CoordJobGetJPAExecutor(coordJobId));
                return job.getStatus().equals(Job.Status.SUCCEEDED);
            }
        });

        CoordinatorJobBean coordJob1 = jpaService.execute(new CoordJobGetJPAExecutor(coordJobId));
        assertFalse(coordJob1.isPending());
        assertEquals(Job.Status.SUCCEEDED, coordJob1.getStatus());
    }


    /**
     * Test : all coord actions are running, job pending is reset
     *
     * @throws Exception
     */
    public void testCoordStatusTransitServiceRunning1() throws Exception {
        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, true, false, 3);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);

        final String jobId = job.getId();
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();
        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorJobBean coordJob = jpaService.execute(new CoordJobGetJPAExecutor(jobId));
                return coordJob.isPending() == false;
            }
        });

        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
        job = jpaService.execute(coordGetCmd);
        assertFalse(job.isPending());
        assertEquals(job.getStatus(), Job.Status.RUNNING);
    }


    /**
     * Test : 2 coord actions are running, 1 suspended and 1 succeeded, check job pending is reset
     * and status changed to RUNNING
     *
     * @throws Exception
     */
    public void testCoordStatusTransitServiceRunning2() throws Exception {
        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);

        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNINGWITHERROR, start, end, true, false, 4);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 4, CoordinatorAction.Status.SUSPENDED, "coord-action-get.xml", 0);

        final String jobId = job.getId();
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();
        waitFor(10 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorJobBean coordJob = jpaService.execute(new CoordJobGetJPAExecutor(jobId));
                return coordJob.getStatus() == Job.Status.RUNNING;
            }
        });

        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
        job = jpaService.execute(coordGetCmd);
        assertFalse(job.isPending());
        assertEquals(job.getStatus(), Job.Status.RUNNING);
    }

    /**
     * Test : Keep the backward support for states on. 2 coord actions are running, 1 killed, check if job pending is reset and state changed to
     * RUNNING. Make sure the status is not RUNNINGWITHERROR
     *
     * @throws Exception
     */
    public void testCoordStatusTransitServiceBackwardSupport() throws Exception {
        Services.get().destroy();
        setSystemProperty(StatusTransitService.CONF_BACKWARD_SUPPORT_FOR_STATES_WITHOUT_ERROR, "true");
        services = new Services();
        setClassesToBeExcluded(services.getConf(), excludedServices);
        services.init();
        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, true, false, 3);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.KILLED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);

        final String jobId = job.getId();
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();
        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorJobBean coordJob = jpaService.execute(new CoordJobGetJPAExecutor(jobId));
                return coordJob.isPending() == false;
            }
        });

        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
        job = jpaService.execute(coordGetCmd);
        assertFalse(job.isPending());
        assertEquals(job.getStatus(), Job.Status.RUNNING);
    }


    /**
     * Test : 2 coord actions are running, 1 killed, check if job pending is reset and state changed to
     * RUNNINGWITHERROR
     *
     * @throws Exception
     */
    public void testCoordStatusTransitServiceRunning3() throws Exception {
        Services.get().destroy();
        setSystemProperty(StatusTransitService.CONF_BACKWARD_SUPPORT_FOR_STATES_WITHOUT_ERROR, "false");
        services = new Services();
        setClassesToBeExcluded(services.getConf(), excludedServices);
        services.init();
        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, true, false, 3);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.KILLED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);

        final String jobId = job.getId();
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();
        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorJobBean coordJob = jpaService.execute(new CoordJobGetJPAExecutor(jobId));
                return coordJob.isPending() == false;
            }
        });

        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
        job = jpaService.execute(coordGetCmd);
        assertFalse(job.isPending());
        assertEquals(job.getStatus(), Job.Status.RUNNINGWITHERROR);
    }

    public void testCoordStatusTransitServiceUpdateLastModifiedTime() throws Exception {
        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, true, false, 3);
        Date lastModifiedDate = job.getLastModifiedTime();
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 1);

        final JPAService jpaService = Services.get().get(JPAService.class);
        Runnable runnable = new StatusTransitRunnable();
        runnable.run();
        sleep(1000);

        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
        job = jpaService.execute(coordGetCmd);
        // As state of job has not changed, lastModified should not be updated
        assertEquals(lastModifiedDate.getTime(), job.getLastModifiedTime().getTime());
        assertEquals(Job.Status.RUNNING, job.getStatus());
    }

    /**
     * Test : all coord actions are running, job pending is reset
     *
     * @throws Exception
     */
    public void testCoordStatusTransitServicePaused() throws Exception {
        Services.get().destroy();
        setSystemProperty(StatusTransitService.CONF_BACKWARD_SUPPORT_FOR_STATES_WITHOUT_ERROR, "false");
        services = new Services();
        setClassesToBeExcluded(services.getConf(), excludedServices);
        services.init();
        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        CoordinatorJobBean coordJob = createCoordJob(CoordinatorJob.Status.PAUSED, start, end, true, false, 3);
        // set some pause time explicity to make sure the job is not unpaused
        coordJob.setPauseTime(DateUtils.parseDateOozieTZ("2009-02-01T01:00Z"));
        final JPAService jpaService = Services.get().get(JPAService.class);
        CoordJobInsertJPAExecutor coordInsertCmd = new CoordJobInsertJPAExecutor(coordJob);
        jpaService.execute(coordInsertCmd);

        addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.KILLED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(coordJob.getId(), 2, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(coordJob.getId(), 3, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);

        final String jobId = coordJob.getId();

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();
        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorJobBean coordJob = jpaService.execute(new CoordJobGetJPAExecutor(jobId));
                return coordJob.isPending() == false;
            }
        });

        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(coordJob.getId());
        coordJob = jpaService.execute(coordGetCmd);
        assertFalse(coordJob.isPending());
        assertEquals(CoordinatorJob.Status.PAUSEDWITHERROR, coordJob.getStatus());
    }

    /**
     * Test : all coord actions are running, job pending is reset
     *
     * @throws Exception
     */
    public void testCoordStatusTransitServicePausedWithError() throws Exception {
        Services.get().destroy();
        setSystemProperty(StatusTransitService.CONF_BACKWARD_SUPPORT_FOR_STATES_WITHOUT_ERROR, "false");
        services = new Services();
        setClassesToBeExcluded(services.getConf(), excludedServices);
        services.init();
        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);

        CoordinatorJobBean job = createCoordJob(CoordinatorJob.Status.PAUSEDWITHERROR, start, end, true, false, 3);
        // set the pause time explicity to make sure the job is not unpaused
        job.setPauseTime(DateUtils.parseDateOozieTZ("2009-02-01T01:00Z"));
        final JPAService jpaService = Services.get().get(JPAService.class);
        CoordJobInsertJPAExecutor coordInsertCmd = new CoordJobInsertJPAExecutor(job);
        jpaService.execute(coordInsertCmd);

        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);

        final String jobId = job.getId();
        assertNotNull(jpaService);

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();
        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorJobBean coordJob = jpaService.execute(new CoordJobGetJPAExecutor(jobId));
                return coordJob.isPending() == false;
            }
        });

        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
        job = jpaService.execute(coordGetCmd);
        assertFalse(job.isPending());
        assertEquals(CoordinatorJob.Status.PAUSED, job.getStatus());
    }
    /**
     * Tests functionality of the StatusTransitService Runnable command. </p> Insert a coordinator job with RUNNING and
     * pending true and coordinator actions with TIMEDOUT state. Then, runs the StatusTransitService runnable and
     * ensures the job state changes to DONEWITHERROR.
     *
     * @throws Exception
     */
    public void testCoordStatusTransitServiceForTimeout() throws Exception {
        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, true, true, 3);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.TIMEDOUT, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.TIMEDOUT, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.TIMEDOUT, "coord-action-get.xml", 0);

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();
        sleep(1000);

        JPAService jpaService = Services.get().get(JPAService.class);
        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
        CoordinatorJobBean coordJob = jpaService.execute(coordGetCmd);
        assertEquals(CoordinatorJob.Status.DONEWITHERROR, coordJob.getStatus());
    }

    /**
     * Inserts a coordinator job in KILLED state with pending materialization.
     * Make sure the status changes to DONEWITHERROR if there is at least one
     * coordinator action in NON-KILLED state
     *
     * @throws Exception
     */
    public void testCoordStatusTransitServiceTransitionToDoneWithError() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.KILLED, true, false);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        Runnable runnable = new StatusTransitRunnable();
        runnable.run();
        sleep(1000);

        JPAService jpaService = Services.get().get(JPAService.class);
        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
        CoordinatorJobBean coordJob = jpaService.execute(coordGetCmd);
        assertEquals(CoordinatorJob.Status.DONEWITHERROR, coordJob.getStatus());

    }

    /**
     * Test : all bundle actions are succeeded - bundle job's status will be updated to succeeded.
     *
     * @throws Exception
     */
    public void testBundleStatusTransitServiceSucceeded1() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.RUNNING, false);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        final String jobId = job.getId();
        BundleActionBean ba1 = addRecordToBundleActionTable(jobId, "action1", 0, Job.Status.SUCCEEDED);
        addRecordToBundleActionTable(jobId, "action2", 0, Job.Status.SUCCEEDED);
        addRecordToBundleActionTable(jobId, "action3", 0, Job.Status.SUCCEEDED);

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();

        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean bundle = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
                return bundle.getStatus().equals(Job.Status.SUCCEEDED);
            }
        });

        job = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
        assertEquals(Job.Status.SUCCEEDED, job.getStatus());
    }

    /**
     * Test : all coord jobs are succeeded - bundle job's status will be updated to succeeded. coordinator action ->
     * coordinator job -> bundle action -> bundle job
     *
     * @throws Exception
     */
    public void testBundleStatusTransitServiceSucceeded2() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.RUNNING, false);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        final String bundleId = job.getId();
        addRecordToBundleActionTable(bundleId, "action1", 0, Job.Status.RUNNING);
        addRecordToBundleActionTable(bundleId, "action2", 0, Job.Status.RUNNING);

        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);

        addRecordToCoordJobTableWithBundle(bundleId, "action1", CoordinatorJob.Status.RUNNING, start, end, true, true, 2);
        addRecordToCoordJobTableWithBundle(bundleId, "action2", CoordinatorJob.Status.RUNNING, start, end, true, true, 2);

        addRecordToCoordActionTable("action1", 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable("action1", 2, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);

        addRecordToCoordActionTable("action2", 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable("action2", 2, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();

        waitFor(15 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean bundle = jpaService.execute(new BundleJobGetJPAExecutor(bundleId));
                return bundle.getStatus().equals(Job.Status.SUCCEEDED);
            }
        });

        job = jpaService.execute(new BundleJobGetJPAExecutor(bundleId));
        assertEquals(Job.Status.SUCCEEDED, job.getStatus());
    }

    /**
     * Insert a coordinator job in KILLED state with one coordinator action as
     * SUCCEEDED. Make sure the status of the job changes to DONEWITHERROR.
     * Also, the status of bundle action and bundle job should change to
     * DONEWITHERROR
     *
     * @throws Exception
     */
    public void testBundleStatusTransitServiceForTerminalStates() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.RUNNING, false);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        final String bundleId = job.getId();
        addRecordToBundleActionTable(bundleId, "action1", 0, Job.Status.KILLED);

        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);

        addRecordToCoordJobTableWithBundle(bundleId, "action1", CoordinatorJob.Status.KILLED, start, end, true, true, 2);

        addRecordToCoordActionTable("action1", 1, CoordinatorAction.Status.KILLED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable("action1", 2, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();

        waitFor(15 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorJobBean coordJob = jpaService.execute(new CoordJobGetJPAExecutor("action1"));
                return coordJob.getStatus().equals(Job.Status.DONEWITHERROR);
            }
        });

        CoordinatorJobBean coordJob = jpaService.execute(new CoordJobGetJPAExecutor("action1"));
        assertEquals(Job.Status.DONEWITHERROR, coordJob.getStatus());

        BundleActionBean bab = jpaService.execute(new BundleActionGetJPAExecutor(bundleId, "action1"));
        assertEquals(Job.Status.DONEWITHERROR, bab.getStatus());

        job = jpaService.execute(new BundleJobGetJPAExecutor(bundleId));
        assertEquals(Job.Status.DONEWITHERROR, job.getStatus());
    }

    /**
     * Test : all coord jobs are succeeded - bundle job's status will be updated to succeeded after suspend and resume.
     * coordinator action -> coordinator job -> bundle action -> bundle job
     *
     * @throws Exception
     */
    public void testBundleStatusTransitServiceSucceeded3() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.RUNNING, false);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        final String bundleId = job.getId();
        addRecordToBundleActionTable(bundleId, "action1", 1, Job.Status.RUNNING);
        addRecordToBundleActionTable(bundleId, "action2", 0, Job.Status.RUNNING);

        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);

        addRecordToCoordJobTableWithBundle(bundleId, "action1", CoordinatorJob.Status.RUNNING, start, end, true, true, 2);
        addRecordToCoordJobTableWithBundle(bundleId, "action2", CoordinatorJob.Status.RUNNING, start, end, true, true, 2);

        addRecordToCoordActionTable("action1", 1, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);
        addRecordToCoordActionTable("action1", 2, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);

        addRecordToCoordActionTable("action2", 1, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);
        addRecordToCoordActionTable("action2", 2, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);

        new BundleJobSuspendXCommand(bundleId).call();

        BundleJobGetJPAExecutor bundleJobGetCmd = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetCmd);

        assertEquals(Job.Status.SUSPENDED, job.getStatus());

        sleep(3000);

        new BundleJobResumeXCommand(bundleId).call();

        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(Job.Status.RUNNING, job.getStatus());
    }

    /**
     * Test : kill a bundle job - bundle job's pending will be updated to false.
     * <p/>
     * The pending is updated bottom-up. workflow job -> coordinator action -> coordinator job -> bundle action ->
     * bundle job
     *
     * @throws Exception
     */
    public void testBundleStatusTransitServiceKilled() throws Exception {
        BundleJobBean bundleJob = this.addRecordToBundleJobTable(Job.Status.KILLED, true);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        final String bundleId = bundleJob.getId();
        addRecordToBundleActionTable(bundleId, "action1-C", 1, Job.Status.KILLED);
        addRecordToBundleActionTable(bundleId, "action2-C", 1, Job.Status.KILLED);

        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);

        addRecordToCoordJobTableWithBundle(bundleId, "action1-C", CoordinatorJob.Status.RUNNING, start, end, false, true, 2);
        addRecordToCoordJobTableWithBundle(bundleId, "action2-C", CoordinatorJob.Status.RUNNING, start, end, false, true, 2);

        WorkflowJobBean wfJob1_1 = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowJobBean wfJob1_2 = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowJobBean wfJob1_3 = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowJobBean wfJob1_4 = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);

        final CoordinatorActionBean coordAction1_1 = addRecordToCoordActionTable("action1-C", 1,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", wfJob1_1.getId(), wfJob1_1.getStatusStr(), 0);
        final CoordinatorActionBean coordAction1_2 = addRecordToCoordActionTable("action1-C", 2,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", wfJob1_2.getId(), wfJob1_2.getStatusStr(), 0);

        final CoordinatorActionBean coordAction1_3 = addRecordToCoordActionTable("action2-C", 1,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", wfJob1_3.getId(), wfJob1_3.getStatusStr(), 0);
        final CoordinatorActionBean coordAction1_4 = addRecordToCoordActionTable("action2-C", 2,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", wfJob1_4.getId(), wfJob1_4.getStatusStr(), 0);

        new CoordKillXCommand("action1-C").call();
        new CoordKillXCommand("action2-C").call();

        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                WorkflowJobBean wfJob = jpaService
                        .execute(new WorkflowJobGetJPAExecutor(coordAction1_4.getExternalId()));
                return wfJob.getStatus().equals(Job.Status.KILLED);
            }
        });

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();

        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean bundle = jpaService.execute(new BundleJobGetJPAExecutor(bundleId));
                return bundle.isPending() == false;
            }
        });

        bundleJob = jpaService.execute(new BundleJobGetJPAExecutor(bundleId));
        assertFalse(bundleJob.isPending());
        assertEquals(Job.Status.KILLED, bundleJob.getStatus());

        BundleActionBean bundleAction1 = jpaService.execute(new BundleActionGetJPAExecutor(bundleId, "action1-C"));
        assertFalse(bundleAction1.isPending());
        assertEquals(Job.Status.KILLED, bundleAction1.getStatus());

        CoordinatorJobBean coordJob1 = jpaService.execute(new CoordJobGetJPAExecutor("action1-C"));
        assertFalse(coordJob1.isPending());
        assertEquals(Job.Status.KILLED, coordJob1.getStatus());

        BundleActionBean bundleAction2 = jpaService.execute(new BundleActionGetJPAExecutor(bundleId, "action2-C"));
        assertFalse(bundleAction2.isPending());
        assertEquals(Job.Status.KILLED, bundleAction2.getStatus());

        CoordinatorJobBean coordJob2 = jpaService.execute(new CoordJobGetJPAExecutor("action2-C"));
        assertFalse(coordJob2.isPending());
        assertEquals(Job.Status.KILLED, coordJob2.getStatus());
    }

    /**
     * Test : kill a bundle job with coord jobs as DONEWITHERROR
     * <p/>
     * Initially the bundle job should be KILLED and then DONEWITHERROR
     *
     * @throws Exception
     */
    public void testBundleStatusTransitServiceKilled1() throws Exception {
        BundleJobBean bundleJob = this.addRecordToBundleJobTable(Job.Status.RUNNING, true);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        final String bundleId = bundleJob.getId();
        addRecordToBundleActionTable(bundleId, "action1", 0, Job.Status.DONEWITHERROR);
        addRecordToBundleActionTable(bundleId, "action2", 0, Job.Status.DONEWITHERROR);

        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);

        addRecordToCoordJobTableWithBundle(bundleId, "action1", CoordinatorJob.Status.DONEWITHERROR, start, end, false, true, 2);
        addRecordToCoordJobTableWithBundle(bundleId, "action2", CoordinatorJob.Status.DONEWITHERROR, start, end, false, true, 2);

        new BundleKillXCommand(bundleId).call();
        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean bundle = jpaService.execute(new BundleJobGetJPAExecutor(bundleId));
                return bundle.getStatus() == Job.Status.KILLED;
            }
        });

        bundleJob = jpaService.execute(new BundleJobGetJPAExecutor(bundleId));
        assertEquals(Job.Status.KILLED, bundleJob.getStatus());

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();

        waitFor(15 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean bundle = jpaService.execute(new BundleJobGetJPAExecutor(bundleId));
                return bundle.getStatus() == Job.Status.DONEWITHERROR;
            }
        });

        bundleJob = jpaService.execute(new BundleJobGetJPAExecutor(bundleId));
        assertEquals(Job.Status.DONEWITHERROR, bundleJob.getStatus());

    }


    /**
     * Test : Make the bundle kill itself by having one of the coord job fail preparation.
     *
     * @throws Exception
     */
    public void testBundleStatusTransitServiceKilled2() throws Exception {
        BundleJobBean bundleJob = this.addRecordToBundleJobTable(Job.Status.RUNNING, true);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        final String bundleId = bundleJob.getId();
        // Add a bundle action with no coordinator to make it fail
        addRecordToBundleActionTable(bundleId, null, 0, Job.Status.KILLED);
        addRecordToBundleActionTable(bundleId, "action2", 0, Job.Status.RUNNING);

        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);

        addRecordToCoordJobTableWithBundle(bundleId, "action2", CoordinatorJob.Status.RUNNING, start, end, true, true, 2);
        addRecordToCoordActionTable("action2", 1, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);

        Runnable runnable = new StatusTransitRunnable();
        // first time, service will call bundle kill
        runnable.run();
        sleep(10000);
        runnable.run();

        waitFor(25 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean bundle = jpaService.execute(new BundleJobGetJPAExecutor(bundleId));
                return bundle.getStatus() == Job.Status.KILLED;
            }
        });

        bundleJob = jpaService.execute(new BundleJobGetJPAExecutor(bundleId));
        assertEquals(Job.Status.KILLED, bundleJob.getStatus());
    }


    /**
     * Test : kill one coord job and keep the other running. Check whether the bundle job's status
     * is updated to RUNNINGWITHERROR
     * @throws Exception
     */
    public void testBundleStatusTransitServiceRunningWithError() throws Exception {
        Services.get().destroy();
        setSystemProperty(StatusTransitService.CONF_BACKWARD_SUPPORT_FOR_STATES_WITHOUT_ERROR, "false");
        services = new Services();
        setClassesToBeExcluded(services.getConf(), excludedServices);
        services.init();
        BundleJobBean bundleJob = this.addRecordToBundleJobTable(Job.Status.RUNNING, true);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        final String bundleId = bundleJob.getId();
        addRecordToBundleActionTable(bundleId, "action1-C", 1, Job.Status.RUNNING);
        addRecordToBundleActionTable(bundleId, "action2-C", 1, Job.Status.RUNNING);

        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);

        addRecordToCoordJobTableWithBundle(bundleId, "action1-C", CoordinatorJob.Status.RUNNING, start, end, false, true, 2);
        addRecordToCoordJobTableWithBundle(bundleId, "action2-C", CoordinatorJob.Status.RUNNING, start, end, true, false, 2);

        WorkflowJobBean wfJob1_1 = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowJobBean wfJob1_2 = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowJobBean wfJob1_3 = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowJobBean wfJob1_4 = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);

        final CoordinatorActionBean coordAction1_1 = addRecordToCoordActionTable("action1-C", 1,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", wfJob1_1.getId(), wfJob1_1.getStatusStr(), 0);
        final CoordinatorActionBean coordAction1_2 = addRecordToCoordActionTable("action1-C", 2,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", wfJob1_2.getId(), wfJob1_2.getStatusStr(), 0);

        final CoordinatorActionBean coordAction1_3 = addRecordToCoordActionTable("action2-C", 1,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", wfJob1_3.getId(), wfJob1_3.getStatusStr(), 1);
        final CoordinatorActionBean coordAction1_4 = addRecordToCoordActionTable("action2-C", 2,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", wfJob1_4.getId(), wfJob1_4.getStatusStr(), 1);

        new CoordKillXCommand("action1-C").call();

        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                WorkflowJobBean wfJob = jpaService
                        .execute(new WorkflowJobGetJPAExecutor(coordAction1_1.getExternalId()));
                return wfJob.getStatus().equals(Job.Status.KILLED);
            }
        });

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();

        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean bundle = jpaService.execute(new BundleJobGetJPAExecutor(bundleId));
                return bundle.isPending() == false;
            }
        });

        bundleJob = jpaService.execute(new BundleJobGetJPAExecutor(bundleId));
        assertTrue(bundleJob.isPending());
        assertEquals(Job.Status.RUNNINGWITHERROR, bundleJob.getStatus());

        BundleActionBean bundleAction1 = jpaService.execute(new BundleActionGetJPAExecutor(bundleId, "action1-C"));
        assertFalse(bundleAction1.isPending());
        assertEquals(Job.Status.KILLED, bundleAction1.getStatus());

        CoordinatorJobBean coordJob1 = jpaService.execute(new CoordJobGetJPAExecutor("action1-C"));
        assertFalse(coordJob1.isPending());
        assertEquals(Job.Status.KILLED, coordJob1.getStatus());

        BundleActionBean bundleAction2 = jpaService.execute(new BundleActionGetJPAExecutor(bundleId, "action2-C"));
        assertTrue(bundleAction2.isPending());
        assertEquals(Job.Status.RUNNING, bundleAction2.getStatus());

        CoordinatorJobBean coordJob2 = jpaService.execute(new CoordJobGetJPAExecutor("action2-C"));
        assertTrue(coordJob2.isPending());
        assertEquals(Job.Status.RUNNING, coordJob2.getStatus());
    }

    /**
     * Test : Suspend a bundle job - bundle job's pending will be updated to false.
     * <p/>
     * The pending is updated bottom-up. workflow job -> coordinator action -> coordinator job -> bundle action ->
     * bundle job
     *
     * @throws Exception
     */
    public void testBundleStatusTransitServiceSuspended() throws Exception {
        BundleJobBean bundleJob = this.addRecordToBundleJobTable(Job.Status.SUSPENDED, true);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        final String bundleId = bundleJob.getId();
        addRecordToBundleActionTable(bundleId, "action1-C", 1, Job.Status.SUSPENDED);
        addRecordToBundleActionTable(bundleId, "action2-C", 1, Job.Status.SUSPENDED);

        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);

        addRecordToCoordJobTableWithBundle(bundleId, "action1-C", CoordinatorJob.Status.RUNNING, start, end, false, false, 2);
        addRecordToCoordJobTableWithBundle(bundleId, "action2-C", CoordinatorJob.Status.RUNNING, start, end, false, false, 2);

        WorkflowJobBean wfJob1_1 = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowJobBean wfJob1_2 = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowJobBean wfJob1_3 = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowJobBean wfJob1_4 = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);

        final CoordinatorActionBean coordAction1_1 = addRecordToCoordActionTable("action1-C", 1,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", wfJob1_1.getId(), wfJob1_1.getStatusStr(), 0);
        final CoordinatorActionBean coordAction1_2 = addRecordToCoordActionTable("action1-C", 2,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", wfJob1_2.getId(), wfJob1_2.getStatusStr(), 0);

        final CoordinatorActionBean coordAction1_3 = addRecordToCoordActionTable("action2-C", 1,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", wfJob1_3.getId(), wfJob1_3.getStatusStr(), 0);
        final CoordinatorActionBean coordAction1_4 = addRecordToCoordActionTable("action2-C", 2,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", wfJob1_4.getId(), wfJob1_4.getStatusStr(), 0);

        new CoordSuspendXCommand("action1-C").call();
        new CoordSuspendXCommand("action2-C").call();

        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                WorkflowJobBean wfJob = jpaService
                        .execute(new WorkflowJobGetJPAExecutor(coordAction1_4.getExternalId()));
                return wfJob.getStatus().equals(Job.Status.SUSPENDED);
            }
        });

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();

        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean bundle = jpaService.execute(new BundleJobGetJPAExecutor(bundleId));
                return bundle.isPending() == false;
            }
        });

        bundleJob = jpaService.execute(new BundleJobGetJPAExecutor(bundleId));
        assertFalse(bundleJob.isPending());
        assertEquals(Job.Status.SUSPENDED, bundleJob.getStatus());

        BundleActionBean bundleAction1 = jpaService.execute(new BundleActionGetJPAExecutor(bundleId, "action1-C"));
        assertFalse(bundleAction1.isPending());
        assertEquals(Job.Status.SUSPENDED, bundleAction1.getStatus());

        CoordinatorJobBean coordJob1 = jpaService.execute(new CoordJobGetJPAExecutor("action1-C"));
        assertFalse(coordJob1.isPending());
        assertEquals(Job.Status.SUSPENDED, coordJob1.getStatus());

        BundleActionBean bundleAction2 = jpaService.execute(new BundleActionGetJPAExecutor(bundleId, "action2-C"));
        assertFalse(bundleAction2.isPending());
        assertEquals(Job.Status.SUSPENDED, bundleAction2.getStatus());

        CoordinatorJobBean coordJob2 = jpaService.execute(new CoordJobGetJPAExecutor("action2-C"));
        assertFalse(coordJob2.isPending());
        assertEquals(Job.Status.SUSPENDED, coordJob2.getStatus());
    }


    /**
     * Test : Check the transition of a bundle job from RUNNING TO SUSPENDEDWITHERROR
     * @throws Exception
     */
    public void testBundleStatusTransitServiceSuspendedWithError() throws Exception {
        Services.get().destroy();
        setSystemProperty(StatusTransitService.CONF_BACKWARD_SUPPORT_FOR_STATES_WITHOUT_ERROR, "false");
        services = new Services();
        setClassesToBeExcluded(services.getConf(), excludedServices);
        services.init();
        BundleJobBean bundleJob = this.addRecordToBundleJobTable(Job.Status.RUNNING, true);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        final String bundleId = bundleJob.getId();
        addRecordToBundleActionTable(bundleId, "action1", 0, Job.Status.SUSPENDED);
        addRecordToBundleActionTable(bundleId, "action2", 0, Job.Status.SUSPENDEDWITHERROR);

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();

        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean bundle = jpaService.execute(new BundleJobGetJPAExecutor(bundleId));
                return bundle.isPending() == false;
            }
        });

        bundleJob = jpaService.execute(new BundleJobGetJPAExecutor(bundleId));
        assertFalse(bundleJob.isPending());
        assertEquals(Job.Status.SUSPENDEDWITHERROR, bundleJob.getStatus());

    }

    /**
     * Test : Check the transition of a PAUSED bundle job to PAUSEDWITHERROR
     * @throws Exception
     */
    public void testBundleStatusTransitServicePausedWithError() throws Exception {
        Services.get().destroy();
        setSystemProperty(StatusTransitService.CONF_BACKWARD_SUPPORT_FOR_STATES_WITHOUT_ERROR, "false");
        services = new Services();
        setClassesToBeExcluded(services.getConf(), excludedServices);
        services.init();
        BundleJobBean bundleJob = createBundleJob(Job.Status.PAUSED, true);
        bundleJob.setPauseTime(DateUtils.parseDateOozieTZ("2009-02-01T01:00Z"));
        final JPAService jpaService = Services.get().get(JPAService.class);
        BundleJobInsertJPAExecutor bundleInsertjpa = new BundleJobInsertJPAExecutor(bundleJob);
        jpaService.execute(bundleInsertjpa);

        final String bundleId = bundleJob.getId();
        addRecordToBundleActionTable(bundleId, "action1", 1, Job.Status.PAUSED);
        addRecordToBundleActionTable(bundleId, "action2", 1, Job.Status.PAUSED);
        addRecordToBundleActionTable(bundleId, "action3", 0, Job.Status.FAILED);

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();

        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean bundle = jpaService.execute(new BundleJobGetJPAExecutor(bundleId));
                return bundle.getStatus() == Job.Status.PAUSEDWITHERROR;
            }
        });

        bundleJob = jpaService.execute(new BundleJobGetJPAExecutor(bundleId));
        assertEquals(Job.Status.PAUSEDWITHERROR, bundleJob.getStatus());
    }


    /**
     * Test : Check the transition of a PAUSEDWITHERROR bundle job to PAUSED
     * @throws Exception
     */
    public void testBundleStatusTransitServicePaused() throws Exception {
        Services.get().destroy();
        setSystemProperty(StatusTransitService.CONF_BACKWARD_SUPPORT_FOR_STATES_WITHOUT_ERROR, "false");
        services = new Services();
        setClassesToBeExcluded(services.getConf(), excludedServices);
        services.init();
        BundleJobBean bundleJob = createBundleJob(Job.Status.PAUSEDWITHERROR, true);
        bundleJob.setPauseTime(DateUtils.parseDateOozieTZ("2009-02-01T01:00Z"));
        final JPAService jpaService = Services.get().get(JPAService.class);
        BundleJobInsertJPAExecutor bundleInsertjpa = new BundleJobInsertJPAExecutor(bundleJob);
        jpaService.execute(bundleInsertjpa);

        final String bundleId = bundleJob.getId();
        addRecordToBundleActionTable(bundleId, "action1", 1, Job.Status.PAUSED);
        addRecordToBundleActionTable(bundleId, "action2", 1, Job.Status.PAUSED);
        addRecordToBundleActionTable(bundleId, "action3", 0, Job.Status.SUCCEEDED);

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();

        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean bundle = jpaService.execute(new BundleJobGetJPAExecutor(bundleId));
                return bundle.getStatus() == Job.Status.PAUSED;
            }
        });

        bundleJob = jpaService.execute(new BundleJobGetJPAExecutor(bundleId));
        assertEquals(Job.Status.PAUSED, bundleJob.getStatus());
    }


    protected WorkflowJobBean addRecordToWfJobTable(String wfId, WorkflowJob.Status jobStatus,
            WorkflowInstance.Status instanceStatus) throws Exception {
        WorkflowApp app = new LiteWorkflowApp("testApp", "<workflow-app/>",
            new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "end")).
                addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));
        Configuration conf = new Configuration();
        Path appUri = new Path(getAppPath(), "workflow.xml");
        conf.set(OozieClient.APP_PATH, appUri.toString());
        conf.set(OozieClient.LOG_TOKEN, "testToken");
        conf.set(OozieClient.USER_NAME, getTestUser());

        WorkflowJobBean wfBean = createWorkflow(app, conf, jobStatus, instanceStatus);
        wfBean.setId(wfId);
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            WorkflowJobInsertJPAExecutor wfInsertCmd = new WorkflowJobInsertJPAExecutor(wfBean);
            jpaService.execute(wfInsertCmd);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test wf job record to table");
            throw je;
        }
        return wfBean;
    }

    /**
     * Tests functionality of the StatusTransitService Runnable command. </p> Insert a coordinator job with RUNNING and
     * pending true and coordinator actions for that job with pending false. Insert a coordinator action with a stale coord job id. Then, runs the StatusTransitService runnable and ensures
     * the job status of the good job changes to SUCCEEDED.
     *
     * @throws Exception
     */
    public void testCoordStatusTransitServiceStaleCoordActions() throws Exception {

        // this block will initialize the lastinstancetime for status transit service
        Runnable runnable = new StatusTransitRunnable();
        runnable.run();

        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);

        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, true, true, 3);
        // add a record with stale reference to coord job id
        addRecordToCoordActionTable("ABCD", 3, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        // add records with reference to correct job ids
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);

        runnable = new StatusTransitRunnable();
        runnable.run();
        sleep(1000);

        JPAService jpaService = Services.get().get(JPAService.class);
        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
        CoordinatorJobBean coordJob = jpaService.execute(coordGetCmd);
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob.getStatus());
    }

}
