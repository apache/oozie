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
import org.apache.oozie.command.coord.CoordKillXCommand;
import org.apache.oozie.command.coord.CoordResumeXCommand;
import org.apache.oozie.command.coord.CoordSuspendXCommand;
import org.apache.oozie.executor.jpa.BundleActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobInsertJPAExecutor;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.lite.EndNodeDef;
import org.apache.oozie.workflow.lite.LiteWorkflowApp;
import org.apache.oozie.workflow.lite.StartNodeDef;
import org.apache.oozie.service.StatusTransitService.StatusTransitRunnable;

public class TestStatusTransitService extends XDataTestCase {
    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
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

        Date start = DateUtils.parseDateUTC("2009-02-01T01:00Z");
        Date end = DateUtils.parseDateUTC("2009-02-02T23:59Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, true, true, 3);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();
        Thread.sleep(1000);

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

        Date start = DateUtils.parseDateUTC("2009-02-01T01:00Z");
        Date end = DateUtils.parseDateUTC("2009-02-02T23:59Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, true, true, 3);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.KILLED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();
        Thread.sleep(1000);

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
        new Services().init();

        Date start = DateUtils.parseDateUTC("2009-02-01T01:00Z");
        Date end = DateUtils.parseDateUTC("2009-02-02T23:59Z");
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
        Thread.sleep(1000);

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
        Date start = DateUtils.parseDateUTC("2009-02-01T01:00Z");
        Date end = DateUtils.parseDateUTC("2009-02-02T23:59Z");
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
        Date start = DateUtils.parseDateUTC("2009-02-01T01:00Z");
        Date end = DateUtils.parseDateUTC("2009-02-02T23:59Z");
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
        Date start = DateUtils.parseDateUTC("2009-02-01T01:00Z");
        Date end = DateUtils.parseDateUTC("2009-02-02T23:59Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.SUSPENDED, start, end, true, false, 3);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);

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
     * Test : coord job suspended by all coord actions are suspended - pending update to false
     *
     * @throws Exception
     */
    public void testCoordStatusTransitServiceSuspendedBottomUp() throws Exception {
        Date start = DateUtils.parseDateUTC("2009-02-01T01:00Z");
        Date end = DateUtils.parseDateUTC("2009-02-02T23:59Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, true, true, 3);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUSPENDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.SUSPENDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.SUSPENDED, "coord-action-get.xml", 0);

        final String jobId = job.getId();
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();
        waitFor(5 * 1000, new Predicate() {
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
     * Test : Suspend and resume a coordinator job which has finished materialization and all actions are succeeded.
     * </p>
     * Coordinator job changes to succeeded after resume
     *
     * @throws Exception
     */
    public void testCoordStatusTransitServiceSuspendAndResume() throws Exception {
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, true);
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

        Thread.sleep(3000);

        new CoordResumeXCommand(coordJobId).call();

        coordJob = jpaService.execute(coordJobGetCmd);

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();

        waitFor(5 * 1000, new Predicate() {
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
        Date start = DateUtils.parseDateUTC("2009-02-01T01:00Z");
        Date end = DateUtils.parseDateUTC("2009-02-02T23:59Z");
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
    }

    /**
     * Test : all coord actions are running, job pending is reset
     *
     * @throws Exception
     */
    public void testCoordStatusTransitServiceRunning2() throws Exception {
        Date start = DateUtils.parseDateUTC("2009-02-01T01:00Z");
        Date end = DateUtils.parseDateUTC("2009-02-02T23:59Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, true, false, 3);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
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
    }

    /**
     * Tests functionality of the StatusTransitService Runnable command. </p> Insert a coordinator job with RUNNING and
     * pending true and coordinator actions with TIMEDOUT state. Then, runs the StatusTransitService runnable and
     * ensures the job state changes to DONEWITHERROR.
     *
     * @throws Exception
     */
    public void testCoordStatusTransitServiceForTimeout() throws Exception {
        Date start = DateUtils.parseDateUTC("2009-02-01T01:00Z");
        Date end = DateUtils.parseDateUTC("2009-02-02T23:59Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, true, true, 3);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.TIMEDOUT, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.TIMEDOUT, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.TIMEDOUT, "coord-action-get.xml", 0);

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();
        Thread.sleep(1000);

        JPAService jpaService = Services.get().get(JPAService.class);
        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
        CoordinatorJobBean coordJob = jpaService.execute(coordGetCmd);
        assertEquals(CoordinatorJob.Status.DONEWITHERROR, coordJob.getStatus());
    }

    @Override
    protected CoordinatorActionBean addRecordToCoordActionTable(String jobId, int actionNum,
            CoordinatorAction.Status status, String resourceXmlName, int pending) throws Exception {
        CoordinatorActionBean action = createCoordAction(jobId, actionNum, status, resourceXmlName, pending);
        action.decrementAndGetPending();
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordActionInsertJPAExecutor coordActionInsertCmd = new CoordActionInsertJPAExecutor(action);
            jpaService.execute(coordActionInsertCmd);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test coord action record to table");
            throw je;
        }
        return action;
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

        addRecordToCoordJobTableWithBundle(bundleId, "action1", CoordinatorJob.Status.RUNNING, true, true, 2);
        addRecordToCoordJobTableWithBundle(bundleId, "action2", CoordinatorJob.Status.RUNNING, true, true, 2);

        addRecordToCoordActionTable("action1", 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable("action1", 2, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);

        addRecordToCoordActionTable("action2", 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable("action2", 2, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();

        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean bundle = jpaService.execute(new BundleJobGetJPAExecutor(bundleId));
                return bundle.getStatus().equals(Job.Status.SUCCEEDED);
            }
        });

        job = jpaService.execute(new BundleJobGetJPAExecutor(bundleId));
        assertEquals(Job.Status.SUCCEEDED, job.getStatus());
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
        addRecordToBundleActionTable(bundleId, "action2", 0, Job.Status.SUCCEEDED);

        addRecordToCoordJobTableWithBundle(bundleId, "action1", CoordinatorJob.Status.RUNNING, true, true, 2);
        addRecordToCoordJobTableWithBundle(bundleId, "action2", CoordinatorJob.Status.SUCCEEDED, true, true, 2);

        addRecordToCoordActionTable("action1", 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable("action1", 2, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);

        addRecordToCoordActionTable("action2", 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable("action2", 2, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);

        new BundleJobSuspendXCommand(bundleId).call();

        BundleJobGetJPAExecutor bundleJobGetCmd = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetCmd);

        assertEquals(Job.Status.SUSPENDED, job.getStatus());

        Thread.sleep(3000);

        new BundleJobResumeXCommand(bundleId).call();

        job = jpaService.execute(bundleJobGetCmd);

        assertTrue(job.getStatus() == Job.Status.RUNNING || job.getStatus() == Job.Status.SUCCEEDED);

        Thread.sleep(3000);

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();

        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean bundle = jpaService.execute(new BundleJobGetJPAExecutor(bundleId));
                return bundle.getStatus().equals(Job.Status.SUCCEEDED);
            }
        });

        job = jpaService.execute(new BundleJobGetJPAExecutor(bundleId));
        assertEquals(Job.Status.SUCCEEDED, job.getStatus());
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
        addRecordToBundleActionTable(bundleId, "action1", 1, Job.Status.KILLED);
        addRecordToBundleActionTable(bundleId, "action2", 1, Job.Status.KILLED);

        addRecordToCoordJobTableWithBundle(bundleId, "action1", CoordinatorJob.Status.RUNNING, false, false, 2);
        addRecordToCoordJobTableWithBundle(bundleId, "action2", CoordinatorJob.Status.RUNNING, false, false, 2);

        final CoordinatorActionBean coordAction1_1 = addRecordToCoordActionTable("action1", 1,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);
        final CoordinatorActionBean coordAction1_2 = addRecordToCoordActionTable("action1", 2,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);

        final CoordinatorActionBean coordAction1_3 = addRecordToCoordActionTable("action2", 1,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);
        final CoordinatorActionBean coordAction1_4 = addRecordToCoordActionTable("action2", 2,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);

        this.addRecordToWfJobTable(coordAction1_1.getExternalId(), WorkflowJob.Status.RUNNING,
                WorkflowInstance.Status.RUNNING);
        this.addRecordToWfJobTable(coordAction1_2.getExternalId(), WorkflowJob.Status.RUNNING,
                WorkflowInstance.Status.RUNNING);
        this.addRecordToWfJobTable(coordAction1_3.getExternalId(), WorkflowJob.Status.RUNNING,
                WorkflowInstance.Status.RUNNING);
        this.addRecordToWfJobTable(coordAction1_4.getExternalId(), WorkflowJob.Status.RUNNING,
                WorkflowInstance.Status.RUNNING);

        new CoordKillXCommand("action1").call();
        new CoordKillXCommand("action2").call();

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

        BundleActionBean bundleAction1 = jpaService.execute(new BundleActionGetJPAExecutor(bundleId, "action1"));
        assertFalse(bundleAction1.isPending());

        CoordinatorJobBean coordJob1 = jpaService.execute(new CoordJobGetJPAExecutor("action1"));
        assertFalse(coordJob1.isPending());

        BundleActionBean bundleAction2 = jpaService.execute(new BundleActionGetJPAExecutor(bundleId, "action2"));
        assertFalse(bundleAction2.isPending());

        CoordinatorJobBean coordJob2 = jpaService.execute(new CoordJobGetJPAExecutor("action2"));
        assertFalse(coordJob2.isPending());
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
        addRecordToBundleActionTable(bundleId, "action1", 1, Job.Status.SUSPENDED);
        addRecordToBundleActionTable(bundleId, "action2", 1, Job.Status.SUSPENDED);

        addRecordToCoordJobTableWithBundle(bundleId, "action1", CoordinatorJob.Status.RUNNING, false, false, 2);
        addRecordToCoordJobTableWithBundle(bundleId, "action2", CoordinatorJob.Status.RUNNING, false, false, 2);

        final CoordinatorActionBean coordAction1_1 = addRecordToCoordActionTable("action1", 1,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);
        final CoordinatorActionBean coordAction1_2 = addRecordToCoordActionTable("action1", 2,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);

        final CoordinatorActionBean coordAction1_3 = addRecordToCoordActionTable("action2", 1,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);
        final CoordinatorActionBean coordAction1_4 = addRecordToCoordActionTable("action2", 2,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);

        this.addRecordToWfJobTable(coordAction1_1.getExternalId(), WorkflowJob.Status.RUNNING,
                WorkflowInstance.Status.RUNNING);
        this.addRecordToWfJobTable(coordAction1_2.getExternalId(), WorkflowJob.Status.RUNNING,
                WorkflowInstance.Status.RUNNING);
        this.addRecordToWfJobTable(coordAction1_3.getExternalId(), WorkflowJob.Status.RUNNING,
                WorkflowInstance.Status.RUNNING);
        this.addRecordToWfJobTable(coordAction1_4.getExternalId(), WorkflowJob.Status.RUNNING,
                WorkflowInstance.Status.RUNNING);

        new CoordSuspendXCommand("action1").call();
        new CoordSuspendXCommand("action2").call();

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

        BundleActionBean bundleAction1 = jpaService.execute(new BundleActionGetJPAExecutor(bundleId, "action1"));
        assertFalse(bundleAction1.isPending());

        CoordinatorJobBean coordJob1 = jpaService.execute(new CoordJobGetJPAExecutor("action1"));
        assertFalse(coordJob1.isPending());

        BundleActionBean bundleAction2 = jpaService.execute(new BundleActionGetJPAExecutor(bundleId, "action2"));
        assertFalse(bundleAction2.isPending());

        CoordinatorJobBean coordJob2 = jpaService.execute(new CoordJobGetJPAExecutor("action2"));
        assertFalse(coordJob2.isPending());
    }

    protected WorkflowJobBean addRecordToWfJobTable(String wfId, WorkflowJob.Status jobStatus,
            WorkflowInstance.Status instanceStatus) throws Exception {
        WorkflowApp app = new LiteWorkflowApp("testApp", "<workflow-app/>", new StartNodeDef("end"))
                .addNode(new EndNodeDef("end"));
        Configuration conf = new Configuration();
        Path appUri = new Path(getAppPath(), "workflow.xml");
        conf.set(OozieClient.APP_PATH, appUri.toString());
        conf.set(OozieClient.LOG_TOKEN, "testToken");
        conf.set(OozieClient.USER_NAME, getTestUser());

        WorkflowJobBean wfBean = createWorkflow(app, conf, "auth", jobStatus, instanceStatus);
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

        Date start = DateUtils.parseDateUTC("2009-02-01T01:00Z");
        Date end = DateUtils.parseDateUTC("2009-02-02T23:59Z");

        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, true, true, 3);
        // add a record with stale reference to coord job id
        addRecordToCoordActionTable("ABCD", 3, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        // add records with reference to correct job ids
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);

        runnable = new StatusTransitRunnable();
        runnable.run();
        Thread.sleep(1000);

        JPAService jpaService = Services.get().get(JPAService.class);
        CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(job.getId());
        CoordinatorJobBean coordJob = jpaService.execute(coordGetCmd);
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob.getStatus());
    }

}
