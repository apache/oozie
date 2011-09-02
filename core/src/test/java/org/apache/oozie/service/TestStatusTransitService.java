/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
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
import org.apache.oozie.command.coord.CoordKillXCommand;
import org.apache.oozie.command.coord.CoordSuspendXCommand;
import org.apache.oozie.executor.jpa.BundleActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
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
    public void testCoordStatusTransitService1() throws Exception {

        Date start = DateUtils.parseDateUTC("2009-02-01T01:00Z");
        Date end = DateUtils.parseDateUTC("2009-02-02T23:59Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, true, 3);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml");
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml");
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml");

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
     * pending false and coordinator actions with pending false. Then, runs the CoordKillXCommand and
     * StatusTransitService runnable and ensures the job pending changes to false.
     *
     * @throws Exception
     */
    public void testCoordStatusTransitService2() throws Exception {
        final JPAService jpaService = Services.get().get(JPAService.class);
        Date start = DateUtils.parseDateUTC("2009-02-01T01:00Z");
        Date end = DateUtils.parseDateUTC("2009-02-02T23:59Z");
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, false, 1);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        final String wfJobId = wfJob.getId();
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", wfJobId, "RUNNING");

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
        assertEquals(true, coordJob.isPending());
        assertEquals(false, coordAction.isPending());

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();

        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorJobBean coordJobBean = jpaService.execute(coordJobGetCmd);
                return !coordJobBean.isPending();
            }
        });

        coordJob = jpaService.execute(coordJobGetCmd);
        assertEquals(false, coordJob.isPending());
    }

    @Override
    protected CoordinatorActionBean addRecordToCoordActionTable(String jobId, int actionNum,
            CoordinatorAction.Status status, String resourceXmlName) throws Exception {
        CoordinatorActionBean action = createCoordAction(jobId, actionNum, status, resourceXmlName);
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
    public void testBundleStatusTransitService1() throws Exception {
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
    public void testBundleStatusTransitService2() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.RUNNING, false);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        final String bundleId = job.getId();
        addRecordToBundleActionTable(bundleId, "action1", 0, Job.Status.RUNNING);
        addRecordToBundleActionTable(bundleId, "action2", 0, Job.Status.RUNNING);

        addRecordToCoordJobTableWithBundle(bundleId, "action1", CoordinatorJob.Status.RUNNING, true, 2);
        addRecordToCoordJobTableWithBundle(bundleId, "action2", CoordinatorJob.Status.RUNNING, true, 2);

        addRecordToCoordActionTable("action1", 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml");
        addRecordToCoordActionTable("action1", 2, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml");

        addRecordToCoordActionTable("action2", 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml");
        addRecordToCoordActionTable("action2", 2, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml");

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
     * The pending is updated bottom-up.
     * workflow job -> coordinator action -> coordinator job -> bundle action -> bundle job
     *
     * @throws Exception
     */
    public void testBundleStatusTransitService3() throws Exception {
        BundleJobBean bundleJob = this.addRecordToBundleJobTable(Job.Status.KILLED, true);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        final String bundleId = bundleJob.getId();
        addRecordToBundleActionTable(bundleId, "action1", 1, Job.Status.KILLED);
        addRecordToBundleActionTable(bundleId, "action2", 1, Job.Status.KILLED);

        addRecordToCoordJobTableWithBundle(bundleId, "action1", CoordinatorJob.Status.RUNNING, false, 2);
        addRecordToCoordJobTableWithBundle(bundleId, "action2", CoordinatorJob.Status.RUNNING, false, 2);

        final CoordinatorActionBean coordAction1_1 = addRecordToCoordActionTable("action1", 1, CoordinatorAction.Status.RUNNING, "coord-action-get.xml");
        final CoordinatorActionBean coordAction1_2 = addRecordToCoordActionTable("action1", 2, CoordinatorAction.Status.RUNNING, "coord-action-get.xml");

        final CoordinatorActionBean coordAction1_3 = addRecordToCoordActionTable("action2", 1, CoordinatorAction.Status.RUNNING, "coord-action-get.xml");
        final CoordinatorActionBean coordAction1_4 = addRecordToCoordActionTable("action2", 2, CoordinatorAction.Status.RUNNING, "coord-action-get.xml");

        this.addRecordToWfJobTable(coordAction1_1.getExternalId(), WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        this.addRecordToWfJobTable(coordAction1_2.getExternalId(), WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        this.addRecordToWfJobTable(coordAction1_3.getExternalId(), WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        this.addRecordToWfJobTable(coordAction1_4.getExternalId(), WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);

        new CoordKillXCommand("action1").call();
        new CoordKillXCommand("action2").call();

        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                WorkflowJobBean wfJob = jpaService.execute(new WorkflowJobGetJPAExecutor(coordAction1_4.getExternalId()));
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
     * The pending is updated bottom-up.
     * workflow job -> coordinator action -> coordinator job -> bundle action -> bundle job
     *
     * @throws Exception
     */
    public void testBundleStatusTransitService4() throws Exception {
        BundleJobBean bundleJob = this.addRecordToBundleJobTable(Job.Status.SUSPENDED, true);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        final String bundleId = bundleJob.getId();
        addRecordToBundleActionTable(bundleId, "action1", 1, Job.Status.SUSPENDED);
        addRecordToBundleActionTable(bundleId, "action2", 1, Job.Status.SUSPENDED);

        addRecordToCoordJobTableWithBundle(bundleId, "action1", CoordinatorJob.Status.RUNNING, false, 2);
        addRecordToCoordJobTableWithBundle(bundleId, "action2", CoordinatorJob.Status.RUNNING, false, 2);

        final CoordinatorActionBean coordAction1_1 = addRecordToCoordActionTable("action1", 1, CoordinatorAction.Status.RUNNING, "coord-action-get.xml");
        final CoordinatorActionBean coordAction1_2 = addRecordToCoordActionTable("action1", 2, CoordinatorAction.Status.RUNNING, "coord-action-get.xml");

        final CoordinatorActionBean coordAction1_3 = addRecordToCoordActionTable("action2", 1, CoordinatorAction.Status.RUNNING, "coord-action-get.xml");
        final CoordinatorActionBean coordAction1_4 = addRecordToCoordActionTable("action2", 2, CoordinatorAction.Status.RUNNING, "coord-action-get.xml");

        this.addRecordToWfJobTable(coordAction1_1.getExternalId(), WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        this.addRecordToWfJobTable(coordAction1_2.getExternalId(), WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        this.addRecordToWfJobTable(coordAction1_3.getExternalId(), WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        this.addRecordToWfJobTable(coordAction1_4.getExternalId(), WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);

        new CoordSuspendXCommand("action1").call();
        new CoordSuspendXCommand("action2").call();

        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                WorkflowJobBean wfJob = jpaService.execute(new WorkflowJobGetJPAExecutor(coordAction1_4.getExternalId()));
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

    /**
     * Test : all bundle actions are done - update bundle job's pending flag to 0;
     *
     * @throws Exception
     */
    public void testBundleStatusTransitServiceX() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.RUNNING, false);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        job.setPending();
        jpaService.execute(new BundleJobUpdateJPAExecutor(job));

        final String jobId = job.getId();
        addRecordToBundleActionTable(jobId, "1", 0, Job.Status.PAUSED);
        addRecordToBundleActionTable(jobId, "2", 0, Job.Status.PAUSED);
        addRecordToBundleActionTable(jobId, "3", 0, Job.Status.PAUSED);

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();

        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean job1 = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
                return job1.getPending() == 0;
            }
        });

        job = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
        assertEquals(0, job.getPending());
    }

    /**
     * Test : none of bundle actions are done - bundle job's pending flag remains 1;
     *
     * @throws Exception
     */
    public void testBundleStatusTransitServiceXX() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.RUNNING, false);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        job.setPending();
        jpaService.execute(new BundleJobUpdateJPAExecutor(job));

        final String jobId = job.getId();
        addRecordToBundleActionTable(jobId, "1", 1, Job.Status.PAUSED);
        addRecordToBundleActionTable(jobId, "2", 1, Job.Status.PAUSED);
        addRecordToBundleActionTable(jobId, "3", 2, Job.Status.PAUSED);

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();

        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean job1 = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
                return job1.getPending() == 1;
            }
        });

        job = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
        assertEquals(job.getPending(), 1);
    }

    /**
     * Test : not all bundle actions are done - bundle job's pending flag remains 1;
     *
     * @throws Exception
     */
    public void testBundleStatusTransitServiceXXX() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.RUNNING, false);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        job.setPending();
        jpaService.execute(new BundleJobUpdateJPAExecutor(job));

        final String jobId = job.getId();
        addRecordToBundleActionTable(jobId, "1", 0, Job.Status.PAUSED);
        addRecordToBundleActionTable(jobId, "2", 1, Job.Status.PAUSED);
        addRecordToBundleActionTable(jobId, "3", 2, Job.Status.PAUSED);

        Runnable runnable = new StatusTransitRunnable();
        runnable.run();

        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean job1 = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
                return job1.getPending() == 1;
            }
        });

        job = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
        assertEquals(job.getPending(), 1);
    }

    protected WorkflowJobBean addRecordToWfJobTable(String wfId, WorkflowJob.Status jobStatus, WorkflowInstance.Status instanceStatus)
            throws Exception {
        WorkflowApp app = new LiteWorkflowApp("testApp", "<workflow-app/>", new StartNodeDef("end"))
                .addNode(new EndNodeDef("end"));
        Configuration conf = new Configuration();
        Path appUri = new Path(getAppPath(), "workflow.xml");
        conf.set(OozieClient.APP_PATH, appUri.toString());
        conf.set(OozieClient.LOG_TOKEN, "testToken");
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set(OozieClient.GROUP_NAME, getTestGroup());
        injectKerberosInfo(conf);
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

}