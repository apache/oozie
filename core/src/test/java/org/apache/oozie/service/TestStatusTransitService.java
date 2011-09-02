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

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.coord.CoordKillXCommand;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.workflow.WorkflowInstance;
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
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.RUNNING,
                "coord-action-get.xml", wfJobId, "RUNNING");

        new CoordKillXCommand(coordJob.getId()).call();

        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                WorkflowJobGetJPAExecutor wfGetCmd = new WorkflowJobGetJPAExecutor(wfJobId);
                WorkflowJobBean wfBean = jpaService.execute(wfGetCmd);
                return wfBean.getStatusStr().equals("KILLED");
            }
        });

        assertNotNull(jpaService);
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(coordJob.getId());
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
        Thread.sleep(1000);

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
     * Test : all bundle actions are done - update bundle job's pending flag to 0;
     *
     * @throws Exception
     */
    public void testBundleStatusTransitService1() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.RUNNING);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        job.setPending();
        jpaService.execute(new BundleJobUpdateJPAExecutor(job));

        final String jobId = job.getId();
        addRecordToBundleActionTable(jobId, "1", 0, Job.Status.PAUSED);
        addRecordToBundleActionTable(jobId, "2", 0, Job.Status.PAUSED);
        addRecordToBundleActionTable(jobId, "3", 0, Job.Status.PAUSED);

        waitFor(120 * 1000, new Predicate() {
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
    public void testBundleStatusTransitService2() throws Exception {
        cleanUpDBTables();
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.RUNNING);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        job.setPending();
        jpaService.execute(new BundleJobUpdateJPAExecutor(job));

        final String jobId = job.getId();
        addRecordToBundleActionTable(jobId, "1", 1, Job.Status.PAUSED);
        addRecordToBundleActionTable(jobId, "2", 1, Job.Status.PAUSED);
        addRecordToBundleActionTable(jobId, "3", 2, Job.Status.PAUSED);

        waitFor(120 * 1000, new Predicate() {
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
    public void testBundleStatusTransitService3() throws Exception {
        cleanUpDBTables();
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.RUNNING);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        job.setPending();
        jpaService.execute(new BundleJobUpdateJPAExecutor(job));

        final String jobId = job.getId();
        addRecordToBundleActionTable(jobId, "1", 0, Job.Status.PAUSED);
        addRecordToBundleActionTable(jobId, "2", 1, Job.Status.PAUSED);
        addRecordToBundleActionTable(jobId, "3", 2, Job.Status.PAUSED);

        waitFor(120 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean job1 = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
                return job1.getPending() == 1;
            }
        });

        job = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
        assertEquals(job.getPending(), 1);
    }

    /**
     * Test : not all bundle actions are succeeded - bundle job's status will be updated to succeeded.
     *
     * @throws Exception
     */
    public void testBundleStatusTransitService4() throws Exception {
        cleanUpDBTables();
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.RUNNING);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        final String jobId = job.getId();
        addRecordToBundleActionTable(jobId, "1", 0, Job.Status.SUCCEEDED);
        addRecordToBundleActionTable(jobId, "2", 1, Job.Status.SUCCEEDED);
        addRecordToBundleActionTable(jobId, "3", 2, Job.Status.SUCCEEDED);

        waitFor(120 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean job1 = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
                return job1.getStatus().equals(Job.Status.SUCCEEDED);
            }
        });

        job = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
        assertEquals(job.getStatus(), Job.Status.SUCCEEDED);
    }

}