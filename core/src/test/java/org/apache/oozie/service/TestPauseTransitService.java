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
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.executor.jpa.BundleActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.PauseTransitService.PauseTransitRunnable;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;

public class TestPauseTransitService extends XDataTestCase {
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
     * Test : Pause a PREP bundle, then delay its pausetime to unpause it.
     *
     * @throws Exception
     */
    public void testPauseUnpause1() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.PREP, false);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        job.setPauseTime(new Date(new Date().getTime() - 30 * 1000));
        job.setKickoffTime(new Date(new Date().getTime() + 3600 * 1000));
        jpaService.execute(new BundleJobUpdateJPAExecutor(job));

        Runnable pauseStartRunnable = new PauseTransitRunnable();
        pauseStartRunnable.run();

        final String jobId = job.getId();
        waitFor(10 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean job1 = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
                return job1.getStatus() == Job.Status.PREPPAUSED;
            }
        });

        job = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
        assertEquals(Job.Status.PREPPAUSED, job.getStatus());

        job.setPauseTime(new Date(new Date().getTime() + 3600 * 1000));
        jpaService.execute(new BundleJobUpdateJPAExecutor(job));

        pauseStartRunnable.run();

        waitFor(10 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean job1 = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
                return job1.getStatus() == Job.Status.PREP;
            }
        });

        job = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
        assertEquals(Job.Status.PREP, job.getStatus());
    }

    /**
     * Test : Pause a PREP bundle, then reset its pausetime to null to unpause it.
     *
     * @throws Exception
     */
    public void testPauseUnpause2() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.PREP, false);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        job.setPauseTime(new Date(new Date().getTime() - 30 * 1000));
        job.setKickoffTime(new Date(new Date().getTime() + 3600 * 1000));
        jpaService.execute(new BundleJobUpdateJPAExecutor(job));

        Runnable pauseStartRunnable = new PauseTransitRunnable();
        pauseStartRunnable.run();

        final String jobId = job.getId();
        waitFor(10 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean job1 = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
                return job1.getStatus() == Job.Status.PREPPAUSED;
            }
        });

        job = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
        assertEquals(Job.Status.PREPPAUSED, job.getStatus());

        job.setPauseTime(null);
        jpaService.execute(new BundleJobUpdateJPAExecutor(job));

        pauseStartRunnable.run();

        waitFor(10 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean job1 = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
                return job1.getStatus() == Job.Status.PREP;
            }
        });

        job = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
        assertEquals(Job.Status.PREP, job.getStatus());
    }

    /**
     * Test : Pause a RUNNING bundle, then check bundle action has been updated to PAUSED by BundleStatusUpdateXCommand
     *
     * @throws Exception
     */
    public void testPauseBundleAndCoordinator() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.RUNNING, false);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        Date pauseTime = new Date(new Date().getTime() - 30 * 1000);
        job.setPauseTime(pauseTime);
        jpaService.execute(new BundleJobUpdateJPAExecutor(job));

        BundleActionBean bundleAction1 = this.addRecordToBundleActionTable(job.getId(), "action1", 0, Job.Status.RUNNING);
        BundleActionBean bundleAction2 = this.addRecordToBundleActionTable(job.getId(), "action2", 0, Job.Status.RUNNING);

        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);

        CoordinatorJobBean coordJob1 = addRecordToCoordJobTable("action1", CoordinatorJob.Status.RUNNING, start, end, false);
        CoordinatorJobBean coordJob2 = addRecordToCoordJobTable("action2", CoordinatorJob.Status.RUNNING, start, end, false);

        coordJob1.setPauseTime(pauseTime);
        coordJob1.setBundleId(job.getId());
        jpaService.execute(new CoordJobUpdateJPAExecutor(coordJob1));
        coordJob2.setPauseTime(pauseTime);
        coordJob2.setBundleId(job.getId());
        jpaService.execute(new CoordJobUpdateJPAExecutor(coordJob2));

        BundleJobGetJPAExecutor bundleJobGetExecutor = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.RUNNING, job.getStatus());

        Runnable pauseStartRunnable = new PauseTransitRunnable();
        pauseStartRunnable.run();

        final String jobId = job.getId();
        waitFor(10 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean bJob1 = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
                return bJob1.getStatus() == Job.Status.PAUSED;
            }
        });

        final String coordJobId1 = coordJob1.getId();
        final String coordJobId2 = coordJob2.getId();
        waitFor(10 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorJobBean cJob1 = jpaService.execute(new CoordJobGetJPAExecutor(coordJobId1));
                return cJob1.getStatus() == Job.Status.PAUSED;
            }
        });

        job = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
        assertEquals(Job.Status.PAUSED, job.getStatus());

        coordJob1 = jpaService.execute(new CoordJobGetJPAExecutor(coordJobId1));
        assertEquals(Job.Status.PAUSED, coordJob1.getStatus());

        coordJob2 = jpaService.execute(new CoordJobGetJPAExecutor(coordJobId2));
        assertEquals(Job.Status.PAUSED, coordJob2.getStatus());

        bundleAction1 = jpaService.execute(new BundleActionGetJPAExecutor(job.getId(), "action1"));
        assertEquals(Job.Status.PAUSED, bundleAction1.getStatus());

        bundleAction2 = jpaService.execute(new BundleActionGetJPAExecutor(job.getId(), "action2"));
        assertEquals(Job.Status.PAUSED, bundleAction2.getStatus());

    }

    /**
     * Test : Unpause a PAUSED bundle, then check bundle action has been updated to RUNNING by BundleStatusUpdateXCommand
     *
     * @throws Exception
     */
    public void testUnpauseBundleAndCoordinator() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.PAUSED, false);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        job.setPauseTime(null);
        jpaService.execute(new BundleJobUpdateJPAExecutor(job));

        BundleActionBean bundleAction1 = this.addRecordToBundleActionTable(job.getId(), "action1", 0, Job.Status.PAUSED);
        BundleActionBean bundleAction2 = this.addRecordToBundleActionTable(job.getId(), "action2", 0, Job.Status.PAUSED);

        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);

        CoordinatorJobBean coordJob1 = addRecordToCoordJobTable("action1", CoordinatorJob.Status.PAUSED, start, end, false);
        CoordinatorJobBean coordJob2 = addRecordToCoordJobTable("action2", CoordinatorJob.Status.PAUSED, start, end, false);

        coordJob1.setPauseTime(null);
        coordJob1.setBundleId(job.getId());
        jpaService.execute(new CoordJobUpdateJPAExecutor(coordJob1));
        coordJob2.setPauseTime(null);
        coordJob2.setBundleId(job.getId());
        jpaService.execute(new CoordJobUpdateJPAExecutor(coordJob2));

        BundleJobGetJPAExecutor bundleJobGetExecutor = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.PAUSED, job.getStatus());

        Runnable pauseStartRunnable = new PauseTransitRunnable();
        pauseStartRunnable.run();

        final String jobId = job.getId();
        waitFor(10 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean bJob1 = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
                return bJob1.getStatus() == Job.Status.RUNNING;
            }
        });

        final String coordJobId1 = coordJob1.getId();
        final String coordJobId2 = coordJob2.getId();
        waitFor(10 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorJobBean cJob1 = jpaService.execute(new CoordJobGetJPAExecutor(coordJobId1));
                return cJob1.getStatus() == Job.Status.RUNNING;
            }
        });

        job = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
        assertEquals(Job.Status.RUNNING, job.getStatus());

        coordJob1 = jpaService.execute(new CoordJobGetJPAExecutor(coordJobId1));
        assertEquals(Job.Status.RUNNING, coordJob1.getStatus());

        coordJob2 = jpaService.execute(new CoordJobGetJPAExecutor(coordJobId2));
        assertEquals(Job.Status.RUNNING, coordJob2.getStatus());

        bundleAction1 = jpaService.execute(new BundleActionGetJPAExecutor(job.getId(), "action1"));
        assertEquals(Job.Status.RUNNING, bundleAction1.getStatus());

        bundleAction2 = jpaService.execute(new BundleActionGetJPAExecutor(job.getId(), "action2"));
        assertEquals(Job.Status.RUNNING, bundleAction2.getStatus());

    }


    /**
     * Test : Pause a RUNNING coordinator, but set oozie.service.StatusTransitService.backward.support.for.coord.status=true
     * and use uri:oozie:coordinator:0.1 namespace, the status should not be changed to PAUSED
     *
     * @throws Exception
     */
    public void testPauseCoordinatorForBackwardSupport() throws Exception {
        Services.get().destroy();
        setSystemProperty(StatusTransitService.CONF_BACKWARD_SUPPORT_FOR_COORD_STATUS, "true");
        services = new Services();
        setClassesToBeExcluded(services.getConf(), excludedServices);
        services.init();

        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        Date pauseTime = new Date(new Date().getTime() - 30 * 1000);

        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);

        CoordinatorJobBean coordJob1 = addRecordToCoordJobTable("action1", CoordinatorJob.Status.RUNNING, start, end, false);
        CoordinatorJobBean coordJob2 = addRecordToCoordJobTable("action2", CoordinatorJob.Status.RUNNING, start, end, false);

        coordJob1.setAppNamespace(SchemaService.COORDINATOR_NAMESPACE_URI_1);
        coordJob1.setPauseTime(pauseTime);
        jpaService.execute(new CoordJobUpdateJPAExecutor(coordJob1));
        coordJob2.setAppNamespace(SchemaService.COORDINATOR_NAMESPACE_URI_1);
        coordJob2.setPauseTime(pauseTime);
        jpaService.execute(new CoordJobUpdateJPAExecutor(coordJob2));

        Runnable pauseStartRunnable = new PauseTransitRunnable();
        pauseStartRunnable.run();

        final String coordJobId1 = coordJob1.getId();
        final String coordJobId2 = coordJob2.getId();
        waitFor(10 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorJobBean cJob1 = jpaService.execute(new CoordJobGetJPAExecutor(coordJobId1));
                return cJob1.getStatus() == Job.Status.RUNNING;
            }
        });

        coordJob1 = jpaService.execute(new CoordJobGetJPAExecutor(coordJobId1));
        assertEquals(Job.Status.RUNNING, coordJob1.getStatus());

        coordJob2 = jpaService.execute(new CoordJobGetJPAExecutor(coordJobId2));
        assertEquals(Job.Status.RUNNING, coordJob2.getStatus());


    }

    /**
     * Test : Start a PREP bundle when its kickoff time reaches.
     *
     * @throws Exception
     */
    public void testStart1() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.PREP, false);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        job.setKickoffTime(new Date(new Date().getTime() - 30 * 1000));
        jpaService.execute(new BundleJobUpdateJPAExecutor(job));

        Runnable pauseStartRunnable = new PauseTransitRunnable();
        pauseStartRunnable.run();

        final String jobId = job.getId();
        waitFor(10 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean job1 = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
                return job1.getStatus() == Job.Status.RUNNING;
            }
        });

        job = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
        assertEquals(Job.Status.RUNNING, job.getStatus());
    }

    /**
     * Test : Start a PREP bundle when its kickoff time is a past time.
     *
     * @throws Exception
     */
    public void testStart2() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.PREP, false);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        job.setKickoffTime(new Date(new Date().getTime() - 30 * 1000));
        jpaService.execute(new BundleJobUpdateJPAExecutor(job));

        Runnable pauseStartRunnable = new PauseTransitRunnable();
        pauseStartRunnable.run();

        final String jobId = job.getId();
        waitFor(10 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean job1 = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
                return job1.getStatus() == Job.Status.RUNNING;
            }
        });

        job = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
        assertEquals(Job.Status.RUNNING, job.getStatus());
    }

    protected CoordinatorJobBean addRecordToCoordJobTable(String coordId, CoordinatorJob.Status status, Date start,
            Date end, boolean pending) throws Exception {
        CoordinatorJobBean coordJob = createCoordJob(status, start, end, pending, false, 0);
        coordJob.setId(coordId);
        coordJob.setAppName(coordId);
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordJobInsertJPAExecutor coordInsertCmd = new CoordJobInsertJPAExecutor(coordJob);
            jpaService.execute(coordInsertCmd);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test coord job record to table");
            throw je;
        }

        return coordJob;

    }

}
