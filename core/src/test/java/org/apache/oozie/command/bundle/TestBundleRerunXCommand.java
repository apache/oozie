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

package org.apache.oozie.command.bundle;

import java.util.Date;

import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.executor.jpa.BundleActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.StatusTransitService;
import org.apache.oozie.test.XDataTestCase;

public class TestBundleRerunXCommand extends XDataTestCase {

    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    /**
     * Test : Rerun bundle job for dateScope
     *
     * @throws Exception
     */
    public void testBundleRerun1() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.SUCCEEDED, false);
        CoordinatorJobBean coord1 = addRecordToCoordJobTable("action1", CoordinatorJob.Status.SUCCEEDED, false, false);
        CoordinatorJobBean coord2 = addRecordToCoordJobTable("action2", CoordinatorJob.Status.SUCCEEDED, false, false);
        this.addRecordToBundleActionTable(job.getId(), coord1.getId(), "action1", 0, Job.Status.SUCCEEDED);
        this.addRecordToBundleActionTable(job.getId(), coord2.getId(), "action2", 0, Job.Status.SUCCEEDED);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetExecutor = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.SUCCEEDED, job.getStatus());

        new BundleRerunXCommand(job.getId(), null, "2009-02-01T00:00Z", false, true).call();

        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.RUNNING, job.getStatus());
    }


    /**
     * Test : Rerun bundle job for coordScope
     *
     * @throws Exception
     */
    public void testBundleRerun2() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.SUCCEEDED, false);
        CoordinatorJobBean coord1 = addRecordToCoordJobTable("action1", CoordinatorJob.Status.SUCCEEDED, false, false);
        CoordinatorJobBean coord2 = addRecordToCoordJobTable("action2", CoordinatorJob.Status.SUCCEEDED, false, false);
        this.addRecordToBundleActionTable(job.getId(), coord1.getId(), "action1", 0, Job.Status.SUCCEEDED);
        this.addRecordToBundleActionTable(job.getId(), coord2.getId(), "action2", 0, Job.Status.SUCCEEDED);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetExecutor = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.SUCCEEDED, job.getStatus());

        new BundleRerunXCommand(job.getId(), "action1", null, false, true).call();

        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.RUNNING, job.getStatus());
    }


    /**
     * Test : Rerun bundle job with a killed coordinator. Make sure the bundle action pending flag is reset.
     *
     * @throws Exception
     */
    public void testBundleRerunKilledCoordinator() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.DONEWITHERROR, false);
        String bundleId = job.getId();
        addRecordToBundleActionTable(bundleId, "action1", 0, Job.Status.KILLED);
        addRecordToCoordJobTableWithBundle(bundleId, "action1", CoordinatorJob.Status.KILLED, false, false, 1);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        new BundleRerunXCommand(bundleId, "action1", null, false, true).call();

        sleep(1000);

        BundleActionGetJPAExecutor bundleActionJPA = new BundleActionGetJPAExecutor(bundleId, "action1");
        BundleActionBean ba = jpaService.execute(bundleActionJPA);
        assertEquals(0, ba.getPending());
    }

    /**
     * Test : Rerun a DONEWITHERROR bundle job. Status should
     * change to RUNNINGWITHERROR
     *
     * @throws Exception
     */
    public void testBundleRerunWithError() throws Exception {
        Services.get().destroy();
        setSystemProperty(StatusTransitService.CONF_BACKWARD_SUPPORT_FOR_STATES_WITHOUT_ERROR, "false");
        services = new Services();
        services.init();
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.DONEWITHERROR, false);
        CoordinatorJobBean coord1 = addRecordToCoordJobTable("action1", CoordinatorJob.Status.SUCCEEDED, false, false);
        CoordinatorJobBean coord2 = addRecordToCoordJobTable("action2", CoordinatorJob.Status.FAILED, false, false);
        this.addRecordToBundleActionTable(job.getId(), coord1.getId(), "action1", 0, Job.Status.SUCCEEDED);
        this.addRecordToBundleActionTable(job.getId(), coord2.getId(), "action2", 0, Job.Status.FAILED);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetExecutor = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.DONEWITHERROR, job.getStatus());

        new BundleRerunXCommand(job.getId(), null, "2009-02-01T00:00Z", false, true).call();

        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.RUNNINGWITHERROR, job.getStatus());
    }


    /**
     * Test : Rerun PREP bundle job
     *
     * @throws Exception
     */
    public void testBundleRerunInPrep() throws Exception {
        Date curr = new Date();
        Date pauseTime = new Date(curr.getTime() - 1000);
        BundleJobBean job = this.addRecordToBundleJobTableWithPausedTime(Job.Status.PREP, false, pauseTime);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetExecutor = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.PREP, job.getStatus());

        new BundleRerunXCommand(job.getId(), "action2", null, false, true).call();

        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.PREP, job.getStatus());
    }

    /**
     * Test : Rerun paused bundle job
     *
     * @throws Exception
     */
    public void testBundleRerunInPaused() throws Exception {
        Date curr = new Date();
        Date pauseTime = new Date(curr.getTime() - 1000);
        BundleJobBean job = this.addRecordToBundleJobTableWithPausedTime(Job.Status.PAUSED, false, pauseTime);
        this.addRecordToBundleActionTable(job.getId(), "action1", 0, Job.Status.SUCCEEDED);
        this.addRecordToBundleActionTable(job.getId(), "action2", 0, Job.Status.PAUSED);
        addRecordToCoordJobTable("action1", CoordinatorJob.Status.SUCCEEDED, false, false);
        addRecordToCoordJobTable("action2", CoordinatorJob.Status.PAUSED, false, false);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetExecutor = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.PAUSED, job.getStatus());

        new BundleRerunXCommand(job.getId(), "action2", null, false, true).call();

        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.PAUSED, job.getStatus());
        assertNotNull(job.getPauseTime());
        assertFalse(job.isPending());
    }

    /**
     * Test : Rerun PAUSEDINERROR bundle job. Status shouldn't change.
     *
     * @throws Exception
     */
    public void testBundleRerunInPausedWithError() throws Exception {
        Date curr = new Date();
        Date pauseTime = new Date(curr.getTime() - 1000);
        Services.get().destroy();
        setSystemProperty(StatusTransitService.CONF_BACKWARD_SUPPORT_FOR_STATES_WITHOUT_ERROR, "false");
        services = new Services();
        services.init();
        BundleJobBean job = this.addRecordToBundleJobTableWithPausedTime(Job.Status.PAUSEDWITHERROR, false, pauseTime);
        this.addRecordToBundleActionTable(job.getId(), "action1", 0, Job.Status.FAILED);
        this.addRecordToBundleActionTable(job.getId(), "action2", 0, Job.Status.PAUSED);
        addRecordToCoordJobTable("action1", CoordinatorJob.Status.FAILED, false, false);
        addRecordToCoordJobTable("action2", CoordinatorJob.Status.PAUSED, false, false);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetExecutor = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.PAUSEDWITHERROR, job.getStatus());

        new BundleRerunXCommand(job.getId(), "action2", null, false, true).call();

        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.PAUSEDWITHERROR, job.getStatus());
        assertNotNull(job.getPauseTime());
        assertFalse(job.isPending());
    }


    /**
     * Test : Rerun suspended bundle job
     *
     * @throws Exception
     */
    public void testBundleRerunInSuspended() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.SUSPENDED, false);
        CoordinatorJobBean coord1 = addRecordToCoordJobTable("action1", CoordinatorJob.Status.SUSPENDED, false, false);
        CoordinatorJobBean coord2 = addRecordToCoordJobTable("action2", CoordinatorJob.Status.SUSPENDED, false, false);
        this.addRecordToBundleActionTable(job.getId(), coord1.getId(), "action1", 0, Job.Status.SUSPENDED);
        this.addRecordToBundleActionTable(job.getId(), coord2.getId(), "action2", 0, Job.Status.SUSPENDED);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetExecutor = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.SUSPENDED, job.getStatus());

        new BundleRerunXCommand(job.getId(), "action2", null, false, true).call();

        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.RUNNING, job.getStatus());
    }

    /**
     * Test : Rerun SUSPENDEDINERROR bundle job
     *
     * @throws Exception
     */
    public void testBundleRerunInSuspendedWithError() throws Exception {
        Services.get().destroy();
        setSystemProperty(StatusTransitService.CONF_BACKWARD_SUPPORT_FOR_STATES_WITHOUT_ERROR, "false");
        services = new Services();
        services.init();
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.SUSPENDEDWITHERROR, false);
        CoordinatorJobBean coord1 = addRecordToCoordJobTable("action1", CoordinatorJob.Status.SUSPENDED, false, false);
        CoordinatorJobBean coord2 = addRecordToCoordJobTable("action2", CoordinatorJob.Status.SUSPENDEDWITHERROR, false, false);
        this.addRecordToBundleActionTable(job.getId(), coord1.getId(), "action1", 0, Job.Status.SUSPENDED);
        this.addRecordToBundleActionTable(job.getId(), coord2.getId(), "action2", 0, Job.Status.SUSPENDEDWITHERROR);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetExecutor = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.SUSPENDEDWITHERROR, job.getStatus());

        new BundleRerunXCommand(job.getId(), "action2", null, false, true).call();

        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.RUNNINGWITHERROR, job.getStatus());
    }


    protected BundleJobBean addRecordToBundleJobTableWithPausedTime(Job.Status jobStatus, boolean pending, Date pausedTime) throws Exception {
        BundleJobBean bundle = createBundleJob(jobStatus, pending);
        bundle.setPauseTime(pausedTime);
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            BundleJobInsertJPAExecutor bundleInsertjpa = new BundleJobInsertJPAExecutor(bundle);
            jpaService.execute(bundleInsertjpa);
        }
        catch (JPAExecutorException ce) {
            ce.printStackTrace();
            fail("Unable to insert the test bundle job record to table");
            throw ce;
        }
        return bundle;
    }

    protected CoordinatorJobBean addRecordToCoordJobTable(String coordId, CoordinatorJob.Status status, boolean pending, boolean doneMatd) throws Exception {
        CoordinatorJobBean coordJob = createCoordJob(status, pending, doneMatd);
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
