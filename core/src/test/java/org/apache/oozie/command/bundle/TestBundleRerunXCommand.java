package org.apache.oozie.command.bundle;

import java.util.Date;

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestBundleRerunXCommand extends XDataTestCase {

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
     * Test : Rerun bundle job for dateScope
     *
     * @throws Exception
     */
    public void testBundleRerun1() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.SUCCEEDED, false);
        this.addRecordToBundleActionTable(job.getId(), "action1", 0, Job.Status.SUCCEEDED);
        this.addRecordToBundleActionTable(job.getId(), "action2", 0, Job.Status.SUCCEEDED);
        addRecordToCoordJobTable("action1", CoordinatorJob.Status.SUCCEEDED, false, false);
        addRecordToCoordJobTable("action2", CoordinatorJob.Status.SUCCEEDED, false, false);

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
        this.addRecordToBundleActionTable(job.getId(), "action1", 0, Job.Status.SUCCEEDED);
        this.addRecordToBundleActionTable(job.getId(), "action2", 0, Job.Status.SUCCEEDED);
        addRecordToCoordJobTable("action1", CoordinatorJob.Status.SUCCEEDED, false, false);
        addRecordToCoordJobTable("action2", CoordinatorJob.Status.SUCCEEDED, false, false);

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
        assertEquals(Job.Status.RUNNING, job.getStatus());
        assertNull(job.getPauseTime());
    }

    /**
     * Test : Rerun suspended bundle job
     *
     * @throws Exception
     */
    public void testBundleRerunInSuspended() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.SUSPENDED, false);
        this.addRecordToBundleActionTable(job.getId(), "action1", 0, Job.Status.SUSPENDED);
        this.addRecordToBundleActionTable(job.getId(), "action2", 0, Job.Status.SUSPENDED);
        addRecordToCoordJobTable("action1", CoordinatorJob.Status.SUSPENDED, false, false);
        addRecordToCoordJobTable("action2", CoordinatorJob.Status.SUSPENDED, false, false);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetExecutor = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.SUSPENDED, job.getStatus());

        new BundleRerunXCommand(job.getId(), "action2", null, false, true).call();

        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.RUNNING, job.getStatus());
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