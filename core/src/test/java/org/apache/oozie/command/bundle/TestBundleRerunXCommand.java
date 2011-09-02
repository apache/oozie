package org.apache.oozie.command.bundle;

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
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
        addRecordToCoordJobTable("action1", CoordinatorJob.Status.SUCCEEDED, false);
        addRecordToCoordJobTable("action2", CoordinatorJob.Status.SUCCEEDED, false);

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
        addRecordToCoordJobTable("action1", CoordinatorJob.Status.SUCCEEDED, false);
        addRecordToCoordJobTable("action2", CoordinatorJob.Status.SUCCEEDED, false);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetExecutor = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.SUCCEEDED, job.getStatus());

        new BundleRerunXCommand(job.getId(), "action1", null, false, true).call();

        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.RUNNING, job.getStatus());
    }

    protected CoordinatorJobBean addRecordToCoordJobTable(String coordId, CoordinatorJob.Status status, boolean pending) throws Exception {
        CoordinatorJobBean coordJob = createCoordJob(status, pending);
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