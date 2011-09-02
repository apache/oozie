package org.apache.oozie.command.bundle;

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
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
     * Test : Rerun bundle job
     *
     * @throws Exception
     */
    public void testBundleRerun1() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.SUCCEEDED);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetExecutor = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(job.getStatus(), Job.Status.SUCCEEDED);

        new BundleRerunXCommand(job.getId(), RestConstants.JOB_COORD_RERUN_DATE, "2009-12-15T01:00Z", false, true).call();

        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(job.getStatus(), Job.Status.RUNNING);
    }

}