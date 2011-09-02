package org.apache.oozie.executor.jpa;

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestCoordJobDeleteJPAExecutor extends XDataTestCase {
    Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        cleanUpDBTables();
        LocalOozie.start();
    }

    @Override
    protected void tearDown() throws Exception {
        LocalOozie.stop();
        services.destroy();
        super.tearDown();
    }

    public void testCoordJobDelete() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING);
        _testCoordJobDelete(job.getId());
    }

    private void _testCoordJobDelete(String jobId) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobDeleteJPAExecutor coordDelCmd = new CoordJobDeleteJPAExecutor(jobId);
        jpaService.execute(coordDelCmd);
        try {
            CoordJobGetJPAExecutor coordGetCmd = new CoordJobGetJPAExecutor(jobId);
            CoordinatorJobBean ret = jpaService.execute(coordGetCmd);
            fail("Job should not be there");
        }
        catch (Exception ex) {
        }
    }

}
