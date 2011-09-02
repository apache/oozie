package org.apache.oozie.executor.jpa;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestCoordActionRemoveJPAExecutor extends XDataTestCase {
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

    public void testCoordActionRemove() throws Exception {
        int actionNum = 1;
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), actionNum,
                CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        _testCoordActionRemove(job.getId(), action.getId());
    }

    private void _testCoordActionRemove(String jobId, String actionId) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordActionRemoveJPAExecutor coordRmvCmd = new CoordActionRemoveJPAExecutor(actionId);
        jpaService.execute(coordRmvCmd);

        try {
            CoordActionGetJPAExecutor coordGetCmd = new CoordActionGetJPAExecutor(actionId);
            CoordinatorActionBean newAction = jpaService.execute(coordGetCmd);
            fail("Action " + actionId + " Should be removed");
        }
        catch (JPAExecutorException je) {

        }
    }

}
