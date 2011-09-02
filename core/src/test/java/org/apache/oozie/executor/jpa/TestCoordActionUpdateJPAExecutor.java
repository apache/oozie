package org.apache.oozie.executor.jpa;

import java.util.List;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestCoordActionUpdateJPAExecutor extends XDataTestCase {
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

    public void testCoordActionUpdate() throws Exception {
        int actionNum = 1;
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), actionNum,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml");
        _testCoordActionUpdate(action);
    }

    private void _testCoordActionUpdate(CoordinatorActionBean action) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        action.setStatus(CoordinatorAction.Status.SUCCEEDED);
        CoordActionUpdateJPAExecutor coordUpdCmd = new CoordActionUpdateJPAExecutor(action);
        jpaService.execute(coordUpdCmd);

        CoordActionGetJPAExecutor coordGetCmd = new CoordActionGetJPAExecutor(action.getId());
        CoordinatorActionBean newAction = jpaService.execute(coordGetCmd);

        assertNotNull(newAction);
        assertEquals(newAction.getStatus(), CoordinatorAction.Status.SUCCEEDED);
    }

}
