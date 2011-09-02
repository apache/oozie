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
package org.apache.oozie.command.coord;

import java.util.Date;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestCoordKillXCommand extends XDataTestCase {
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
     * Test : kill job and action (READY) successfully
     *
     * @throws Exception
     */
    public void testCoordKillSuccess1() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.READY, "coord-action-get.xml");

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(job.getId());
        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(action.getId());

        job = jpaService.execute(coordJobGetCmd);
        action = jpaService.execute(coordActionGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.RUNNING);
        assertEquals(action.getStatus(), CoordinatorAction.Status.READY);

        new CoordKillXCommand(job.getId()).call();

        job = jpaService.execute(coordJobGetCmd);
        action = jpaService.execute(coordActionGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.KILLED);
        assertEquals(action.getStatus(), CoordinatorAction.Status.KILLED);
    }

    /**
     * Test : kill job and action (RUNNING) successfully
     *
     * @throws Exception
     */
    public void testCoordKillSuccess2() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.RUNNING, "coord-action-get.xml");

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(job.getId());
        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(action.getId());

        job = jpaService.execute(coordJobGetCmd);
        action = jpaService.execute(coordActionGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.RUNNING);
        assertEquals(action.getStatus(), CoordinatorAction.Status.RUNNING);

        new CoordKillXCommand(job.getId()).call();

        job = jpaService.execute(coordJobGetCmd);
        action = jpaService.execute(coordActionGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.KILLED);
        assertEquals(action.getStatus(), CoordinatorAction.Status.KILLED);
    }

    /**
     * Test : kill job successfully but failed to kill an already successful action
     *
     * @throws Exception
     */
    public void testCoordKillFailedOnAction() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml");

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(job.getId());
        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(action.getId());

        job = jpaService.execute(coordJobGetCmd);
        action = jpaService.execute(coordActionGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.SUCCEEDED);
        assertEquals(action.getStatus(), CoordinatorAction.Status.SUCCEEDED);

        new CoordKillXCommand(job.getId()).call();

        job = jpaService.execute(coordJobGetCmd);
        action = jpaService.execute(coordActionGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.SUCCEEDED);
        assertEquals(action.getStatus(), CoordinatorAction.Status.SUCCEEDED);
    }

    /**
     * Test : kill job failed. Job does not exist.
     *
     * @throws Exception
     */
    public void testCoordKillFailed() throws Exception {
        final String testJobId = "0000001-" + new Date().getTime() + "-testCoordKill-C";

        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.READY, "coord-action-get.xml");

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(job.getId());
        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(action.getId());

        job = jpaService.execute(coordJobGetCmd);
        action = jpaService.execute(coordActionGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.SUCCEEDED);
        assertEquals(action.getStatus(), CoordinatorAction.Status.READY);

        try {
            new CoordKillXCommand(testJobId).call();
            fail("Job doesn't exist. Should fail.");
        } catch (CommandException ce) {
            //Job doesn't exist. Exception is expected.
        }
    }

}