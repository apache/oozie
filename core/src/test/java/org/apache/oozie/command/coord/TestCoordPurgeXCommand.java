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
package org.apache.oozie.command.coord;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;

public class TestCoordPurgeXCommand extends XDataTestCase {
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
     * Test : purge succeeded coord job and action successfully
     *
     * @throws Exception
     */
    public void testSucCoordPurgeXCommand() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetJPAExecutor coordJobGetExecutor = new CoordJobGetJPAExecutor(job.getId());
        CoordActionGetJPAExecutor coordActionGetExecutor = new CoordActionGetJPAExecutor(action.getId());

        job = jpaService.execute(coordJobGetExecutor);
        action = jpaService.execute(coordActionGetExecutor);
        assertEquals(job.getStatus(), CoordinatorJob.Status.SUCCEEDED);
        assertEquals(action.getStatus(), CoordinatorAction.Status.SUCCEEDED);

        new CoordPurgeXCommand(7, 10).call();

        try {
            job = jpaService.execute(coordJobGetExecutor);
            fail("Job should be purged. Should fail.");
        }
        catch (JPAExecutorException je) {
            // Job doesn't exist. Exception is expected.
        }

        try {
            jpaService.execute(coordActionGetExecutor);
            fail("Action should be purged. Should fail.");
        }
        catch (JPAExecutorException je) {
            // Job doesn't exist. Exception is expected.
        }

    }

    /**
     * Test : purge failed coord job and action successfully
     *
     * @throws Exception
     */
    public void testFailCoordPurgeXCommand() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.FAILED, false, false);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.FAILED,
                "coord-action-get.xml", 0);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetJPAExecutor coordJobGetExecutor = new CoordJobGetJPAExecutor(job.getId());
        CoordActionGetJPAExecutor coordActionGetExecutor = new CoordActionGetJPAExecutor(action.getId());

        job = jpaService.execute(coordJobGetExecutor);
        action = jpaService.execute(coordActionGetExecutor);
        assertEquals(job.getStatus(), CoordinatorJob.Status.FAILED);
        assertEquals(action.getStatus(), CoordinatorAction.Status.FAILED);

        new CoordPurgeXCommand(7, 10).call();

        try {
            job = jpaService.execute(coordJobGetExecutor);
            fail("Job should be purged. Should fail.");
        }
        catch (JPAExecutorException je) {
            // Job doesn't exist. Exception is expected.
        }

        try {
            jpaService.execute(coordActionGetExecutor);
            fail("Action should be purged. Should fail.");
        }
        catch (JPAExecutorException je) {
            // Job doesn't exist. Exception is expected.
        }

    }

    /**
     * Test : purge killed coord job and action successfully
     *
     * @throws Exception
     */
    public void testKillCoordPurgeXCommand() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.KILLED, false, false);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.KILLED,
                "coord-action-get.xml", 0);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetJPAExecutor coordJobGetExecutor = new CoordJobGetJPAExecutor(job.getId());
        CoordActionGetJPAExecutor coordActionGetExecutor = new CoordActionGetJPAExecutor(action.getId());

        job = jpaService.execute(coordJobGetExecutor);
        action = jpaService.execute(coordActionGetExecutor);
        assertEquals(job.getStatus(), CoordinatorJob.Status.KILLED);
        assertEquals(action.getStatus(), CoordinatorAction.Status.KILLED);

        new CoordPurgeXCommand(7, 10).call();

        try {
            job = jpaService.execute(coordJobGetExecutor);
            fail("Job should be purged. Should fail.");
        }
        catch (JPAExecutorException je) {
            // Job doesn't exist. Exception is expected.
        }

        try {
            jpaService.execute(coordActionGetExecutor);
            fail("Action should be purged. Should fail.");
        }
        catch (JPAExecutorException je) {
            // Job doesn't exist. Exception is expected.
        }

    }

    /**
     * Test : purge coord job and action failed
     *
     * @throws Exception
     */
    public void testCoordPurgeXCommandFailed() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetJPAExecutor coordJobGetExecutor = new CoordJobGetJPAExecutor(job.getId());
        CoordActionGetJPAExecutor coordActionGetExecutor = new CoordActionGetJPAExecutor(action.getId());

        job = jpaService.execute(coordJobGetExecutor);
        action = jpaService.execute(coordActionGetExecutor);
        assertEquals(job.getStatus(), CoordinatorJob.Status.RUNNING);
        assertEquals(action.getStatus(), CoordinatorAction.Status.SUCCEEDED);

        new CoordPurgeXCommand(7, 10).call();

        try {
            job = jpaService.execute(coordJobGetExecutor);
        }
        catch (JPAExecutorException je) {
            fail("Job should not be purged. Should fail.");
        }

        try {
            jpaService.execute(coordActionGetExecutor);
        }
        catch (JPAExecutorException je) {
            fail("Action should not be purged. Should fail.");
        }

    }

    @Override
    protected CoordinatorJobBean addRecordToCoordJobTable(CoordinatorJob.Status status, boolean pending, boolean doneMatd) throws Exception {
        CoordinatorJobBean coordJob = createCoordJob(status, pending, doneMatd);
        coordJob.setLastModifiedTime(DateUtils.parseDateOozieTZ("2009-12-18T01:00Z"));
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
