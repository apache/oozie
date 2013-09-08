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

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobUpdateJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.SchemaService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.StatusTransitService;
import org.apache.oozie.test.XDataTestCase;

public class TestCoordResumeXCommand extends XDataTestCase {
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
     * Test : suspend a RUNNING coordinator job and resume to RUNNING
     *
     * @throws Exception
     */
    public void testCoordSuspendAndResumeForRunning() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(job.getId());
        job = jpaService.execute(coordJobGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.RUNNING);

        new CoordSuspendXCommand(job.getId()).call();
        job = jpaService.execute(coordJobGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.SUSPENDED);

        new CoordResumeXCommand(job.getId()).call();
        job = jpaService.execute(coordJobGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.RUNNING);
    }


    /**
     * Test : suspend a RUNNINGWITHERROR coordinator job and check the status to RUNNINGWITHERROR on resume
     *
     * @throws Exception
     */
    public void testCoordSuspendWithErrorAndResumeWithErrorForRunning() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNINGWITHERROR, false, false);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(job.getId());
        job = jpaService.execute(coordJobGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.RUNNINGWITHERROR);

        new CoordSuspendXCommand(job.getId()).call();
        job = jpaService.execute(coordJobGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.SUSPENDEDWITHERROR);

        new CoordResumeXCommand(job.getId()).call();
        job = jpaService.execute(coordJobGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.RUNNINGWITHERROR);
    }

    /**
     * Test : suspend a PREP coordinator job and resume to PREP
     *
     * @throws Exception
     */
    public void testCoordSuspendAndResumeForPrep() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, false, false);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(job.getId());
        job = jpaService.execute(coordJobGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.PREP);

        new CoordSuspendXCommand(job.getId()).call();
        job = jpaService.execute(coordJobGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.PREPSUSPENDED);

        new CoordResumeXCommand(job.getId()).call();
        job = jpaService.execute(coordJobGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.PREP);
    }

    /**
     * Test : suspend a PREP coordinator job and resume to PREP
     *
     * @throws Exception
     */
    public void testCoordSuspendAndResumeForPrepWithBackwardCompatibility() throws Exception {
        Services.get().destroy();
        setSystemProperty(StatusTransitService.CONF_BACKWARD_SUPPORT_FOR_COORD_STATUS, "true");
        services = new Services();
        services.init();
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, false, false);
        job.setAppNamespace(SchemaService.COORDINATOR_NAMESPACE_URI_1);
        JPAService jpaService = Services.get().get(JPAService.class);
        jpaService.execute(new CoordJobUpdateJPAExecutor(job));

        assertNotNull(jpaService);
        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(job.getId());
        job = jpaService.execute(coordJobGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.PREP);

        new CoordSuspendXCommand(job.getId()).call();
        job = jpaService.execute(coordJobGetCmd);

        assertEquals(job.getStatus(), CoordinatorJob.Status.SUSPENDED);

        new CoordResumeXCommand(job.getId()).call();
        job = jpaService.execute(coordJobGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.RUNNING);
    }
}
