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

import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.executor.jpa.BundleActionInsertJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobUpdateJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;

public class TestBundleChangeXCommand extends XDataTestCase {
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
     * Test : Change pause time of a bundle
     *
     * @throws Exception
     */
    public void testBundleChange1() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.PREP, false);
        String dateStr = "2099-01-01T01:00Z";

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetCmd = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(job.getPauseTime(), null);

        new BundleJobChangeXCommand(job.getId(), "pausetime=" + dateStr).call();

        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(job.getPauseTime(), DateUtils.parseDateOozieTZ(dateStr));
    }

    /**
     * Test : Change pause time of a bundle that contains a SUCCEEDED coordinator job,
     * The coordinator should also change its pause time.
     *
     * @throws Exception
     */
    public void testBundleChange2() throws Exception {
        BundleJobBean bundleJob = this.addRecordToBundleJobTable(Job.Status.RUNNING, false);

        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        coordJob.setBundleId(bundleJob.getId());
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        jpaService.execute(new CoordJobUpdateJPAExecutor(coordJob));

        BundleActionBean bundleAction = new BundleActionBean();
        bundleAction.setBundleActionId("11111");
        bundleAction.setCoordId(coordJob.getId());
        bundleAction.setBundleId(bundleJob.getId());
        bundleAction.setStatus(Job.Status.SUCCEEDED);
        jpaService.execute(new BundleActionInsertJPAExecutor(bundleAction));

        String dateStr = "2099-01-01T01:00Z";
        BundleJobGetJPAExecutor bundleJobGetCmd = new BundleJobGetJPAExecutor(bundleJob.getId());
        bundleJob = jpaService.execute(bundleJobGetCmd);
        assertEquals(bundleJob.getPauseTime(), null);

        new BundleJobChangeXCommand(bundleJob.getId(), "pausetime=" + dateStr).call();
        bundleJob = jpaService.execute(bundleJobGetCmd);
        assertEquals(DateUtils.parseDateOozieTZ(dateStr), bundleJob.getPauseTime());

        final String coordJobId = coordJob.getId();
        waitFor(60000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorJobBean coordJob1 = jpaService.execute(new CoordJobGetJPAExecutor(coordJobId));
                return (coordJob1.getPauseTime() != null);
            }
        });

        coordJob = jpaService.execute(new CoordJobGetJPAExecutor(coordJob.getId()));
        assertEquals(DateUtils.parseDateOozieTZ(dateStr), coordJob.getPauseTime());
    }

    /**
     * Negative Test : pause time is not a valid date
     *
     * @throws Exception
     */
    public void testBundleChangeNegative1() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.PREP, false);
        String dateStr = "2099-01-01Ta1:00Z";

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetCmd = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(job.getPauseTime(), null);

        try {
            new BundleJobChangeXCommand(job.getId(), "pausetime=" + dateStr).call();
            fail("Should not reach here");
        }
        catch (XException e) {
            assertEquals(ErrorCode.E1317, e.getErrorCode());
        }
    }

    /**
     * Negative Test : pause time is a past time
     *
     * @throws Exception
     */
    public void testBundleChangeNegative2() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.PREP, false);
        String dateStr = "2009-01-01T01:00Z";

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetCmd = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(job.getPauseTime(), null);

        try {
            new BundleJobChangeXCommand(job.getId(), "pausetime=" + dateStr).call();
            fail("Should not reach here");
        }
        catch (XException e) {
            assertEquals(ErrorCode.E1317, e.getErrorCode());
        }
    }

    /**
     * Test : Change end time of a bundle
     *
     * @throws Exception
     */
    public void testBundleChange3() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.PREP, false);
        String dateStr = "2099-01-01T01:00Z";

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetCmd = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(job.getEndTime(), null);

        new BundleJobChangeXCommand(job.getId(), "endtime=" + dateStr).call();

        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(job.getEndTime(), DateUtils.parseDateOozieTZ(dateStr));
    }
}
