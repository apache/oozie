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

import java.util.Date;

import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.executor.jpa.BundleActionInsertJPAExecutor;
import org.apache.oozie.executor.jpa.BundleActionQueryExecutor;
import org.apache.oozie.executor.jpa.BundleActionQueryExecutor.BundleActionQuery;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobQueryExecutor;
import org.apache.oozie.executor.jpa.BundleJobQueryExecutor.BundleJobQuery;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.PauseTransitService.PauseTransitRunnable;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;

public class TestBundleChangeXCommand extends XDataTestCase {
    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
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
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB_BUNDLEID, coordJob);

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

    public void testBundlePauseExtendMaterializesCoordinator() throws Exception {
        BundleJobBean bundle = this.addRecordToBundleJobTable(Job.Status.PAUSED, false);
        Date startTime = new Date();
        Date endTime = new Date(startTime.getTime() + (20 * 60 * 1000));
        // coord job with num actions materialized = 1
        CoordinatorJobBean coord = addRecordToCoordJobTable(CoordinatorJob.Status.PAUSED, startTime, endTime, true,
                false, 1);
        coord.setPauseTime(startTime); // setting dummy old pause time
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB_CHANGE, coord);
        // persist corresponding bundle action bean
        BundleActionBean bundleAction = this.addRecordToBundleActionTable(bundle.getId(), "COORD-TEST", 1,
                Job.Status.PAUSED);

        coord.setBundleId(bundle.getId());
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB_BUNDLEID, coord);
        bundleAction.setCoordId(coord.getId());
        BundleActionQueryExecutor.getInstance().executeUpdate(
                BundleActionQuery.UPDATE_BUNDLE_ACTION_STATUS_PENDING_MODTIME_COORDID, bundleAction);

        Date later = new Date(System.currentTimeMillis() + 20 * 1000); // 20 sec later
        Date evenLater = new Date(later.getTime() + 10 * 60 * 1000); // 10 min later

        bundle = BundleJobQueryExecutor.getInstance().get(BundleJobQuery.GET_BUNDLE_JOB, bundle.getId());
        assertEquals(Job.Status.PAUSED, bundle.getStatus());
        coord = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB, coord.getId());
        assertEquals(Job.Status.PAUSED, coord.getStatus());

        // coordinator has previous actions
        coord.setNextMaterializedTime(new Date(startTime.getTime() + (300 * 1000))); // before the new pausetime
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coord);
        Date lastMod = coord.getLastModifiedTime();
        addRecordToCoordActionTable(coord.getId(), 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);

        // change pausetime to even later
        new BundleJobChangeXCommand(bundle.getId(), "pausetime=" + DateUtils.formatDateOozieTZ(evenLater)).call();
        sleep(1000); // time for the queued CoordChangeXCommand to complete

        Runnable runnable = new PauseTransitRunnable();
        runnable.run(); // simulating run which would happen usually after interval
        Thread.sleep(1000);

        bundle = BundleJobQueryExecutor.getInstance().get(BundleJobQuery.GET_BUNDLE_JOB, bundle.getId());
        assertEquals(Job.Status.RUNNING, bundle.getStatus());
        assertEquals(DateUtils.formatDateOozieTZ(evenLater), DateUtils.formatDateOozieTZ(bundle.getPauseTime()));
        coord = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB, coord.getId());
        assertEquals(Job.Status.RUNNING, coord.getStatus());
        assertEquals(DateUtils.formatDateOozieTZ(evenLater), DateUtils.formatDateOozieTZ(coord.getPauseTime()));
        assertTrue(coord.getLastModifiedTime().after(lastMod));

    }
}
