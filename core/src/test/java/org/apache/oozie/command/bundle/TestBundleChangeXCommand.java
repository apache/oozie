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
package org.apache.oozie.command.bundle;

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.Job;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
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
    public void testBundleChange() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.PREP);
        String dateStr = "2099-01-01T01:00Z";

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetCmd = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(job.getPauseTime(), null);

        new BundleJobChangeXCommand(job.getId(), "pausetime=" + dateStr).call();

        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(job.getPauseTime(), DateUtils.parseDateUTC(dateStr));
    }
    
    /**
     * Negative Test : pause time is not a valid date
     *
     * @throws Exception
     */
    public void testBundleChangeNegative1() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.PREP);
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
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.PREP);
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
}