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
package org.apache.oozie.service;

import java.util.Date;

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.client.Job;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobUpdateJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestBundlePauseStartService extends XDataTestCase {
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
     * Test : Pause a PREP bundle, then delay its pausetime to unpause it.
     *
     * @throws Exception
     */
    public void testPauseUnpause1() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.PREP);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        job.setPauseTime(new Date(new Date().getTime() + 30 * 1000));
        job.setKickoffTime(new Date(new Date().getTime() + 3600 * 1000));
        jpaService.execute(new BundleJobUpdateJPAExecutor(job));

        final String jobId = job.getId();
        waitFor(120 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean job1 = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
                return job1.getStatus() == Job.Status.PREPPAUSED;
            }
        });

        job = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
        assertEquals(Job.Status.PREPPAUSED, job.getStatus());

        job.setPauseTime(new Date(new Date().getTime() + 3600 * 1000));
        jpaService.execute(new BundleJobUpdateJPAExecutor(job));

        waitFor(120 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean job1 = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
                return job1.getStatus() == Job.Status.PREP;
            }
        });

        job = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
        assertEquals(Job.Status.PREP, job.getStatus());
    }

    /**
     * Test : Pause a PREP bundle, then reset its pausetime to null to unpause it.
     *
     * @throws Exception
     */
    public void testPauseUnpause2() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.PREP);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        job.setPauseTime(new Date(new Date().getTime() + 30 * 1000));
        job.setKickoffTime(new Date(new Date().getTime() + 3600 * 1000));
        jpaService.execute(new BundleJobUpdateJPAExecutor(job));

        final String jobId = job.getId();
        waitFor(120 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean job1 = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
                return job1.getStatus() == Job.Status.PREPPAUSED;
            }
        });

        job = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
        assertEquals(Job.Status.PREPPAUSED, job.getStatus());

        job.setPauseTime(null);
        jpaService.execute(new BundleJobUpdateJPAExecutor(job));

        waitFor(120 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean job1 = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
                return job1.getStatus() == Job.Status.PREP;
            }
        });

        job = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
        assertEquals(Job.Status.PREP, job.getStatus());
    }

    /**
     * Test : Start a PREP bundle when its kickoff time reaches.
     *
     * @throws Exception
     */
    public void testStart1() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.PREP);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        job.setKickoffTime(new Date(new Date().getTime() + 30 * 1000));
        jpaService.execute(new BundleJobUpdateJPAExecutor(job));

        final String jobId = job.getId();
        waitFor(120 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean job1 = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
                return job1.getStatus() == Job.Status.RUNNING;
            }
        });

        job = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
        assertEquals(Job.Status.RUNNING, job.getStatus());
   }

    /**
     * Test : Start a PREP bundle when its kickoff time is a past time.
     *
     * @throws Exception
     */
    public void testStart2() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.PREP);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        job.setKickoffTime(new Date(new Date().getTime() - 30 * 1000));
        jpaService.execute(new BundleJobUpdateJPAExecutor(job));

        final String jobId = job.getId();
        waitFor(120 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean job1 = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
                return job1.getStatus() == Job.Status.RUNNING;
            }
        });

        job = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
        assertEquals(Job.Status.RUNNING, job.getStatus());
   }

}