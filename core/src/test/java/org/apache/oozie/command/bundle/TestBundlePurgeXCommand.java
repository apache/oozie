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
import org.apache.oozie.client.Job;
import org.apache.oozie.executor.jpa.BundleActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;

public class TestBundlePurgeXCommand extends XDataTestCase {
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
     * Test : purge succeeded bundle job and action successfully
     *
     * @throws Exception
     */
    public void testSucBundlePurgeXCommand() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateUTC("2011-01-01T01:00Z"));
        this.addRecordToBundleActionTable(job.getId(), "action1", 0, Job.Status.SUCCEEDED);
        this.addRecordToBundleActionTable(job.getId(), "action2", 0, Job.Status.SUCCEEDED);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetExecutor = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.SUCCEEDED, job.getStatus());

        BundleActionGetJPAExecutor bundleActionGetExecutor1 = new BundleActionGetJPAExecutor(job.getId(), "action1");
        BundleActionBean action1 = jpaService.execute(bundleActionGetExecutor1);
        assertEquals(Job.Status.SUCCEEDED, action1.getStatus());

        BundleActionGetJPAExecutor bundleActionGetExecutor2 = new BundleActionGetJPAExecutor(job.getId(), "action2");
        BundleActionBean action2 = jpaService.execute(bundleActionGetExecutor2);
        assertEquals(Job.Status.SUCCEEDED, action2.getStatus());

        new BundlePurgeXCommand(7, 10).call();

        try {
            job = jpaService.execute(bundleJobGetExecutor);
            fail("Job should be purged. Should fail.");
        }
        catch (JPAExecutorException je) {
            // Job doesn't exist. Exception is expected.
        }

        try {
            jpaService.execute(bundleActionGetExecutor1);
            fail("Action should be purged. Should fail.");
        }
        catch (JPAExecutorException je) {
            // Job doesn't exist. Exception is expected.
        }

        try {
            jpaService.execute(bundleActionGetExecutor2);
            fail("Action should be purged. Should fail.");
        }
        catch (JPAExecutorException je) {
            // Job doesn't exist. Exception is expected.
        }

    }

    /**
     * Test : purge failed bundle job and action successfully
     *
     * @throws Exception
     */
    public void testFailBundlePurgeXCommand() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.DONEWITHERROR, DateUtils.parseDateUTC("2011-01-01T01:00Z"));
        this.addRecordToBundleActionTable(job.getId(), "action1", 0, Job.Status.FAILED);
        this.addRecordToBundleActionTable(job.getId(), "action2", 0, Job.Status.SUCCEEDED);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetExecutor = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.DONEWITHERROR, job.getStatus());

        BundleActionGetJPAExecutor bundleActionGetExecutor1 = new BundleActionGetJPAExecutor(job.getId(), "action1");
        BundleActionBean action1 = jpaService.execute(bundleActionGetExecutor1);
        assertEquals(Job.Status.FAILED, action1.getStatus());

        BundleActionGetJPAExecutor bundleActionGetExecutor2 = new BundleActionGetJPAExecutor(job.getId(), "action2");
        BundleActionBean action2 = jpaService.execute(bundleActionGetExecutor2);
        assertEquals(Job.Status.SUCCEEDED, action2.getStatus());

        new BundlePurgeXCommand(7, 10).call();

        try {
            job = jpaService.execute(bundleJobGetExecutor);
            fail("Job should be purged. Should fail.");
        }
        catch (JPAExecutorException je) {
            // Job doesn't exist. Exception is expected.
        }

        try {
            jpaService.execute(bundleActionGetExecutor1);
            fail("Action should be purged. Should fail.");
        }
        catch (JPAExecutorException je) {
            // Job doesn't exist. Exception is expected.
        }

        try {
            jpaService.execute(bundleActionGetExecutor2);
            fail("Action should be purged. Should fail.");
        }
        catch (JPAExecutorException je) {
            // Job doesn't exist. Exception is expected.
        }

    }

    /**
     * Test : purge killed bundle job and action successfully
     *
     * @throws Exception
     */
    public void testKillBundlePurgeXCommand() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.KILLED, DateUtils.parseDateUTC("2011-01-01T01:00Z"));
        this.addRecordToBundleActionTable(job.getId(), "action1", 0, Job.Status.KILLED);
        this.addRecordToBundleActionTable(job.getId(), "action2", 0, Job.Status.KILLED);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetExecutor = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.KILLED, job.getStatus());

        BundleActionGetJPAExecutor bundleActionGetExecutor1 = new BundleActionGetJPAExecutor(job.getId(), "action1");
        BundleActionBean action1 = jpaService.execute(bundleActionGetExecutor1);
        assertEquals(Job.Status.KILLED, action1.getStatus());

        BundleActionGetJPAExecutor bundleActionGetExecutor2 = new BundleActionGetJPAExecutor(job.getId(), "action2");
        BundleActionBean action2 = jpaService.execute(bundleActionGetExecutor2);
        assertEquals(Job.Status.KILLED, action2.getStatus());

        new BundlePurgeXCommand(7, 10).call();

        try {
            job = jpaService.execute(bundleJobGetExecutor);
            fail("Job should be purged. Should fail.");
        }
        catch (JPAExecutorException je) {
            // Job doesn't exist. Exception is expected.
        }

        try {
            jpaService.execute(bundleActionGetExecutor1);
            fail("Action should be purged. Should fail.");
        }
        catch (JPAExecutorException je) {
            // Job doesn't exist. Exception is expected.
        }

        try {
            jpaService.execute(bundleActionGetExecutor2);
            fail("Action should be purged. Should fail.");
        }
        catch (JPAExecutorException je) {
            // Job doesn't exist. Exception is expected.
        }

    }

    /**
     * Test : purge bundle job and action failed
     *
     * @throws Exception
     */
    public void testBundlePurgeXCommandFailed() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.RUNNING, DateUtils.parseDateUTC("2011-01-01T01:00Z"));
        this.addRecordToBundleActionTable(job.getId(), "action1", 0, Job.Status.RUNNING);
        this.addRecordToBundleActionTable(job.getId(), "action2", 0, Job.Status.SUCCEEDED);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetExecutor = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.RUNNING, job.getStatus());

        BundleActionGetJPAExecutor bundleActionGetExecutor1 = new BundleActionGetJPAExecutor(job.getId(), "action1");
        BundleActionBean action1 = jpaService.execute(bundleActionGetExecutor1);
        assertEquals(Job.Status.RUNNING, action1.getStatus());

        BundleActionGetJPAExecutor bundleActionGetExecutor2 = new BundleActionGetJPAExecutor(job.getId(), "action2");
        BundleActionBean action2 = jpaService.execute(bundleActionGetExecutor2);
        assertEquals(Job.Status.SUCCEEDED, action2.getStatus());

        new BundlePurgeXCommand(7, 10).call();

        try {
            job = jpaService.execute(bundleJobGetExecutor);
        }
        catch (JPAExecutorException je) {
            // Job should exist. Exception is not expected.
            fail("Job should exist. If not, fail it.");
        }

        try {
            jpaService.execute(bundleActionGetExecutor1);
        }
        catch (JPAExecutorException je) {
            // Job should exist. Exception is not expected.
            fail("Action should exist. If not, fail it.");
        }

        try {
            jpaService.execute(bundleActionGetExecutor2);
        }
        catch (JPAExecutorException je) {
            // Job should exist. Exception is not expected.
            fail("Action should exist. If not, fail it.");
        }
    }

    protected BundleJobBean addRecordToBundleJobTable(Job.Status jobStatus, Date lastModifiedTime) throws Exception {
        BundleJobBean bundle = createBundleJob(jobStatus, false);
        bundle.setLastModifiedTime(lastModifiedTime);
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            BundleJobInsertJPAExecutor bundleInsertjpa = new BundleJobInsertJPAExecutor(bundle);
            jpaService.execute(bundleInsertjpa);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test bundle job record to table");
            throw je;
        }
        return bundle;
    }
}
