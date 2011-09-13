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

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.client.Job;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestBundlePauseUnpauseXCommand extends XDataTestCase {
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
     * Test : Pause a PREP bundle
     *
     * @throws Exception
     */
    public void testBundlePauseUnpause1() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.PREP, false);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetCmd = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(job.getStatus(), Job.Status.PREP);

        new BundlePauseXCommand(job).call();

        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(job.getStatus(), Job.Status.PREPPAUSED);
        
        new BundleUnpauseXCommand(job).call();
        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(job.getStatus(), Job.Status.PREP);
    }
    
    /**
     * Test : Pause a RUNNING bundle
     *
     * @throws Exception
     */
    public void testBundlePauseUnpause2() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.RUNNING, false);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetCmd = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(job.getStatus(), Job.Status.RUNNING);

        new BundlePauseXCommand(job).call();

        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(job.getStatus(), Job.Status.PAUSED);
        
        new BundleUnpauseXCommand(job).call();
        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(job.getStatus(), Job.Status.RUNNING);
    }

    /**
     * Test : Pause a RUNNINGWITHERROR bundle
     *
     * @throws Exception
     */
    public void testBundlePauseUnpause3() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.RUNNINGWITHERROR, false);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetCmd = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(job.getStatus(), Job.Status.RUNNINGWITHERROR);

        new BundlePauseXCommand(job).call();

        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(job.getStatus(), Job.Status.PAUSEDWITHERROR);
        
        new BundleUnpauseXCommand(job).call();
        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(job.getStatus(), Job.Status.RUNNINGWITHERROR);
    }

    /**
     * Test : Negative case - pause a suspended bundle
     *
     * @throws Exception
     */
    public void testBundlePauseUnpauseNeg1() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.SUSPENDED, false);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetCmd = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(job.getStatus(), Job.Status.SUSPENDED);

        try {
            new BundlePauseXCommand(job).call();
            fail("should not reach here.");
        }
        catch (Exception ex) {
        }
    }
}
