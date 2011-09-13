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

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.BundleActionsGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.XConfiguration;

public class TestBundleStartXCommand extends XDataTestCase {

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
     * Test : Start bundle job
     *
     * @throws Exception
     */
    public void testBundleStart1() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.PREP, false);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetExecutor = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(job.getStatus(), Job.Status.PREP);

        new BundleStartXCommand(job.getId()).call();

        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(job.getStatus(), Job.Status.RUNNING);

        Thread.sleep(2000);

        BundleActionsGetJPAExecutor bundleActionsGetExecutor = new BundleActionsGetJPAExecutor(job.getId());
        List<BundleActionBean> actions = jpaService.execute(bundleActionsGetExecutor);

        assertEquals(2, actions.size());
        assertEquals(true, actions.get(0).isCritical());
        assertEquals(job.getId(), actions.get(0).getBundleId());
        assertEquals(false, actions.get(1).isCritical());
        assertEquals(job.getId(), actions.get(0).getBundleId());
    }

    /**
     * Test : Start bundle job
     *
     * @throws Exception
     */
    public void testBundleStart2() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.PREP, false);

        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        Configuration jobConf = null;
        try {
            jobConf = new XConfiguration(new StringReader(job.getConf()));
        }
        catch (IOException ioe) {
            log.warn("Configuration parse error. read from DB :" + job.getConf(), ioe);
            throw new CommandException(ErrorCode.E1005, ioe);
        }

        Path appPath = new Path(jobConf.get(OozieClient.BUNDLE_APP_PATH), "bundle.xml");
        jobConf.set(OozieClient.BUNDLE_APP_PATH, appPath.toString());

        BundleSubmitXCommand submitCmd = new BundleSubmitXCommand(jobConf, job.getAuthToken());
        submitCmd.call();

        BundleJobGetJPAExecutor bundleJobGetExecutor = new BundleJobGetJPAExecutor(submitCmd.getJob().getId());
        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(job.getStatus(), Job.Status.PREP);

        new BundleStartXCommand(job.getId()).call();

        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(job.getStatus(), Job.Status.RUNNING);

        Thread.sleep(2000);

        final BundleActionsGetJPAExecutor bundleActionsGetExecutor = new BundleActionsGetJPAExecutor(job.getId());
        List<BundleActionBean> actions = jpaService.execute(bundleActionsGetExecutor);
        assertEquals(2, actions.size());

        waitFor(200000, new Predicate() {
            public boolean evaluate() throws Exception {
                List<BundleActionBean> actions = jpaService.execute(bundleActionsGetExecutor);
                return actions.get(0).getStatus().equals(Job.Status.RUNNING)
                        && actions.get(1).getStatus().equals(Job.Status.RUNNING);
            }
        });

        actions = jpaService.execute(bundleActionsGetExecutor);
        assertEquals(Job.Status.RUNNING, actions.get(0).getStatus());
        assertEquals(true, actions.get(0).isCritical());
        assertEquals(job.getId(), actions.get(0).getBundleId());
        assertEquals(Job.Status.RUNNING, actions.get(1).getStatus());
        assertEquals(false, actions.get(1).isCritical());
        assertEquals(job.getId(), actions.get(1).getBundleId());
    }

    /**
     * Test : Start bundle job with dryrun
     *
     * @throws Exception
     */
    public void testBundleStartDryrun() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.PREP, false);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetExecutor = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(job.getStatus(), Job.Status.PREP);

        new BundleStartXCommand(job.getId(), true).call();

        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(job.getStatus(), Job.Status.RUNNING);

        Thread.sleep(2000);

        BundleActionsGetJPAExecutor bundleActionsGetExecutor = new BundleActionsGetJPAExecutor(job.getId());
        List<BundleActionBean> actions = jpaService.execute(bundleActionsGetExecutor);

        assertEquals(2, actions.size());
        assertEquals(true, actions.get(0).isCritical());
        assertEquals(job.getId(), actions.get(0).getBundleId());
        assertEquals(false, actions.get(1).isCritical());
        assertEquals(job.getId(), actions.get(1).getBundleId());
    }

    /**
     * Test : Start bundle job but jobId is wrong
     *
     * @throws Exception
     */
    public void testBundleStartNegative1() throws Exception {
        this.addRecordToBundleJobTable(Job.Status.PREP, false);

        try {
            new BundleStartXCommand("bundle-id").call();
            fail("Job doesn't exist. Should fail.");
        }
        catch (CommandException ce) {
            // Job doesn't exist. Exception is expected.
        }
    }

    /**
     * Test : Start bundle job that contains bad coordinator job
     *
     * @throws Exception
     */
    public void testBundleStartNegative2() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTableNegative(Job.Status.PREP);

        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        final BundleJobGetJPAExecutor bundleJobGetExecutor = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(job.getStatus(), Job.Status.PREP);

        new BundleStartXCommand(job.getId()).call();

        waitFor(120000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean job1 = jpaService.execute(bundleJobGetExecutor);
                return job1.getStatus().equals(Job.Status.FAILED);
            }
        });
        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(Job.Status.FAILED, job.getStatus());
    }
}
