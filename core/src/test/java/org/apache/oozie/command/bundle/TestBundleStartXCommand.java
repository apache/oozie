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
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.BundleActionQueryExecutor;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.BundleActionQueryExecutor.BundleActionQuery;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.service.StatusTransitService.StatusTransitRunnable;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.XConfiguration;

public class TestBundleStartXCommand extends XDataTestCase {

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

        sleep(2000);

        List<BundleActionBean> actions = BundleActionQueryExecutor.getInstance().getList(
                BundleActionQuery.GET_BUNDLE_ACTIONS_STATUS_UNIGNORED_FOR_BUNDLE, job.getId());

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

        BundleSubmitXCommand submitCmd = new BundleSubmitXCommand(jobConf);
        submitCmd.call();

        BundleJobGetJPAExecutor bundleJobGetExecutor = new BundleJobGetJPAExecutor(submitCmd.getJob().getId());
        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals("bundle-app-name", job.getAppName());
        job = jpaService.execute(bundleJobGetExecutor);

        assertEquals(job.getStatus(), Job.Status.PREP);

        new BundleStartXCommand(job.getId()).call();

        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(job.getStatus(), Job.Status.RUNNING);

        sleep(2000);

        List<BundleActionBean> actions = BundleActionQueryExecutor.getInstance().getList(
                BundleActionQuery.GET_BUNDLE_ACTIONS_STATUS_UNIGNORED_FOR_BUNDLE, job.getId());
        assertEquals(2, actions.size());

        final String jobId = job.getId();
        waitFor(200000, new Predicate() {
            public boolean evaluate() throws Exception {
                List<BundleActionBean> actions = BundleActionQueryExecutor.getInstance().getList(
                        BundleActionQuery.GET_BUNDLE_ACTIONS_STATUS_UNIGNORED_FOR_BUNDLE, jobId);
                return actions.get(0).getStatus().equals(Job.Status.RUNNING)
                        && actions.get(1).getStatus().equals(Job.Status.RUNNING);
            }
        });

        actions = BundleActionQueryExecutor.getInstance().getList(BundleActionQuery.GET_BUNDLE_ACTIONS_STATUS_UNIGNORED_FOR_BUNDLE,
                job.getId());
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

        sleep(2000);

        List<BundleActionBean> actions = BundleActionQueryExecutor.getInstance().getList(
                BundleActionQuery.GET_BUNDLE_ACTIONS_STATUS_UNIGNORED_FOR_BUNDLE, job.getId());

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

    public static class DummyUUIDService extends UUIDService {
        boolean firstTime = true;
        @Override
        public String generateId(ApplicationType type) {
            if (type.equals(ApplicationType.COORDINATOR) && firstTime) {
                firstTime = false;
                return "dummy-coord-id";
            }
            else {
                return super.generateId(type);
            }
        }
    }

    public void testBundleStartWithFailedCoordinator() throws Exception {
        services.destroy();
        services = new Services();
        String excludeServices[] = { "org.apache.oozie.service.UUIDService",
                "org.apache.oozie.service.StatusTransitService" };
        Configuration conf = services.getConf();
        setClassesToBeExcluded(conf, excludeServices);
        conf.set(Services.CONF_SERVICE_CLASSES,
                conf.get(Services.CONF_SERVICE_CLASSES) + "," + DummyUUIDService.class.getName());
        services.init();
        CoordinatorJobBean coordJob = new CoordinatorJobBean();
        coordJob.setId("dummy-coord-id");
        JPAService jpaService = Services.get().get(JPAService.class);
        CoordJobInsertJPAExecutor coordInsertCmd = new CoordJobInsertJPAExecutor(coordJob);
        jpaService.execute(coordInsertCmd);
        BundleJobBean job = addRecordToBundleJobTable(Job.Status.PREP, false);
        BundleJobGetJPAExecutor bundleJobGetExecutor = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(job.getStatus(), Job.Status.PREP);
        new BundleStartXCommand(job.getId()).call();
        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(job.getStatus(), Job.Status.RUNNING);
        sleep(2000);
        List<BundleActionBean> actions = BundleActionQueryExecutor.getInstance().getList(
                BundleActionQuery.GET_BUNDLE_ACTIONS_STATUS_UNIGNORED_FOR_BUNDLE, job.getId());
        assertNull(actions.get(0).getCoordId());
        assertEquals(Job.Status.FAILED, actions.get(0).getStatus());
        Runnable runnable = new StatusTransitRunnable();
        //1st run of StatusTransitionService changes bundle to running
        runnable.run();
        sleep(2000);
        //2nd run changes bundle to DoneWithError
        runnable.run();
        sleep(2000);
        job = jpaService.execute(bundleJobGetExecutor);
        assertEquals(job.getStatus(), Job.Status.DONEWITHERROR);
    }


}
