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
import org.apache.oozie.client.Job.Status;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.BundleActionQueryExecutor;
import org.apache.oozie.executor.jpa.BundleActionQueryExecutor.BundleActionQuery;
import org.apache.oozie.executor.jpa.BundleJobQueryExecutor.BundleJobQuery;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.StatusTransitService.StatusTransitRunnable;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.XConfiguration;

public class TestBundleKillXCommand extends XDataTestCase {

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
     * Test : Kill bundle job
     *
     * @throws Exception
     */
    public void testBundleKill1() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.PREP, false);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetJPAExecutor bundleJobGetCmd = new BundleJobGetJPAExecutor(job.getId());
        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(Job.Status.PREP, job.getStatus());

        new BundleKillXCommand(job.getId()).call();

        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(Job.Status.KILLED, job.getStatus());
    }

    /**
     * Test : Kill bundle job
     *
     * @throws Exception
     */
    public void testBundleKill2() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.PREP, false);

        final JPAService jpaService = Services.get().get(JPAService.class);

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

        new BundleKillXCommand(job.getId()).call();

        BundleSubmitXCommand submitCmd = new BundleSubmitXCommand(jobConf);
        submitCmd.call();

        BundleJobGetJPAExecutor bundleJobGetCmd = new BundleJobGetJPAExecutor(submitCmd.getJob().getId());
        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(Job.Status.PREP, job.getStatus());

        new BundleStartXCommand(job.getId()).call();

        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(Job.Status.RUNNING, job.getStatus());

        sleep(2000);

        List<BundleActionBean> actions = BundleActionQueryExecutor.getInstance().getList(
                BundleActionQuery.GET_BUNDLE_ACTIONS_STATUS_UNIGNORED_FOR_BUNDLE, job.getId());

        assertEquals(2, actions.size());
        assertNotNull(actions.get(0).getCoordId());
        assertNotNull(actions.get(1).getCoordId());

        new BundleKillXCommand(job.getId()).call();

        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(Job.Status.KILLED, job.getStatus());

        actions = BundleActionQueryExecutor.getInstance().getList(BundleActionQuery.GET_BUNDLE_ACTIONS_STATUS_UNIGNORED_FOR_BUNDLE,
                job.getId());

        assertEquals(true, actions.get(0).isPending());
        assertEquals(true, actions.get(1).isPending());

        final CoordJobGetJPAExecutor coordGetCmd1 = new CoordJobGetJPAExecutor(actions.get(0).getCoordId());
        final CoordJobGetJPAExecutor coordGetCmd2 = new CoordJobGetJPAExecutor(actions.get(1).getCoordId());


        waitFor(200000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorJobBean job1 = jpaService.execute(coordGetCmd1);
                return job1.getStatus().equals(CoordinatorJobBean.Status.KILLED);
            }
        });

        CoordinatorJobBean job1 = jpaService.execute(coordGetCmd1);
        assertEquals(CoordinatorJobBean.Status.KILLED, job1.getStatus());

        waitFor(200000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorJobBean job2 = jpaService.execute(coordGetCmd2);
                return job2.getStatus().equals(CoordinatorJobBean.Status.KILLED);
            }
        });

        CoordinatorJobBean job2 = jpaService.execute(coordGetCmd2);
        assertEquals(CoordinatorJobBean.Status.KILLED, job2.getStatus());

        final Runnable runnable = new StatusTransitRunnable();
        runnable.run();
        sleep(1000);

        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(Job.Status.KILLED, job.getStatus());

        actions = BundleActionQueryExecutor.getInstance().getList(
                BundleActionQuery.GET_BUNDLE_ACTIONS_STATUS_UNIGNORED_FOR_BUNDLE, job.getId());
        for (BundleActionBean action : actions) {
            assertEquals(0, action.getPending());
            assertEquals(CoordinatorJobBean.Status.KILLED, action.getStatus());
        }

        // If bundle kill + Status Transit service left the bundle action with Pending=1
        // due to race condition, killing the bundle again should reset the pending.
        job.setPending();
        job.setStatus(Status.RUNNING);
        actions.get(0).incrementAndGetPending();
        BundleJobQueryExecutor.getInstance().executeUpdate(BundleJobQuery.UPDATE_BUNDLE_JOB_STATUS_PENDING, job);
        BundleActionQueryExecutor.getInstance().executeUpdate(
                BundleActionQuery.UPDATE_BUNDLE_ACTION_PENDING_MODTIME, actions.get(0));

        runnable.run();
        sleep(1000);
        new BundleKillXCommand(job.getId()).call();
        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(Job.Status.KILLED, job.getStatus());

        runnable.run();
        sleep(1000);
        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(Job.Status.KILLED, job.getStatus());
        actions = BundleActionQueryExecutor.getInstance().getList(
                BundleActionQuery.GET_BUNDLE_ACTIONS_STATUS_UNIGNORED_FOR_BUNDLE, job.getId());
        for (BundleActionBean action : actions) {
            assertEquals(0, action.getPending());
            assertEquals(CoordinatorJobBean.Status.KILLED, action.getStatus());
        }

    }

    /**
     * Test : Kill bundle job
     *
     * @throws Exception
     */
    public void testBundleKill3() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.PREP, false);

        JPAService jpaService = Services.get().get(JPAService.class);
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

        BundleJobGetJPAExecutor bundleJobGetCmd = new BundleJobGetJPAExecutor(submitCmd.getJob().getId());
        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(Job.Status.PREP, job.getStatus());

        new BundleKillXCommand(job.getId()).call();

        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(Job.Status.KILLED, job.getStatus());
    }

    /**
     * Test : Kill bundle job but jobId is wrong
     *
     * @throws Exception
     */
    public void testBundleKillFailed() throws Exception {
        this.addRecordToBundleJobTable(Job.Status.PREP, false);

        try {
            new BundleKillXCommand("bundle-id").call();
            fail("Job doesn't exist. Should fail.");
        } catch (CommandException ce) {
            //Job doesn't exist. Exception is expected.
        }
    }
}
