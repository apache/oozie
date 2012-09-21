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
package org.apache.oozie.service;

import java.io.File;
import java.io.FileWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleEngine;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorEngine;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.DagEngine;
import org.apache.oozie.DagEngineException;
import org.apache.oozie.ForTestingActionExecutor;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.command.wf.PurgeXCommand;
import org.apache.oozie.executor.jpa.BundleActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobUpdateJPAExecutor;
import org.apache.oozie.service.PurgeService.PurgeRunnable;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;

/**
 * Test cases for checking the correct functionality of the PurgeService.
 */
public class TestPurgeService extends XDataTestCase {
    private Services services;
    String[] excludedServices = { "org.apache.oozie.service.StatusTransitService",
            "org.apache.oozie.service.PauseTransitService", "org.apache.oozie.service.PurgeService",
            "org.apache.oozie.service.CoordMaterializeTriggerService", "org.apache.oozie.service.RecoveryService" };

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        setSystemProperty(SchemaService.WF_CONF_EXT_SCHEMAS, "wf-ext-schema.xsd");
        services = new Services();
        setClassesToBeExcluded(services.getConf(), excludedServices);
        services.init();
        services.get(ActionService.class).register(ForTestingActionExecutor.class);
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }


    /**
     * Tests the {@link org.apache.oozie.service.PurgeService}.
     * </p>
     * Creates and runs a new workflow job to completion.
     * Attempts to purge jobs older than a day. Verifies the presence of the job in the system.
     * </p>
     * Sets the end date for the same job to make it qualify for the purge criteria.
     * Calls the purge service, and ensure the job does not exist in the system.
     */
    public void testPurgeServiceForWorkflow() throws Exception {
        Reader reader = IOUtils.getResourceAsReader("wf-ext-schema-valid.xml", -1);
        Writer writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
        IOUtils.copyCharStream(reader, writer);

        final DagEngine engine = new DagEngine("u", "a");
        Configuration conf = new XConfiguration();
        conf.set(OozieClient.APP_PATH, "file://" + getTestCaseDir() + File.separator + "workflow.xml");
        conf.setStrings(OozieClient.USER_NAME, getTestUser());
        conf.setStrings(OozieClient.GROUP_NAME, getTestGroup());

        conf.set(OozieClient.LOG_TOKEN, "t");

        conf.set("external-status", "ok");
        conf.set("signal-value", "based_on_action_status");
        final String jobId = engine.submitJob(conf, true);

        waitFor(5000, new Predicate() {
            public boolean evaluate() throws Exception {
                return (engine.getJob(jobId).getStatus() == WorkflowJob.Status.SUCCEEDED);
            }
        });
        assertEquals(WorkflowJob.Status.SUCCEEDED, engine.getJob(jobId).getStatus());
        new PurgeXCommand(1, 10000).call();
        sleep(1000);

        JPAService jpaService = Services.get().get(JPAService.class);
        WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(jobId);
        WorkflowJobBean wfBean = jpaService.execute(wfJobGetCmd);
        Date endDate = new Date(System.currentTimeMillis() - 2 * 24 * 60 * 60 * 1000);
        wfBean.setEndTime(endDate);
        WorkflowJobUpdateJPAExecutor wfUpdateCmd = new WorkflowJobUpdateJPAExecutor(wfBean);
        jpaService.execute(wfUpdateCmd);

        Runnable purgeRunnable = new PurgeRunnable(1, 1, 1, 100);
        purgeRunnable.run();

        waitFor(10000, new Predicate() {
            public boolean evaluate() throws Exception {
                try {
                    engine.getJob(jobId).getStatus();
                }
                catch (Exception ex) {
                    return true;
                }
                return false;
            }
        });

        try {
            engine.getJob(jobId).getStatus();
            fail("Job should be purged. Should fail.");
        }
        catch (Exception ex) {
            assertEquals(ex.getClass(), DagEngineException.class);
            DagEngineException dex = (DagEngineException) ex;
            assertEquals(ErrorCode.E0604, dex.getErrorCode());
        }

    }

    /**
     * Tests the {@link org.apache.oozie.service.PurgeService}.
     * </p>
     * Creates a new coordinator job. Attempts to purge jobs older than a day.
     * Verifies the presence of the job in the system.
     * </p>
     * Sets the end date for the same job to make it qualify for the purge criteria.
     * Calls the purge service, and ensure the job does not exist in the system.
     */
    public void testPurgeServiceForCoordinator() throws Exception {
        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, start, end, false, false, 0);

        final String jobId = job.getId();
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetJPAExecutor coordJobGetExecutor = new CoordJobGetJPAExecutor(job.getId());
        CoordActionGetJPAExecutor coordActionGetExecutor = new CoordActionGetJPAExecutor(action.getId());

        job = jpaService.execute(coordJobGetExecutor);
        action = jpaService.execute(coordActionGetExecutor);
        assertEquals(job.getStatus(), CoordinatorJob.Status.SUCCEEDED);
        assertEquals(action.getStatus(), CoordinatorAction.Status.SUCCEEDED);

        Runnable purgeRunnable = new PurgeRunnable(1, 1, 1, 100);
        purgeRunnable.run();

        final CoordinatorEngine engine = new CoordinatorEngine("u", "a");
        waitFor(10000, new Predicate() {
            public boolean evaluate() throws Exception {
                try {
                    engine.getCoordJob(jobId).getStatus();
                }
                catch (Exception ex) {
                    return true;
                }
                return false;
            }
        });

        try {
            job = jpaService.execute(coordJobGetExecutor);
            fail("Job should be purged. Should fail.");
        }
        catch (JPAExecutorException je) {
            // Job doesn't exist. Exception is expected.
        }

        try {
            jpaService.execute(coordActionGetExecutor);
            fail("Action should be purged. Should fail.");
        }
        catch (JPAExecutorException je) {
            // Job doesn't exist. Exception is expected.
        }
    }

    /**
     * Tests the {@link org.apache.oozie.service.PurgeService}.
     * </p>
     * Creates a new Bundle job. Attempts to purge jobs older than a day.
     * Verifies the presence of the job in the system.
     * </p>
     * Sets the end date for the same job to make it qualify for the purge criteria.
     * Calls the purge service, and ensure the job does not exist in the system.
     */
    public void testPurgeServiceForBundle() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateOozieTZ(
            "2011-01-01T01:00Z"));
        final String jobId = job.getId();
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

        Runnable purgeRunnable = new PurgeRunnable(1, 1, 1, 100);
        purgeRunnable.run();

        final BundleEngine engine = new BundleEngine("u", "a");
        waitFor(10000, new Predicate() {
            public boolean evaluate() throws Exception {
                try {
                    engine.getBundleJob(jobId).getStatus();
                }
                catch (Exception ex) {
                    return true;
                }
                return false;
            }
        });

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

    @Override
    protected CoordinatorJobBean addRecordToCoordJobTable(CoordinatorJob.Status status, Date start, Date end,
            boolean pending, boolean doneMatd, int lastActionNum) throws Exception {
        CoordinatorJobBean coordJob = createCoordJob(status, start, end, pending, doneMatd, lastActionNum);
        coordJob.setLastModifiedTime(DateUtils.parseDateOozieTZ("2009-12-18T01:00Z"));
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordJobInsertJPAExecutor coordInsertCmd = new CoordJobInsertJPAExecutor(coordJob);
            jpaService.execute(coordInsertCmd);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test coord job record to table");
            throw je;
        }

        return coordJob;
    }

}
