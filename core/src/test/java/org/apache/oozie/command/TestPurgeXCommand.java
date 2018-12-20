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

package org.apache.oozie.command;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.executor.jpa.BundleActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.QueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor.WorkflowJobQuery;
import org.apache.oozie.service.CoordMaterializeTriggerService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.LiteWorkflowStoreService;
import org.apache.oozie.service.PauseTransitService;
import org.apache.oozie.service.PurgeService;
import org.apache.oozie.service.RecoveryService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.StatusTransitService;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.service.WorkflowStoreService;
import org.apache.oozie.service.UUIDService.ApplicationType;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.WorkflowLib;
import org.apache.oozie.workflow.lite.EndNodeDef;
import org.apache.oozie.workflow.lite.LiteWorkflowApp;
import org.apache.oozie.workflow.lite.LiteWorkflowInstance;
import org.apache.oozie.workflow.lite.StartNodeDef;

public class TestPurgeXCommand extends XDataTestCase {
    private JPAService jpaService;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        new Services().init();
        Services.get().get(StatusTransitService.class).destroy();
        Services.get().get(PauseTransitService.class).destroy();
        Services.get().get(PurgeService.class).destroy();
        Services.get().get(CoordMaterializeTriggerService.class).destroy();
        Services.get().get(RecoveryService.class).destroy();
        jpaService = Services.get().get(JPAService.class);
    }

    @Override
    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    /**
     * Test : purge succeeded wf job and action successfully. Creates and runs a
     * new job to completion. Attempts to purge jobs older than a day. Verifies
     * the presence of the job in the system. </p> Sets the end date for the
     * same job to make it qualify for the purge criteria. Calls the purge
     * command, and ensure the job does not exist in the system.
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testSucJobPurgeXCommand() throws Exception {
        WorkflowJobBean job = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean action = addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.OK);

        new PurgeXCommand(7, 1, 1, 10).call();

        assertWorkflowJobPurged(job);
        assertWorkflowActionPurged(action);
    }

    /**
     * Test : purge failed wf job and action successfully
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testFailJobPurgeXCommand() throws Exception {
        WorkflowJobBean job = addRecordToWfJobTable(WorkflowJob.Status.FAILED, WorkflowInstance.Status.FAILED);
        WorkflowActionBean action = addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.FAILED);

        new PurgeXCommand(7, 1, 1, 10).call();

        assertWorkflowJobPurged(job);
        assertWorkflowActionPurged(action);
    }

    /**
    * Test : purge killed wf job and action successfully
    *
    * @throws Exception if cannot insert records to the database
    */
    public void testKillJobPurgeXCommand() throws Exception {
        WorkflowJobBean job = addRecordToWfJobTable(WorkflowJob.Status.KILLED, WorkflowInstance.Status.KILLED);
        WorkflowActionBean action = addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.KILLED);

        new PurgeXCommand(7, 1, 1, 10).call();

        assertWorkflowJobPurged(job);
        assertWorkflowActionPurged(action);
    }

    /**
     * Test : purge wf job and action failed
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeXCommandFailed() throws Exception {
        WorkflowJobBean job = addRecordToWfJobTableForNegCase(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action = addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.RUNNING);

        new PurgeXCommand(7, 1, 1, 10).call();

        assertWorkflowJobNotPurged(job);
        assertWorkflowActionNotPurged(action);
    }

    /**
     * Test : purge succeeded coord job and action successfully
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testSucCoordPurgeXCommand() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);

        new PurgeXCommand(1, 7, 1, 10).call();

        assertCoordinatorJobPurged(job);
        assertCoordinatorActionPurged(action);
    }

    /**
     * Test : purge failed coord job and action successfully
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testFailCoordPurgeXCommand() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.FAILED, false, false);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.FAILED,
                "coord-action-get.xml", 0);

        new PurgeXCommand(1, 7, 1, 10).call();

        assertCoordinatorJobPurged(job);
        assertCoordinatorActionPurged(action);
    }

    /**
     * Test : purge killed coord job and action successfully
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testKillCoordPurgeXCommand() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.KILLED, false, false);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.KILLED,
                "coord-action-get.xml", 0);

        new PurgeXCommand(1, 7, 1, 10).call();

        assertCoordinatorJobPurged(job);
        assertCoordinatorActionPurged(action);
    }

    /**
     * Test : purge coord job and action failed
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testCoordPurgeXCommandFailed() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);

        new PurgeXCommand(1, 7, 1, 10).call();

        assertCoordinatorJobNotPurged(job);
        assertCoordinatorActionNotPurged(action);
    }

    /**
     * Test : purge succeeded bundle job and action successfully
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testSucBundlePurgeXCommand() throws Exception {
        BundleJobBean job = addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateOozieTZ(
            "2011-01-01T01:00Z"));
        CoordinatorJobBean coordJob1 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        coordJob1.setAppName("action1");
        CoordinatorJobBean coordJob2 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        coordJob2.setAppName("action2");
        BundleActionBean bundleAction1 = addRecordToBundleActionTable(job.getId(), coordJob1.getAppName(), 0, Job.Status.SUCCEEDED);
        BundleActionBean bundleAction2 = addRecordToBundleActionTable(job.getId(), coordJob2.getAppName(), 0, Job.Status.SUCCEEDED);

        new PurgeXCommand(1, 1, 7, 10).call();

        assertBundleJobPurged(job);

        assertBundleActionPurged(bundleAction1);
        assertBundleActionPurged(bundleAction2);
    }

    /**
     * Test : purge failed bundle job and action successfully
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testFailBundlePurgeXCommand() throws Exception {
        BundleJobBean job = addRecordToBundleJobTable(Job.Status.DONEWITHERROR, DateUtils.parseDateOozieTZ(
            "2011-01-01T01:00Z"));
        CoordinatorJobBean coordJob1 = addRecordToCoordJobTable(CoordinatorJob.Status.DONEWITHERROR, false, false);
        coordJob1.setAppName("action1");
        CoordinatorJobBean coordJob2 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        coordJob2.setAppName("action2");

        BundleActionBean bundleAction1 = addRecordToBundleActionTable(job.getId(), coordJob1.getAppName(), 0, Job.Status.FAILED);
        BundleActionBean bundleAction2 = addRecordToBundleActionTable(job.getId(), coordJob2.getAppName(), 0, Job.Status.SUCCEEDED);

        new PurgeXCommand(1, 1, 7, 10).call();

        assertBundleJobPurged(job);

        assertBundleActionPurged(bundleAction1);
        assertBundleActionPurged(bundleAction2);
    }

    /**
     * Test : purge killed bundle job and action successfully
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testKillBundlePurgeXCommand() throws Exception {
        BundleJobBean job = addRecordToBundleJobTable(Job.Status.KILLED, DateUtils.parseDateOozieTZ(
            "2011-01-01T01:00Z"));
        CoordinatorJobBean coordJob1 = addRecordToCoordJobTable(CoordinatorJob.Status.KILLED, false, false);
        coordJob1.setAppName("action1");
        CoordinatorJobBean coordJob2 = addRecordToCoordJobTable(CoordinatorJob.Status.KILLED, false, false);
        coordJob2.setAppName("action2");
        BundleActionBean bundleAction1 = addRecordToBundleActionTable(job.getId(), "action1", 0, Job.Status.KILLED);
        BundleActionBean bundleAction2 = addRecordToBundleActionTable(job.getId(), "action2", 0, Job.Status.KILLED);

        new PurgeXCommand(1, 1, 7, 10).call();

        assertBundleJobPurged(job);

        assertBundleActionPurged(bundleAction1);
        assertBundleActionPurged(bundleAction2);
    }

    /**
     * Test : purge bundle job and action failed
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testBundlePurgeXCommandFailed() throws Exception {
        BundleJobBean job = addRecordToBundleJobTable(Job.Status.RUNNING, DateUtils.parseDateOozieTZ(
            "2011-01-01T01:00Z"));
        CoordinatorJobBean coordJob1 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        coordJob1.setAppName("action1");
        CoordinatorJobBean coordJob2 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        coordJob2.setAppName("action2");
        BundleActionBean bundleActionBean1 = addRecordToBundleActionTable(job.getId(), "action1", 0, Job.Status.RUNNING);
        BundleActionBean bundleActionBean2 = addRecordToBundleActionTable(job.getId(), "action2", 0, Job.Status.SUCCEEDED);

        new PurgeXCommand(1, 1, 7, 10).call();

        assertBundleJobNotPurged(job);
        assertBundleActionNotPurged(bundleActionBean1);
        assertBundleActionNotPurged(bundleActionBean2);
    }

    /**
     * Test : The workflow should get purged, but the coordinator parent shouldn't get purged --> neither will get purged
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeCoordWithWFChild1() throws Exception {
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob.getId(), "SUCCEEDED", 0);

        new PurgeXCommand(7, getNumDaysToNotBePurged(coordJob.getLastModifiedTime()), 1, 10).call();

        assertCoordinatorJobNotPurged(coordJob);
        assertCoordinatorActionNotPurged(coordAction);
        assertWorkflowJobNotPurged(wfJob);
        assertWorkflowActionNotPurged(wfAction);
    }

    /**
     * Test : The workflow should get purged, but the coordinator parent shouldn't get purged -->
     * the workflow and corresponding coord actions will get purged after we turn the purge.old.coord.action on
     * Coordinator itself will not be purged
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeLongRunningCoordWithWFChild() throws Exception {
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob.getId(), "SUCCEEDED", 0);

        new PurgeXCommand(7, getNumDaysToNotBePurged(coordJob.getLastModifiedTime()), 1, 10, true).call();

        assertCoordinatorJobNotPurged(coordJob);
        assertCoordinatorActionPurged(coordAction);
        assertWorkflowJobPurged(wfJob);
        assertWorkflowActionPurged(wfAction);
    }

    /**
     * Test : The workflow should get purged, but the coordinator parent shouldn't get purged -->
     * the workflow and corresponding coord actions will NOT get purged after we turn the purge.old.coord.action off
     * Neither will be purged
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeLongRunningCoordWithWFChildNegative() throws Exception {
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob.getId(), "SUCCEEDED", 0);

        new PurgeXCommand(7, getNumDaysToNotBePurged(coordJob.getLastModifiedTime()), 1, 10, false).call();

        assertCoordinatorJobNotPurged(coordJob);
        assertCoordinatorActionNotPurged(coordAction);
        assertWorkflowJobNotPurged(wfJob);
        assertWorkflowActionNotPurged(wfAction);
    }

    /**
     * Test : The workflow should not get purged, but the coordinator parent should get purged --> neither will get purged
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeCoordWithWFChild2() throws Exception {
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob.getId(), "SUCCEEDED", 0);

        new PurgeXCommand(getNumDaysToNotBePurged(wfJob.getEndTime()), 7, 1, 10).call();

        assertCoordinatorJobNotPurged(coordJob);
        assertCoordinatorActionNotPurged(coordAction);
        assertWorkflowJobNotPurged(wfJob);
        assertWorkflowActionNotPurged(wfAction);
    }

    /**
     * Test : The workflows should not get purged, but the coordinator parent should get purged --> none will get purged
     * There are more workflow children than the limit
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeCoordWithWFChild2MoreThanLimit() throws Exception {
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        WorkflowJobBean wfJob1 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob2 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob3 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob4 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob5 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction1 = addRecordToWfActionTable(wfJob1.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction2 = addRecordToWfActionTable(wfJob2.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction3 = addRecordToWfActionTable(wfJob3.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction4 = addRecordToWfActionTable(wfJob4.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction5 = addRecordToWfActionTable(wfJob5.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction1 = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob1.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction2 = addRecordToCoordActionTable(coordJob.getId(), 2, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob2.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction3 = addRecordToCoordActionTable(coordJob.getId(), 3, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob3.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction4 = addRecordToCoordActionTable(coordJob.getId(), 4, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob4.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction5 = addRecordToCoordActionTable(coordJob.getId(), 5, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob5.getId(), "SUCCEEDED", 0);

        new PurgeXCommand(getNumDaysToNotBePurged(wfJob1.getEndTime()), 7, 1, 3).call();

        assertCoordinatorJobNotPurged(coordJob);

        assertCoordinatorActionNotPurged(coordAction1);
        assertCoordinatorActionNotPurged(coordAction2);
        assertCoordinatorActionNotPurged(coordAction3);
        assertCoordinatorActionNotPurged(coordAction4);
        assertCoordinatorActionNotPurged(coordAction5);

        assertWorkflowJobNotPurged(wfJob1);
        assertWorkflowJobNotPurged(wfJob2);
        assertWorkflowJobNotPurged(wfJob3);
        assertWorkflowJobNotPurged(wfJob4);
        assertWorkflowJobNotPurged(wfJob5);

        assertWorkflowActionNotPurged(wfAction1);
        assertWorkflowActionNotPurged(wfAction2);
        assertWorkflowActionNotPurged(wfAction3);
        assertWorkflowActionNotPurged(wfAction4);
        assertWorkflowActionNotPurged(wfAction5);
    }

    /**
     * Test : The workflow should get purged, and the coordinator parent should get purged --> both will get purged
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeCoordWithWFChild3() throws Exception {
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob.getId(), "SUCCEEDED", 0);

        new PurgeXCommand(7, 7, 1, 10).call();

        assertCoordinatorJobPurged(coordJob);
        assertCoordinatorActionPurged(coordAction);
        assertWorkflowJobPurged(wfJob);
        assertWorkflowActionPurged(wfAction);
    }

    /**
     * Test : The workflow should get purged, and the coordinator parent should get purged --> both will get purged
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeCoordWithWFChild3MoreThanLimit() throws Exception {
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        WorkflowJobBean wfJob1 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob2 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob3 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob4 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob5 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction1 = addRecordToWfActionTable(wfJob1.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction2 = addRecordToWfActionTable(wfJob2.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction3 = addRecordToWfActionTable(wfJob3.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction4 = addRecordToWfActionTable(wfJob4.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction5 = addRecordToWfActionTable(wfJob5.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction1 = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob1.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction2 = addRecordToCoordActionTable(coordJob.getId(), 2, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob2.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction3 = addRecordToCoordActionTable(coordJob.getId(), 3, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob3.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction4 = addRecordToCoordActionTable(coordJob.getId(), 4, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob4.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction5 = addRecordToCoordActionTable(coordJob.getId(), 5, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob5.getId(), "SUCCEEDED", 0);

        new PurgeXCommand(7, 7, 1, 10).call();

        assertCoordinatorJobPurged(coordJob);

        assertCoordinatorActionPurged(coordAction1);
        assertCoordinatorActionPurged(coordAction2);
        assertCoordinatorActionPurged(coordAction3);
        assertCoordinatorActionPurged(coordAction4);
        assertCoordinatorActionPurged(coordAction5);

        assertWorkflowJobPurged(wfJob1);
        assertWorkflowJobPurged(wfJob2);
        assertWorkflowJobPurged(wfJob3);
        assertWorkflowJobPurged(wfJob4);
        assertWorkflowJobPurged(wfJob5);

        assertWorkflowActionPurged(wfAction1);
        assertWorkflowActionPurged(wfAction2);
        assertWorkflowActionPurged(wfAction3);
        assertWorkflowActionPurged(wfAction4);
        assertWorkflowActionPurged(wfAction5);
    }

    /**
     * Test : The workflow and coordinator should get purged, but the bundle parent shouldn't get purged --> none will get purged
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeBundleWithCoordChildWithWFChild1() throws Exception {
        BundleJobBean bundleJob = addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-01-01T01:00Z"));
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob.getId(), "SUCCEEDED", 0);
        BundleActionBean bundleAction = addRecordToBundleActionTable(bundleJob.getId(), coordJob.getId(), coordJob.getAppName(), 0,
                Job.Status.SUCCEEDED);

        new PurgeXCommand(7, 7, getNumDaysToNotBePurged(bundleJob.getLastModifiedTime()), 10).call();

        assertBundleJobNotPurged(bundleJob);
        assertBundleActionNotPurged(bundleAction);
        assertCoordinatorJobNotPurged(coordJob);
        assertCoordinatorActionNotPurged(coordAction);
        assertWorkflowJobNotPurged(wfJob);
        assertWorkflowActionNotPurged(wfAction);
    }

    /**
     * Test : The workflow and coordinator should not get purged, but the bundle parent should get purged --> none will get purged
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeBundleWithCoordChildWithWFChild2() throws Exception {
        BundleJobBean bundleJob = addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-01-01T01:00Z"));
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob.getId(), "SUCCEEDED", 0);
        BundleActionBean bundleAction = addRecordToBundleActionTable(bundleJob.getId(), coordJob.getId(), coordJob.getAppName(), 0,
                Job.Status.SUCCEEDED);

        new PurgeXCommand(getNumDaysToNotBePurged(wfJob.getEndTime()),
                getNumDaysToNotBePurged(coordJob.getLastModifiedTime()), 7, 10).call();

        assertBundleJobNotPurged(bundleJob);
        assertBundleActionNotPurged(bundleAction);
        assertCoordinatorJobNotPurged(coordJob);
        assertCoordinatorActionNotPurged(coordAction);
        assertWorkflowJobNotPurged(wfJob);
        assertWorkflowActionNotPurged(wfAction);
    }

    /**
     * Test : The workflow and coordinator should not get purged, but the bundle parent should get purged --> none will get purged
     * There are more coordinator children than the limit
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeBundleWithCoordChildWithWFChild2MoreThanLimit() throws Exception {
        BundleJobBean bundleJob = addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-01-01T01:00Z"));
        CoordinatorJobBean coordJob1 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        CoordinatorJobBean coordJob2 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        coordJob2.setAppName("coord2");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob2);
        CoordinatorJobBean coordJob3 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        coordJob3.setAppName("coord3");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob3);
        CoordinatorJobBean coordJob4 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        coordJob4.setAppName("coord4");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob4);
        CoordinatorJobBean coordJob5 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        coordJob5.setAppName("coord5");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob5);
        WorkflowJobBean wfJob1 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob2 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob3 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob4 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob5 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction1 = addRecordToWfActionTable(wfJob1.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction2 = addRecordToWfActionTable(wfJob2.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction3 = addRecordToWfActionTable(wfJob3.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction4 = addRecordToWfActionTable(wfJob4.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction5 = addRecordToWfActionTable(wfJob5.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction1 = addRecordToCoordActionTable(coordJob1.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob1.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction2 = addRecordToCoordActionTable(coordJob2.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob2.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction3 = addRecordToCoordActionTable(coordJob3.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob3.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction4 = addRecordToCoordActionTable(coordJob4.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob4.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction5 = addRecordToCoordActionTable(coordJob5.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob5.getId(), "SUCCEEDED", 0);
        BundleActionBean bundleAction1 = addRecordToBundleActionTable(bundleJob.getId(), coordJob1.getId(), coordJob1.getAppName(),
                0, Job.Status.SUCCEEDED);
        BundleActionBean bundleAction2 = addRecordToBundleActionTable(bundleJob.getId(), coordJob2.getId(), coordJob2.getAppName(),
                0, Job.Status.SUCCEEDED);
        BundleActionBean bundleAction3 = addRecordToBundleActionTable(bundleJob.getId(), coordJob3.getId(), coordJob3.getAppName(),
                0, Job.Status.SUCCEEDED);
        BundleActionBean bundleAction4 = addRecordToBundleActionTable(bundleJob.getId(), coordJob4.getId(), coordJob4.getAppName(),
                0, Job.Status.SUCCEEDED);
        BundleActionBean bundleAction5 = addRecordToBundleActionTable(bundleJob.getId(), coordJob5.getId(), coordJob5.getAppName(),
                0, Job.Status.SUCCEEDED);

        new PurgeXCommand(getNumDaysToNotBePurged(wfJob1.getEndTime()),
                getNumDaysToNotBePurged(coordJob1.getLastModifiedTime()), 7, 3).call();

        assertBundleJobNotPurged(bundleJob);

        assertBundleActionNotPurged(bundleAction1);
        assertBundleActionNotPurged(bundleAction2);
        assertBundleActionNotPurged(bundleAction3);
        assertBundleActionNotPurged(bundleAction4);
        assertBundleActionNotPurged(bundleAction5);

        assertCoordinatorJobNotPurged(coordJob1);
        assertCoordinatorJobNotPurged(coordJob2);
        assertCoordinatorJobNotPurged(coordJob3);
        assertCoordinatorJobNotPurged(coordJob4);
        assertCoordinatorJobNotPurged(coordJob5);

        assertCoordinatorActionNotPurged(coordAction1);
        assertCoordinatorActionNotPurged(coordAction2);
        assertCoordinatorActionNotPurged(coordAction3);
        assertCoordinatorActionNotPurged(coordAction4);
        assertCoordinatorActionNotPurged(coordAction5);

        assertWorkflowJobNotPurged(wfJob1);
        assertWorkflowJobNotPurged(wfJob2);
        assertWorkflowJobNotPurged(wfJob3);
        assertWorkflowJobNotPurged(wfJob4);
        assertWorkflowJobNotPurged(wfJob5);

        assertWorkflowActionNotPurged(wfAction1);
        assertWorkflowActionNotPurged(wfAction2);
        assertWorkflowActionNotPurged(wfAction3);
        assertWorkflowActionNotPurged(wfAction4);
        assertWorkflowActionNotPurged(wfAction5);
    }

    /**
     * Test : The workflow and coordinator should get purged, and the bundle parent should get purged --> all will get purged
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeBundleWithCoordChildWithWFChild3() throws Exception {
        BundleJobBean bundleJob = addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-01-01T01:00Z"));
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob.getId(), "SUCCEEDED", 0);
        BundleActionBean bundleAction = addRecordToBundleActionTable(bundleJob.getId(), coordJob.getId(), coordJob.getAppName(), 0,
                Job.Status.SUCCEEDED);

        new PurgeXCommand(7, 7, 7, 10).call();

        assertBundleJobPurged(bundleJob);
        assertBundleActionPurged(bundleAction);
        assertCoordinatorJobPurged(coordJob);
        assertCoordinatorActionPurged(coordAction);
        assertWorkflowJobPurged(wfJob);
        assertWorkflowActionPurged(wfAction);
    }

    /**
     * Test : The workflow and coordinator should get purged, and the bundle parent should get purged --> all will get purged
     * There are more coordinator children than the limit
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeBundleWithCoordChildWithWFChild3MoreThanLimit() throws Exception {
        BundleJobBean bundleJob = addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-01-01T01:00Z"));
        CoordinatorJobBean coordJob1 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        CoordinatorJobBean coordJob2 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        coordJob2.setAppName("coord2");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob2);
        CoordinatorJobBean coordJob3 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        coordJob3.setAppName("coord3");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob3);
        CoordinatorJobBean coordJob4 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        coordJob4.setAppName("coord4");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob4);
        CoordinatorJobBean coordJob5 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        coordJob5.setAppName("coord5");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob5);
        WorkflowJobBean wfJob1 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob2 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob3 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob4 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean wfJob5 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction1 = addRecordToWfActionTable(wfJob1.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction2 = addRecordToWfActionTable(wfJob2.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction3 = addRecordToWfActionTable(wfJob3.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction4 = addRecordToWfActionTable(wfJob4.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction5 = addRecordToWfActionTable(wfJob5.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction1 = addRecordToCoordActionTable(coordJob1.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob1.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction2 = addRecordToCoordActionTable(coordJob2.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob2.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction3 = addRecordToCoordActionTable(coordJob3.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob3.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction4 = addRecordToCoordActionTable(coordJob4.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob4.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordAction5 = addRecordToCoordActionTable(coordJob5.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob5.getId(), "SUCCEEDED", 0);
        BundleActionBean bundleAction1 = addRecordToBundleActionTable(bundleJob.getId(), coordJob1.getId(), coordJob1.getAppName(),
                0, Job.Status.SUCCEEDED);
        BundleActionBean bundleAction2 = addRecordToBundleActionTable(bundleJob.getId(), coordJob2.getId(), coordJob2.getAppName(),
                0, Job.Status.SUCCEEDED);
        BundleActionBean bundleAction3 = addRecordToBundleActionTable(bundleJob.getId(), coordJob3.getId(), coordJob3.getAppName(),
                0, Job.Status.SUCCEEDED);
        BundleActionBean bundleAction4 = addRecordToBundleActionTable(bundleJob.getId(), coordJob4.getId(), coordJob4.getAppName(),
                0, Job.Status.SUCCEEDED);
        BundleActionBean bundleAction5 = addRecordToBundleActionTable(bundleJob.getId(), coordJob5.getId(), coordJob5.getAppName(),
                0, Job.Status.SUCCEEDED);

        new PurgeXCommand(7, 7, 7, 10).call();

        assertBundleJobPurged(bundleJob);

        assertBundleActionPurged(bundleAction1);
        assertBundleActionPurged(bundleAction2);
        assertBundleActionPurged(bundleAction3);
        assertBundleActionPurged(bundleAction4);
        assertBundleActionPurged(bundleAction5);

        assertCoordinatorJobPurged(coordJob1);
        assertCoordinatorJobPurged(coordJob2);
        assertCoordinatorJobPurged(coordJob3);
        assertCoordinatorJobPurged(coordJob4);
        assertCoordinatorJobPurged(coordJob5);

        assertCoordinatorActionPurged(coordAction1);
        assertCoordinatorActionPurged(coordAction2);
        assertCoordinatorActionPurged(coordAction3);
        assertCoordinatorActionPurged(coordAction4);
        assertCoordinatorActionPurged(coordAction5);

        assertWorkflowJobPurged(wfJob1);
        assertWorkflowJobPurged(wfJob2);
        assertWorkflowJobPurged(wfJob3);
        assertWorkflowJobPurged(wfJob4);
        assertWorkflowJobPurged(wfJob5);

        assertWorkflowActionPurged(wfAction1);
        assertWorkflowActionPurged(wfAction2);
        assertWorkflowActionPurged(wfAction3);
        assertWorkflowActionPurged(wfAction4);
        assertWorkflowActionPurged(wfAction5);
    }

    /**
     * Test : The subworkflow should get purged, but the workflow parent shouldn't get purged --> neither will get purged
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeWFWithSubWF1() throws Exception {
        WorkflowJobBean wfJob = addRecordToWfJobTableForNegCase(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowJobBean subwfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        WorkflowActionBean subwfAction = addRecordToWfActionTable(subwfJob.getId(), "1", WorkflowAction.Status.OK);

        new PurgeXCommand(7, 1, 1, 10).call();

        assertWorkflowJobNotPurged(wfJob);
        assertWorkflowActionNotPurged(wfAction);
        assertWorkflowJobNotPurged(subwfJob);
        assertWorkflowActionNotPurged(subwfAction);
    }

    /**
     * Test : The subworkflow shouldn't get purged, but the workflow parent should get purged --> neither will get purged
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeWFWithSubWF2() throws Exception {
        WorkflowJobBean wfJob = addRecordToWfJobTableForNegCase(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowJobBean subwfJob = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING,
                wfJob.getId());
        WorkflowActionBean subwfAction = addRecordToWfActionTable(subwfJob.getId(), "1", WorkflowAction.Status.RUNNING);

        new PurgeXCommand(7, 1, 1, 10).call();

        assertWorkflowJobNotPurged(wfJob);
        assertWorkflowActionNotPurged(wfAction);
        assertWorkflowJobNotPurged(subwfJob);
        assertWorkflowActionNotPurged(subwfAction);
    }

    /**
     * Test : The subworkflows shouldn't get purged, but the workflow parent should get purged --> none will get purged
     * There are more subworkflow children than the limit
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeWFWithSubWF2MoreThanLimit() throws Exception {
        WorkflowJobBean wfJob = addRecordToWfJobTableForNegCase(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction1 = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction2 = addRecordToWfActionTable(wfJob.getId(), "2", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction3 = addRecordToWfActionTable(wfJob.getId(), "3", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction4 = addRecordToWfActionTable(wfJob.getId(), "4", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction5 = addRecordToWfActionTable(wfJob.getId(), "5", WorkflowAction.Status.OK);
        WorkflowJobBean subwfJob1 = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING,
                wfJob.getId());
        WorkflowJobBean subwfJob2 = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING,
                wfJob.getId());
        WorkflowJobBean subwfJob3 = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING,
                wfJob.getId());
        WorkflowJobBean subwfJob4 = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING,
                wfJob.getId());
        WorkflowJobBean subwfJob5 = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING,
                wfJob.getId());
        WorkflowActionBean subwfAction1 = addRecordToWfActionTable(subwfJob1.getId(), "1", WorkflowAction.Status.RUNNING);
        WorkflowActionBean subwfAction2 = addRecordToWfActionTable(subwfJob2.getId(), "1", WorkflowAction.Status.RUNNING);
        WorkflowActionBean subwfAction3 = addRecordToWfActionTable(subwfJob3.getId(), "1", WorkflowAction.Status.RUNNING);
        WorkflowActionBean subwfAction4 = addRecordToWfActionTable(subwfJob4.getId(), "1", WorkflowAction.Status.RUNNING);
        WorkflowActionBean subwfAction5 = addRecordToWfActionTable(subwfJob5.getId(), "1", WorkflowAction.Status.RUNNING);

        new PurgeXCommand(7, 1, 1, 10).call();

        assertWorkflowJobNotPurged(wfJob);

        assertWorkflowActionNotPurged(wfAction1);
        assertWorkflowActionNotPurged(wfAction2);
        assertWorkflowActionNotPurged(wfAction3);
        assertWorkflowActionNotPurged(wfAction4);
        assertWorkflowActionNotPurged(wfAction5);

        assertWorkflowJobNotPurged(subwfJob1);
        assertWorkflowJobNotPurged(subwfJob2);
        assertWorkflowJobNotPurged(subwfJob3);
        assertWorkflowJobNotPurged(subwfJob4);
        assertWorkflowJobNotPurged(subwfJob5);

        assertWorkflowActionNotPurged(subwfAction1);
        assertWorkflowActionNotPurged(subwfAction2);
        assertWorkflowActionNotPurged(subwfAction3);
        assertWorkflowActionNotPurged(subwfAction4);
        assertWorkflowActionNotPurged(subwfAction5);
    }

    /**
     * Test : The subworkflow should get purged, and the workflow parent should get purged --> both will get purged
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeWFWithSubWF3() throws Exception {
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowJobBean subwfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        WorkflowActionBean subwfAction = addRecordToWfActionTable(subwfJob.getId(), "1", WorkflowAction.Status.OK);

        new PurgeXCommand(7, 1, 1, 10).call();

        assertWorkflowJobPurged(wfJob);
        assertWorkflowActionPurged(wfAction);
        assertWorkflowJobPurged(subwfJob);
        assertWorkflowActionPurged(subwfAction);
    }

    /**
     * Test : The subworkflow should get purged, and the workflow parent should get purged --> both will get purged
     * There are more subworkflow children than the limit
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeWFWithSubWF3MoreThanLimit() throws Exception {
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction1 = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction2 = addRecordToWfActionTable(wfJob.getId(), "2", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction3 = addRecordToWfActionTable(wfJob.getId(), "3", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction4 = addRecordToWfActionTable(wfJob.getId(), "4", WorkflowAction.Status.OK);
        WorkflowActionBean wfAction5 = addRecordToWfActionTable(wfJob.getId(), "5", WorkflowAction.Status.OK);
        WorkflowJobBean subwfJob1 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        WorkflowJobBean subwfJob2 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        WorkflowJobBean subwfJob3 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        WorkflowJobBean subwfJob4 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        WorkflowJobBean subwfJob5 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        WorkflowActionBean subwfAction1 = addRecordToWfActionTable(subwfJob1.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean subwfAction2 = addRecordToWfActionTable(subwfJob2.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean subwfAction3 = addRecordToWfActionTable(subwfJob3.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean subwfAction4 = addRecordToWfActionTable(subwfJob4.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean subwfAction5 = addRecordToWfActionTable(subwfJob5.getId(), "1", WorkflowAction.Status.OK);

        new PurgeXCommand(7, 1, 1, 3).call();

        assertWorkflowJobPurged(wfJob);

        assertWorkflowActionPurged(wfAction1);
        assertWorkflowActionPurged(wfAction2);
        assertWorkflowActionPurged(wfAction3);
        assertWorkflowActionPurged(wfAction4);
        assertWorkflowActionPurged(wfAction5);

        assertWorkflowJobPurged(subwfJob1);
        assertWorkflowJobPurged(subwfJob2);
        assertWorkflowJobPurged(subwfJob3);
        assertWorkflowJobPurged(subwfJob4);
        assertWorkflowJobPurged(subwfJob5);

        assertWorkflowActionPurged(subwfAction1);
        assertWorkflowActionPurged(subwfAction2);
        assertWorkflowActionPurged(subwfAction3);
        assertWorkflowActionPurged(subwfAction4);
        assertWorkflowActionPurged(subwfAction5);
    }

    /**
     * Test : Tbe subsubworkflow shouldn't get purged,
     *        the subworkflow should get purged,
     *        the workflow parent should get purged --> neither will get purged
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeWFWithSubSubWF1() throws Exception {
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowJobBean subwfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        WorkflowActionBean subwfAction = addRecordToWfActionTable(subwfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowJobBean subsubwfJob = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING,
                subwfJob.getId());
        WorkflowActionBean subsubwfAction = addRecordToWfActionTable(subsubwfJob.getId(), "1", WorkflowAction.Status.RUNNING);

        new PurgeXCommand(7, 1, 1, 10).call();

        assertWorkflowJobNotPurged(wfJob);
        assertWorkflowActionNotPurged(wfAction);
        assertWorkflowJobNotPurged(subwfJob);
        assertWorkflowActionNotPurged(subwfAction);
        assertWorkflowJobNotPurged(subsubwfJob);
        assertWorkflowActionNotPurged(subsubwfAction);
    }

    /**
     * Test : Tbe subsubworkflow should get purged,
     *        the subworkflow shouldn't get purged,
     *        the workflow parent should get purged --> neither will get purged
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeWFWithSubSubWF2() throws Exception {
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowJobBean subwfJob = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING,
                wfJob.getId());
        WorkflowActionBean subwfAction = addRecordToWfActionTable(subwfJob.getId(), "1", WorkflowAction.Status.RUNNING);
        WorkflowJobBean subsubwfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                subwfJob.getId());
        WorkflowActionBean subsubwfAction = addRecordToWfActionTable(subsubwfJob.getId(), "1", WorkflowAction.Status.OK);

        new PurgeXCommand(7, 1, 1, 10).call();

        assertWorkflowJobNotPurged(wfJob);
        assertWorkflowActionNotPurged(wfAction);
        assertWorkflowJobNotPurged(subwfJob);
        assertWorkflowActionNotPurged(subwfAction);
        assertWorkflowJobNotPurged(subsubwfJob);
        assertWorkflowActionNotPurged(subsubwfAction);
    }

    /**
     * Test : Tbe subsubworkflows should get purged,
     *        the subworkflow should get purged,
     *        the workflow parent should get purged --> all will get purged
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeWFWithSubSubWF3() throws Exception {
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowJobBean subwfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        WorkflowActionBean subwfAction = addRecordToWfActionTable(subwfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowJobBean subsub1wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                subwfJob.getId());
        WorkflowJobBean subsub2wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                subwfJob.getId());
        WorkflowActionBean subsub1wfAction = addRecordToWfActionTable(subsub1wfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean subsub2wfAction = addRecordToWfActionTable(subsub2wfJob.getId(), "1", WorkflowAction.Status.OK);

        new PurgeXCommand(7, 1, 1, 10).call();

        assertWorkflowJobPurged(wfJob);
        assertWorkflowActionPurged(wfAction);
        assertWorkflowJobPurged(subwfJob);
        assertWorkflowActionPurged(subwfAction);
        assertWorkflowJobPurged(subsub1wfJob);
        assertWorkflowJobPurged(subsub2wfJob);
        assertWorkflowActionPurged(subsub1wfAction);
        assertWorkflowActionPurged(subsub2wfAction);
    }

    private void assertWorkflowJobNotPurged(WorkflowJobBean workflowJobBean) {
        try {
            WorkflowJobGetJPAExecutor jpaExecutor = new WorkflowJobGetJPAExecutor(workflowJobBean.getId());
            jpaService.execute(jpaExecutor);
        } catch (JPAExecutorException je) {
            fail("Workflow job "+workflowJobBean.getId()+" should not have been purged");
        }
    }

    private void assertWorkflowJobPurged(WorkflowJobBean workflowJobBean) {
        try {
            WorkflowJobGetJPAExecutor jpaExecutor = new WorkflowJobGetJPAExecutor(workflowJobBean.getId());
            jpaService.execute(jpaExecutor);
            fail("Workflow job "+workflowJobBean.getId()+" should have been purged");
        } catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }
    }

    private void assertWorkflowActionNotPurged(WorkflowActionBean workflowActionBean) {
        try {
            WorkflowActionGetJPAExecutor jpaExecutor = new WorkflowActionGetJPAExecutor(workflowActionBean.getId());
            jpaService.execute(jpaExecutor);
        } catch (JPAExecutorException je) {
            fail("Workflow action "+workflowActionBean.getId()+" should not have been purged");
        }
    }

    private void assertWorkflowActionPurged(WorkflowActionBean workflowActionBean) {
        try {
            WorkflowActionGetJPAExecutor jpaExecutor = new WorkflowActionGetJPAExecutor(workflowActionBean.getId());
            jpaService.execute(jpaExecutor);
            fail("Workflow job "+workflowActionBean.getId()+" should have been purged");
        } catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }
    }

    private void assertCoordinatorJobNotPurged(CoordinatorJobBean coordinatorJobBean) {
        try {
            CoordJobGetJPAExecutor jpaExecutor = new CoordJobGetJPAExecutor(coordinatorJobBean.getId());
            jpaService.execute(jpaExecutor);
        } catch (JPAExecutorException je) {
            fail("Coordinator job "+ coordinatorJobBean.getId()+" should not have been purged");
        }
    }

    private void assertCoordinatorJobPurged(CoordinatorJobBean coordinatorJobBean) {
        try {
            CoordJobGetJPAExecutor jpaExecutor = new CoordJobGetJPAExecutor(coordinatorJobBean.getId());
            jpaService.execute(jpaExecutor);
            fail("Coordinator job "+coordinatorJobBean.getId()+" should have been purged");
        } catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }
    }

    private void assertCoordinatorActionNotPurged(CoordinatorActionBean coordinatorActionBean) {
        try {
            CoordActionGetJPAExecutor jpaExecutor = new CoordActionGetJPAExecutor(coordinatorActionBean.getId());
            jpaService.execute(jpaExecutor);
        } catch (JPAExecutorException je) {
            fail("Coordinator action "+coordinatorActionBean.getId()+" should not have been purged");
        }
    }

    private void assertCoordinatorActionPurged(CoordinatorActionBean coordinatorActionBean) {
        try {
            CoordActionGetJPAExecutor jpaExecutor = new CoordActionGetJPAExecutor(coordinatorActionBean.getId());
            jpaService.execute(jpaExecutor);
            fail("Coordinator action "+coordinatorActionBean.getId()+" should have been purged");
        } catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }
    }

    private void assertBundleJobNotPurged(BundleJobBean bundleJobBean) {
        try {
            BundleJobGetJPAExecutor jpaExecutor = new BundleJobGetJPAExecutor(bundleJobBean.getId());
            jpaService.execute(jpaExecutor);
        } catch (JPAExecutorException je) {
            fail("Bundle job "+bundleJobBean.getId()+" should not have been purged");
        }
    }

    private void assertBundleJobPurged(BundleJobBean bundleJobBean) {
        try {
            BundleJobGetJPAExecutor jpaExecutor = new BundleJobGetJPAExecutor(bundleJobBean.getId());
            jpaService.execute(jpaExecutor);
            fail("Bundle job "+bundleJobBean.getId()+" should have been purged");
        } catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0604, je.getErrorCode());
        }
    }

    private void assertBundleActionNotPurged(BundleActionBean bundleActionBean) {
        try {
            BundleActionGetJPAExecutor jpaExecutor = new BundleActionGetJPAExecutor(bundleActionBean.getBundleId(), bundleActionBean.getCoordName());
            jpaService.execute(jpaExecutor);
        } catch (JPAExecutorException je) {
            fail("Bundle action "+bundleActionBean.getBundleActionId()+" should not have been purged");
        }
    }

    private void assertBundleActionPurged(BundleActionBean bundleActionBean) {
        try {
            BundleActionGetJPAExecutor jpaExecutor = new BundleActionGetJPAExecutor(bundleActionBean.getBundleId(), bundleActionBean.getCoordName());
            jpaService.execute(jpaExecutor);
            fail("Bundle action "+bundleActionBean.getBundleActionId()+" should have been purged");
        } catch (JPAExecutorException je) {
            assertEquals(ErrorCode.E0605, je.getErrorCode());
        }
    }

    /**
     * Test : The subworkflow should get purged, and the workflow parent should get purged --> both will get purged
     * Subworkflow has terminated, last modified time is known, but end time is null
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeWFWithEndedSubWFWithNullEndTimeValidLastModifiedTime() throws Exception {
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction1 = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);

        WorkflowJobBean subwfJob1 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        subwfJob1.setEndTime(null);
        System.out.println("subwfJob1:"+subwfJob1.getLastModifiedTime()+" "+wfJob.getLastModifiedTime()+" "+subwfJob1.getEndTime());
        //WorkflowActionBean subwfAction1 = addRecordToWfActionTable(subwfJob1.getId(), "1", WorkflowAction.Status.OK);

        //final WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(wfJob.getId());
        //final WorkflowActionGetJPAExecutor wfAction1GetCmd = new WorkflowActionGetJPAExecutor(wfAction1.getId());
        final WorkflowJobGetJPAExecutor subwfJob1GetCmd = new WorkflowJobGetJPAExecutor(subwfJob1.getId());
        //final WorkflowActionGetJPAExecutor subwfAction1GetCmd = new WorkflowActionGetJPAExecutor(subwfAction1.getId());

        //wfJob = jpaService.execute(wfJobGetCmd);
        //wfAction1 = jpaService.execute(wfAction1GetCmd);
        subwfJob1 = jpaService.execute(subwfJob1GetCmd);
        //subwfAction1 = jpaService.execute(subwfAction1GetCmd);

        /*assertEquals(WorkflowJob.Status.SUCCEEDED, wfJob.getStatus());
        assertEquals(WorkflowAction.Status.OK, wfAction1.getStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED, subwfJob1.getStatus());
        assertEquals(WorkflowAction.Status.OK, subwfAction1.getStatus());*/

        final QueryExecutor<WorkflowJobBean, WorkflowJobQueryExecutor.WorkflowJobQuery> workflowJobQueryExecutor =
                WorkflowJobQueryExecutor.getInstance();
        workflowJobQueryExecutor.executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW, subwfJob1);
        System.out.println("subwfJob1_:"+subwfJob1.getLastModifiedTime()+" "+wfJob.getLastModifiedTime()+" "+subwfJob1.getEndTime());
        //subwfJob1 = jpaService.execute(subwfJob1GetCmd);
        //System.out.println("subwfJob1__:"+subwfJob1.getLastModifiedTime()+" "+wfJob.getLastModifiedTime()+" "+subwfJob1.getEndTime());
        final int wfOlderThanDays = 7;
        final int coordOlderThanDays = 1;
        final int bundleOlderThanDays = 1;
        final int limit = 3;
        new PurgeXCommand(wfOlderThanDays, coordOlderThanDays, bundleOlderThanDays, limit).call();

        assertWorkflowJobPurged(wfJob);
        assertWorkflowJobPurged(subwfJob1);
    }

    /**
     * Test : The subworkflow and workflow should get purged, but the coordinator parent shouldn't get purged --> none will get
     * purged
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeCoordWithWFChildWithSubWF1() throws Exception {
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowJobBean subwfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        WorkflowActionBean subwfAction = addRecordToWfActionTable(subwfJob.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob.getId(), "SUCCEEDED", 0);

        new PurgeXCommand(7, getNumDaysToNotBePurged(coordJob.getLastModifiedTime()), 1, 10).call();

        assertCoordinatorJobNotPurged(coordJob);
        assertCoordinatorActionNotPurged(coordAction);
        assertWorkflowJobNotPurged(wfJob);
        assertWorkflowActionNotPurged(wfAction);
        assertWorkflowJobNotPurged(subwfJob);
        assertWorkflowActionNotPurged(subwfAction);
    }

    /**
     * Test : The subworkflow and workflow should not get purged, but the coordinator parent should get purged --> none will get
     * purged
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeCoordWithWFChildWithSubWF2() throws Exception {
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowJobBean subwfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        WorkflowActionBean subwfAction = addRecordToWfActionTable(subwfJob.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob.getId(), "SUCCEEDED", 0);

        new PurgeXCommand(getNumDaysToNotBePurged(wfJob.getEndTime()), 7, 1, 10).call();

        assertCoordinatorJobNotPurged(coordJob);
        assertCoordinatorActionNotPurged(coordAction);
        assertWorkflowJobNotPurged(wfJob);
        assertWorkflowActionNotPurged(wfAction);
        assertWorkflowJobNotPurged(subwfJob);
        assertWorkflowActionNotPurged(subwfAction);
    }

    /**
     * Test : The subworkflow and workflow should get purged, and the coordinator parent should get purged --> all will get
     * purged
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeCoordWithWFChildWithSubWF3() throws Exception {
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowJobBean subwfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        WorkflowActionBean subwfAction = addRecordToWfActionTable(subwfJob.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob.getId(), "SUCCEEDED", 0);

        new PurgeXCommand(7, 7, 1, 10).call();

        assertCoordinatorJobPurged(coordJob);
        assertCoordinatorActionPurged(coordAction);
        assertWorkflowJobPurged(wfJob);
        assertWorkflowActionPurged(wfAction);
        assertWorkflowJobPurged(subwfJob);
        assertWorkflowActionPurged(subwfAction);
    }


    /**
     * Test : The subworkflow and workflow should get purged, and the coordinator parent should get purged --> all will get
     * purged
     *
     * Coordinator parent finished Workflow and its subworkflow have terminated, last modified time is known, but end time is null
     * for workflow and subworkflow
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeCoordWithWFChildWithSubWFNullEndTimeValidLastModifiedTime() throws Exception {
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);

        final QueryExecutor<WorkflowJobBean, WorkflowJobQueryExecutor.WorkflowJobQuery> workflowJobQueryExecutor =
                WorkflowJobQueryExecutor.getInstance();
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        wfJob.setLastModifiedTime(wfJob.getEndTime());
        wfJob.setEndTime(null);
        workflowJobQueryExecutor.executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW, wfJob);

        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowJobBean subwfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        subwfJob.setLastModifiedTime(subwfJob.getEndTime());
        subwfJob.setEndTime(null);
        workflowJobQueryExecutor.executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW, subwfJob);

        WorkflowActionBean subwfAction = addRecordToWfActionTable(subwfJob.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob.getId(), "SUCCEEDED", 0);

        final int wfOlderThanDays = 7;
        final int coordOlderThanDays = 7;
        final int bundleOlderThanDays = 1;
        final int limit = 10;
        new PurgeXCommand(wfOlderThanDays, coordOlderThanDays, bundleOlderThanDays, limit).call();

        assertCoordinatorJobPurged(coordJob);
        assertCoordinatorActionPurged(coordAction);
        assertWorkflowJobPurged(wfJob);
        assertWorkflowActionPurged(wfAction);
        assertWorkflowJobPurged(subwfJob);
        assertWorkflowActionPurged(subwfAction);
    }

    /**
     * Test : The subworkflow, workflow, and coordinator should get purged, but the bundle parent shouldn't get purged --> none will
     * get purged
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeBundleWithCoordChildWithWFChildWithSubWF1() throws Exception {
        BundleJobBean bundleJob = addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-01-01T01:00Z"));
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowJobBean subwfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        WorkflowActionBean subwfAction = addRecordToWfActionTable(subwfJob.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob.getId(), "SUCCEEDED", 0);
        BundleActionBean bundleAction = addRecordToBundleActionTable(bundleJob.getId(), coordJob.getId(), coordJob.getAppName(), 0,
                Job.Status.SUCCEEDED);

        new PurgeXCommand(7, 7, getNumDaysToNotBePurged(bundleJob.getLastModifiedTime()), 10).call();

        assertBundleJobNotPurged(bundleJob);
        assertBundleActionNotPurged(bundleAction);
        assertCoordinatorJobNotPurged(coordJob);
        assertCoordinatorActionNotPurged(coordAction);
        assertWorkflowJobNotPurged(wfJob);
        assertWorkflowActionNotPurged(wfAction);
        assertWorkflowJobNotPurged(subwfJob);
        assertWorkflowActionNotPurged(subwfAction);
    }

    /**
     * Test : The subworkflow, workflow, and coordinator should not get purged, but the bundle parent should get purged --> none
     * will get purged
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeBundleWithCoordChildWithWFChildWithSubWF2() throws Exception {
        BundleJobBean bundleJob = addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-01-01T01:00Z"));
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowJobBean subwfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        WorkflowActionBean subwfAction = addRecordToWfActionTable(subwfJob.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob.getId(), "SUCCEEDED", 0);
        BundleActionBean bundleAction = addRecordToBundleActionTable(bundleJob.getId(), coordJob.getId(), coordJob.getAppName(), 0,
                Job.Status.SUCCEEDED);

        new PurgeXCommand(getNumDaysToNotBePurged(wfJob.getEndTime()),
                getNumDaysToNotBePurged(coordJob.getLastModifiedTime()), 7, 10).call();

        assertBundleJobNotPurged(bundleJob);
        assertBundleActionNotPurged(bundleAction);
        assertCoordinatorJobNotPurged(coordJob);
        assertCoordinatorActionNotPurged(coordAction);
        assertWorkflowJobNotPurged(wfJob);
        assertWorkflowActionNotPurged(wfAction);
        assertWorkflowJobNotPurged(subwfJob);
        assertWorkflowActionNotPurged(subwfAction);
    }

    /**
     * Test : The subworkflow, workflow, and coordinator should get purged, and the bundle parent should get purged --> all
     * will get purged
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeBundleWithCoordChildWithWFChildWithSubWF3() throws Exception {
        BundleJobBean bundleJob = addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-01-01T01:00Z"));
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowJobBean subwfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        WorkflowActionBean subwfAction = addRecordToWfActionTable(subwfJob.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJob.getId(), "SUCCEEDED", 0);
        BundleActionBean bundleAction = addRecordToBundleActionTable(bundleJob.getId(), coordJob.getId(), coordJob.getAppName(), 0,
                Job.Status.SUCCEEDED);

        new PurgeXCommand(7, 7, 7, 10).call();

        assertBundleJobPurged(bundleJob);
        assertBundleActionPurged(bundleAction);
        assertCoordinatorJobPurged(coordJob);
        assertCoordinatorActionPurged(coordAction);
        assertWorkflowJobPurged(wfJob);
        assertWorkflowActionPurged(wfAction);
        assertWorkflowJobPurged(subwfJob);
        assertWorkflowActionPurged(subwfAction);
    }

    /**
     * Test : Test purging a lot of jobs (with different parent-child relationships and purge-eligibility) in one go
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeLotsOfJobs() throws Exception {
        assertNotNull(jpaService);

        /* Job relationships:
        bundleJobA              yes     yes
            coordJobA           yes     ^
                wfJobA          yes     ^
                    subwfJobA   yes     ^
        bundleJobB              no      no
            coordJobB           yes     ^
                wfJobB          no      ^
        coordJobC               no      no
            wfJobC              no      ^
                subwfJobC       yes     ^
        coordJobD               yes     no
            wfJobD              no      ^
        wfJobE                  yes     yes
        wfJobF                  yes     no
            subwfJobF           no      ^
        */
        BundleJobBean bundleJobA = addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-05-01T01:00Z"));
        BundleJobBean bundleJobB = addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-06-01T01:00Z"));
        CoordinatorJobBean coordJobA = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        setLastModifiedTime(coordJobA, "2011-03-01T01:00Z");
        CoordinatorJobBean coordJobB = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        setLastModifiedTime(coordJobB, "2011-03-01T01:00Z");
        CoordinatorJobBean coordJobC = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        setLastModifiedTime(coordJobC, "2011-07-01T01:00Z");
        CoordinatorJobBean coordJobD = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        setLastModifiedTime(coordJobD, "2011-06-01T01:00Z");
        WorkflowJobBean wfJobA = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        setEndTime(wfJobA, "2011-02-01T01:00Z");
        WorkflowJobBean wfJobB = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        setEndTime(wfJobB, "2011-03-01T01:00Z");
        WorkflowJobBean wfJobC = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        setEndTime(wfJobC, "2011-06-01T01:00Z");
        WorkflowJobBean wfJobD = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        setEndTime(wfJobD, "2011-06-01T01:00Z");
        WorkflowJobBean wfJobE = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        setEndTime(wfJobE, "2011-01-01T01:00Z");
        WorkflowJobBean wfJobF = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        setEndTime(wfJobF, "2011-01-01T01:00Z");
        WorkflowActionBean wfActionA = addRecordToWfActionTable(wfJobA.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfActionB = addRecordToWfActionTable(wfJobB.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfActionC = addRecordToWfActionTable(wfJobC.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfActionD = addRecordToWfActionTable(wfJobD.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfActionE = addRecordToWfActionTable(wfJobE.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean wfActionF = addRecordToWfActionTable(wfJobF.getId(), "1", WorkflowAction.Status.OK);
        WorkflowJobBean subwfJobA = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJobA.getId());
        setEndTime(subwfJobA, "2011-01-01T01:00Z");
        WorkflowJobBean subwfJobC = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJobC.getId());
        setEndTime(subwfJobC, "2011-01-01T01:00Z");
        WorkflowJobBean subwfJobF = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJobF.getId());
        setEndTime(subwfJobF, "2011-04-01T01:00Z");
        WorkflowActionBean subwfActionA = addRecordToWfActionTable(subwfJobA.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean subwfActionC = addRecordToWfActionTable(subwfJobC.getId(), "1", WorkflowAction.Status.OK);
        WorkflowActionBean subwfActionF = addRecordToWfActionTable(subwfJobF.getId(), "1", WorkflowAction.Status.OK);
        CoordinatorActionBean coordActionA = addRecordToCoordActionTable(coordJobA.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJobA.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordActionB = addRecordToCoordActionTable(coordJobB.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJobB.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordActionC = addRecordToCoordActionTable(coordJobC.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJobC.getId(), "SUCCEEDED", 0);
        CoordinatorActionBean coordActionD = addRecordToCoordActionTable(coordJobD.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", wfJobD.getId(), "SUCCEEDED", 0);
        BundleActionBean bundleActionA = addRecordToBundleActionTable(bundleJobA.getId(), coordJobA.getId(), coordJobA.getAppName(),
                0,Job.Status.SUCCEEDED);
        BundleActionBean bundleActionB = addRecordToBundleActionTable(bundleJobB.getId(), coordJobB.getId(), coordJobB.getAppName(),
                0, Job.Status.SUCCEEDED);

        new PurgeXCommand(getNumDaysToNotBePurged(wfJobB.getEndTime()),
                          getNumDaysToNotBePurged(coordJobC.getLastModifiedTime()),
                          getNumDaysToNotBePurged(bundleJobB.getLastModifiedTime()),
                          10).call();

        assertBundleJobPurged(bundleJobA);
        assertBundleActionPurged(bundleActionA);
        assertBundleJobNotPurged(bundleJobB);
        assertBundleActionNotPurged(bundleActionB);
        assertCoordinatorJobPurged(coordJobA);
        assertCoordinatorActionPurged(coordActionA);
        assertCoordinatorJobNotPurged(coordJobB);
        assertCoordinatorActionNotPurged(coordActionB);
        assertCoordinatorJobNotPurged(coordJobC);
        assertCoordinatorActionNotPurged(coordActionC);
        assertCoordinatorJobNotPurged(coordJobD);
        assertCoordinatorActionNotPurged(coordActionD);
        assertWorkflowJobPurged(wfJobA);
        assertWorkflowActionPurged(wfActionA);
        assertWorkflowJobNotPurged(wfJobB);
        assertWorkflowActionNotPurged(wfActionB);
        assertWorkflowJobNotPurged(wfJobC);
        assertWorkflowActionNotPurged(wfActionC);
        assertWorkflowJobNotPurged(wfJobD);
        assertWorkflowActionNotPurged(wfActionD);
        assertWorkflowJobPurged(wfJobE);
        assertWorkflowActionPurged(wfActionE);
        assertWorkflowJobNotPurged(wfJobF);
        assertWorkflowActionNotPurged(wfActionF);
        assertWorkflowJobPurged(subwfJobA);
        assertWorkflowActionPurged(subwfActionA);
        assertWorkflowJobNotPurged(subwfJobC);
        assertWorkflowActionNotPurged(subwfActionC);
        assertWorkflowJobNotPurged(subwfJobF);
        assertWorkflowActionNotPurged(subwfActionF);
    }

    protected WorkflowJobBean addRecordToWfJobTableForNegCase(WorkflowJob.Status jobStatus,
            WorkflowInstance.Status instanceStatus) throws Exception {
        WorkflowApp app =
            new LiteWorkflowApp("testApp", "<workflow-app/>",
                new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "end")).
                    addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));
        Configuration conf = new Configuration();
        Path appUri = new Path(getAppPath(), "workflow.xml");
        conf.set(OozieClient.APP_PATH, appUri.toString());
        conf.set(OozieClient.LOG_TOKEN, "testToken");
        conf.set(OozieClient.USER_NAME, getTestUser());

        WorkflowJobBean wfBean = createWorkflow(app, conf, jobStatus, instanceStatus);
        //Set start time to 100 days from now
        wfBean.setStartTime(new Date(System.currentTimeMillis() + (long)100*24*60*60*1000));
        //Set end time to 100 days + 2 hours from now
        wfBean.setEndTime(new Date(System.currentTimeMillis() + (long)100*24*60*60*1000 + (long)2*60*60*1000));

        try {
            WorkflowJobInsertJPAExecutor wfInsertCmd = new WorkflowJobInsertJPAExecutor(wfBean);
            jpaService.execute(wfInsertCmd);
        }
        catch (JPAExecutorException ce) {
            ce.printStackTrace();
            fail("Unable to insert the test wf job record to table");
            throw ce;
        }
        return wfBean;
    }

    @Override
    protected WorkflowJobBean createWorkflow(WorkflowApp app, Configuration conf,
            WorkflowJob.Status jobStatus, WorkflowInstance.Status instanceStatus) throws Exception {
        WorkflowAppService wps = Services.get().get(WorkflowAppService.class);
        Configuration protoActionConf = wps.createProtoActionConf(conf, true);
        WorkflowLib workflowLib = Services.get().get(WorkflowStoreService.class).getWorkflowLibWithNoDB();
        WorkflowInstance wfInstance = workflowLib.createInstance(app, conf);
        ((LiteWorkflowInstance) wfInstance).setStatus(instanceStatus);
        WorkflowJobBean workflow = new WorkflowJobBean();
        workflow.setId(Services.get().get(UUIDService.class).generateId(ApplicationType.WORKFLOW));
        workflow.setAppName(app.getName());
        workflow.setAppPath(conf.get(OozieClient.APP_PATH));
        workflow.setConf(XmlUtils.prettyPrint(conf).toString());
        workflow.setProtoActionConf(XmlUtils.prettyPrint(protoActionConf).toString());
        workflow.setCreatedTime(new Date());
        workflow.setLogToken(conf.get(OozieClient.LOG_TOKEN, ""));
        workflow.setStatus(jobStatus);
        workflow.setRun(0);
        workflow.setUser(conf.get(OozieClient.USER_NAME));
        workflow.setWorkflowInstance(wfInstance);
        workflow.setStartTime(DateUtils.parseDateOozieTZ("2009-12-18T01:00Z"));
        workflow.setEndTime(DateUtils.parseDateOozieTZ("2009-12-18T03:00Z"));
        return workflow;
    }

    @Override
    protected CoordinatorJobBean addRecordToCoordJobTable(CoordinatorJob.Status status, boolean pending, boolean doneMatd)
            throws Exception {
        CoordinatorJobBean coordJob = createCoordJob(status, pending, doneMatd);
        coordJob.setLastModifiedTime(DateUtils.parseDateOozieTZ("2009-12-18T01:00Z"));
        try {
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

    protected BundleJobBean addRecordToBundleJobTable(Job.Status jobStatus, Date lastModifiedTime) throws Exception {
        BundleJobBean bundle = createBundleJob(jobStatus, false);
        bundle.setLastModifiedTime(lastModifiedTime);
        try {
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

    /**
     * Return a number of days from today such that the job (workflow, coord, or bundle) ending at jobDate will NOT be purged
     * @param jobDate The date that the job was last modified
     * @return 5 + the number of days from today to jobDate
     */
    public static int getNumDaysToNotBePurged(Date jobDate) {
        int days = (int) (((new Date()).getTime() - jobDate.getTime()) / 1000 / 60 / 60 / 24);
        return days + 5;
    }

    public static CoordinatorJobBean setLastModifiedTime(CoordinatorJobBean job, String date) throws Exception {
        job.setLastModifiedTime(DateUtils.parseDateOozieTZ(date));
        try {
            CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB_LAST_MODIFIED_TIME, job);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to update coord job last modified time");
            throw je;
        }
        return job;
    }

    public static WorkflowJobBean setEndTime(WorkflowJobBean job, String date) throws Exception {
        job.setEndTime(DateUtils.parseDateOozieTZ(date));
        try {
            WorkflowJobQueryExecutor.getInstance().executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_INSTANCE_MOD_END, job);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to update workflow job last modified time");
            throw je;
        }
        return job;
    }
}
