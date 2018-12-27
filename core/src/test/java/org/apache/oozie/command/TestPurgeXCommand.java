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
    private static final int TEST_CHILD_NUM = 5;
    private static final int WF_OLDER_THAN_7_DAYS = 7;
    private static final int WF_OLDER_THAN_1_DAY = 1;
    private static final int COORD_OLDER_THAN_7_DAYS = 7;
    private static final int COORD_OLDER_THAN_1_DAY = 1;
    private static final int BUNDLE_OLDER_THAN_7_DAYS = 7;
    private static final int BUNDLE_OLDER_THAN_1_DAY = 1;
    private static final int LIMIT_10_ITEMS = 10;
    private static final int LIMIT_3_ITEMS = 3;

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

        new PurgeXCommand(WF_OLDER_THAN_7_DAYS, COORD_OLDER_THAN_1_DAY, BUNDLE_OLDER_THAN_1_DAY, LIMIT_10_ITEMS).call();

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

        new PurgeXCommand(WF_OLDER_THAN_7_DAYS, COORD_OLDER_THAN_1_DAY, BUNDLE_OLDER_THAN_1_DAY, LIMIT_10_ITEMS).call();

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

        new PurgeXCommand(WF_OLDER_THAN_7_DAYS, COORD_OLDER_THAN_1_DAY, BUNDLE_OLDER_THAN_1_DAY, LIMIT_10_ITEMS).call();

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

        new PurgeXCommand(WF_OLDER_THAN_7_DAYS, COORD_OLDER_THAN_1_DAY, BUNDLE_OLDER_THAN_1_DAY, LIMIT_10_ITEMS).call();

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

        new PurgeXCommand(WF_OLDER_THAN_1_DAY, COORD_OLDER_THAN_7_DAYS, BUNDLE_OLDER_THAN_1_DAY, LIMIT_10_ITEMS).call();

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

        new PurgeXCommand(WF_OLDER_THAN_1_DAY, COORD_OLDER_THAN_7_DAYS, BUNDLE_OLDER_THAN_1_DAY, LIMIT_10_ITEMS).call();

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

        new PurgeXCommand(WF_OLDER_THAN_1_DAY, COORD_OLDER_THAN_7_DAYS, BUNDLE_OLDER_THAN_1_DAY, LIMIT_10_ITEMS).call();

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

        new PurgeXCommand(WF_OLDER_THAN_1_DAY, COORD_OLDER_THAN_7_DAYS, BUNDLE_OLDER_THAN_1_DAY, LIMIT_10_ITEMS).call();

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

        new PurgeXCommand(WF_OLDER_THAN_1_DAY, COORD_OLDER_THAN_1_DAY, BUNDLE_OLDER_THAN_7_DAYS, LIMIT_10_ITEMS).call();

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

        new PurgeXCommand(WF_OLDER_THAN_1_DAY, COORD_OLDER_THAN_1_DAY, BUNDLE_OLDER_THAN_7_DAYS, LIMIT_10_ITEMS).call();

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

        new PurgeXCommand(WF_OLDER_THAN_1_DAY, COORD_OLDER_THAN_1_DAY, BUNDLE_OLDER_THAN_7_DAYS, LIMIT_10_ITEMS).call();

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

        new PurgeXCommand(WF_OLDER_THAN_1_DAY, COORD_OLDER_THAN_1_DAY, BUNDLE_OLDER_THAN_7_DAYS, LIMIT_10_ITEMS).call();

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

        new PurgeXCommand(WF_OLDER_THAN_7_DAYS, getNumDaysToNotBePurged(coordJob.getLastModifiedTime()), BUNDLE_OLDER_THAN_1_DAY, LIMIT_10_ITEMS).call();

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

        new PurgeXCommand(WF_OLDER_THAN_7_DAYS, getNumDaysToNotBePurged(coordJob.getLastModifiedTime()), BUNDLE_OLDER_THAN_1_DAY, LIMIT_10_ITEMS, true).call();

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

        new PurgeXCommand(WF_OLDER_THAN_7_DAYS, getNumDaysToNotBePurged(coordJob.getLastModifiedTime()), BUNDLE_OLDER_THAN_1_DAY, LIMIT_10_ITEMS, false).call();

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

        new PurgeXCommand(getNumDaysToNotBePurged(wfJob.getEndTime()), COORD_OLDER_THAN_7_DAYS, BUNDLE_OLDER_THAN_1_DAY, LIMIT_10_ITEMS).call();

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
        WorkflowJobBean[] wfJobs = new WorkflowJobBean[TEST_CHILD_NUM];
        WorkflowActionBean[] wfActions = new WorkflowActionBean[TEST_CHILD_NUM];
        CoordinatorActionBean[] coordActions = new CoordinatorActionBean[TEST_CHILD_NUM];
        for (int i=0; i<TEST_CHILD_NUM; ++i) {
            wfJobs[i] = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
            wfActions[i] = addRecordToWfActionTable(wfJobs[i].getId(), "1", WorkflowAction.Status.OK);
            coordActions[i] = addRecordToCoordActionTable(coordJob.getId(), i, CoordinatorAction.Status.SUCCEEDED,
                    "coord-action-get.xml", wfJobs[i].getId(), "SUCCEEDED", 0);
        }

        new PurgeXCommand(getNumDaysToNotBePurged(wfJobs[0].getEndTime()), COORD_OLDER_THAN_7_DAYS, BUNDLE_OLDER_THAN_1_DAY,  LIMIT_3_ITEMS).call();

        assertCoordinatorJobNotPurged(coordJob);
        assertCoordinatorActionsNotPurged(coordActions);
        assertWorkflowJobsNotPurged(wfJobs);
        assertWorkflowActionsNotPurged(wfActions);
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

        new PurgeXCommand(WF_OLDER_THAN_7_DAYS, COORD_OLDER_THAN_7_DAYS, BUNDLE_OLDER_THAN_1_DAY, LIMIT_10_ITEMS).call();

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
        WorkflowJobBean[] wfJobs = new WorkflowJobBean[TEST_CHILD_NUM];
        WorkflowActionBean[] wfActions = new WorkflowActionBean[TEST_CHILD_NUM];
        CoordinatorActionBean[] coordActions = new CoordinatorActionBean[TEST_CHILD_NUM];
        for (int i=0; i<TEST_CHILD_NUM; ++i) {
            wfJobs[i] = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
            wfActions[i] = addRecordToWfActionTable(wfJobs[i].getId(), "1", WorkflowAction.Status.OK);
            coordActions[i] = addRecordToCoordActionTable(coordJob.getId(), i, CoordinatorAction.Status.SUCCEEDED,
                    "coord-action-get.xml", wfJobs[i].getId(), "SUCCEEDED", 0);
        }

        new PurgeXCommand(WF_OLDER_THAN_7_DAYS, COORD_OLDER_THAN_7_DAYS, BUNDLE_OLDER_THAN_1_DAY, LIMIT_10_ITEMS).call();

        assertCoordinatorJobPurged(coordJob);
        assertCoordinatorActionsPurged(coordActions);
        assertWorkflowJobsPurged(wfJobs);
        assertWorkflowActionsPurged(wfActions);
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

        new PurgeXCommand(WF_OLDER_THAN_7_DAYS, COORD_OLDER_THAN_7_DAYS, getNumDaysToNotBePurged(bundleJob.getLastModifiedTime()), LIMIT_10_ITEMS).call();

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
                getNumDaysToNotBePurged(coordJob.getLastModifiedTime()), BUNDLE_OLDER_THAN_7_DAYS, LIMIT_10_ITEMS).call();

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
        CoordinatorJobBean[] coordJobs = new CoordinatorJobBean[TEST_CHILD_NUM];
        WorkflowJobBean[] wfJobs = new WorkflowJobBean[TEST_CHILD_NUM];
        WorkflowActionBean[] wfActions = new WorkflowActionBean[TEST_CHILD_NUM];
        CoordinatorActionBean[] coordActions = new CoordinatorActionBean[TEST_CHILD_NUM];
        BundleActionBean[] bundleActions = new BundleActionBean[TEST_CHILD_NUM];
        for (int i=0; i<TEST_CHILD_NUM; ++i) {
            coordJobs[i] = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
            coordJobs[i].setAppName("coord" + i);
            CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJobs[i]);
            wfJobs[i] = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
            wfActions[i] = addRecordToWfActionTable(wfJobs[i].getId(), "1", WorkflowAction.Status.OK);
            coordActions[i] = addRecordToCoordActionTable(coordJobs[i].getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                    "coord-action-get.xml", wfJobs[i].getId(), "SUCCEEDED", 0);

            bundleActions[i] = addRecordToBundleActionTable(bundleJob.getId(), coordJobs[i].getId(), coordJobs[i].getAppName(),
                    0, Job.Status.SUCCEEDED);
        }

        new PurgeXCommand(getNumDaysToNotBePurged(wfJobs[0].getEndTime()),
                getNumDaysToNotBePurged(coordJobs[0].getLastModifiedTime()), BUNDLE_OLDER_THAN_7_DAYS,  LIMIT_3_ITEMS).call();

        assertBundleJobNotPurged(bundleJob);
        assertBundleActionsNotPurged(bundleActions);
        assertCoordinatorJobsNotPurged(coordJobs);
        assertCoordinatorActionsNotPurged(coordActions);
        assertWorkflowJobsNotPurged(wfJobs);
        assertWorkflowActionsNotPurged(wfActions);
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

        new PurgeXCommand(WF_OLDER_THAN_7_DAYS, COORD_OLDER_THAN_7_DAYS, BUNDLE_OLDER_THAN_7_DAYS, LIMIT_10_ITEMS).call();

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
        CoordinatorJobBean[] coordJobs = new CoordinatorJobBean[TEST_CHILD_NUM];
        WorkflowJobBean[] wfJobs = new WorkflowJobBean[TEST_CHILD_NUM];
        WorkflowActionBean[] wfActions = new WorkflowActionBean[TEST_CHILD_NUM];
        CoordinatorActionBean[] coordActions = new CoordinatorActionBean[TEST_CHILD_NUM];
        BundleActionBean[] bundleActions = new BundleActionBean[TEST_CHILD_NUM];
        for (int i=0; i<TEST_CHILD_NUM; ++i) {
            coordJobs[i] = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
            coordJobs[i].setAppName("coord" + i);
            CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJobs[i]);
            wfJobs[i] = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
            wfActions[i] = addRecordToWfActionTable(wfJobs[i].getId(), "1", WorkflowAction.Status.OK);
            coordActions[i] = addRecordToCoordActionTable(coordJobs[i].getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                    "coord-action-get.xml", wfJobs[i].getId(), "SUCCEEDED", 0);
            bundleActions[i] = addRecordToBundleActionTable(bundleJob.getId(), coordJobs[i].getId(), coordJobs[i].getAppName(),
                    0, Job.Status.SUCCEEDED);
        }

        new PurgeXCommand(WF_OLDER_THAN_7_DAYS, COORD_OLDER_THAN_7_DAYS, BUNDLE_OLDER_THAN_7_DAYS, LIMIT_10_ITEMS).call();

        assertBundleJobPurged(bundleJob);
        assertBundleActionsPurged(bundleActions);
        assertCoordinatorJobsPurged(coordJobs);
        assertCoordinatorActionsPurged(coordActions);
        assertWorkflowJobsPurged(wfJobs);
        assertWorkflowActionsPurged(wfActions);
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

        new PurgeXCommand(WF_OLDER_THAN_7_DAYS, COORD_OLDER_THAN_1_DAY, BUNDLE_OLDER_THAN_1_DAY, LIMIT_10_ITEMS).call();

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

        new PurgeXCommand(WF_OLDER_THAN_7_DAYS, COORD_OLDER_THAN_1_DAY, BUNDLE_OLDER_THAN_1_DAY, LIMIT_10_ITEMS).call();

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
        WorkflowActionBean[] wfActions = new WorkflowActionBean[TEST_CHILD_NUM];
        WorkflowJobBean[] subwfJobs = new WorkflowJobBean[TEST_CHILD_NUM];
        WorkflowActionBean[] subwfActions = new WorkflowActionBean[TEST_CHILD_NUM];
        for (int i=0; i<TEST_CHILD_NUM; ++i) {
            wfActions[i] = addRecordToWfActionTable(wfJob.getId(), String.format("action%d",i), WorkflowAction.Status.OK);
            subwfJobs[i] = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING,
                    wfJob.getId());
            subwfActions[i] = addRecordToWfActionTable(subwfJobs[i].getId(), String.format("action%d",i), WorkflowAction.Status.RUNNING);
        }

        new PurgeXCommand(WF_OLDER_THAN_7_DAYS, COORD_OLDER_THAN_1_DAY, BUNDLE_OLDER_THAN_1_DAY, LIMIT_10_ITEMS).call();

        assertWorkflowJobNotPurged(wfJob);
        assertWorkflowActionsNotPurged(wfActions);
        assertWorkflowJobsNotPurged(subwfJobs);
        assertWorkflowActionsNotPurged(subwfActions);
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

        new PurgeXCommand(WF_OLDER_THAN_7_DAYS, COORD_OLDER_THAN_1_DAY, BUNDLE_OLDER_THAN_1_DAY, LIMIT_10_ITEMS).call();

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
        WorkflowActionBean[] wfActions = new WorkflowActionBean[TEST_CHILD_NUM];
        WorkflowJobBean[] subwfJobs = new WorkflowJobBean[TEST_CHILD_NUM];
        WorkflowActionBean[] subwfActions = new WorkflowActionBean[TEST_CHILD_NUM];
        for (int i=0; i<TEST_CHILD_NUM; ++i) {
            wfActions[i] = addRecordToWfActionTable(wfJob.getId(), String.format("action%d", i), WorkflowAction.Status.OK);
            subwfJobs[i] = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                    wfJob.getId());
            subwfActions[i] = addRecordToWfActionTable(subwfJobs[i].getId(), "1", WorkflowAction.Status.OK);
        }

        new PurgeXCommand(WF_OLDER_THAN_7_DAYS, COORD_OLDER_THAN_1_DAY, BUNDLE_OLDER_THAN_1_DAY,  LIMIT_3_ITEMS).call();

        assertWorkflowJobPurged(wfJob);
        assertWorkflowActionsPurged(wfActions);
        assertWorkflowJobsPurged(subwfJobs);
        assertWorkflowActionsPurged(subwfActions);
    }

    /**
     * Test : The subsubworkflow shouldn't get purged,
     *        the subworkflow should get purged,
     *        the workflow parent should get purged --> neither will get purged
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeWFWithPurgeableSubWFNonPurgeableSubSubWF() throws Exception {
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowJobBean subwfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                wfJob.getId());
        WorkflowActionBean subwfAction = addRecordToWfActionTable(subwfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowJobBean subsubwfJob = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING,
                subwfJob.getId());
        WorkflowActionBean subsubwfAction = addRecordToWfActionTable(subsubwfJob.getId(), "1", WorkflowAction.Status.RUNNING);

        new PurgeXCommand(WF_OLDER_THAN_7_DAYS, COORD_OLDER_THAN_1_DAY, BUNDLE_OLDER_THAN_1_DAY, LIMIT_10_ITEMS).call();

        assertWorkflowJobNotPurged(wfJob);
        assertWorkflowActionNotPurged(wfAction);
        assertWorkflowJobNotPurged(subwfJob);
        assertWorkflowActionNotPurged(subwfAction);
        assertWorkflowJobNotPurged(subsubwfJob);
        assertWorkflowActionNotPurged(subsubwfAction);
    }

    /**
     * Test : The subsubworkflow should get purged,
     *        the subworkflow shouldn't get purged,
     *        the workflow parent should get purged --> neither will get purged
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeWFWithNonPurgeableSubWFPurgeableSubSubWF() throws Exception {
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowActionBean wfAction = addRecordToWfActionTable(wfJob.getId(), "1", WorkflowAction.Status.OK);
        WorkflowJobBean subwfJob = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING,
                wfJob.getId());
        WorkflowActionBean subwfAction = addRecordToWfActionTable(subwfJob.getId(), "1", WorkflowAction.Status.RUNNING);
        WorkflowJobBean subsubwfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                subwfJob.getId());
        WorkflowActionBean subsubwfAction = addRecordToWfActionTable(subsubwfJob.getId(), "1", WorkflowAction.Status.OK);

        new PurgeXCommand(WF_OLDER_THAN_7_DAYS, COORD_OLDER_THAN_1_DAY, BUNDLE_OLDER_THAN_1_DAY, LIMIT_10_ITEMS).call();

        assertWorkflowJobNotPurged(wfJob);
        assertWorkflowActionNotPurged(wfAction);
        assertWorkflowJobNotPurged(subwfJob);
        assertWorkflowActionNotPurged(subwfAction);
        assertWorkflowJobNotPurged(subsubwfJob);
        assertWorkflowActionNotPurged(subsubwfAction);
    }

    /**
     * Test : The subsubworkflows should get purged,
     *        the subworkflow should get purged,
     *        the workflow parent should get purged --> all will get purged
     *
     * @throws Exception if cannot insert records to the database
     */
    public void testPurgeWFWithPurgeableSubWFPurgeableSubSubWF() throws Exception {
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

        new PurgeXCommand(WF_OLDER_THAN_7_DAYS, COORD_OLDER_THAN_1_DAY, BUNDLE_OLDER_THAN_1_DAY, LIMIT_10_ITEMS).call();

        assertWorkflowJobPurged(wfJob);
        assertWorkflowActionPurged(wfAction);
        assertWorkflowJobPurged(subwfJob);
        assertWorkflowActionPurged(subwfAction);
        assertWorkflowJobPurged(subsub1wfJob);
        assertWorkflowJobPurged(subsub2wfJob);
        assertWorkflowActionPurged(subsub1wfAction);
        assertWorkflowActionPurged(subsub2wfAction);
    }

    private void assertBundleActionsPurged(BundleActionBean... bundleActionBeans) {
        for (BundleActionBean bean : bundleActionBeans) {
            assertBundleActionPurged(bean);
        }
    }

    private void assertBundleActionsNotPurged(BundleActionBean... bundleActionBeans) {
        for (BundleActionBean bean : bundleActionBeans) {
            assertBundleActionNotPurged(bean);
        }
    }

    private void assertCoordinatorJobsPurged(CoordinatorJobBean... coordinatorJobBeans) {
        for (CoordinatorJobBean bean : coordinatorJobBeans) {
            assertCoordinatorJobPurged(bean);
        }
    }

    private void assertCoordinatorJobsNotPurged(CoordinatorJobBean... coordinatorJobBeans) {
        for (CoordinatorJobBean bean : coordinatorJobBeans) {
            assertCoordinatorJobNotPurged(bean);
        }
    }

    private void assertCoordinatorActionsPurged(CoordinatorActionBean... coordinatorActionBeans) {
        for (CoordinatorActionBean bean : coordinatorActionBeans) {
            assertCoordinatorActionPurged(bean);
        }
    }

    private void assertCoordinatorActionsNotPurged(CoordinatorActionBean... coordinatorActionBeans) {
        for (CoordinatorActionBean bean : coordinatorActionBeans) {
            assertCoordinatorActionNotPurged(bean);
        }
    }

    private void assertWorkflowJobsPurged(WorkflowJobBean... workflowJobBeans) {
        for (WorkflowJobBean bean : workflowJobBeans) {
            assertWorkflowJobPurged(bean);
        }
    }

    private void assertWorkflowJobsNotPurged(WorkflowJobBean... workflowJobBeans) {
        for (WorkflowJobBean bean : workflowJobBeans) {
            assertWorkflowJobNotPurged(bean);
        }
    }

    private void assertWorkflowActionsPurged(WorkflowActionBean... workflowActionBeans) {
        for (WorkflowActionBean bean : workflowActionBeans) {
            assertWorkflowActionPurged(bean);
        }
    }

    private void assertWorkflowActionsNotPurged(WorkflowActionBean... workflowActionBeans) {
        for (WorkflowActionBean bean : workflowActionBeans) {
            assertWorkflowActionNotPurged(bean);
        }
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
        subwfJob1.setLastModifiedTime(wfJob.getEndTime());
        subwfJob1.setEndTime(null);
        final QueryExecutor<WorkflowJobBean, WorkflowJobQueryExecutor.WorkflowJobQuery> workflowJobQueryExecutor =
                WorkflowJobQueryExecutor.getInstance();
        workflowJobQueryExecutor.executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW, subwfJob1);

        new PurgeXCommand(WF_OLDER_THAN_7_DAYS, COORD_OLDER_THAN_1_DAY, BUNDLE_OLDER_THAN_1_DAY, LIMIT_3_ITEMS).call();

        assertWorkflowJobPurged(wfJob);
        assertWorkflowActionPurged(wfAction1);
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

        new PurgeXCommand(WF_OLDER_THAN_7_DAYS, getNumDaysToNotBePurged(coordJob.getLastModifiedTime()), BUNDLE_OLDER_THAN_1_DAY, LIMIT_10_ITEMS).call();

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

        new PurgeXCommand(getNumDaysToNotBePurged(wfJob.getEndTime()), COORD_OLDER_THAN_7_DAYS, BUNDLE_OLDER_THAN_1_DAY, LIMIT_10_ITEMS).call();

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

        new PurgeXCommand(WF_OLDER_THAN_7_DAYS, COORD_OLDER_THAN_7_DAYS, BUNDLE_OLDER_THAN_1_DAY, LIMIT_10_ITEMS).call();

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

        new PurgeXCommand(WF_OLDER_THAN_7_DAYS, COORD_OLDER_THAN_7_DAYS, BUNDLE_OLDER_THAN_1_DAY, LIMIT_10_ITEMS).call();

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

        new PurgeXCommand(WF_OLDER_THAN_7_DAYS, COORD_OLDER_THAN_7_DAYS, getNumDaysToNotBePurged(bundleJob.getLastModifiedTime()), LIMIT_10_ITEMS).call();

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
                getNumDaysToNotBePurged(coordJob.getLastModifiedTime()), BUNDLE_OLDER_THAN_7_DAYS, LIMIT_10_ITEMS).call();

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

        new PurgeXCommand(WF_OLDER_THAN_7_DAYS, COORD_OLDER_THAN_7_DAYS, BUNDLE_OLDER_THAN_7_DAYS, LIMIT_10_ITEMS).call();

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
                LIMIT_10_ITEMS).call();

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

    private WorkflowJobBean addRecordToWfJobTableForNegCase(WorkflowJob.Status jobStatus,
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

    private BundleJobBean addRecordToBundleJobTable(Job.Status jobStatus, Date lastModifiedTime) throws Exception {
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

    private static void setEndTime(WorkflowJobBean job, String date) throws Exception {
        job.setEndTime(DateUtils.parseDateOozieTZ(date));
        try {
            WorkflowJobQueryExecutor.getInstance().executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_INSTANCE_MOD_END, job);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to update workflow job last modified time");
            throw je;
        }
    }
}
