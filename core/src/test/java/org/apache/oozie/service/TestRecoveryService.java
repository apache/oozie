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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorEngine;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.DagEngine;
import org.apache.oozie.ForTestingActionExecutor;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.hadoop.LauncherMapperHelper;
import org.apache.oozie.action.hadoop.MapReduceActionExecutor;
import org.apache.oozie.action.hadoop.MapperReducerForTest;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.CoordinatorJob.Execution;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.wf.ActionXCommand;
import org.apache.oozie.command.wf.ActionXCommand.ActionExecutorContext;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.dependency.FSURIHandler;
import org.apache.oozie.dependency.HCatURIHandler;
import org.apache.oozie.executor.jpa.BundleActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionInsertJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor.WorkflowActionQuery;
import org.apache.oozie.service.RecoveryService.RecoveryRunnable;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.HCatURI;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.workflow.WorkflowInstance;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.net.URI;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class TestRecoveryService extends XDataTestCase {
    private Services services;
    private String server;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        server = getMetastoreAuthority();
        setSystemProperty(SchemaService.WF_CONF_EXT_SCHEMAS, "wf-ext-schema.xsd");
        services = new Services();
        services.init();
        services.get(ActionService.class).registerAndInitExecutor(ForTestingActionExecutor.class);

    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    /**
     * Tests functionality of the Recovery Service Runnable command. </p> Starts an action which behaves like an Async
     * Action (Action and Job state set to Running). Changes the action configuration to run in sync mode and updates
     * the store. Runs the recovery runnable, and ensures the state of the action and job have not changed. </p> Changes
     * the state of the action from RUNNING to PREP and updates the store. Again, runs the recovery runnable and ensures
     * the state changes to OK and the job completes successfully.
     *
     * @throws Exception
     */
    public void testWorkflowActionRecoveryService() throws Exception {
        Reader reader = IOUtils.getResourceAsReader("wf-ext-schema-valid.xml", -1);
        Writer writer = new FileWriter(new File(getTestCaseDir(), "workflow.xml"));
        createTestCaseSubDir("lib");
        IOUtils.copyCharStream(reader, writer);

        final DagEngine engine = new DagEngine(getTestUser());
        Configuration conf = new XConfiguration();
        conf.set(OozieClient.APP_PATH, getTestCaseFileUri("workflow.xml"));
        conf.set(OozieClient.USER_NAME, getTestUser());

        conf.set(OozieClient.LOG_TOKEN, "t");

        conf.set("external-status", "ok");
        conf.set("signal-value", "based_on_action_status");
        conf.set("running-mode", "async");

        //TODO CHECK, without this we get JPA concurrency exceptions, ODD
        sleep(1000);

        final String jobId = engine.submitJob(conf, true);

        //TODO CHECK, without this we get JPA concurrency exceptions, ODD
        sleep(1000);

        waitFor(5000, new Predicate() {
            public boolean evaluate() throws Exception {
                return (engine.getJob(jobId).getStatus() == WorkflowJob.Status.RUNNING);
            }
        });

        sleep(1000);
        final WorkflowStore store = Services.get().get(WorkflowStoreService.class).create();
        store.beginTrx();
        List<WorkflowActionBean> actions = store.getActionsForWorkflow(jobId, false);
        WorkflowActionBean action = null;
        for (WorkflowActionBean bean : actions) {
            if (bean.getType().equals("test")) {
                action = bean;
                break;
            }
        }
        assertNotNull(action);
        final String actionId = action.getId();
        assertEquals(WorkflowActionBean.Status.RUNNING, action.getStatus());
        String actionConf = action.getConf();
        String fixedActionConf = actionConf.replaceAll("async", "sync");
        action.setConf(fixedActionConf);
        action.setPending();
        store.updateAction(action);
        store.commitTrx();
        store.closeTrx();

        Runnable recoveryRunnable = new RecoveryRunnable(0, 60, 60);
        recoveryRunnable.run();
        sleep(3000);

        final WorkflowStore store2 = Services.get().get(WorkflowStoreService.class).create();
        assertEquals(WorkflowJob.Status.RUNNING, engine.getJob(jobId).getStatus());
        store2.beginTrx();
        WorkflowActionBean action2 = store2.getAction(actionId, false);
        assertEquals(WorkflowActionBean.Status.RUNNING, action2.getStatus());
        action2.setStatus(WorkflowActionBean.Status.PREP);
        action2.setPending();
        store2.updateAction(action2);
        store2.commitTrx();
        store2.closeTrx();

        sleep(1000);
        recoveryRunnable.run();
        sleep(3000);

        waitFor(10000, new Predicate() {
            public boolean evaluate() throws Exception {
                return (engine.getWorkflowAction(actionId).getStatus() == WorkflowActionBean.Status.OK);
            }
        });

        // getPendingActions works correctly only with MYSQL - following assertsfail with hsql - to be investigated
        // assertEquals(WorkflowJob.Status.SUCCEEDED, engine.getJob(jobId).getStatus());
        final WorkflowStore store3 = Services.get().get(WorkflowStoreService.class).create();
        store3.beginTrx();
        WorkflowActionBean action3 = store3.getAction(actionId, false);
        assertEquals(WorkflowActionBean.Status.OK, action3.getStatus());
        store3.commitTrx();
        store3.closeTrx();
    }

    /**
     * Tests functionality of the Recovery Service Runnable command. </p> Starts an action with USER_RETRY status.
     * Runs the recovery runnable, and ensures the state changes to OK and the job completes successfully.
     *
     * @throws Exception
     */
    public void testWorkflowActionRecoveryUserRetry() throws Exception {
        final JPAService jpaService = Services.get().get(JPAService.class);
        WorkflowJobBean job1 = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action1 = this.addRecordToWfActionTable(job1.getId(), "1", WorkflowAction.Status.USER_RETRY);

        WorkflowJobBean job2 = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action2 = createWorkflowActionSetPending(job2.getId(), WorkflowAction.Status.USER_RETRY);
        //Default recovery created time is 7 days.
        action2.setCreatedTime(new Date(new Date().getTime() - 8 * RecoveryService.ONE_DAY_MILLISCONDS));
        WorkflowActionInsertJPAExecutor actionInsertCmd = new WorkflowActionInsertJPAExecutor(action2);
        jpaService.execute(actionInsertCmd);

        Runnable recoveryRunnable = new RecoveryRunnable(0, 60, 60);
        recoveryRunnable.run();
        sleep(3000);

        final WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(action1.getId());

        waitFor(5000, new Predicate() {
            public boolean evaluate() throws Exception {
                WorkflowActionBean a = jpaService.execute(wfActionGetCmd);
                return a.getExternalId() != null;
            }
        });
        action1 = jpaService.execute(wfActionGetCmd);
        assertNotNull(action1.getExternalId());
        assertEquals(WorkflowAction.Status.RUNNING, action1.getStatus());

        //Action 2 should not get recover as it's created time is older then 7 days
        action2= WorkflowActionQueryExecutor.getInstance().get(WorkflowActionQuery.GET_ACTION, action2.getId());
        assertNull(action2.getExternalId());
        assertEquals(WorkflowAction.Status.USER_RETRY, action2.getStatus());

        ActionExecutorContext context = new ActionXCommand.ActionExecutorContext(job1, action1, false, false);
        MapReduceActionExecutor actionExecutor = new MapReduceActionExecutor();
        JobConf conf = actionExecutor.createBaseHadoopConf(context, XmlUtils.parseXml(action1.getConf()));
        String user = conf.get("user.name");
        String group = conf.get("group.name");
        JobClient jobClient = Services.get().get(HadoopAccessorService.class).createJobClient(user, conf);

        String launcherId = action1.getExternalId();

        final RunningJob launcherJob = jobClient.getJob(JobID.forName(launcherId));

        waitFor(240 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return launcherJob.isComplete();
            }
        });
        assertTrue(launcherJob.isSuccessful());
        Map<String, String> actionData = LauncherMapperHelper.getActionData(getFileSystem(), context.getActionDir(),
                conf);
        assertTrue(LauncherMapperHelper.hasIdSwap(actionData));
    }

    /**
     * If the bundle action is in PREP state and coord is not yet created, recovery should submit new coord
     * @throws Exception
     */
    public void testBundleRecoveryCoordCreate() throws Exception {
        CoordinatorStore store = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        final BundleActionBean bundleAction;
        final BundleJobBean bundle;
        store.beginTrx();
        try {
            bundle = addRecordToBundleJobTable(Job.Status.RUNNING, false);
            bundleAction = addRecordToBundleActionTable(bundle.getId(), "coord1", 1, Job.Status.PREP);
            store.commitTrx();
        }
        finally {
            store.closeTrx();
        }
        final JPAService jpaService = Services.get().get(JPAService.class);

        sleep(3000);
        Runnable recoveryRunnable = new RecoveryRunnable(0, 1,1);
        recoveryRunnable.run();

        waitFor(10000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleActionBean mybundleAction =
                        jpaService.execute(new BundleActionGetJPAExecutor(bundle.getId(), "coord1"));
                try {
                    if (mybundleAction.getCoordId() != null) {
                        CoordinatorJobBean coord = jpaService.execute(new CoordJobGetJPAExecutor(mybundleAction.getCoordId()));
                        return true;
                    }
                } catch (Exception e) {
                }
                return false;
            }
        });

        BundleActionBean mybundleAction = jpaService.execute(new BundleActionGetJPAExecutor(bundle.getId(), "coord1"));
        assertNotNull(mybundleAction.getCoordId());

        try {
            jpaService.execute(new CoordJobGetJPAExecutor(mybundleAction.getCoordId()));
        } catch(Exception e) {
            e.printStackTrace();
            fail("Expected coord " + mybundleAction.getCoordId() + " to be created");
        }
    }

    /**
     * If the bundle action is in PREP state and coord is already created, recovery should not submit new coord
     * @throws Exception
     */
    public void testBundleRecoveryCoordExists() throws Exception {
        CoordinatorStore store = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        final BundleActionBean bundleAction;
        final BundleJobBean bundle;
        final CoordinatorJob coord;
        store.beginTrx();
        try {
            bundle = addRecordToBundleJobTable(Job.Status.RUNNING, false);
            coord = addRecordToCoordJobTable(Job.Status.PREP, false, false);
            bundleAction = addRecordToBundleActionTable(bundle.getId(), coord.getId(), "coord1", 1, Job.Status.PREP);
            store.commitTrx();
        }
        finally {
            store.closeTrx();
        }
        final JPAService jpaService = Services.get().get(JPAService.class);

        sleep(3000);
        Runnable recoveryRunnable = new RecoveryRunnable(0, 1,1);
        recoveryRunnable.run();

        waitFor(3000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleActionBean mybundleAction =
                        jpaService.execute(new BundleActionGetJPAExecutor(bundle.getId(), "coord1"));
                return !mybundleAction.getCoordId().equals(coord.getId());
            }
        });

        BundleActionBean mybundleAction = jpaService.execute(new BundleActionGetJPAExecutor(bundle.getId(), "coord1"));
        assertEquals(coord.getId(), mybundleAction.getCoordId());
    }

    /**
     * Tests functionality of the Recovery Service Runnable command. </p> Insert a coordinator job with RUNNING and
     * action with SUBMITTED. Then, runs the recovery runnable and ensures the action status changes to RUNNING.
     *
     * @throws Exception
     */
    public void testCoordActionRecoveryServiceForSubmitted() throws Exception {
        final String jobId = "0000000-" + new Date().getTime() + "-testCoordRecoveryService-C";
        final int actionNum = 1;
        final String actionId = jobId + "@" + actionNum;
        final CoordinatorEngine ce = new CoordinatorEngine(getTestUser());
        CoordinatorStore store = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store.beginTrx();
        try {
            createTestCaseSubDir("one-op");
            createTestCaseSubDir("one-op", "lib");
            createTestCaseSubDir("workflows");
            createTestCaseSubDir("in");
            addRecordToJobTable(jobId, store, getTestCaseDir());
            addRecordToActionTable(jobId, actionNum, actionId, store, getTestCaseDir());
            store.commitTrx();
        }
        finally {
            store.closeTrx();
        }

        sleep(3000);
        Runnable recoveryRunnable = new RecoveryRunnable(0, 1,1);
        recoveryRunnable.run();

        waitFor(10000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorActionBean bean = ce.getCoordAction(actionId);
                return (bean.getStatus() == CoordinatorAction.Status.RUNNING || bean.getStatus() == CoordinatorAction.Status.SUCCEEDED);
            }
        });

        CoordinatorStore store2 = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store2.beginTrx();
        CoordinatorActionBean action = store2.getCoordinatorAction(actionId, false);
        if (action.getStatus() == CoordinatorAction.Status.RUNNING
                || action.getStatus() == CoordinatorAction.Status.SUCCEEDED) {

        }
        else {
            fail();
        }
        store2.commitTrx();
        store2.closeTrx();
    }

    /**
     * Tests functionality of the Recovery Service Runnable command. </p> Insert a coordinator job with RUNNING and
     * action with WAITING. Then, runs the recovery runnable and ensures the action status changes to READY.
     *
     * @throws Exception
     */
    public void testCoordActionRecoveryServiceForWaiting() throws Exception {

        CoordinatorJobBean job = addRecordToCoordJobTableForWaiting("coord-job-for-action-input-check.xml",
                CoordinatorJob.Status.RUNNING, false, true);

        CoordinatorActionBean action = addRecordToCoordActionTableForWaiting(job.getId(), 1,
                CoordinatorAction.Status.WAITING, "coord-action-for-action-input-check.xml");

        createDir(new File(getTestCaseDir(), "/2009/29/"));
        createDir(new File(getTestCaseDir(), "/2009/22/"));
        createDir(new File(getTestCaseDir(), "/2009/15/"));
        createDir(new File(getTestCaseDir(), "/2009/08/"));

        sleep(3000);

        Runnable recoveryRunnable = new RecoveryRunnable(0, 1, 1);
        recoveryRunnable.run();

        final String actionId = action.getId();
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        waitFor(10000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordActionGetJPAExecutor coordGetCmd = new CoordActionGetJPAExecutor(actionId);
                CoordinatorActionBean newAction = jpaService.execute(coordGetCmd);
                return (newAction.getStatus() != CoordinatorAction.Status.WAITING);
            }
        });

        CoordActionGetJPAExecutor coordGetCmd = new CoordActionGetJPAExecutor(actionId);
        action = jpaService.execute(coordGetCmd);
        if (action.getStatus() == CoordinatorAction.Status.WAITING) {
            fail("recovery waiting coord action failed, action is WAITING");
        }
    }


    public void testCoordActionRecoveryServiceForWaitingRegisterPartition() throws Exception {
        services.destroy();
        services = super.setupServicesForHCatalog();
        services.getConf().set(URIHandlerService.URI_HANDLERS,
                FSURIHandler.class.getName() + "," + HCatURIHandler.class.getName());
        services.getConf().setLong(RecoveryService.CONF_PUSH_DEPENDENCY_INTERVAL, 1);
        services.init();

        String db = "default";
        String table = "tablename";

        // dep1 is not available and dep2 is available
        String newHCatDependency1 = "hcat://" + server + "/" + db + "/" + table + "/dt=20120430;country=brazil";
        String newHCatDependency2 = "hcat://" + server + "/" + db + "/" + table + "/dt=20120430;country=usa";
        String newHCatDependency = newHCatDependency1 + CoordELFunctions.INSTANCE_SEPARATOR + newHCatDependency2;

        HCatAccessorService hcatService = services.get(HCatAccessorService.class);
        JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        PartitionDependencyManagerService pdms = services.get(PartitionDependencyManagerService.class);
        assertFalse(jmsService.isListeningToTopic(hcatService.getJMSConnectionInfo(new URI(newHCatDependency1)), db
                + "." + table));

        populateTable(db, table);
        String actionId = addInitRecords(newHCatDependency);
        CoordinatorAction ca = checkCoordActionDependencies(actionId, newHCatDependency);
        assertEquals(CoordinatorAction.Status.WAITING, ca.getStatus());
        // Register the missing dependencies to PDMS assuming CoordPushDependencyCheckCommand did this.
        pdms.addMissingDependency(new HCatURI(newHCatDependency1), actionId);
        pdms.addMissingDependency(new HCatURI(newHCatDependency2), actionId);

        sleep(2000);
        Runnable recoveryRunnable = new RecoveryRunnable(0, 1, 1);
        recoveryRunnable.run();
        sleep(2000);

        // Recovery service should have discovered newHCatDependency2 and JMS Connection should exist
        // and newHCatDependency1 should be in PDMS waiting list
        assertTrue(jmsService.isListeningToTopic(hcatService.getJMSConnectionInfo(new URI(newHCatDependency2)), "hcat."
                + db + "." + table));
        checkCoordActionDependencies(actionId, newHCatDependency1);

        assertNull(pdms.getWaitingActions(new HCatURI(newHCatDependency2)));
        Collection<String> waitingActions = pdms.getWaitingActions(new HCatURI(newHCatDependency1));
        assertEquals(1, waitingActions.size());
        assertTrue(waitingActions.contains(actionId));
    }

    private void populateTable(String db, String table) throws Exception {
        dropTable(db, table, true);
        dropDatabase(db, true);
        createDatabase(db);
        createTable(db, table, "dt,country");
        addPartition(db, table, "dt=20120430;country=usa");
        addPartition(db, table, "dt=20120412;country=brazil");
        addPartition(db, table, "dt=20120413;country=brazil");
    }

    private CoordinatorActionBean checkCoordActionDependencies(String actionId, String expDeps) throws Exception {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            CoordinatorActionBean action = jpaService.execute(new CoordActionGetJPAExecutor(actionId));
            assertEquals(expDeps, action.getPushMissingDependencies());
            return action;
        }
        catch (JPAExecutorException se) {
            throw new Exception("Action ID " + actionId + " was not stored properly in db");
        }
    }

    /**
     * Tests functionality of the Recovery Service Runnable command. </p> Insert a coordinator job with SUSPENDED and
     * action with SUSPENDED and workflow with RUNNING. Then, runs the recovery runnable and ensures the workflow status changes to SUSPENDED.
     *
     * @throws Exception
     */
    public void testCoordActionRecoveryServiceForSuspended() throws Exception {

        Date start = DateUtils.parseDateOozieTZ("2009-02-01T01:00Z");
        Date end = DateUtils.parseDateOozieTZ("2009-02-02T23:59Z");
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUSPENDED, start, end, false, false, 1);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        final String wfJobId = wfJob.getId();
        addRecordToCoordActionTable(coordJob.getId(), 1,
                CoordinatorAction.Status.SUSPENDED, "coord-action-get.xml", wfJobId, "RUNNING", 2);

        sleep(3000);

        Runnable recoveryRunnable = new RecoveryRunnable(0, 1, 1);
        recoveryRunnable.run();

        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        waitFor(10000, new Predicate() {
            public boolean evaluate() throws Exception {
                WorkflowJobGetJPAExecutor wfGetCmd = new WorkflowJobGetJPAExecutor(wfJobId);
                WorkflowJobBean ret = jpaService.execute(wfGetCmd);
                return (ret.getStatus() == WorkflowJob.Status.SUSPENDED);
            }
        });

        WorkflowJobGetJPAExecutor wfGetCmd = new WorkflowJobGetJPAExecutor(wfJobId);
        WorkflowJobBean ret = jpaService.execute(wfGetCmd);
        assertEquals(WorkflowJob.Status.SUSPENDED, ret.getStatus());
    }

    /**
     * Tests functionality of the Recovery Service Runnable command. </p> Insert a coordinator job with KILLED and
     * action with KILLED and workflow with RUNNING. Then, runs the recovery runnable and ensures the workflow status changes to KILLED.
     *
     * @throws Exception
     */
    public void testCoordActionRecoveryServiceForKilled() throws Exception {

        Date start = DateUtils.parseDateOozieTZ("2009-02-01T01:00Z");
        Date end = DateUtils.parseDateOozieTZ("2009-02-02T23:59Z");
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.KILLED, start, end, false, false, 1);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        final String wfJobId = wfJob.getId();
        addRecordToCoordActionTable(coordJob.getId(), 1,
                CoordinatorAction.Status.KILLED, "coord-action-get.xml", wfJobId, "RUNNING", 1);

        sleep(3000);

        Runnable recoveryRunnable = new RecoveryRunnable(0, 1, 1);
        recoveryRunnable.run();

        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        waitFor(10000, new Predicate() {
            public boolean evaluate() throws Exception {
                WorkflowJobGetJPAExecutor wfGetCmd = new WorkflowJobGetJPAExecutor(wfJobId);
                WorkflowJobBean ret = jpaService.execute(wfGetCmd);
                return (ret.getStatus() == WorkflowJob.Status.KILLED);
            }
        });

        WorkflowJobGetJPAExecutor wfGetCmd = new WorkflowJobGetJPAExecutor(wfJobId);
        WorkflowJobBean ret = jpaService.execute(wfGetCmd);
        assertEquals(WorkflowJob.Status.KILLED, ret.getStatus());
    }

    /**
     * Tests functionality of the Recovery Service Runnable command. </p> Insert a coordinator job with RUNNING and
     * action with RUNNING and workflow with SUSPENDED. Then, runs the recovery runnable and ensures the workflow status changes to RUNNING.
     *
     * @throws Exception
     */
    public void testCoordActionRecoveryServiceForResume() throws Exception {

        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUSPENDED, WorkflowInstance.Status.SUSPENDED);
        final String wfJobId = wfJob.getId();
        addRecordToCoordActionTable(coordJob.getId(), 1,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", wfJobId, "SUSPENDED", 1);

        sleep(3000);

        Runnable recoveryRunnable = new RecoveryRunnable(0, 1, 1);
        recoveryRunnable.run();

        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        waitFor(10000, new Predicate() {
            public boolean evaluate() throws Exception {
                WorkflowJobGetJPAExecutor wfGetCmd = new WorkflowJobGetJPAExecutor(wfJobId);
                WorkflowJobBean ret = jpaService.execute(wfGetCmd);
                return (ret.getStatus() == WorkflowJob.Status.RUNNING);
            }
        });

        WorkflowJobGetJPAExecutor wfGetCmd = new WorkflowJobGetJPAExecutor(wfJobId);
        WorkflowJobBean ret = jpaService.execute(wfGetCmd);
        assertEquals(WorkflowJob.Status.RUNNING, ret.getStatus());
    }

    protected CoordinatorActionBean addRecordToCoordActionTableForWaiting(String jobId, int actionNum,
            CoordinatorAction.Status status, String resourceXmlName) throws Exception {
        CoordinatorActionBean action = createCoordAction(jobId, actionNum, status, resourceXmlName, 0);
        String testDir = getTestCaseDir();
        String missDeps = getTestCaseFileUri("2009/29/_SUCCESS") + "#"
                + getTestCaseFileUri("2009/22/_SUCCESS") + "#"
                + getTestCaseFileUri("2009/15/_SUCCESS") + "#"
                + getTestCaseFileUri("2009/08/_SUCCESS");

        missDeps = missDeps.replaceAll("#testDir", testDir);
        action.setMissingDependencies(missDeps);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordActionInsertJPAExecutor coordActionInsertCmd = new CoordActionInsertJPAExecutor(action);
            jpaService.execute(coordActionInsertCmd);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test coord action record to table");
            throw je;
        }
        return action;
    }

    @Override
    protected String getCoordActionXml(Path appPath, String resourceXmlName) {
        try {
            Reader reader = IOUtils.getResourceAsReader(resourceXmlName, -1);
            String appXml = IOUtils.getReaderAsString(reader, -1);
            String testDir = getTestCaseDir();
            appXml = appXml.replaceAll("#testDir", testDir);
            return appXml;
        }
        catch (IOException ioe) {
            throw new RuntimeException(XLog.format("Could not get "+ resourceXmlName, ioe));
        }
    }

    private void addRecordToActionTable(String jobId, int actionNum, String actionId, CoordinatorStore store, String baseDir) throws StoreException, IOException {
        CoordinatorActionBean action = new CoordinatorActionBean();
        action.setJobId(jobId);
        action.setId(actionId);
        action.setActionNumber(actionNum);
        action.setNominalTime(new Date());
        action.setLastModifiedTime(new Date());
        action.setStatus(CoordinatorAction.Status.SUBMITTED);
        String appPath = getTestCaseFileUri("one-op/workflow.xml");
        String actionXml = "<coordinator-app xmlns='uri:oozie:coordinator:0.2' xmlns:sla='uri:oozie:sla:0.1' name='NAME' frequency=\"1\" start='2009-02-01T01:00Z' end='2009-02-03T23:59Z' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'  instance-number=\"1\" action-nominal-time=\"2009-02-01T01:00Z\">";
        actionXml += "<controls>";
        actionXml += "<timeout>10</timeout>";
        actionXml += "<concurrency>2</concurrency>";
        actionXml += "<execution>LIFO</execution>";
        actionXml += "</controls>";
        actionXml += "<input-events>";
        actionXml += "<data-in name='A' dataset='a'>";
        actionXml += "<dataset name='a' frequency='7' initial-instance='2009-02-01T01:00Z' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        actionXml += "<uri-template>" + getTestCaseFileUri("workflows/workflows/${YEAR}/${DAY}") + "</uri-template>";
        actionXml += "</dataset>";
        actionXml += "<instance>${coord:latest(0)}</instance>";
        actionXml += "</data-in>";
        actionXml += "</input-events>";
        actionXml += "<output-events>";
        actionXml += "<data-out name='LOCAL_A' dataset='local_a'>";
        actionXml += "<dataset name='local_a' frequency='7' initial-instance='2009-02-01T01:00Z' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        actionXml += "<uri-template>" + getTestCaseFileUri("workflows/${YEAR}/${DAY}") + "</uri-template>";
        actionXml += "</dataset>";
        actionXml += "<instance>${coord:current(-1)}</instance>";
        actionXml += "</data-out>";
        actionXml += "</output-events>";
        actionXml += "<action>";
        actionXml += "<workflow>";
        actionXml += "<app-path>" + appPath + "</app-path>";
        actionXml += "<configuration>";
        actionXml += "<property>";
        actionXml += "<name>inputA</name>";
        actionXml += "<value>" + getTestCaseFileUri("workflows/US/2009/02/") + "</value>";
        actionXml += "</property>";
        actionXml += "<property>";
        actionXml += "<name>inputB</name>";
        actionXml += "<value>" + getTestCaseFileUri("workflows/US/2009/01/") + "</value>";
        actionXml += "</property>";
        actionXml += "</configuration>";
        actionXml += "</workflow>";
        actionXml += "</action>";
        actionXml += "</coordinator-app>";
        action.setActionXml(actionXml);

        String createdConf = "<configuration> ";
        createdConf += "<property> <name>execution_order</name> <value>LIFO</value> </property>";
        createdConf += "<property> <name>user.name</name> <value>" + getTestUser() + "</value> </property>";
        createdConf += "<property> <name>group.name</name> <value>other</value> </property>";
        createdConf += "<property> <name>app-path</name> <value>" + appPath + "</value> </property>";
        createdConf += "<property> <name>jobTracker</name> ";
        createdConf += "<value>localhost:9001</value></property>";
        createdConf += "<property> <name>nameNode</name> <value>hdfs://localhost:9000</value></property>";
        createdConf += "<property> <name>queueName</name> <value>default</value></property>";

        createdConf += "</configuration> ";

        XConfiguration conf = new XConfiguration(new StringReader(createdConf));

        createdConf = conf.toXmlString(false);

        action.setCreatedConf(createdConf);
        store.insertCoordinatorAction(action);
        String content = "<workflow-app xmlns='uri:oozie:workflow:0.1'  xmlns:sla='uri:oozie:sla:0.1' name='one-op-wf'>";
        content += "<start to='fs1'/><action name='fs1'><fs><mkdir path='/tmp'/></fs><ok to='end'/><error to='end'/></action>";
        content += "<end name='end' /></workflow-app>";
        writeToFile(content, baseDir + "/one-op/");
    }

    private void writeToFile(String content, String appPath) throws IOException {
        createDir(new File(appPath));
        File wf = new File(appPath, "workflow.xml");
        PrintWriter out = null;
        try {
            out = new PrintWriter(new FileWriter(wf));
            out.println(content);
        }
        catch (IOException iex) {
            iex.printStackTrace();
            throw iex;
        }
        finally {
            if (out != null) {
                out.close();
            }
        }

    }

    private void createDir(File dir) {
        new File(dir, "_SUCCESS").mkdirs();
    }

    private void addRecordToJobTable(String jobId, CoordinatorStore store, String baseDir) throws StoreException {
        CoordinatorJobBean coordJob = new CoordinatorJobBean();
        coordJob.setId(jobId);
        coordJob.setAppName("testApp");
        coordJob.setAppPath("testAppPath");
        coordJob.setStatus(CoordinatorJob.Status.RUNNING);
        coordJob.setCreatedTime(new Date());
        coordJob.setLastModifiedTime(new Date());
        coordJob.setUser(getTestUser());
        coordJob.setGroup(getTestGroup());
        coordJob.setTimeZone("UTC");

        String confStr = "<configuration></configuration>";
        coordJob.setConf(confStr);
        String appXml = "<coordinator-app xmlns='uri:oozie:coordinator:0.2' name='NAME' frequency=\"1\" start='2009-02-01T01:00Z' end='2009-02-03T23:59Z'";
        appXml += " timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        appXml += "<controls>";
        appXml += "<timeout>10</timeout>";
        appXml += "<concurrency>2</concurrency>";
        appXml += "<execution>LIFO</execution>";
        appXml += "</controls>";
        appXml += "<input-events>";
        appXml += "<data-in name='A' dataset='a'>";
        appXml += "<dataset name='a' frequency='7' initial-instance='2009-02-01T01:00Z' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        appXml += "<uri-template>" + getTestCaseFileUri("workflows/${YEAR}/${DAY}") + "</uri-template>";
        appXml += "</dataset>";
        appXml += "<instance>${coord:latest(0)}</instance>";
        appXml += "</data-in>";
        appXml += "</input-events>";
        appXml += "<output-events>";
        appXml += "<data-out name='LOCAL_A' dataset='local_a'>";
        appXml += "<dataset name='local_a' frequency='7' initial-instance='2009-02-01T01:00Z' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        appXml += "<uri-template>" + getTestCaseFileUri("workflows/${YEAR}/${DAY}") + "</uri-template>";
        appXml += "</dataset>";
        appXml += "<instance>${coord:current(-1)}</instance>";
        appXml += "</data-out>";
        appXml += "</output-events>";
        appXml += "<action>";
        appXml += "<workflow>";
        appXml += "<app-path>" + getTestCaseFileUri("workflows") + "</app-path>";
        appXml += "<configuration>";
        appXml += "<property>";
        appXml += "<name>inputA</name>";
        appXml += "<value>${coord:dataIn('A')}</value>";
        appXml += "</property>";
        appXml += "<property>";
        appXml += "<name>inputB</name>";
        appXml += "<value>${coord:dataOut('LOCAL_A')}</value>";
        appXml += "</property>";
        appXml += "</configuration>";
        appXml += "</workflow>";
        appXml += "</action>";
        appXml += "</coordinator-app>";
        coordJob.setJobXml(appXml);
        coordJob.setLastActionNumber(0);
        coordJob.setFrequency("1");
        coordJob.setExecutionOrder(Execution.FIFO);
        coordJob.setConcurrency(1);
        try {
            coordJob.setEndTime(DateUtils.parseDateOozieTZ("2009-02-03T23:59Z"));
            coordJob.setStartTime(DateUtils.parseDateOozieTZ("2009-02-01T23:59Z"));
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not set Date/time");
        }

        try {
            store.insertCoordinatorJob(coordJob);
        }
        catch (StoreException se) {
            se.printStackTrace();
            store.rollbackTrx();
            fail("Unable to insert the test job record to table");
            throw se;
        }
    }

    @Override
    protected WorkflowActionBean addRecordToWfActionTable(String wfId, String actionName, WorkflowAction.Status status)
            throws Exception {
        WorkflowActionBean action = createWorkflowActionSetPending(wfId, status);
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            WorkflowActionInsertJPAExecutor actionInsertCmd = new WorkflowActionInsertJPAExecutor(action);
            jpaService.execute(actionInsertCmd);
        }
        catch (JPAExecutorException ce) {
            ce.printStackTrace();
            fail("Unable to insert the test wf action record to table");
            throw ce;
        }
        return action;
    }

    protected WorkflowActionBean createWorkflowActionSetPending(String wfId, WorkflowAction.Status status)
            throws Exception {
        WorkflowActionBean action = new WorkflowActionBean();
        String actionname = "testAction";
        action.setName(actionname);
        action.setCred("null");
        action.setId(Services.get().get(UUIDService.class).generateChildId(wfId, actionname));
        action.setJobId(wfId);
        action.setType("map-reduce");
        action.setTransition("transition");
        action.setStatus(status);
        action.setCreatedTime(new Date());
        action.setStartTime(new Date());
        action.setEndTime(new Date());
        action.setLastCheckTime(new Date());
        action.setPending();
        action.setUserRetryCount(1);
        action.setUserRetryMax(2);
        action.setUserRetryInterval(1);

        Path inputDir = new Path(getFsTestCaseDir(), "input");
        Path outputDir = new Path(getFsTestCaseDir(), "output");

        FileSystem fs = getFileSystem();
        Writer w = new OutputStreamWriter(fs.create(new Path(inputDir, "data.txt")));
        w.write("dummy\n");
        w.write("dummy\n");
        w.close();

        String actionXml = "<map-reduce>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>" + "<configuration>"
                + "<property><name>mapred.mapper.class</name><value>" + MapperReducerForTest.class.getName()
                + "</value></property>" + "<property><name>mapred.reducer.class</name><value>"
                + MapperReducerForTest.class.getName() + "</value></property>"
                + "<property><name>mapred.input.dir</name><value>" + inputDir.toString() + "</value></property>"
                + "<property><name>mapred.output.dir</name><value>" + outputDir.toString() + "</value></property>"
                + "</configuration>" + "</map-reduce>";
        action.setConf(actionXml);

        return action;
    }
}
