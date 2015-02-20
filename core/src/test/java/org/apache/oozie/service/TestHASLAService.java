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

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.AppType;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.event.JobEvent.EventStatus;
import org.apache.oozie.client.event.SLAEvent;
import org.apache.oozie.client.event.SLAEvent.SLAStatus;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.event.EventQueue;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.SLASummaryQueryExecutor;
import org.apache.oozie.executor.jpa.SLASummaryQueryExecutor.SLASummaryQuery;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor.WorkflowJobQuery;
import org.apache.oozie.sla.SLACalcStatus;
import org.apache.oozie.sla.SLACalculatorMemory;
import org.apache.oozie.sla.SLARegistrationBean;
import org.apache.oozie.sla.SLASummaryBean;
import org.apache.oozie.sla.TestSLAService;
import org.apache.oozie.sla.listener.SLAEventListener;
import org.apache.oozie.sla.service.SLAService;
import org.apache.oozie.test.ZKXTestCase;
import org.apache.oozie.util.JobUtils;
import org.apache.oozie.util.Pair;
import org.apache.oozie.workflow.WorkflowInstance;

public class TestHASLAService extends ZKXTestCase {

    private static StringBuilder output = new StringBuilder();

    protected void setUp() throws Exception {
        Configuration conf = new Configuration(false);
        conf.set(Services.CONF_SERVICE_EXT_CLASSES, "org.apache.oozie.service.EventHandlerService,"
                + "org.apache.oozie.sla.service.SLAService");
        conf.setClass(EventHandlerService.CONF_LISTENERS, DummySLAEventListener.class, SLAEventListener.class);
        conf.setLong(SLAService.CONF_JOB_EVENT_LATENCY, 0);
        // manually do check in this test
        conf.setInt(SLAService.CONF_SLA_CHECK_INITIAL_DELAY, 100000);
        conf.setInt(SLAService.CONF_SLA_CHECK_INTERVAL, 100000);
        conf.setInt(EventHandlerService.CONF_WORKER_THREADS, 0);
        super.setUp(conf);
        Services.get().setService(ZKJobsConcurrencyService.class);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testSLAFailOverWithHA() throws Exception {

        SLAService slas = Services.get().get(SLAService.class);
        SLACalculatorMemory slaCalcMem = (SLACalculatorMemory) slas.getSLACalculator();
        EventHandlerService ehs = Services.get().get(EventHandlerService.class);

        // start another dummy oozie instance (dummy sla and eventhandler
        // services)
        DummyZKOozie dummyOozie_1 = null;
        try {
            dummyOozie_1 = new DummyZKOozie("a", "http://blah");
            DummySLACalculatorMemory dummyCalc = new DummySLACalculatorMemory();
            EventHandlerService dummyEhs = new EventHandlerService();
            dummyCalc.setEventHandlerService(dummyEhs);
            dummyEhs.init(Services.get());
            dummyCalc.init(Services.get().getConf());

            // Case 1 workflow job submitted to dummy server,
            // but before start running, the dummy server is down
            createWorkflow("job-1");
            SLARegistrationBean sla1 = TestSLAService._createSLARegistration("job-1", AppType.WORKFLOW_JOB);
            sla1.setExpectedStart(new Date(System.currentTimeMillis() - 2 * 3600 * 1000)); // 2 hr before
            sla1.setExpectedEnd(new Date(System.currentTimeMillis() - 1 * 3600 * 1000)); // 1 hr before
            sla1.setExpectedDuration(10 * 60 * 1000); // 10 mins
            dummyCalc.addRegistration(sla1.getId(), sla1);

            // Case 2. workflow job submitted to dummy server, start running,
            // then the dummy server is down
            createWorkflow("job-2");
            SLARegistrationBean sla2 = TestSLAService._createSLARegistration("job-2", AppType.WORKFLOW_JOB);
            sla2.setExpectedStart(new Date(System.currentTimeMillis() - 2 * 3600 * 1000)); // 2hr before
            sla2.setExpectedEnd(new Date(System.currentTimeMillis() + 1 * 3600 * 1000)); // 1hr ahead
            sla2.setExpectedDuration(10 * 60 * 1000); // 10 mins
            dummyCalc.addRegistration(sla2.getId(), sla2);
            dummyCalc.addJobStatus(sla2.getId(), WorkflowJob.Status.RUNNING.name(), EventStatus.STARTED, new Date(),
                    new Date());

            dummyCalc.updateAllSlaStatus();
            dummyEhs.new EventWorker().run();
            assertTrue(output.toString().contains(sla2.getId() + " Sla START - MISS!!!"));

            // suppose dummy Server is down
            dummyCalc.clear();
            dummyCalc = null;
            dummyOozie_1.teardown();

            slaCalcMem.updateAllSlaStatus();

            // Job 1 started running on the living server --> start miss
            slaCalcMem.addJobStatus(sla1.getId(), WorkflowJob.Status.RUNNING.name(), EventStatus.STARTED, new Date(),
                    new Date());
            // job 1 is added to slamap of living oozie server
            assertNotNull(slaCalcMem.get(sla1.getId()));
            ehs.new EventWorker().run();
            assertTrue(output.toString().contains(sla1.getId() + " Sla START - MISS!!!"));

            // Job 1 succeeded on the living server --> duration met and end miss
            slaCalcMem.addJobStatus(sla1.getId(), WorkflowJob.Status.SUCCEEDED.name(), EventStatus.SUCCESS, new Date(),
                    new Date());
            ehs.new EventWorker().run();
            assertTrue(output.toString().contains(sla1.getId() + " Sla DURATION - MET!!!"));
            assertTrue(output.toString().contains(sla1.getId() + " Sla END - MISS!!!"));

            // Job 2 succeeded on the living server --> duration met and end met
            slaCalcMem.addJobStatus(sla2.getId(), WorkflowJob.Status.SUCCEEDED.name(), EventStatus.SUCCESS, new Date(),
                    new Date());
            // eventProc >= 7(already processed duration/end met), should be removed from slaMap
            assertNull(slaCalcMem.get(sla2.getId()));
            ehs.new EventWorker().run();
            assertTrue(output.toString().contains(sla2.getId() + " Sla DURATION - MET!!!"));
            assertTrue(output.toString().contains(sla2.getId() + " Sla END - MET!!!"));
        }
        finally {
            if (dummyOozie_1 != null) {
                dummyOozie_1.teardown();
            }
        }
    }

    public void updateCoordAction(String id, String status) throws JPAExecutorException {
        CoordinatorActionBean action = new CoordinatorActionBean();
        action.setId(id);
        action.setStatusStr(status);
        action.setLastModifiedTime(new Date());
        CoordActionQueryExecutor.getInstance().executeUpdate(CoordActionQuery.UPDATE_COORD_ACTION_STATUS_PENDING_TIME,
                action);
    }

    public void testSLAUpdateWithHA() throws Exception {

        String id1 = "0000001-130521183438837-oozie-test-C@1";
        String id2 = "0000002-130521183438837-oozie-test-C@1";
        String id3 = "0000003-130521183438837-oozie-test-C@1";
        String id4 = "0000004-130521183438837-oozie-test-C@1";
        String id5 = "0000005-130521183438837-oozie-test-C@1";
        String id6 = "0000006-130521183438837-oozie-test-C@1";
        Date expectedStartTS = new Date(System.currentTimeMillis() - 2 * 3600 * 1000); // 2 hrs passed
        Date expectedEndTS1 = new Date(System.currentTimeMillis() + 1 * 3600 * 1000); // 1 hour ahead
        Date expectedEndTS2 = new Date(System.currentTimeMillis() - 1 * 3600 * 1000); // 1 hour passed
        // Coord Action of jobs 1-4 not started yet
        createDBEntry(id1, expectedStartTS, expectedEndTS1);
        createDBEntry(id2, expectedStartTS, expectedEndTS1);
        createDBEntry(id3, expectedStartTS, expectedEndTS1);
        createDBEntry(id4, expectedStartTS, expectedEndTS1);
        // Coord Action of jobs 5-6 already started and currently running (to test history set)
        createDBEntryForStarted(id5, expectedStartTS, expectedEndTS2);
        createDBEntryForStarted(id6, expectedStartTS, expectedEndTS2);

        SLAService slas = Services.get().get(SLAService.class);
        SLACalculatorMemory slaCalcMem = (SLACalculatorMemory) slas.getSLACalculator();
        slaCalcMem.init(Services.get().getConf());
        List<String> slaMapKeys = new ArrayList<String>();
        Iterator<String> itr = slaCalcMem.iterator();
        while (itr.hasNext()) {
            slaMapKeys.add(itr.next());
        }
        assertEquals(6, slaMapKeys.size());

        DummyZKOozie dummyOozie_1 = null;
        try {
            // start another dummy oozie instance (dummy sla and event handler services)
            dummyOozie_1 = new DummyZKOozie("a", "http://blah");
            DummySLACalculatorMemory dummySlaCalcMem = new DummySLACalculatorMemory();
            EventHandlerService dummyEhs = new EventHandlerService();
            dummySlaCalcMem.setEventHandlerService(dummyEhs);
            dummyEhs.init(Services.get());
            dummySlaCalcMem.init(Services.get().getConf());
            slaMapKeys = new ArrayList<String>();
            itr = dummySlaCalcMem.iterator();
            while (itr.hasNext()) {
                slaMapKeys.add(itr.next());
            }
            assertEquals(6, slaMapKeys.size());

            // Coord Action 1,3 run and update status on *non-dummy* server
            updateCoordAction(id1, "RUNNING");
            slaCalcMem
                    .addJobStatus(id1, CoordinatorAction.Status.RUNNING.name(), EventStatus.STARTED, new Date(), null);
            updateCoordAction(id3, "FAILED");
            slaCalcMem.addJobStatus(id3, CoordinatorAction.Status.FAILED.name(), EventStatus.FAILURE, null, new Date());

            // Coord Action 2,4 run and update status on *dummy* server
            updateCoordAction(id2, "RUNNING");
            dummySlaCalcMem.addJobStatus(id2, CoordinatorAction.Status.RUNNING.name(), EventStatus.STARTED, new Date(),
                    null);
            updateCoordAction(id4, "FAILED");
            dummySlaCalcMem.addJobStatus(id4, CoordinatorAction.Status.FAILED.name(), EventStatus.FAILURE, null,
                    new Date());

            // Both servers iterate SlaMap (updateAllSlaStatus)
            slaCalcMem.updateAllSlaStatus();
            dummySlaCalcMem.updateAllSlaStatus();

            // SlaMap on both Servers synced
            SLACalcStatus sla1_nodummy = slaCalcMem.get(id1);
            SLACalcStatus sla1_dummy = dummySlaCalcMem.get(id1);
            SLACalcStatus sla2_nodummy = slaCalcMem.get(id2);
            SLACalcStatus sla2_dummy = dummySlaCalcMem.get(id2);
            assertEquals(1, sla1_nodummy.getEventProcessed());
            assertEquals(1, sla1_dummy.getEventProcessed());
            assertEquals(1, sla2_dummy.getEventProcessed());
            assertEquals(1, sla2_nodummy.getEventProcessed());
            assertFalse(slaCalcMem.isJobIdInSLAMap(id3));
            assertFalse(dummySlaCalcMem.isJobIdInSLAMap(id3));
            assertFalse(slaCalcMem.isJobIdInSLAMap(id4));
            assertFalse(dummySlaCalcMem.isJobIdInSLAMap(id4));

            Byte eventProc = (Byte) SLASummaryQueryExecutor.getInstance().getSingleValue(
                    SLASummaryQuery.GET_SLA_SUMMARY_EVENTPROCESSED, id3);
            assertEquals(8, eventProc.byteValue());
            eventProc = (Byte) SLASummaryQueryExecutor.getInstance().getSingleValue(
                    SLASummaryQuery.GET_SLA_SUMMARY_EVENTPROCESSED, id4);
            assertEquals(8, eventProc.byteValue());

            // Action 5 was processed as END_MISS in updateAllSlaStatus, put into history set
            assertTrue(slaCalcMem.isJobIdInHistorySet(id5));
            assertTrue(dummySlaCalcMem.isJobIdInHistorySet(id6));
            // Action 6 was processed as END_MISS in updateAllSlaStatus, put into history set
            assertTrue(slaCalcMem.isJobIdInHistorySet(id5));
            assertTrue(dummySlaCalcMem.isJobIdInHistorySet(id6));

            eventProc = (Byte) SLASummaryQueryExecutor.getInstance().getSingleValue(
                    SLASummaryQuery.GET_SLA_SUMMARY_EVENTPROCESSED, id5);
            assertEquals(7, eventProc.byteValue());
            eventProc = (Byte) SLASummaryQueryExecutor.getInstance().getSingleValue(
                    SLASummaryQuery.GET_SLA_SUMMARY_EVENTPROCESSED, id6);
            assertEquals(7, eventProc.byteValue());

            // Action 1 Succeeded on non-dummy server
            updateCoordAction(id1, "SUCCEEDED");
            slaCalcMem.addJobStatus(id1, CoordinatorAction.Status.SUCCEEDED.name(), EventStatus.SUCCESS, new Date(
                    System.currentTimeMillis() - 1800 * 1000), new Date());

            // Action 2 Succeeded on dummy server
            updateCoordAction(id2, "SUCCEEDED");
            dummySlaCalcMem.addJobStatus(id2, CoordinatorAction.Status.SUCCEEDED.name(), EventStatus.SUCCESS, new Date(
                    System.currentTimeMillis() - 1800 * 1000), new Date());

            // Both servers iterate SlaMap (updateAllSlaStatus)
            slaCalcMem.updateAllSlaStatus();
            dummySlaCalcMem.updateAllSlaStatus();

            // Action 1, 2 are removed from both servers
            assertNull(slaCalcMem.get(id1));
            assertNull(dummySlaCalcMem.get(id1));
            assertNull(slaCalcMem.get(id2));
            assertNull(dummySlaCalcMem.get(id2));
            eventProc = (Byte) SLASummaryQueryExecutor.getInstance().getSingleValue(
                    SLASummaryQuery.GET_SLA_SUMMARY_EVENTPROCESSED, id1);
            assertEquals(8, eventProc.byteValue());
            eventProc = (Byte) SLASummaryQueryExecutor.getInstance().getSingleValue(
                    SLASummaryQuery.GET_SLA_SUMMARY_EVENTPROCESSED, id2);
            assertEquals(8, eventProc.byteValue());

            // Test HistoryPurgeWorker purges Action 5,6 from history set
            updateCoordAction(id5, "SUCCEEDED");
            slaCalcMem.new HistoryPurgeWorker().run();
            assertFalse(slaCalcMem.isJobIdInHistorySet(id5));
            updateCoordAction(id6, "SUCCEEDED");
            dummySlaCalcMem.new HistoryPurgeWorker().run();
            assertFalse(dummySlaCalcMem.isJobIdInHistorySet(id6));

        }
        finally {
            if (dummyOozie_1 != null) {
                dummyOozie_1.teardown();
            }
        }
    }

    public void testNoDuplicateEventsInHA() throws Exception {
        String id1 = "0000001-130521183438837-oozie-test-C@1";
        Date expectedStartTS = new Date(System.currentTimeMillis() - 2 * 3600 * 1000); // get MISS
        Date expectedEndTS = new Date(System.currentTimeMillis() - 1 * 3600 * 1000); // get MISS
        createDBEntry(id1, expectedStartTS, expectedEndTS);

        SLAService slas = Services.get().get(SLAService.class);
        SLACalculatorMemory slaCalcMem = (SLACalculatorMemory) slas.getSLACalculator();
        slaCalcMem.init(Services.get().getConf()); // loads the job in sla map

        EventHandlerService ehs = Services.get().get(EventHandlerService.class);
        EventQueue ehs_q = ehs.getEventQueue();

        DummyZKOozie dummyOozie_1 = null;
        try {
            // start another dummy oozie instance (dummy sla and event handler services)
            dummyOozie_1 = new DummyZKOozie("a", "http://blah");
            DummySLACalculatorMemory dummySlaCalcMem = new DummySLACalculatorMemory();
            dummySlaCalcMem.init(Services.get().getConf());
            EventHandlerService dummyEhs = new EventHandlerService();
            dummySlaCalcMem.setEventHandlerService(dummyEhs);
            dummyEhs.init(Services.get());
            EventQueue dummyEhs_q = dummyEhs.getEventQueue();

            // Action started on Server 1
            updateCoordAction(id1, "RUNNING");
            slaCalcMem
                    .addJobStatus(id1, CoordinatorAction.Status.RUNNING.name(), EventStatus.STARTED, new Date(), null);
            SLACalcStatus s1 = (SLACalcStatus) ehs_q.poll();
            assertEquals(SLAStatus.IN_PROCESS, s1.getSLAStatus());

            // Action ended on Server 2
            updateCoordAction(id1, "FAILED");
            dummySlaCalcMem.addJobStatus(id1, CoordinatorAction.Status.FAILED.name(), EventStatus.FAILURE, new Date(
                    System.currentTimeMillis() - 1800 * 1000),
                    new Date());
            dummyEhs_q.poll(); // getting rid of the duration_miss event
            SLACalcStatus s2 = (SLACalcStatus) dummyEhs_q.poll();
            assertEquals(SLAStatus.MISS, s2.getSLAStatus());

            slaCalcMem.updateAllSlaStatus();
            dummySlaCalcMem.updateAllSlaStatus();
            assertEquals(0, ehs_q.size()); // no dupe event should be created again by Server 1
        }
        finally {
            if (dummyOozie_1 != null) {
                dummyOozie_1.teardown();
            }
        }
    }

    public void testSLAAlertCommandWithHA() throws Exception {

        //Test SLA ALERT commands in HA mode.
        //slaCalcMem1 is for server 1 and slaCalcMem2 is for server2

        String id = "0000001-130521183438837-oozie-test-C@1";
        Date expectedStartTS = new Date(System.currentTimeMillis() - 2 * 3600 * 1000); // 2 hrs passed
        Date expectedEndTS1 = new Date(System.currentTimeMillis() + 1 * 3600 * 1000); // 1 hour ahead
        // Coord Action of jobs 1-4 not started yet
        createDBEntry(id, expectedStartTS, expectedEndTS1);

        SLAService slas = Services.get().get(SLAService.class);
        SLACalculatorMemory slaCalcMem1 = (SLACalculatorMemory) slas.getSLACalculator();
        slaCalcMem1.init(Services.get().get(ConfigurationService.class).getConf());
        List<String> idList = new ArrayList<String>();
        idList.add(id);
        slaCalcMem1.disableAlert(idList);
        assertTrue(slaCalcMem1.get(id).getSLAConfigMap().containsKey(OozieClient.SLA_DISABLE_ALERT));

        DummyZKOozie dummyOozie_1 = null;
        try {
            // start another dummy oozie instance (dummy sla and event handler services)
            dummyOozie_1 = new DummyZKOozie("a", "http://blah");
            DummySLACalculatorMemory slaCalcMem2 = new DummySLACalculatorMemory();
            EventHandlerService dummyEhs = new EventHandlerService();
            slaCalcMem2.setEventHandlerService(dummyEhs);

            // So that job sla updated doesn't run automatically
            Services.get().get(ConfigurationService.class).getConf().setInt(SLAService.CONF_SLA_CHECK_INTERVAL, 100000);
            Services.get().get(ConfigurationService.class).getConf().setInt(SLAService.CONF_SLA_CHECK_INITIAL_DELAY, 100000);
            dummyEhs.init(Services.get());
            slaCalcMem2.init(Services.get().get(ConfigurationService.class).getConf());

            slaCalcMem2.updateAllSlaStatus();
            assertTrue(slaCalcMem2.get(id).getSLAConfigMap().containsKey(OozieClient.SLA_DISABLE_ALERT));

            String newParams = RestConstants.SLA_MAX_DURATION + "=5";
            List<Pair<String, Map<String, String>>> jobIdsSLAPair = new ArrayList<Pair<String, Map<String, String>>>();
            jobIdsSLAPair.add(new Pair<String, Map<String, String>>(id, JobUtils.parseChangeValue(newParams)));
            slaCalcMem1.changeDefinition(jobIdsSLAPair);
            assertEquals(slaCalcMem1.get(id).getExpectedDuration(), 5 * 60 * 1000);

            //Before update, default is 10.
            assertEquals(slaCalcMem2.get(id).getExpectedDuration(), 10 * 60 * 1000);

            slaCalcMem2.updateAllSlaStatus();
            assertEquals(slaCalcMem2.get(id).getExpectedDuration(), 5 * 60 * 1000);

            newParams = RestConstants.SLA_MAX_DURATION + "=15";
            jobIdsSLAPair.clear();
            jobIdsSLAPair.add(new Pair<String, Map<String, String>>(id, JobUtils.parseChangeValue(newParams)));
            slaCalcMem1.changeDefinition(jobIdsSLAPair);

            // Before update
            assertEquals(slaCalcMem2.get(id).getExpectedDuration(), 5 * 60 * 1000);
            slaCalcMem2.updateAllSlaStatus();
            assertEquals(slaCalcMem2.get(id).getExpectedDuration(), 15 * 60 * 1000);

        }
        finally {
            if (dummyOozie_1 != null) {
                dummyOozie_1.teardown();
            }
        }
    }

    private void createDBEntry(String actionId, Date expectedStartTS, Date expectedEndTS) throws Exception {
        ArrayList<JsonBean> insertList = new ArrayList<JsonBean>();
        CoordinatorActionBean coordAction = new CoordinatorActionBean();
        Date modTime = new Date(System.currentTimeMillis() - 1000 * 3600 * 2);
        coordAction.setId(actionId);
        coordAction.setJobId(actionId.split("@", -1)[0]);
        coordAction.setStatusStr("WAITING");
        coordAction.setLastModifiedTime(modTime);

        CoordinatorJobBean coordJob = new CoordinatorJobBean();
        coordJob.setId(actionId.split("@", -1)[0]);
        coordJob.setUser("dummy");
        coordJob.setAppName("dummy");
        coordJob.setStatusStr("RUNNING");
        coordJob.setAppNamespace("dummy");

        SLASummaryBean sla = new SLASummaryBean();
        sla.setId(actionId);
        sla.setAppType(AppType.COORDINATOR_ACTION);
        sla.setJobStatus("WAITING");
        sla.setSLAStatus(SLAStatus.NOT_STARTED);
        sla.setEventProcessed(0);
        sla.setLastModifiedTime(modTime);
        sla.setExpectedStart(expectedStartTS);
        sla.setExpectedEnd(expectedEndTS);
        sla.setExpectedDuration(10 * 60 * 1000);
        SLARegistrationBean reg = new SLARegistrationBean();
        reg.setId(actionId);
        insertList.add(coordAction);
        insertList.add(coordJob);
        insertList.add(sla);
        insertList.add(reg);
        BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(insertList, null, null);
    }

    private void createDBEntryForStarted(String actionId, Date expectedStartTS, Date expectedEndTS) throws Exception {
        ArrayList<JsonBean> insertList = new ArrayList<JsonBean>();
        Date modTime = new Date();
        WorkflowJobBean wf = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING,
                actionId);
        wf.setStatus(wf.getStatus());
        wf.setStartTime(expectedStartTS);
        wf.setLastModifiedTime(modTime);
        WorkflowJobQueryExecutor.getInstance().executeUpdate(
                WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_INSTANCE_MOD_START_END, wf);

        CoordinatorActionBean coordAction = new CoordinatorActionBean();
        coordAction.setId(actionId);
        coordAction.setJobId(actionId.split("@", -1)[0]);
        coordAction.setStatusStr("RUNNING");
        coordAction.setLastModifiedTime(modTime);
        coordAction.setExternalId(wf.getId());

        CoordinatorJobBean coordJob = new CoordinatorJobBean();
        coordJob.setId(actionId.split("@", -1)[0]);
        coordJob.setUser("dummy");
        coordJob.setAppName("dummy");
        coordJob.setStatusStr("RUNNING");
        coordJob.setAppNamespace("dummy");

        SLASummaryBean sla = new SLASummaryBean();
        sla.setId(actionId);
        sla.setAppType(AppType.COORDINATOR_ACTION);
        sla.setJobStatus("RUNNING");
        sla.setSLAStatus(SLAStatus.IN_PROCESS);
        sla.setEventProcessed(1);
        sla.setLastModifiedTime(modTime);
        sla.setExpectedStart(expectedStartTS);
        sla.setActualStart(expectedStartTS);
        sla.setExpectedEnd(expectedEndTS);
        sla.setExpectedDuration(10 * 60 * 1000);
        SLARegistrationBean reg = new SLARegistrationBean();
        reg.setId(actionId);
        insertList.add(coordAction);

        insertList.add(coordJob);
        insertList.add(sla);
        insertList.add(reg);
        BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(insertList, null, null);
    }

    private void createWorkflow(String id) throws Exception {
        List<JsonBean> insertList = new ArrayList<JsonBean>();
        WorkflowJobBean workflow = new WorkflowJobBean();
        workflow.setId(id);
        workflow.setStatusStr("PREP");
        workflow.setStartTime(new Date());
        workflow.setSlaXml("<sla></sla>");
        insertList.add(workflow);
        BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(insertList, null, null);
    }

    public static class DummySLAEventListener extends SLAEventListener {

        @Override
        public void onStartMet(SLAEvent sla) {
            output.append(sla.getId() + " Sla START - MET!!!");
        }

        @Override
        public void onStartMiss(SLAEvent sla) {
            output.append(sla.getId() + " Sla START - MISS!!!");
        }

        @Override
        public void onEndMet(SLAEvent sla) {
            output.append(sla.getId() + " Sla END - MET!!!");
        }

        @Override
        public void onEndMiss(SLAEvent sla) {
            output.append(sla.getId() + " Sla END - MISS!!!");
        }

        @Override
        public void onDurationMet(SLAEvent sla) {
            output.append(sla.getId() + " Sla DURATION - MET!!!");
        }

        @Override
        public void onDurationMiss(SLAEvent sla) {
            output.append(sla.getId() + " Sla DURATION - MISS!!!");
        }

        @Override
        public void init(Configuration conf) {
        }

        @Override
        public void destroy() {
        }
    }
}

class DummySLACalculatorMemory extends SLACalculatorMemory {
    public void setEventHandlerService(EventHandlerService ehs) {
        this.eventHandler = ehs;
    }
}
