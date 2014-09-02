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

package org.apache.oozie.sla;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.AppType;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.event.JobEvent.EventStatus;
import org.apache.oozie.client.event.SLAEvent;
import org.apache.oozie.client.event.SLAEvent.SLAStatus;
import org.apache.oozie.executor.jpa.SLARegistrationQueryExecutor;
import org.apache.oozie.executor.jpa.SLASummaryQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor;
import org.apache.oozie.executor.jpa.SLARegistrationQueryExecutor.SLARegQuery;
import org.apache.oozie.executor.jpa.SLASummaryQueryExecutor.SLASummaryQuery;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor.WorkflowJobQuery;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.listener.SLAEventListener;
import org.apache.oozie.sla.service.SLAService;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.workflow.WorkflowInstance;
import org.jdom.Element;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSLAService extends XDataTestCase {

    static StringBuilder output = new StringBuilder();

    @Override
    @Before
    protected void setUp() throws Exception {
        super.setUp();
        Services services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_EXT_CLASSES, "org.apache.oozie.service.EventHandlerService,"
                + "org.apache.oozie.sla.service.SLAService");
        conf.setClass(EventHandlerService.CONF_LISTENERS, DummySLAEventListener.class, SLAEventListener.class);
        conf.setLong(SLAService.CONF_JOB_EVENT_LATENCY, 0);
        conf.setInt(EventHandlerService.CONF_WORKER_THREADS, 0);
        services.init();
    }

    @Override
    @After
    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    @Test
    public void testBasicService() throws Exception {
        Services services = Services.get();
        SLAService slas = services.get(SLAService.class);
        assertNotNull(slas);
        assertTrue(SLAService.isEnabled());

        services.destroy();
        services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_EXT_CLASSES, "");
        services.init();
        assertFalse(SLAService.isEnabled());
    }

    @Test
    public void testUpdateSLA() throws Exception {
        SLAService slas = Services.get().get(SLAService.class);
        assertNotNull(slas);
        assertTrue(SLAService.isEnabled());

        EventHandlerService ehs = Services.get().get(EventHandlerService.class);
        // test start-miss
        SLARegistrationBean sla1 = _createSLARegistration("job-1", AppType.WORKFLOW_JOB);
        sla1.setExpectedStart(new Date(System.currentTimeMillis() - 1 * 1 * 3600 * 1000)); //1 hour back
        sla1.setExpectedEnd(new Date(System.currentTimeMillis() - 1 * 1 * 3600 * 1000)); //1 hour back
        sla1.setExpectedDuration(10 * 60 * 1000); //10 mins
        slas.addRegistrationEvent(sla1);
        assertEquals(1, slas.getSLACalculator().size());
        slas.runSLAWorker();
        ehs.new EventWorker().run();
        assertEventNoDuplicates(output.toString(), "Sla START - MISS!!!");
        assertEventNoDuplicates(output.toString(), "Sla END - MISS!!!");
        output.setLength(0);

        // test different jobs and events start-met and end-miss
        sla1 = _createSLARegistration("job-2", AppType.WORKFLOW_JOB);
        sla1.setExpectedStart(new Date(System.currentTimeMillis() + 1 * 3600 * 1000)); //1 hour ahead
        sla1.setExpectedEnd(new Date(System.currentTimeMillis() + 2 * 3600 * 1000)); //2 hours ahead
        slas.addRegistrationEvent(sla1);
        slas.addStatusEvent(sla1.getId(), WorkflowJob.Status.RUNNING.name(), EventStatus.STARTED, new Date(),
                new Date());
        SLARegistrationBean sla2 = _createSLARegistration("job-3", AppType.COORDINATOR_JOB);
        sla2.setExpectedStart(new Date(System.currentTimeMillis() + 1 * 3600 * 1000)); //1 hour ahead only for testing
        sla2.setExpectedEnd(new Date(System.currentTimeMillis() - 2 * 3600 * 1000)); //2 hours back
        sla2.setExpectedDuration(10); //to process duration too
        slas.addRegistrationEvent(sla2);
        assertEquals(3, slas.getSLACalculator().size());
        Date startTime = new Date();
        slas.addStatusEvent(sla2.getId(), CoordinatorJob.Status.RUNNING.name(), EventStatus.STARTED, startTime,
                null);
        slas.addStatusEvent(sla2.getId(), CoordinatorJob.Status.SUCCEEDED.name(), EventStatus.SUCCESS, startTime,
                new Date());
        slas.runSLAWorker();
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains(sla1.getId() + " Sla START - MET!!!"));
        assertTrue(output.toString().contains(sla2.getId() + " Sla END - MISS!!!"));
        assertTrue(output.toString().contains(sla2.getId() + " Sla DURATION - MET!!!"));
        output.setLength(0);

        // test same job multiple events (start-miss, end-miss) through regular check
        WorkflowJobBean job4 = addRecordToWfJobTable(WorkflowJob.Status.KILLED, WorkflowInstance.Status.KILLED);
        sla2 = _createSLARegistration(job4.getId(), AppType.WORKFLOW_JOB);
        sla2.setExpectedStart(new Date(System.currentTimeMillis() - 2 * 3600 * 1000)); //2 hours back
        sla2.setExpectedEnd(new Date(System.currentTimeMillis() - 1 * 3600 * 1000)); //1 hour back
        slas.addRegistrationEvent(sla2);
        assertEquals(3, slas.getSLACalculator().size()); // tests job slaProcessed == 7 removed from map
        slas.runSLAWorker();
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains(sla2.getId() + " Sla START - MISS!!!"));
        assertTrue(output.toString().contains(sla2.getId() + " Sla END - MISS!!!"));
        output.setLength(0);
        // As expected duration is not set, duration shall be processed and job removed from map
        assertEquals(2, slas.getSLACalculator().size());
        // test same job multiple events (start-met, end-met) through job status event
        sla1 = _createSLARegistration("action@1", AppType.COORDINATOR_ACTION);
        sla1.setExpectedStart(new Date(System.currentTimeMillis() + 1 * 3600 * 1000)); //1 hour ahead
        sla1.setExpectedEnd(new Date(System.currentTimeMillis() + 2 * 3600 * 1000)); //2 hours ahead
        slas.addRegistrationEvent(sla1);
        assertEquals(3, slas.getSLACalculator().size());
        slas.addStatusEvent(sla1.getId(), CoordinatorAction.Status.RUNNING.name(), EventStatus.STARTED, new Date(),
                new Date());
        slas.addStatusEvent(sla1.getId(), CoordinatorAction.Status.SUCCEEDED.name(), EventStatus.SUCCESS,
                new Date(), new Date());
        slas.runSLAWorker();
        assertEquals(2, ehs.getEventQueue().size());
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains(sla1.getId() + " Sla START - MET!!!"));
        assertTrue(output.toString().contains(sla1.getId() + " Sla END - MET!!!"));
    }

    @Test
    public void testEndMissDBConfirm() throws Exception {
        SLAService slas = Services.get().get(SLAService.class);
        EventHandlerService ehs = Services.get().get(EventHandlerService.class);
        JPAService jpaService = Services.get().get(JPAService.class);

        // CASE 1: positive test WF job
        WorkflowJobBean job1 = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);
        SLARegistrationBean sla = _createSLARegistration(job1.getId(), AppType.WORKFLOW_JOB);
        sla.setExpectedEnd(new Date(System.currentTimeMillis() - 1 * 1800 * 1000)); // half hour back
        slas.addRegistrationEvent(sla);

        // CASE 2: negative test WF job
        WorkflowJobBean job2 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        job2.setEndTime(new Date(System.currentTimeMillis() - 1 * 1800 * 1000));
        job2.setStartTime(new Date(System.currentTimeMillis() - 1 * 2000 * 1000));
        job2.setLastModifiedTime(new Date());
        WorkflowJobQueryExecutor.getInstance().executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_INSTANCE_MOD_START_END, job2);
        sla = _createSLARegistration(job2.getId(), AppType.WORKFLOW_JOB);
        sla.setExpectedEnd(new Date(System.currentTimeMillis() - 1 * 1500 * 1000)); // in past but > actual end
        sla.setExpectedDuration(100); //unreasonable to cause MISS
        slas.addRegistrationEvent(sla);

        // CASE 3: positive test Coord action
        CoordinatorActionBean action1 = addRecordToCoordActionTable("coord-action-1", 1,
                CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0);
        WorkflowJobBean extWf = new WorkflowJobBean();
        extWf.setId(action1.getExternalId());
        extWf.setEndTime(new Date(System.currentTimeMillis() - 1 * 1800 * 1000));
        extWf.setStartTime(new Date(System.currentTimeMillis() - 1 * 2100 * 1000));
        jpaService.execute(new WorkflowJobInsertJPAExecutor(extWf));
        sla = _createSLARegistration(action1.getId(), AppType.COORDINATOR_ACTION);
        sla.setExpectedEnd(new Date(System.currentTimeMillis() - 1 * 2000 * 1000)); // past
        slas.addRegistrationEvent(sla);

        // CASE 4: positive test coord action
        CoordinatorActionBean action2 = addRecordToCoordActionTable("coord-action-2", 1,
                CoordinatorAction.Status.FAILED, "coord-action-get.xml", 0);
        extWf = new WorkflowJobBean();
        extWf.setId(action2.getExternalId());
        // actual end before expected. but action is failed
        extWf.setEndTime(new Date(System.currentTimeMillis() - 1 * 1800 * 1000));
        extWf.setStartTime(new Date(System.currentTimeMillis() - 1 * 2000 * 1000));
        jpaService.execute(new WorkflowJobInsertJPAExecutor(extWf));
        sla = _createSLARegistration(action2.getId(), AppType.COORDINATOR_ACTION);
        sla.setExpectedEnd(new Date(System.currentTimeMillis() - 1 * 1500 * 1000));
        slas.addRegistrationEvent(sla);

        // CASE 5: negative test coord action
        CoordinatorActionBean action3 = addRecordToCoordActionTable("coord-action-3", 1,
                CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
        extWf = new WorkflowJobBean();
        extWf.setId(action3.getExternalId());
        extWf.setStartTime(new Date(System.currentTimeMillis() - 1 * 2100 * 1000));
        extWf.setEndTime(new Date(System.currentTimeMillis() - 1 * 1800 * 1000));
        jpaService.execute(new WorkflowJobInsertJPAExecutor(extWf));
        sla = _createSLARegistration(action3.getId(), AppType.COORDINATOR_ACTION);
        sla.setExpectedStart(new Date(System.currentTimeMillis() - 1 * 3600 * 1000)); // cause start_miss
        sla.setExpectedEnd(new Date(System.currentTimeMillis() - 1 * 1500 * 1000)); // in past but > actual end, end_met
        sla.setExpectedDuration(0); //cause duration miss
        slas.addRegistrationEvent(sla);

        slas.runSLAWorker();
        ehs.new EventWorker().run();
        int count = 0;
        for (int ptr = output.indexOf("END - MISS"); ptr < output.length() && ptr > 0; ptr = output.indexOf(
                "END - MISS", ptr + 1)) {
            count++;
        }
        assertEquals(3, count); // only 3 out of the 5 are correct end_misses
        assertEventNoDuplicates(output.toString(), job1.getId() + " Sla END - MISS!!!");
        assertEventNoDuplicates(output.toString(), action1.getId() + " Sla END - MISS!!!");
        assertEventNoDuplicates(output.toString(), action2.getId() + " Sla END - MISS!!!");
        assertEventNoDuplicates(output.toString(), job2.getId() + " Sla END - MET!!!");
        assertEventNoDuplicates(output.toString(), job2.getId() + " Sla DURATION - MISS!!!");
        assertEventNoDuplicates(output.toString(), action3.getId() + " Sla START - MISS!!!");
        assertEventNoDuplicates(output.toString(), action3.getId() + " Sla DURATION - MISS!!!");
        assertEventNoDuplicates(output.toString(), action3.getId() + " Sla END - MET!!!");

        // negative on MISS after DB check, updated with actual times
        SLASummaryBean slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, job2.getId());
        assertEquals(job2.getStartTime(), slaSummary.getActualStart());
        assertEquals(job2.getEndTime(), slaSummary.getActualEnd());
        assertEquals(job2.getEndTime().getTime() - job2.getStartTime().getTime(), slaSummary.getActualDuration());
        assertEquals(job2.getStatusStr(), slaSummary.getJobStatus());
        assertEquals(SLAEvent.EventStatus.END_MET, slaSummary.getEventStatus());
        assertEquals(SLAStatus.MET, slaSummary.getSLAStatus());
        assertEquals(8, slaSummary.getEventProcessed());
        assertNull(slas.getSLACalculator().get(job2.getId())); //removed from memory

        // positives but also updated with actual times immediately after DB check
        slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, action2.getId());
        extWf = jpaService.execute(new WorkflowJobGetJPAExecutor(action2.getExternalId()));
        assertEquals(extWf.getStartTime(), slaSummary.getActualStart());
        assertEquals(extWf.getEndTime(), slaSummary.getActualEnd());
        assertEquals(extWf.getEndTime().getTime() - extWf.getStartTime().getTime(), slaSummary.getActualDuration());
        assertEquals(action2.getStatusStr(), slaSummary.getJobStatus());
        assertEquals(SLAEvent.EventStatus.END_MISS, slaSummary.getEventStatus());
        assertEquals(SLAStatus.MISS, slaSummary.getSLAStatus());
        assertEquals(8, slaSummary.getEventProcessed());
        assertNull(slas.getSLACalculator().get(action2.getId())); //removed from memory

        slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, action1.getId());
        extWf = jpaService.execute(new WorkflowJobGetJPAExecutor(action1.getExternalId()));
        assertEquals(extWf.getStartTime(), slaSummary.getActualStart());
        assertEquals(extWf.getEndTime(), slaSummary.getActualEnd());
        assertEquals(extWf.getEndTime().getTime() - extWf.getStartTime().getTime(), slaSummary.getActualDuration());
        assertEquals(action1.getStatusStr(), slaSummary.getJobStatus());
        assertEquals(SLAEvent.EventStatus.END_MISS, slaSummary.getEventStatus());
        assertEquals(SLAStatus.MISS, slaSummary.getSLAStatus());
        assertEquals(8, slaSummary.getEventProcessed());
        assertNull(slas.getSLACalculator().get(action1.getId())); //removed from memory

    }

    /**
     * Test SLAOperations handles unexpected alert-events in xml
     * @throws Exception
     */
    public void testSLAOperations() throws Exception {
        String slaXml = " <sla:info xmlns:sla='uri:oozie:sla:0.2'>"
                + " <sla:nominal-time>2009-03-06T10:00Z</sla:nominal-time>" + " <sla:should-start>5</sla:should-start>"
                + " <sla:should-end>120</sla:should-end>" + " <sla:max-duration>100</sla:max-duration>"
                + " <sla:alert-events>\"invalid_event_miss\'</sla:alert-events>"
                + " <sla:alert-contact>abc@example.com</sla:alert-contact>" + "</sla:info>";
        Element eSla = XmlUtils.parseXml(slaXml);
        SLAOperations.createSlaRegistrationEvent(eSla, "job-id1", "parent-id1", AppType.WORKFLOW_JOB, getTestUser(),
                "test-appname", log, false);
        SLARegistrationBean reg = SLARegistrationQueryExecutor.getInstance().get(SLARegQuery.GET_SLA_REG_ALL, "job-id1");
        assertEquals("END_MISS", reg.getAlertEvents());
    }

    public static SLARegistrationBean _createSLARegistration(String jobId, AppType appType) {
        SLARegistrationBean bean = new SLARegistrationBean();
        bean.setId(jobId);
        bean.setAppType(appType);
        return bean;
    }

    public static void assertEventNoDuplicates(String outputStr, String eventMsg) {
        int index = outputStr.indexOf(eventMsg);
        assertTrue(index != -1);
        //No duplicates
        assertTrue(outputStr.indexOf(eventMsg, index + 1) == -1);
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
