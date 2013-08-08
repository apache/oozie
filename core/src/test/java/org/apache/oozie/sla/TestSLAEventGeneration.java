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

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.AppType;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.event.Event;
import org.apache.oozie.client.event.JobEvent;
import org.apache.oozie.client.event.SLAEvent.SLAStatus;
import org.apache.oozie.client.event.SLAEvent.EventStatus;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.coord.CoordActionStartXCommand;
import org.apache.oozie.command.coord.CoordActionUpdateXCommand;
import org.apache.oozie.command.coord.CoordKillXCommand;
import org.apache.oozie.command.coord.CoordRerunXCommand;
import org.apache.oozie.command.coord.CoordSubmitXCommand;
import org.apache.oozie.command.wf.KillXCommand;
import org.apache.oozie.command.wf.ReRunXCommand;
import org.apache.oozie.command.wf.StartXCommand;
import org.apache.oozie.command.wf.SubmitXCommand;
import org.apache.oozie.event.CoordinatorActionEvent;
import org.apache.oozie.event.listener.JobEventListener;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.sla.SLASummaryGetJPAExecutor;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.listener.SLAJobEventListener;
import org.apache.oozie.sla.service.SLAService;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSLAEventGeneration extends XDataTestCase {
    Services services;
    EventHandlerService ehs = null;
    JPAService jpa;
    Calendar cal;
    String alert_events = "START_MISS,END_MET,END_MISS";

    private static final String SLA_XML_1 = "<workflow-app xmlns=\"uri:oozie:workflow:0.2\" "
            + "xmlns:sla=\"uri:oozie:sla:0.1\" name=\"test-wf-job-sla\">" + "<start to=\"myjava\"/>"
            + "<action name=\"myjava\"> <java>" + "<job-tracker>jt</job-tracker>" + "<name-node>nn</name-node>"
            + "<main-class>org.apache.oozie.example.DemoJavaMain</main-class>" + "</java> <ok to=\"end\"/>"
            + "<error to=\"end\"/>" + "</action>" + "<end name=\"end\"/> " + "<sla:info>"
            + "<sla:app-name>test-wf-job-sla</sla:app-name>" + "<sla:nominal-time>${nominal_time}</sla:nominal-time>"
            + "<sla:should-start>${10 * MINUTES}</sla:should-start>"
            + "<sla:should-end>${30 * MINUTES}</sla:should-end>"
            + "<sla:notification-msg>My Notification Message</sla:notification-msg>"
            + "<sla:alert-contact>alert@example.com</sla:alert-contact>"
            + "<sla:dev-contact>dev@example.com</sla:dev-contact>" + "<sla:qa-contact>qa@example.com</sla:qa-contact>"
            + "<sla:se-contact>se@example.com</sla:se-contact>"
            + "<sla:alert-frequency>LAST_HOUR</sla:alert-frequency>"
            + "<sla:alert-percentage>10</sla:alert-percentage>" + "<sla:upstream-apps>upstream-job</sla:upstream-apps>"
            + "</sla:info>" + "</workflow-app>";

    @Override
    @Before
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        Configuration conf = services.getConf();
        String excludeServices[] = { "org.apache.oozie.service.StatusTransitService" };
        setClassesToBeExcluded(conf, excludeServices);
        conf.set(Services.CONF_SERVICE_EXT_CLASSES,
                EventHandlerService.class.getName() + "," + SLAService.class.getName());
        conf.setClass(EventHandlerService.CONF_LISTENERS, SLAJobEventListener.class, JobEventListener.class);
        conf.setInt(EventHandlerService.CONF_WORKER_INTERVAL, 10000);
        conf.setInt(EventHandlerService.CONF_WORKER_THREADS, 0);
        conf.setInt(EventHandlerService.CONF_BATCH_SIZE, 1);
        services.init();
        jpa = services.get(JPAService.class);
        ehs = services.get(EventHandlerService.class);
        cal = Calendar.getInstance();
        cleanUpDBTables();
    }

    @Override
    @After
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    /**
     * Test for SLA Events generated through Workflow Job Commands
     *
     * @throws Exception
     */
    @Test
    public void testWorkflowJobSLANew() throws Exception {
        assertNotNull(ehs);
        SLAService slas = services.get(SLAService.class);
        assertNotNull(slas);

        String wfXml = IOUtils.getResourceAsString("wf-job-sla.xml", -1);
        Path appPath = getFsTestCaseDir();
        writeToFile(wfXml, appPath, "workflow.xml");
        Configuration conf = new XConfiguration();
        conf.set(OozieClient.APP_PATH, appPath.toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        _testWorkflowJobCommands(conf, slas, true);
    }

    /**
     * Test for SLA Events generated through wf rerun
     *
     * @throws Exception
     */
    @Test
    public void testWorkflowJobSLARerun() throws Exception {
        SLAService slas = services.get(SLAService.class);

        String wfXml = IOUtils.getResourceAsString("wf-job-sla.xml", -1);
        Path appPath = getFsTestCaseDir();
        writeToFile(wfXml, appPath, "workflow.xml");
        Configuration conf = new XConfiguration();
        conf.set(OozieClient.APP_PATH, appPath.toString());
        conf.set(OozieClient.USER_NAME, getTestUser());

        cal.setTime(new Date());
        cal.add(Calendar.MINUTE, -40); // for start_miss
        Date nominal = cal.getTime();
        String nominalTime = DateUtils.formatDateOozieTZ(nominal);
        conf.set("nominal_time", nominalTime);
        cal.setTime(nominal);
        cal.add(Calendar.MINUTE, 10); // as per the sla xml
        String expectedStart = DateUtils.formatDateOozieTZ(cal.getTime());
        cal.setTime(nominal);
        cal.add(Calendar.MINUTE, 30); // as per the sla xml
        String expectedEnd = DateUtils.formatDateOozieTZ(cal.getTime());

        // Call SubmitX
        SubmitXCommand sc = new SubmitXCommand(conf);
        String jobId = sc.call();
        SLACalcStatus slaEvent = slas.getSLACalculator().get(jobId);
        assertEquals(jobId, slaEvent.getId());
        assertEquals("test-wf-job-sla", slaEvent.getAppName());
        assertEquals(AppType.WORKFLOW_JOB, slaEvent.getAppType());
        assertEquals(nominalTime, DateUtils.formatDateOozieTZ(slaEvent.getNominalTime()));
        assertEquals(expectedStart, DateUtils.formatDateOozieTZ(slaEvent.getExpectedStart()));
        assertEquals(expectedEnd, DateUtils.formatDateOozieTZ(slaEvent.getExpectedEnd()));

        slas.runSLAWorker();
        slaEvent = (SLACalcStatus) ehs.getEventQueue().poll();
        assertEquals(SLAStatus.NOT_STARTED, slaEvent.getSLAStatus());
        assertEquals(EventStatus.START_MISS, slaEvent.getEventStatus());
        slas.getSLACalculator().clear();

        JPAService jpaService = Services.get().get(JPAService.class);
        WorkflowJobBean wfBean = jpaService.execute(new WorkflowJobGetJPAExecutor(jobId));
        // set job status to succeeded, so rerun doesn't fail
        wfBean.setStatus(WorkflowJob.Status.SUCCEEDED);
        jpaService.execute(new WorkflowJobUpdateJPAExecutor(wfBean));

        // change conf for rerun
        cal.setTime(new Date());
        cal.add(Calendar.MINUTE, -20); // for start_miss
        nominalTime = DateUtils.formatDateOozieTZ(cal.getTime());

        conf.set("nominal_time", nominalTime);
        nominal = cal.getTime();
        cal.add(Calendar.MINUTE, 10); // as per the sla xml
        expectedStart = DateUtils.formatDateOozieTZ(cal.getTime());
        cal.setTime(nominal);
        cal.add(Calendar.MINUTE, 30); // as per the sla xml
        expectedEnd = DateUtils.formatDateOozieTZ(cal.getTime());

        ReRunXCommand rerun = new ReRunXCommand(jobId, conf);
        rerun.call();
        slaEvent = slas.getSLACalculator().get(jobId);
        // assert for new conf
        assertNotNull(slaEvent);
        assertEquals(jobId, slaEvent.getId());
        assertEquals("test-wf-job-sla", slaEvent.getAppName());
        assertEquals(AppType.WORKFLOW_JOB, slaEvent.getAppType());

        // assert for new conf
        assertEquals(nominalTime, DateUtils.formatDateOozieTZ(slaEvent.getNominalTime()));
        assertEquals(expectedStart, DateUtils.formatDateOozieTZ(slaEvent.getExpectedStart()));
        assertEquals(expectedEnd, DateUtils.formatDateOozieTZ(slaEvent.getExpectedEnd()));

        // assert for values in summary bean to be reset
        SLASummaryBean slaSummary = jpaService.execute(new SLASummaryGetJPAExecutor(jobId));
        assertEquals( 0, slaSummary.getEventProcessed());
        assertEquals(-1, slaSummary.getActualDuration());
        assertNull(slaSummary.getActualStart());
        assertNull(slaSummary.getActualEnd());
        assertEquals("PREP", slaSummary.getJobStatus());
        assertEquals(SLAStatus.NOT_STARTED, slaSummary.getSLAStatus());
        assertNull(slaEvent.getEventStatus());

        ehs.getEventQueue().clear();
        slas.runSLAWorker();
        slaEvent = (SLACalcStatus) ehs.getEventQueue().poll();
        assertEquals(SLAStatus.NOT_STARTED, slaEvent.getSLAStatus());
        assertEquals(EventStatus.START_MISS, slaEvent.getEventStatus());

    }

    /**
     * Test for SLA Events generated through wf action rerun
     *
     * @throws Exception
     */
    @Test
    public void testWorkflowActionSLARerun() throws Exception {
        SLAService slas = services.get(SLAService.class);
        String wfXml = IOUtils.getResourceAsString("wf-action-sla.xml", -1);
        Path appPath = getFsTestCaseDir();
        writeToFile(wfXml, appPath, "workflow.xml");
        Configuration conf = new XConfiguration();
        conf.set(OozieClient.APP_PATH, appPath.toString());
        conf.set(OozieClient.USER_NAME, getTestUser());

        cal.setTime(new Date());
        cal.add(Calendar.MINUTE, -20); // for start_miss
        Date nominal = cal.getTime();
        String nominalTime = DateUtils.formatDateOozieTZ(nominal);
        conf.set("nominal_time", nominalTime);

        // Call SubmitX
        SubmitXCommand sc = new SubmitXCommand(conf);
        String jobId = sc.call();
        String actionId = jobId+"@grouper";

        slas.getSLACalculator().clear();
        JPAService jpaService = Services.get().get(JPAService.class);
        WorkflowJobBean wfBean = jpaService.execute(new WorkflowJobGetJPAExecutor(jobId));
        // set job status to succeeded, so rerun doesn't fail
        wfBean.setStatus(WorkflowJob.Status.SUCCEEDED);
        jpaService.execute(new WorkflowJobUpdateJPAExecutor(wfBean));

        // change conf for rerun
        cal.setTime(new Date());
        nominalTime = DateUtils.formatDateOozieTZ(cal.getTime());
        conf.set("nominal_time", nominalTime);
        nominal = cal.getTime();
        cal.add(Calendar.MINUTE, 10); // as per the sla xml
        String expectedStart = DateUtils.formatDateOozieTZ(cal.getTime());
        cal.setTime(nominal);
        cal.add(Calendar.MINUTE, 30); // as per the sla xml
        String expectedEnd = DateUtils.formatDateOozieTZ(cal.getTime());

        ReRunXCommand rerun = new ReRunXCommand(jobId, conf);
        rerun.call();
        SLACalcStatus slaEvent = slas.getSLACalculator().get(actionId);
        assertNotNull(slaEvent);
        // assert for action configs
        assertEquals(actionId, slaEvent.getId());
        assertEquals("test-wf-action-sla", slaEvent.getAppName());
        assertEquals(AppType.WORKFLOW_ACTION, slaEvent.getAppType());
        // assert for new conf
        assertEquals(nominalTime, DateUtils.formatDateOozieTZ(slaEvent.getNominalTime()));
        assertEquals(expectedStart, DateUtils.formatDateOozieTZ(slaEvent.getExpectedStart()));
        assertEquals(expectedEnd, DateUtils.formatDateOozieTZ(slaEvent.getExpectedEnd()));

    }

    /**
     * Test coord rerun with no SLA config works as before
     *
     * @throws Exception
     */
    @Test
    public void testCoordRerunNoSLA() throws Exception {
        CoordinatorJobBean job = this.addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.FAILED, "coord-rerun-action1.xml", 0);

        try {
            new CoordRerunXCommand(job.getId(), RestConstants.JOB_COORD_RERUN_DATE, "2009-12-15T01:00Z", false, true)
                    .call();
        }
        catch (CommandException ce) {
            if (ce.getErrorCode() == ErrorCode.E0604) {
                fail("Coord Rerun with no SLA should not throw " + ce.getMessage() + " exception");
            }
        }
    }

    @Test
    public void testSLASchema1BackwardCompatibility() throws Exception {
        assertNotNull(ehs);
        SLAService slas = services.get(SLAService.class);
        assertNotNull(slas);

        Path appPath = getFsTestCaseDir();
        writeToFile(SLA_XML_1, appPath, "workflow.xml");
        Configuration conf = new XConfiguration();
        conf.set(OozieClient.APP_PATH, appPath.toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        cal.setTime(new Date());
        cal.add(Calendar.MINUTE, -20); // for start_miss
        Date nominal = cal.getTime();
        String nominalTime = DateUtils.formatDateOozieTZ(nominal);
        conf.set("nominal_time", nominalTime);
        _testWorkflowJobCommands(conf, slas, false);
    }

    /**
     * Test for SLA Events generated through Coordinator Action commands
     *
     * @throws Exception
     */
    @Test
    public void testCoordinatorActionCommands() throws Exception {
        // reduce noise from WF Job events (also default) by setting it to only
        // coord action
        ehs.setAppTypes(new HashSet<String>(Arrays.asList(new String[] { "coordinator_action" })));
        SLAService slas = services.get(SLAService.class);

        String coordXml = IOUtils.getResourceAsString("coord-action-sla.xml", -1);
        Path appPath = getFsTestCaseDir();
        writeToFile(coordXml, appPath, "coordinator.xml");
        Configuration conf = new XConfiguration();
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath.toString());
        String wfXml = IOUtils.getResourceAsString("wf-credentials.xml", -1);
        writeToFile(wfXml, appPath, "workflow.xml");
        conf.set("wfAppPath", appPath.toString());
        conf.set(OozieClient.USER_NAME, getTestUser());

        cal.setTime(new Date());
        cal.add(Calendar.MINUTE, -20); // for start_miss
        Date nominal = cal.getTime();
        String nominalTime = DateUtils.formatDateOozieTZ(nominal);
        conf.set("nominal_time", nominalTime);
        cal.setTime(nominal);
        cal.add(Calendar.MINUTE, 10); // as per the sla xml
        String expectedStart = DateUtils.formatDateOozieTZ(cal.getTime());
        cal.setTime(nominal);
        cal.add(Calendar.MINUTE, 30); // as per the sla xml
        String expectedEnd = DateUtils.formatDateOozieTZ(cal.getTime());
        String appName = "test-coord-sla";

        // testing creation of new sla registration via Submit + Materialize
        // command
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);
        String jobId = sc.call();
        Thread.sleep(500); // waiting for materialize command to run
        final CoordActionGetJPAExecutor getCmd = new CoordActionGetJPAExecutor(jobId + "@1");
        CoordinatorActionBean action = jpa.execute(getCmd);
        String actionId = action.getId();
        SLACalcStatus slaEvent = slas.getSLACalculator().get(actionId);
        assertEquals(actionId, slaEvent.getId());
        assertEquals(appName, slaEvent.getAppName());
        assertEquals(AppType.COORDINATOR_ACTION, slaEvent.getAppType());
        assertEquals(nominalTime, DateUtils.formatDateOozieTZ(slaEvent.getNominalTime()));
        assertEquals(expectedStart, DateUtils.formatDateOozieTZ(slaEvent.getExpectedStart()));
        assertEquals(expectedEnd, DateUtils.formatDateOozieTZ(slaEvent.getExpectedEnd()));
        assertEquals(30 * 60 * 1000, slaEvent.getExpectedDuration());
        assertEquals(alert_events, slaEvent.getAlertEvents());
        waitForEventGeneration(200);
        slas.runSLAWorker();
        slaEvent = skipToSLAEvent();
        assertEquals(SLAStatus.NOT_STARTED, slaEvent.getSLAStatus());
        assertEquals(EventStatus.START_MISS, slaEvent.getEventStatus());

        // test that sla processes the Job Event from Start command
        action.setStatus(CoordinatorAction.Status.SUBMITTED);
        final CoordActionUpdateJPAExecutor writeCmd = new CoordActionUpdateJPAExecutor(action);
        jpa.execute(writeCmd);
        new CoordActionStartXCommand(actionId, getTestUser(), appName, jobId).call();
        slaEvent = slas.getSLACalculator().get(actionId);
        slaEvent.setEventProcessed(0); //resetting for testing sla event
        ehs.new EventWorker().run();
        slaEvent = skipToSLAEvent();
        assertEquals(actionId, slaEvent.getId());
        assertNotNull(slaEvent.getActualStart());
        assertEquals(SLAStatus.IN_PROCESS, slaEvent.getSLAStatus());
        assertEquals(WorkflowJob.Status.RUNNING.name(), slaEvent.getJobStatus());

        // test that sla processes the Job Event from Kill command
        ehs.getEventQueue().clear();
        new CoordKillXCommand(jobId).call();
        waitForEventGeneration(200);
        ehs.new EventWorker().run();
        waitForEventGeneration(300);
        slaEvent = (SLACalcStatus) ehs.getEventQueue().poll();
        assertEquals(EventStatus.DURATION_MISS, slaEvent.getEventStatus());
        slaEvent = skipToSLAEvent();
        assertEquals(actionId, slaEvent.getId());
        assertNotNull(slaEvent.getActualEnd());
        assertEquals(EventStatus.END_MISS, slaEvent.getEventStatus());
        assertEquals(SLAStatus.MISS, slaEvent.getSLAStatus());
        assertEquals(WorkflowJob.Status.KILLED.name(), slaEvent.getJobStatus());
    }

    /**
     * Test Coord action KILLED from WAITING generates corresponding events Job
     * - FAILURE and SLA - END_MISS
     *
     * @throws Exception
     */
    public void testFailureAndMissEventsOnKill() throws Exception {
        assertEquals(0, ehs.getEventQueue().size());
        // CASE 1: Coord Job status - RUNNING (similar to RunningWithError,Paused and PausedWithError for
        // this test's purpose)
        CoordinatorJobBean job = this.addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.WAITING,
                "coord-action-sla1.xml", 0);
        // reset dummy externalId set by above test method
        action.setExternalId(null);
        jpa.execute(new CoordActionUpdateJPAExecutor(action));
        services.get(SLAService.class).addRegistrationEvent(
                TestSLAService._createSLARegistration(action.getId(), AppType.COORDINATOR_ACTION));

        new CoordKillXCommand(job.getId()).call();
        assertEquals(1, ehs.getEventQueue().size());

        CoordinatorActionEvent jobEvent = (CoordinatorActionEvent) ehs.getEventQueue().peek();
        assertEquals(AppType.COORDINATOR_ACTION, jobEvent.getAppType());
        assertEquals(JobEvent.EventStatus.FAILURE, jobEvent.getEventStatus());
        assertEquals(action.getId(), jobEvent.getId());

        ehs.new EventWorker().run();
        SLACalcStatus slaEvent = (SLACalcStatus) ehs.getEventQueue().poll();
        assertEquals(EventStatus.END_MISS, slaEvent.getEventStatus());
        assertEquals(SLAStatus.MISS, slaEvent.getSLAStatus());
        assertEquals(CoordinatorAction.Status.KILLED.name(), slaEvent.getJobStatus());
        assertEquals(action.getId(), slaEvent.getId());
        assertNotNull(slaEvent.getActualEnd());

        // CASE 2: Coord Job status - PAUSED - Should not create event via CoordKill
        // but via CoordActionUpdate
        assertEquals(0, ehs.getEventQueue().size());
        job = this.addRecordToCoordJobTable(CoordinatorJob.Status.PAUSED, false, false);
        action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.RUNNING, "coord-action-sla1.xml",
                0);
        services.get(SLAService.class).addRegistrationEvent(
                TestSLAService._createSLARegistration(action.getId(), AppType.COORDINATOR_ACTION));

        new CoordKillXCommand(job.getId()).call();
        assertEquals(0, ehs.getEventQueue().size());

        WorkflowJobBean wf = new WorkflowJobBean();
        wf.setId(action.getExternalId());
        wf.setStatus(WorkflowJob.Status.KILLED);
        jpa.execute(new WorkflowJobInsertJPAExecutor(wf));
        new CoordActionUpdateXCommand(wf).call();
        assertEquals(1, ehs.getEventQueue().size());

        jobEvent = (CoordinatorActionEvent) ehs.getEventQueue().peek();
        assertEquals(AppType.COORDINATOR_ACTION, jobEvent.getAppType());
        assertEquals(JobEvent.EventStatus.FAILURE, jobEvent.getEventStatus());
        assertEquals(action.getId(), jobEvent.getId());

        ehs.new EventWorker().run();
        slaEvent = (SLACalcStatus) ehs.getEventQueue().poll();
        assertEquals(EventStatus.END_MISS, slaEvent.getEventStatus());
        assertEquals(SLAStatus.MISS, slaEvent.getSLAStatus());
        assertEquals(CoordinatorAction.Status.KILLED.name(), slaEvent.getJobStatus());
        assertEquals(action.getId(), slaEvent.getId());
        assertNotNull(slaEvent.getActualEnd());

    }

    private void waitForEventGeneration(int wait) {
        waitFor(wait, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return ehs.getEventQueue().peek() != null;
            }
        });
    }

    private SLACalcStatus skipToSLAEvent() {
        Event someEvent;
        do {
            someEvent = ehs.getEventQueue().poll();
        } while (!(someEvent instanceof SLACalcStatus));
        return (SLACalcStatus) someEvent;
    }

    private void _testWorkflowJobCommands(Configuration conf, SLAService slas, boolean isNew)
            throws Exception {
        cal.setTime(new Date());
        cal.add(Calendar.MINUTE, -20); // for start_miss
        Date nominal = cal.getTime();
        String nominalTime = DateUtils.formatDateOozieTZ(nominal);
        conf.set("nominal_time", nominalTime);
        cal.setTime(nominal);
        cal.add(Calendar.MINUTE, 10); // as per the sla xml
        String expectedStart = DateUtils.formatDateOozieTZ(cal.getTime());
        cal.setTime(nominal);
        cal.add(Calendar.MINUTE, 30); // as per the sla xml
        String expectedEnd = DateUtils.formatDateOozieTZ(cal.getTime());

        // testing creation of new sla registration via Submit command
        SubmitXCommand sc = new SubmitXCommand(conf);
        String jobId = sc.call();
        SLACalcStatus slaEvent = slas.getSLACalculator().get(jobId);
        assertEquals(jobId, slaEvent.getId());
        assertEquals("test-wf-job-sla", slaEvent.getAppName());
        assertEquals(AppType.WORKFLOW_JOB, slaEvent.getAppType());
        assertEquals(nominalTime, DateUtils.formatDateOozieTZ(slaEvent.getNominalTime()));
        assertEquals(expectedStart, DateUtils.formatDateOozieTZ(slaEvent.getExpectedStart()));
        assertEquals(expectedEnd, DateUtils.formatDateOozieTZ(slaEvent.getExpectedEnd()));
        if (isNew) {
            assertEquals(30 * 60 * 1000, slaEvent.getExpectedDuration());
            assertEquals(alert_events, slaEvent.getAlertEvents());
        }
        slas.runSLAWorker();
        slaEvent = (SLACalcStatus) ehs.getEventQueue().poll();
        assertEquals(SLAStatus.NOT_STARTED, slaEvent.getSLAStatus());
        assertEquals(EventStatus.START_MISS, slaEvent.getEventStatus());

        // test that sla processes the Job Event from Start command
        new StartXCommand(jobId).call();
        slaEvent = slas.getSLACalculator().get(jobId);
        slaEvent.setEventProcessed(0); //resetting to receive sla events
        waitForEventGeneration(200);
        ehs.new EventWorker().run();
        waitForEventGeneration(300);
        slaEvent = (SLACalcStatus) ehs.getEventQueue().poll();
        assertEquals(jobId, slaEvent.getId());
        assertNotNull(slaEvent.getActualStart());
        assertEquals(SLAStatus.IN_PROCESS, slaEvent.getSLAStatus());
        assertEquals(WorkflowJob.Status.RUNNING.name(), slaEvent.getJobStatus());
        ehs.getEventQueue().clear();

        // test that sla processes the Job Event from Kill command
        // job must already be killed (test xml) so set some status for KillX to work
        JPAService jpaService = Services.get().get(JPAService.class);
        WorkflowJobBean wf = jpaService.execute(new WorkflowJobGetJPAExecutor(jobId));
        wf.setStatus(WorkflowJob.Status.FAILED);
        jpaService.execute(new WorkflowJobUpdateJPAExecutor(wf));
        new KillXCommand(jobId).call();
        waitForEventGeneration(1000); //wait for wf-action kill event to generate
        Thread.sleep(200); //wait for wf job kill event to generate
        ehs.new EventWorker().run();
        waitForEventGeneration(2000); // time for listeners to run
        ehs.getEventQueue().poll(); // ignore duration event
        slaEvent = (SLACalcStatus) ehs.getEventQueue().poll();
        assertEquals(jobId, slaEvent.getId());
        assertNotNull(slaEvent.getActualEnd());
        assertEquals(EventStatus.END_MISS, slaEvent.getEventStatus());
        assertEquals(SLAStatus.MISS, slaEvent.getSLAStatus());
        assertEquals(WorkflowJob.Status.FAILED.name(), slaEvent.getJobStatus());
    }

}
