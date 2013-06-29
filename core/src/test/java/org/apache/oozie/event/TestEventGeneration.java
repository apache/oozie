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
package org.apache.oozie.event;

import java.io.File;
import java.io.FileWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.AppType;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.DagEngine;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.control.ControlNodeActionExecutor;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.event.Event;
import org.apache.oozie.client.event.JobEvent;
import org.apache.oozie.client.event.JobEvent.EventStatus;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.coord.CoordActionCheckXCommand;
import org.apache.oozie.command.coord.CoordActionInputCheckXCommand;
import org.apache.oozie.command.coord.CoordMaterializeTransitionXCommand;
import org.apache.oozie.command.coord.CoordRerunXCommand;
import org.apache.oozie.command.coord.CoordResumeXCommand;
import org.apache.oozie.command.coord.CoordinatorXCommand;
import org.apache.oozie.command.wf.ActionCheckXCommand;
import org.apache.oozie.command.wf.ActionKillXCommand;
import org.apache.oozie.command.wf.ActionStartXCommand;
import org.apache.oozie.command.wf.ActionXCommand;
import org.apache.oozie.command.wf.KillXCommand;
import org.apache.oozie.command.wf.ResumeXCommand;
import org.apache.oozie.command.wf.SignalXCommand;
import org.apache.oozie.command.wf.StartXCommand;
import org.apache.oozie.command.wf.SuspendXCommand;
import org.apache.oozie.command.wf.WorkflowXCommand;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionInsertJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobUpdateJPAExecutor;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.lite.ActionNodeDef;
import org.apache.oozie.workflow.lite.EndNodeDef;
import org.apache.oozie.workflow.lite.LiteWorkflowApp;
import org.apache.oozie.workflow.lite.LiteWorkflowInstance;
import org.apache.oozie.workflow.lite.StartNodeDef;
import org.apache.oozie.workflow.lite.TestLiteWorkflowLib.TestActionNodeHandler;
import org.apache.oozie.workflow.lite.TestLiteWorkflowLib.TestControlNodeHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Testcase to test that events are correctly generated from corresponding
 * Commands and inserted into events queue
 */
public class TestEventGeneration extends XDataTestCase {

    EventQueue queue;
    Services services;
    EventHandlerService ehs;
    JPAService jpaService;

    @Override
    @Before
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_EXT_CLASSES, "org.apache.oozie.service.EventHandlerService");
        services.init();
        ehs = services.get(EventHandlerService.class);
        queue = ehs.getEventQueue();
        jpaService = services.get(JPAService.class);
    }

    @Override
    @After
    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    @Test
    public void testWorkflowJobEvent() throws Exception {
        assertEquals(0, queue.size());
        WorkflowJobBean job = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);

        // Starting job
        new StartXCommand(job.getId()).call();
        WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(job.getId());
        job = jpaService.execute(wfJobGetCmd);
        assertEquals(WorkflowJob.Status.RUNNING, job.getStatus());
        assertEquals(1, queue.size());
        JobEvent event = (JobEvent) queue.poll();
        assertNotNull(event);
        assertEquals(EventStatus.STARTED, event.getEventStatus());
        assertEquals(AppType.WORKFLOW_JOB, event.getAppType());
        assertEquals(job.getId(), event.getId());
        assertEquals(job.getUser(), event.getUser());
        assertEquals(job.getAppName(), event.getAppName());
        assertEquals(job.getStartTime(), event.getStartTime());
        assertEquals(0, queue.size());

        // Suspending job
        new SuspendXCommand(job.getId()).call();
        job = jpaService.execute(wfJobGetCmd);
        assertEquals(WorkflowJob.Status.SUSPENDED, job.getStatus());
        assertEquals(1, queue.size());
        event = (JobEvent) queue.poll();
        assertNotNull(event);
        assertEquals(EventStatus.SUSPEND, event.getEventStatus());
        assertEquals(AppType.WORKFLOW_JOB, event.getAppType());
        assertEquals(job.getId(), event.getId());
        assertEquals(job.getUser(), event.getUser());
        assertEquals(job.getAppName(), event.getAppName());
        assertEquals(0, queue.size());

        // Resuming job
        new ResumeXCommand(job.getId()).call();
        job = jpaService.execute(wfJobGetCmd);
        assertEquals(WorkflowJob.Status.RUNNING, job.getStatus());
        assertEquals(1, queue.size());
        event = (JobEvent) queue.poll();
        assertNotNull(event);
        assertEquals(AppType.WORKFLOW_JOB, event.getAppType());
        assertEquals(job.getId(), event.getId());
        assertEquals(job.getUser(), event.getUser());
        assertEquals(job.getAppName(), event.getAppName());
        assertEquals(job.getStartTime(), event.getStartTime());
        assertEquals(0, queue.size());

        // Killing job
        new KillXCommand(job.getId()).call();
        job = jpaService.execute(wfJobGetCmd);
        assertEquals(WorkflowJob.Status.KILLED, job.getStatus());
        assertEquals(1, queue.size());
        event = (JobEvent) queue.poll();
        assertNotNull(event);
        assertEquals(EventStatus.FAILURE, event.getEventStatus());
        assertEquals(AppType.WORKFLOW_JOB, event.getAppType());
        assertEquals(job.getId(), event.getId());
        assertEquals(job.getUser(), event.getUser());
        assertEquals(job.getAppName(), event.getAppName());
        assertEquals(job.getStartTime(), event.getStartTime());
        assertEquals(job.getEndTime(), event.getEndTime());
        assertEquals(0, queue.size());

        // Successful job (testing SignalX)
        job = _createWorkflowJob();
        LiteWorkflowInstance wfInstance = (LiteWorkflowInstance) job.getWorkflowInstance();
        wfInstance.start();
        job.setWfInstance(wfInstance);
        jpaService.execute(new WorkflowJobUpdateJPAExecutor(job));
        WorkflowActionBean wfAction = jpaService.execute(new WorkflowActionGetJPAExecutor(job.getId() + "@one"));
        new SignalXCommand(job.getId(), wfAction.getId()).call();
        job = jpaService.execute(new WorkflowJobGetJPAExecutor(job.getId()));
        assertEquals(WorkflowJob.Status.SUCCEEDED, job.getStatus());
        assertEquals(1, queue.size());
        event = (JobEvent) queue.poll();
        assertNotNull(event);
        assertEquals(AppType.WORKFLOW_JOB, event.getAppType());
        assertEquals(job.getId(), event.getId());
        assertEquals(job.getUser(), event.getUser());
        assertEquals(job.getAppName(), event.getAppName());
        assertEquals(job.getStartTime(), event.getStartTime());
        assertEquals(job.getEndTime(), event.getEndTime());

    }

    @Test
    public void testCoordinatorActionEvent() throws Exception {
        // avoid noise from other apptype events by setting it to only
        // coord action
        ehs.setAppTypes(new HashSet<String>(Arrays.asList("coordinator_action")));
        assertEquals(queue.size(), 0);
        Date startTime = DateUtils.parseDateOozieTZ("2013-01-01T10:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2013-01-01T10:01Z");
        CoordinatorJobBean coord = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, false,
                false, 0);
        modifyCoordForRunning(coord);

        // Action WAITING on materialization
        new CoordMaterializeTransitionXCommand(coord.getId(), 3600).call();
        final CoordActionGetJPAExecutor coordGetCmd = new CoordActionGetJPAExecutor(coord.getId() + "@1");
        CoordinatorActionBean action = jpaService.execute(coordGetCmd);
        assertEquals(CoordinatorAction.Status.WAITING, action.getStatus());
        assertEquals(1, queue.size());
        JobEvent event = (JobEvent) queue.poll();
        assertNotNull(event);
        assertEquals(EventStatus.WAITING, event.getEventStatus());
        assertEquals(AppType.COORDINATOR_ACTION, event.getAppType());
        assertEquals(action.getId(), event.getId());
        assertEquals(action.getJobId(), event.getParentId());
        assertEquals(action.getNominalTime(), ((CoordinatorActionEvent) event).getNominalTime());
        assertNull(event.getStartTime());
        assertEquals(coord.getUser(), event.getUser());
        assertEquals(coord.getAppName(), event.getAppName());
        assertEquals(0, queue.size());

        // Make Action ready
        new CoordActionInputCheckXCommand(action.getId(), coord.getId()).call();
        action = jpaService.execute(coordGetCmd);
        assertEquals(CoordinatorAction.Status.READY, action.getStatus());

        waitFor(4 * 100, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return jpaService.execute(coordGetCmd).getStatus() == CoordinatorAction.Status.RUNNING;
            }
        });

        action = jpaService.execute(coordGetCmd);
        event = (JobEvent) queue.poll();
        assertEquals(EventStatus.STARTED, event.getEventStatus());
        assertEquals(AppType.COORDINATOR_ACTION, event.getAppType());
        assertEquals(action.getId(), event.getId());
        assertEquals(action.getJobId(), event.getParentId());
        assertEquals(action.getNominalTime(), ((CoordinatorActionEvent) event).getNominalTime());
        WorkflowJobBean wjb = jpaService.execute(new WorkflowJobGetJPAExecutor(action.getExternalId()));
        assertEquals(wjb.getStartTime(), event.getStartTime());
        assertEquals(coord.getUser(), event.getUser());
        assertEquals(coord.getAppName(), event.getAppName());

        // Action Success
        action = jpaService.execute(coordGetCmd);
        WorkflowJobBean wfJob = jpaService.execute(new WorkflowJobGetJPAExecutor(action.getExternalId()));
        wfJob.setStatus(WorkflowJob.Status.SUCCEEDED);
        jpaService.execute(new WorkflowJobUpdateJPAExecutor(wfJob));
        action.setStatus(CoordinatorAction.Status.RUNNING);
        jpaService.execute(new CoordActionUpdateJPAExecutor(action));
        new CoordActionCheckXCommand(action.getId(), 0).call();
        action = jpaService.execute(coordGetCmd);
        assertEquals(CoordinatorAction.Status.SUCCEEDED, action.getStatus());
        List<Event> list =  queue.pollBatch();
        event = (JobEvent)list.get(list.size()-1);
        assertEquals(EventStatus.SUCCESS, event.getEventStatus());
        assertEquals(AppType.COORDINATOR_ACTION, event.getAppType());
        assertEquals(action.getId(), event.getId());
        assertEquals(action.getJobId(), event.getParentId());
        assertEquals(action.getNominalTime(), ((CoordinatorActionEvent) event).getNominalTime());
        assertEquals(wfJob.getStartTime(), event.getStartTime());
        assertEquals(coord.getUser(), event.getUser());
        assertEquals(coord.getAppName(), event.getAppName());

        // Action Failure
        action.setStatus(CoordinatorAction.Status.RUNNING);
        jpaService.execute(new CoordActionUpdateJPAExecutor(action));
        wfJob.setStatus(WorkflowJob.Status.FAILED);
        jpaService.execute(new WorkflowJobUpdateJPAExecutor(wfJob));
        new CoordActionCheckXCommand(action.getId(), 0).call();
        action = jpaService.execute(coordGetCmd);
        assertEquals(CoordinatorAction.Status.FAILED, action.getStatus());
        event = (JobEvent) queue.poll();
        assertEquals(EventStatus.FAILURE, event.getEventStatus());
        assertEquals(AppType.COORDINATOR_ACTION, event.getAppType());
        assertEquals(action.getId(), event.getId());
        assertEquals(action.getJobId(), event.getParentId());
        assertEquals(action.getNominalTime(), ((CoordinatorActionEvent) event).getNominalTime());
        assertEquals(wfJob.getStartTime(), event.getStartTime());
        assertEquals(coord.getUser(), event.getUser());
        assertEquals(coord.getAppName(), event.getAppName());

        // Action start on Coord Resume
        coord.setStatus(CoordinatorJobBean.Status.SUSPENDED);
        jpaService.execute(new CoordJobUpdateJPAExecutor(coord));
        action.setStatus(CoordinatorAction.Status.SUSPENDED);
        jpaService.execute(new CoordActionUpdateJPAExecutor(action));
        wfJob.setStatus(WorkflowJob.Status.SUSPENDED);
        WorkflowInstance wfInstance = wfJob.getWorkflowInstance();
        ((LiteWorkflowInstance) wfInstance).setStatus(WorkflowInstance.Status.SUSPENDED);
        wfJob.setWorkflowInstance(wfInstance);
        jpaService.execute(new WorkflowJobUpdateJPAExecutor(wfJob));
        new CoordResumeXCommand(coord.getId()).call();
        Thread.sleep(5000);
        CoordinatorActionEvent cevent = (CoordinatorActionEvent) queue.poll();
        assertEquals(EventStatus.STARTED, cevent.getEventStatus());
        assertEquals(AppType.COORDINATOR_ACTION, cevent.getAppType());
        assertEquals(action.getId(), cevent.getId());
        assertEquals(action.getJobId(), cevent.getParentId());
        assertEquals(action.getNominalTime(), cevent.getNominalTime());
        assertEquals(wfJob.getStartTime(), cevent.getStartTime());

        // Action going to WAITING on Coord Rerun
        action.setStatus(CoordinatorAction.Status.SUCCEEDED);
        jpaService.execute(new CoordActionUpdateJPAExecutor(action));
        queue.clear();
        new CoordRerunXCommand(coord.getId(), RestConstants.JOB_COORD_RERUN_ACTION, "1", false, true)
                .call();
        waitFor(3 * 100, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return jpaService.execute(coordGetCmd).getStatus() == CoordinatorAction.Status.WAITING;
            }
        });
        cevent = (CoordinatorActionEvent) queue.poll();
        assertEquals(EventStatus.WAITING, cevent.getEventStatus());
        assertEquals(AppType.COORDINATOR_ACTION, cevent.getAppType());
        assertEquals(action.getId(), cevent.getId());
        assertEquals(action.getJobId(), cevent.getParentId());
        assertEquals(action.getNominalTime(), cevent.getNominalTime());
        assertEquals(wfJob.getStartTime(), event.getStartTime());
        assertNotNull(cevent.getMissingDeps());

    }

    @Test
    public void testWorkflowActionEvent() throws Exception {
        assertEquals(queue.size(), 0);
        // avoid noise from other apptype events by setting it to only
        // workflow action
        ehs.setAppTypes(new HashSet<String>(Arrays.asList("workflow_action")));
        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action = this.addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.PREP, true);
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(action.getId());

        // Starting job
        new ActionStartXCommand(action.getId(), "map-reduce").call();
        action = jpaService.execute(wfActionGetCmd);
        assertEquals(WorkflowAction.Status.RUNNING, action.getStatus());
        assertEquals(1, queue.size());
        WorkflowActionEvent event = (WorkflowActionEvent) queue.poll();
        assertNotNull(event);
        assertEquals(EventStatus.STARTED, event.getEventStatus());
        assertEquals(AppType.WORKFLOW_ACTION, event.getAppType());
        assertEquals(action.getId(), event.getId());
        assertEquals(job.getUser(), event.getUser());
        assertEquals(action.getName(), event.getAppName());
        assertEquals(action.getStartTime(), event.getStartTime());
        assertEquals(0, queue.size());

        // Suspending job
        ActionExecutor.Context context = new ActionXCommand.ActionExecutorContext(job, action, false, false);
        ActionExecutor executor = Services.get().get(ActionService.class).getExecutor(action.getType());
        ActionCheckXCommandForTest dac = new ActionCheckXCommandForTest(context, executor, action.getId());
        dac.execute();
        action = dac.getAction();
        assertEquals(WorkflowAction.Status.START_MANUAL, action.getStatus());
        assertEquals(1, queue.size());
        event = (WorkflowActionEvent) queue.poll();
        assertNotNull(event);
        assertEquals(EventStatus.SUSPEND, event.getEventStatus());
        assertEquals(AppType.WORKFLOW_ACTION, event.getAppType());
        assertEquals(action.getId(), event.getId());
        assertEquals(job.getUser(), event.getUser());
        assertEquals(action.getName(), event.getAppName());
        assertEquals(0, queue.size());

        // Killing job
        action.setStatus(WorkflowAction.Status.KILLED);
        action.setPendingOnly();
        action.setEndTime(null); //its already set by XTestCase add action record method above
        jpaService.execute(new WorkflowActionUpdateJPAExecutor(action));
        new ActionKillXCommand(action.getId()).call();
        action = jpaService.execute(wfActionGetCmd);
        assertEquals(WorkflowAction.Status.KILLED, action.getStatus());
        assertEquals(1, queue.size());
        event = (WorkflowActionEvent) queue.poll();
        assertNotNull(event);
        assertEquals(EventStatus.FAILURE, event.getEventStatus());
        assertEquals(AppType.WORKFLOW_ACTION, event.getAppType());
        assertEquals(action.getId(), event.getId());
        assertEquals(job.getUser(), event.getUser());
        assertEquals(action.getName(), event.getAppName());
        assertEquals(action.getStartTime(), event.getStartTime());
        assertNotNull(action.getEndTime());
        assertNotNull(event.getEndTime());
        assertEquals(action.getEndTime(), event.getEndTime());
        assertEquals(0, queue.size());

    }

    @Test
    public void testWorkflowJobEventError() throws Exception {
        final WorkflowJobBean job = addRecordToWfJobTable(WorkflowJob.Status.FAILED, WorkflowInstance.Status.FAILED);
        // create event with error code and message
        WorkflowXCommand<Void> myCmd = new KillXCommand(job.getId()) {
            @Override
            protected Void execute() {
                WorkflowXCommand.generateEvent(job, "errorCode", "errorMsg");
                return null;
            }
        };
        myCmd.call();
        WorkflowJobEvent event = (WorkflowJobEvent) queue.poll();
        assertNotNull(event);
        assertEquals("errorCode", event.getErrorCode());
        assertEquals("errorMsg", event.getErrorMessage());
        assertEquals(EventStatus.FAILURE, event.getEventStatus());

    }

    @Test
    public void testCoordinatorActionEventDependencies() throws Exception {
        final CoordinatorJobBean coord = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        final CoordinatorActionBean action = addRecordToCoordActionTable(coord.getId(), 1,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);
        WorkflowJobBean wjb = new WorkflowJobBean();
        wjb.setId(action.getExternalId());
        jpaService.execute(new WorkflowJobUpdateJPAExecutor(wjb));

        CoordinatorXCommand<Void> myCmd = new CoordActionCheckXCommand(action.getId(), 0) {
            @Override
            protected Void execute() {
                CoordinatorXCommand.generateEvent(action, coord.getUser(), coord.getAppName(), null);
                return null;
            }
        };

        // CASE 1: Only pull missing deps
        action.setMissingDependencies("pull");
        jpaService.execute(new CoordActionUpdateJPAExecutor(action));
        myCmd.call();
        CoordinatorActionEvent event = (CoordinatorActionEvent) queue.poll();
        assertNotNull(event);
        assertEquals("pull", event.getMissingDeps());

        // CASE 2: Only hcat (push) missing deps
        action.setMissingDependencies(null);
        action.setPushMissingDependencies("push");
        jpaService.execute(new CoordActionUpdateJPAExecutor(action));
        myCmd.call();
        event = (CoordinatorActionEvent) queue.poll();
        assertNotNull(event);
        assertEquals("push", event.getMissingDeps());

        // CASE 3: Both types
        action.setMissingDependencies("pull");
        jpaService.execute(new CoordActionUpdateJPAExecutor(action));
        myCmd.call();
        event = (CoordinatorActionEvent) queue.poll();
        assertNotNull(event);
        assertEquals("pull" + CoordELFunctions.INSTANCE_SEPARATOR + "push", event.getMissingDeps());

    }

    @Test
    public void testForNoDuplicates() throws Exception {
        // test workflow job events
        Reader reader = IOUtils.getResourceAsReader("wf-no-op.xml", -1);
        Writer writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
        IOUtils.copyCharStream(reader, writer);

        final DagEngine engine = new DagEngine(getTestUser());
        Configuration conf = new XConfiguration();
        conf.set(OozieClient.APP_PATH, "file://" + getTestCaseDir() + File.separator + "workflow.xml");
        conf.set(OozieClient.USER_NAME, getTestUser());

        final String jobId1 = engine.submitJob(conf, true);
        final WorkflowJobGetJPAExecutor readCmd = new WorkflowJobGetJPAExecutor(jobId1);

        waitFor(1 * 100, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return jpaService.execute(readCmd).getStatus() == WorkflowJob.Status.SUCCEEDED;
            }
        });
        assertEquals(2, queue.size());
        assertEquals(EventStatus.STARTED, ((JobEvent)queue.poll()).getEventStatus());
        assertEquals(EventStatus.SUCCESS, ((JobEvent)queue.poll()).getEventStatus());
        queue.clear();

        // test coordinator action events (failure case)
        Date startTime = DateUtils.parseDateOozieTZ("2009-02-01T23:59Z");
        Date endTime = DateUtils.parseDateOozieTZ("2009-02-02T23:59Z");
        CoordinatorJobBean coord = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, false,
                false, 0);
        _modifyCoordForFailureAction(coord, "wf-invalid-fork.xml");
        new CoordMaterializeTransitionXCommand(coord.getId(), 3600).call();
        final CoordJobGetJPAExecutor readCmd1 = new CoordJobGetJPAExecutor(coord.getId());
        waitFor(1 * 100, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                CoordinatorJobBean bean = jpaService.execute(readCmd1);
                return bean.getStatus() == CoordinatorJob.Status.SUCCEEDED
                        || bean.getStatus() == CoordinatorJob.Status.KILLED;
            }
        });
        assertEquals(2, queue.size());
        assertEquals(EventStatus.WAITING, ((JobEvent)queue.poll()).getEventStatus());
        assertEquals(EventStatus.FAILURE, ((JobEvent)queue.poll()).getEventStatus());

        // test coordinator action events (failure from ActionStartX)
        ehs.getAppTypes().add("workflow_action");
        coord = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, false, false, 0);
        CoordinatorActionBean action = addRecordToCoordActionTable(coord.getId(), 1, CoordinatorAction.Status.RUNNING,
                "coord-action-sla1.xml", 0);
        WorkflowJobBean wf = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING,
                action.getId());
        action.setExternalId(wf.getId());
        jpaService.execute(new CoordActionUpdateJPAExecutor(action));

        String waId = _createWorkflowAction(wf.getId(), "wf-action");
        new ActionStartXCommand(waId, action.getType()).call();

        final WorkflowJobGetJPAExecutor readCmd2 = new WorkflowJobGetJPAExecutor(jobId1);
        waitFor(1 * 100, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return jpaService.execute(readCmd2).getStatus() == WorkflowJob.Status.KILLED;
            }
        });
        assertEquals(3, queue.size());
        JobEvent wfActionEvent = (JobEvent) queue.poll();
        assertEquals(EventStatus.FAILURE, wfActionEvent.getEventStatus());
        assertEquals(waId, wfActionEvent.getId());
        assertEquals(AppType.WORKFLOW_ACTION, wfActionEvent.getAppType());
        JobEvent wfJobEvent = (JobEvent) queue.poll();
        assertEquals(EventStatus.FAILURE, wfJobEvent.getEventStatus());
        assertEquals(wf.getId(), wfJobEvent.getId());
        assertEquals(AppType.WORKFLOW_JOB, wfJobEvent.getAppType());
        JobEvent coordActionEvent = (JobEvent) queue.poll();
        assertEquals(EventStatus.FAILURE, coordActionEvent.getEventStatus());
        assertEquals(action.getId(), coordActionEvent.getId());
        assertEquals(AppType.COORDINATOR_ACTION, coordActionEvent.getAppType());
    }

    private class ActionCheckXCommandForTest extends ActionCheckXCommand {

        ActionExecutor.Context context;
        ActionExecutor executor;
        WorkflowActionBean action;
        JPAService jpa;

        public ActionCheckXCommandForTest(ActionExecutor.Context context, ActionExecutor executor, String actionId)
                throws JPAExecutorException {
            super(actionId);
            this.context = context;
            this.executor = executor;
            jpa = Services.get().get(JPAService.class);
            this.action = jpa.execute(new WorkflowActionGetJPAExecutor(actionId));
        }

        @Override
        public Void execute() throws CommandException {
            handleNonTransient(context, executor, WorkflowAction.Status.START_MANUAL);
            action = (WorkflowActionBean) ((ActionExecutorContext) context).getAction();
            if (!(executor instanceof ControlNodeActionExecutor) && EventHandlerService.isEnabled()) {
                generateEvent(action, getTestUser());
            }
            return null;
        }

        public WorkflowActionBean getAction() {
            return action;
        }

    }

    private WorkflowJobBean _createWorkflowJob() throws Exception {
        LiteWorkflowApp app = new LiteWorkflowApp("my-app", "<workflow-app/>",
                new StartNodeDef(TestControlNodeHandler.class, "one"))
                .addNode(new ActionNodeDef("one", "<java></java>", TestActionNodeHandler.class, "end", "end"))
                .addNode(new EndNodeDef("end", TestControlNodeHandler.class));
        Configuration conf = new Configuration();
        Path appUri = new Path(getAppPath(), "workflow.xml");
        conf.set(OozieClient.APP_PATH, appUri.toString());
        conf.set(OozieClient.LOG_TOKEN, "testToken");
        conf.set(OozieClient.USER_NAME, getTestUser());
        WorkflowJobBean workflow = createWorkflow(app, conf, WorkflowJob.Status.PREP,
                WorkflowInstance.Status.PREP);
        String executionPath = "/";

        assertNotNull(jpaService);
        WorkflowJobInsertJPAExecutor wfInsertCmd = new WorkflowJobInsertJPAExecutor(workflow);
        jpaService.execute(wfInsertCmd);
        WorkflowActionBean wfAction = addRecordToWfActionTable(workflow.getId(), "one", WorkflowAction.Status.OK,
                executionPath, true);
        wfAction.setPending();
        wfAction.setSignalValue(WorkflowAction.Status.OK.name());
        jpaService.execute(new WorkflowActionUpdateJPAExecutor(wfAction));

        return workflow;
    }

    private void _modifyCoordForFailureAction(CoordinatorJobBean coord, String resourceXml) throws Exception {
        String wfXml = IOUtils.getResourceAsString(resourceXml, -1);
        writeToFile(wfXml, getFsTestCaseDir(), "workflow.xml");
        String coordXml = coord.getJobXml();
        coord.setJobXml(coordXml.replace("hdfs:///tmp/workflows/", getFsTestCaseDir() + "/workflow.xml"));
        jpaService.execute(new CoordJobUpdateJPAExecutor(coord));
    }

    private String _createWorkflowAction(String wfId, String actionName) throws JPAExecutorException {
        WorkflowActionBean action = new WorkflowActionBean();
        action.setName(actionName);
        action.setId(Services.get().get(UUIDService.class).generateChildId(wfId, actionName));
        action.setJobId(wfId);
        action.setType("java");
        action.setTransition("transition");
        action.setStatus(WorkflowAction.Status.PREP);
        action.setStartTime(new Date());
        action.setEndTime(new Date());
        action.setLastCheckTime(new Date());
        action.setCred("null");
        action.setPendingOnly();

        String actionXml = "<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>" + "<main-class>" + "${dummy}" + "</java>";
        action.setConf(actionXml);
        jpaService.execute(new WorkflowActionInsertJPAExecutor(action));
        return action.getId();
    }

}
