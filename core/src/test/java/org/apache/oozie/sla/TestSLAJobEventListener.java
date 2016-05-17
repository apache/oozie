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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.AppType;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.event.SLAEvent.EventStatus;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.event.CoordinatorActionEvent;
import org.apache.oozie.event.WorkflowActionEvent;
import org.apache.oozie.event.WorkflowJobEvent;
import org.apache.oozie.event.listener.JobEventListener;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor;
import org.apache.oozie.executor.jpa.SLASummaryQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.executor.jpa.SLASummaryQueryExecutor.SLASummaryQuery;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor.WorkflowJobQuery;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.listener.SLAJobEventListener;
import org.apache.oozie.sla.service.SLAService;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.DateUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSLAJobEventListener extends XTestCase {
    Services services;
    static StringBuilder output = new StringBuilder();

    @Override
    @Before
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_EXT_CLASSES, "org.apache.oozie.service.EventHandlerService,"
                + "org.apache.oozie.sla.service.SLAService");
        conf.setClass(EventHandlerService.CONF_LISTENERS, SLAJobEventListener.class, JobEventListener.class);
        services.init();
    }

    @Override
    @After
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    @Test
    public void testListenerConfigured() throws Exception {
        EventHandlerService ehs = services.get(EventHandlerService.class);
        assertNotNull(ehs);
        assertTrue(SLAService.isEnabled());
        assertTrue(ehs.listEventListeners().contains(SLAJobEventListener.class.getCanonicalName()));
    }

    @Test
    public void testOnJobEvent() throws Exception {
        SLAService slas = services.get(SLAService.class);
        SLAJobEventListener listener = new SLAJobEventListener();
        listener.init(services.getConf());
        // add dummy registration events to the SLAService map
        SLARegistrationBean job = _createSLARegBean("wf1-W", AppType.WORKFLOW_JOB);
        job.setExpectedStart(DateUtils.parseDateUTC("2012-07-22T00:00Z"));
        job.setExpectedEnd(DateUtils.parseDateUTC("2012-07-23T00:00Z"));
        slas.addRegistrationEvent(job);
        assertEquals(1, slas.getSLACalculator().size());
        Date actualStart = DateUtils.parseDateUTC("2012-07-22T01:00Z");

        createWorkflow("wf1-W", actualStart);
        WorkflowJobEvent wfe = new WorkflowJobEvent("wf1-W", "caId1", WorkflowJob.Status.RUNNING, "user1",
                "wf-app-name1", actualStart, null);
        listener.onWorkflowJobEvent(wfe);
        SLACalcStatus serviceObj = slas.getSLACalculator().get("wf1-W");

        // job will be checked against DB.. since it's old job. all event will get evaluted and job will move to history set.
        // check that start sla has been calculated
        assertEquals(EventStatus.END_MISS, serviceObj.getEventStatus());
        assertEquals(7, serviceObj.getEventProcessed()); //Job switching to running is only partially
        assertEquals(0, slas.getSLACalculator().size());


        createWorkflowAction("wfId1-W@wa1", "wf1-W");
        job = _createSLARegBean("wfId1-W@wa1", AppType.WORKFLOW_ACTION);
        job.setExpectedEnd(DateUtils.parseDateUTC("2012-07-22T01:00Z"));

        slas.addRegistrationEvent(job);
        assertEquals(1, slas.getSLACalculator().size());
        job.setExpectedStart(DateUtils.parseDateUTC("2012-07-22T00:00Z"));
        WorkflowActionEvent wae = new WorkflowActionEvent("wfId1-W@wa1", "wf1-W", WorkflowAction.Status.RUNNING, "user1",
                "wf-app-name1", actualStart, null);
        listener.onWorkflowActionEvent(wae);
        serviceObj = slas.getSLACalculator().get("wfId1-W@wa1");
        // check that start sla has been calculated
        assertEquals(EventStatus.END_MISS, serviceObj.getEventStatus());
        createCoord("cj1-C");

        CoordinatorActionBean coordAction= createCoordAction("cj1-C@ca1", "cj1-C");
        job = _createSLARegBean("cj1-C@ca1", AppType.COORDINATOR_ACTION);
        job.setExpectedEnd(DateUtils.parseDateUTC("2012-07-22T01:00Z"));
        Date actualEnd = DateUtils.parseDateUTC("2012-07-22T02:00Z");
        slas.addRegistrationEvent(job);
        assertEquals(1, slas.getSLACalculator().size());
        CoordinatorActionEvent cae = new CoordinatorActionEvent("cj1-C@ca1", "cj1-C", CoordinatorAction.Status.RUNNING, "user1",
                "coord-app-name1", null, actualEnd, null);
        listener.onCoordinatorActionEvent(cae);
        coordAction.setStatus(CoordinatorAction.Status.KILLED);
        coordAction.setLastModifiedTime(new Date());
        CoordActionQueryExecutor.getInstance().executeUpdate(CoordActionQuery.UPDATE_COORD_ACTION_STATUS_PENDING_TIME, coordAction);

        cae = new CoordinatorActionEvent("cj1-C@ca1", "cj1-C", CoordinatorAction.Status.KILLED, "user1",
                "coord-app-name1", null, actualEnd, null);
        listener.onCoordinatorActionEvent(cae);
        SLASummaryBean summary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, "cj1-C@ca1");
        // check that all events are processed
        assertEquals(8, summary.getEventProcessed());
        assertEquals(EventStatus.END_MISS, summary.getEventStatus());
        //all jobs are processed
        assertEquals(0, slas.getSLACalculator().size());

        job = _createSLARegBean("wf2-W", AppType.WORKFLOW_JOB);
        job.setExpectedStart(new Date(System.currentTimeMillis() - 2 * 3600 * 1000)); //2 hour before
        job.setExpectedEnd(new Date(System.currentTimeMillis() + 1 * 3600 * 1000)); //1 hours after
        slas.addRegistrationEvent(job);
        assertEquals(1, slas.getSLACalculator().size());

        createWorkflow("wf2-W", new Date());
        wfe = new WorkflowJobEvent("wf2-W", "caId2", WorkflowJob.Status.RUNNING, "user1",
                "wf-app-name1", null, null);
        listener.onWorkflowJobEvent(wfe);
        serviceObj = slas.getSLACalculator().get("wf2-W");

        assertEquals(EventStatus.START_MISS, serviceObj.getEventStatus());
        assertEquals(3, serviceObj.getEventProcessed()); //Only duration and start are processed. Duration = -1
        assertEquals(1, slas.getSLACalculator().size());

    }

    private SLARegistrationBean _createSLARegBean(String jobId, AppType appType) {
        SLARegistrationBean reg = new SLARegistrationBean();
        reg.setId(jobId);
        reg.setAppType(appType);
        return reg;
    }
    private WorkflowJobBean createWorkflow(String id, Date actualStart) throws Exception {
        List<JsonBean> insertList = new ArrayList<JsonBean>();
        WorkflowJobBean workflow = new WorkflowJobBean();
        workflow.setId(id);
        workflow.setStatusStr("PREP");
        workflow.setStartTime(actualStart);
        workflow.setSlaXml("<sla></sla>");
        insertList.add(workflow);
        BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(insertList, null, null);
        return workflow;
    }

    private WorkflowActionBean createWorkflowAction(String id, String parentId) throws Exception {
        List<JsonBean> insertList = new ArrayList<JsonBean>();
        WorkflowActionBean action = new WorkflowActionBean();
        action.setId(id);
        action.setJobId(parentId);
        insertList.add(action);
        BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(insertList, null, null);
        return action;
    }

    private CoordinatorActionBean createCoordAction(String id, String parentId) throws Exception {
        List<JsonBean> insertList = new ArrayList<JsonBean>();
        CoordinatorActionBean action = new CoordinatorActionBean();
        action.setId(id);
        action.setJobId(parentId);
        insertList.add(action);
        BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(insertList, null, null);
        return action;
    }

    private CoordinatorJobBean createCoord(String id) throws Exception {
        List<JsonBean> insertList = new ArrayList<JsonBean>();
        CoordinatorJobBean job = new CoordinatorJobBean();
        job.setId(id);
        insertList.add(job);
        BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(insertList, null, null);
        return job;
    }
}
