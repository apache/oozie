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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.oozie.AppType;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.event.SLAEvent;
import org.apache.oozie.client.event.JobEvent.EventStatus;
import org.apache.oozie.client.event.SLAEvent.SLAStatus;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.executor.jpa.BatchQueryExecutor.UpdateEntry;
import org.apache.oozie.executor.jpa.CoordActionInsertJPAExecutor;
import org.apache.oozie.executor.jpa.SLARegistrationQueryExecutor;
import org.apache.oozie.executor.jpa.SLARegistrationQueryExecutor.SLARegQuery;
import org.apache.oozie.executor.jpa.SLASummaryQueryExecutor.SLASummaryQuery;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.SLASummaryQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionInsertJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor.WorkflowJobQuery;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.service.SLAService;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.workflow.WorkflowInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSLACalculatorMemory extends XDataTestCase {

    private JPAService jpaService;

    @Override
    @Before
    protected void setUp() throws Exception {
        super.setUp();
        Services services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_EXT_CLASSES, "org.apache.oozie.service.EventHandlerService,"
                + "org.apache.oozie.sla.service.SLAService");
        services.init();
        jpaService = Services.get().get(JPAService.class);
    }

    @Override
    @After
    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    private void createWorkflow(List<String> idList) throws Exception {
        List<JsonBean> insertList = new ArrayList<JsonBean>();
        for (String id : idList) {
            WorkflowJobBean workflow = new WorkflowJobBean();
            workflow.setId(id);
            workflow.setStatusStr("PREP");
            workflow.setStartTime(new Date());
            insertList.add(workflow);
        }
        BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(insertList, null, null);
    }

    @Test
    public void testLoadOnRestart() throws Exception {
        SLACalculatorMemory slaCalcMemory = new SLACalculatorMemory();
        slaCalcMemory.init(Services.get().getConf());
        SLARegistrationBean slaRegBean1 = _createSLARegistration("job-1", AppType.WORKFLOW_JOB);
        String jobId1 = slaRegBean1.getId();
        SLARegistrationBean slaRegBean2 = _createSLARegistration("job-2", AppType.WORKFLOW_JOB);
        String jobId2 = slaRegBean2.getId();
        SLARegistrationBean slaRegBean3 = _createSLARegistration("job-3", AppType.WORKFLOW_JOB);
        String jobId3 = slaRegBean3.getId();
        List<String> idList = new ArrayList<String>();
        idList.add(slaRegBean1.getId());
        idList.add(slaRegBean2.getId());
        idList.add(slaRegBean3.getId());
        createWorkflow(idList);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd");
        slaRegBean1.setAppName("app-name");
        slaRegBean1.setExpectedDuration(123);
        slaRegBean1.setExpectedEnd(sdf.parse("2012-02-07"));
        slaRegBean1.setExpectedStart(sdf.parse("2011-02-07"));
        slaRegBean1.setNominalTime(sdf.parse("2012-01-06"));
        slaRegBean1.setUser("user");
        slaRegBean1.setParentId("parentId");
        slaRegBean1.setUpstreamApps("upstreamApps");
        slaRegBean1.setNotificationMsg("notificationMsg");
        slaRegBean1.setAlertContact("a@abc.com");
        slaRegBean1.setAlertEvents("MISS");
        slaRegBean1.setJobData("jobData");

        slaCalcMemory.addRegistration(jobId1, slaRegBean1);
        slaCalcMemory.addRegistration(jobId2, slaRegBean2);
        slaCalcMemory.addRegistration(jobId3, slaRegBean3);

        SLACalcStatus calc1 = slaCalcMemory.get(jobId1);
        SLACalcStatus calc2 = slaCalcMemory.get(jobId2);
        SLACalcStatus calc3 = slaCalcMemory.get(jobId3);

        calc1.setEventProcessed(5);
        calc2.setEventProcessed(6);
        calc3.setEventProcessed(7);

        calc1.setEventStatus(SLAEvent.EventStatus.END_MISS);
        calc1.setSLAStatus(SLAEvent.SLAStatus.MISS);
        calc1.setJobStatus(WorkflowJob.Status.FAILED.toString());
        // set last modified time 5 days back
        Date lastModifiedTime = new Date(System.currentTimeMillis() - 5*24*60*60*1000);
        calc1.setLastModifiedTime(lastModifiedTime);

        List<UpdateEntry> updateList = new ArrayList<UpdateEntry>();
        SLASummaryBean bean = new SLASummaryBean(calc1);
        bean.setActualStart(sdf.parse("2011-03-09"));
        bean.setActualEnd(sdf.parse("2011-03-10"));
        bean.setActualDuration(456);
        updateList.add(new UpdateEntry<SLASummaryQuery>(SLASummaryQuery.UPDATE_SLA_SUMMARY_ALL, bean));
        updateList.add(new UpdateEntry<SLASummaryQuery>(SLASummaryQuery.UPDATE_SLA_SUMMARY_ALL,
                new SLASummaryBean(calc2)));
        updateList.add(new UpdateEntry<SLASummaryQuery>(SLASummaryQuery.UPDATE_SLA_SUMMARY_ALL,
                new SLASummaryBean(calc3)));

        BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(null, updateList, null);

        slaCalcMemory = new SLACalculatorMemory();
        slaCalcMemory.init(Services.get().getConf());

        assertEquals(2, slaCalcMemory.size());

        SLACalcStatus calc = slaCalcMemory.get(jobId1);
        assertEquals("job-1", calc.getId());
        assertEquals(AppType.WORKFLOW_JOB, calc.getAppType());
        assertEquals("app-name", calc.getAppName());
        assertEquals(123, calc.getExpectedDuration());
        assertEquals(sdf.parse("2012-02-07"), calc.getExpectedEnd());
        assertEquals(sdf.parse("2011-02-07"), calc.getExpectedStart());
        assertEquals(sdf.parse("2012-01-06"), calc.getNominalTime());
        assertEquals("user", calc.getUser());
        assertEquals("parentId", calc.getParentId());
        assertEquals("upstreamApps", calc.getUpstreamApps());
        assertEquals("notificationMsg", calc.getNotificationMsg());
        assertEquals("a@abc.com", calc.getAlertContact());
        assertEquals("MISS", calc.getAlertEvents());
        assertEquals("jobData", calc.getJobData());
        assertEquals(sdf.parse("2011-03-09"), calc.getActualStart());
        assertEquals(sdf.parse("2011-03-10"), calc.getActualEnd());
        assertEquals(456, calc.getActualDuration());
        assertEquals(SLAEvent.EventStatus.END_MISS, calc1.getEventStatus());
        assertEquals(SLAEvent.SLAStatus.MISS, calc1.getSLAStatus());
        assertEquals(WorkflowJob.Status.FAILED.toString(), calc1.getJobStatus());
        assertEquals(lastModifiedTime, calc1.getLastModifiedTime());

        assertEquals(5, calc.getEventProcessed());
        assertEquals(6, slaCalcMemory.get(jobId2).getEventProcessed());
        // jobId3 should be in history set as eventprocessed is 7 (111)
        assertEquals(2, slaCalcMemory.size()); // 2 out of 3 jobs in map
        slaCalcMemory.addJobStatus(jobId3, WorkflowJob.Status.SUCCEEDED.toString(), EventStatus.SUCCESS,
                sdf.parse("2011-03-09"), sdf.parse("2011-04-09"));
        SLASummaryBean slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId3);
        assertEquals(8, slaSummary.getEventProcessed());
        assertEquals(sdf.parse("2011-03-09"), slaSummary.getActualStart());
        assertEquals(sdf.parse("2011-04-09"), slaSummary.getActualEnd());
        assertEquals(WorkflowJob.Status.SUCCEEDED.toString(), slaSummary.getJobStatus());
    }


    @Test
    public void testWorkflowJobSLAStatusOnRestart() throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd");
        SLACalculatorMemory slaCalcMemory = new SLACalculatorMemory();
        slaCalcMemory.init(Services.get().getConf());
        SLARegistrationBean slaRegBean1 = _createSLARegistration("job-1", AppType.WORKFLOW_JOB);
        String jobId1 = slaRegBean1.getId();
        slaRegBean1.setExpectedEnd(sdf.parse("2013-03-07"));
        slaRegBean1.setExpectedStart(sdf.parse("2012-03-07"));
        slaCalcMemory.addRegistration(jobId1, slaRegBean1);
        SLACalcStatus calc1 = slaCalcMemory.get(jobId1);
        calc1.setEventProcessed(1);
        calc1.setSLAStatus(SLAEvent.SLAStatus.IN_PROCESS);
        calc1.setJobStatus(WorkflowJob.Status.RUNNING.name());
        calc1.setLastModifiedTime(new Date());
        SLASummaryBean slaSummaryBean = new SLASummaryBean(calc1);

        SLASummaryQueryExecutor.getInstance().executeUpdate(SLASummaryQuery.UPDATE_SLA_SUMMARY_ALL,slaSummaryBean);

        SLASummaryBean slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId1);

        // Simulate a lost success event
        WorkflowJobBean wjb = new WorkflowJobBean();
        wjb.setId(jobId1);
        wjb.setStatus(WorkflowJob.Status.SUCCEEDED);
        wjb.setStartTime(sdf.parse("2012-02-07"));
        wjb.setEndTime(sdf.parse("2013-02-07"));
        wjb.setLastModifiedTime(new Date());
        WorkflowJobQueryExecutor.getInstance().insert(wjb);

        slaCalcMemory = new SLACalculatorMemory();
        slaCalcMemory.init(Services.get().getConf());

        // As job succeeded, it should not be in memory
        assertEquals(0, slaCalcMemory.size());
        slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId1);
        assertEquals("job-1", slaSummary.getId());
        assertEquals(8, slaSummary.getEventProcessed());
        assertEquals(AppType.WORKFLOW_JOB, slaSummary.getAppType());
        assertEquals("SUCCEEDED", slaSummary.getJobStatus());
        assertEquals(SLAEvent.SLAStatus.MET, slaSummary.getSLAStatus());
        assertEquals(sdf.parse("2012-02-07"), slaSummary.getActualStart());
        assertEquals(sdf.parse("2013-02-07"), slaSummary.getActualEnd());
        assertEquals(sdf.parse("2013-02-07").getTime() - sdf.parse("2012-02-07").getTime(),
                slaSummary.getActualDuration());

        // Simulate a lost failed event
        wjb.setStatus(WorkflowJob.Status.FAILED);
        wjb.setLastModifiedTime(new Date());
        WorkflowJobQueryExecutor.getInstance().executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_MODTIME, wjb);

        // Reset the summary Bean
        calc1.setEventProcessed(1);
        calc1.setSLAStatus(SLAEvent.SLAStatus.IN_PROCESS);
        calc1.setJobStatus(WorkflowJob.Status.RUNNING.name());
        slaSummaryBean = new SLASummaryBean(calc1);

        SLASummaryQueryExecutor.getInstance().executeUpdate(SLASummaryQuery.UPDATE_SLA_SUMMARY_ALL, slaSummaryBean);

        slaCalcMemory = new SLACalculatorMemory();
        slaCalcMemory.init(Services.get().getConf());

        assertEquals(0, slaCalcMemory.size());
        slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId1);
        assertEquals("FAILED", slaSummary.getJobStatus());
        assertEquals(8, slaSummary.getEventProcessed());
        assertEquals(sdf.parse("2012-02-07"), slaSummary.getActualStart());
        assertEquals(sdf.parse("2013-02-07"), slaSummary.getActualEnd());
        assertEquals(SLAEvent.SLAStatus.MISS, slaSummary.getSLAStatus());

        // Simulate a lost RUNNING event
        wjb.setStatus(WorkflowJob.Status.RUNNING);
        wjb.setLastModifiedTime(new Date());
        WorkflowJobQueryExecutor.getInstance().executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_MODTIME, wjb);

        // Reset the summary Bean
        calc1.setEventProcessed(0);
        calc1.setSLAStatus(SLAEvent.SLAStatus.NOT_STARTED);
        calc1.setJobStatus(null);
        slaSummaryBean = new SLASummaryBean(calc1);

        SLASummaryQueryExecutor.getInstance().executeUpdate(SLASummaryQuery.UPDATE_SLA_SUMMARY_ALL, slaSummaryBean);

        slaCalcMemory = new SLACalculatorMemory();
        slaCalcMemory.init(Services.get().getConf());

        assertEquals(1, slaCalcMemory.size());
        SLACalcStatus calc = slaCalcMemory.get(jobId1);
        assertEquals(1, calc.getEventProcessed());
        assertEquals("RUNNING", calc.getJobStatus());
        assertEquals(sdf.parse("2012-02-07"), calc.getActualStart());
        assertNull(calc.getActualEnd());
        assertEquals(-1, calc.getActualDuration());
        assertEquals(SLAEvent.SLAStatus.IN_PROCESS, calc.getSLAStatus());
    }

    @Test
    public void testWorkflowActionSLAStatusOnRestart() throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd");
        SLACalculatorMemory slaCalcMemory = new SLACalculatorMemory();
        slaCalcMemory.init(Services.get().getConf());
        SLARegistrationBean slaRegBean1 = _createSLARegistration("job@1", AppType.WORKFLOW_ACTION);
        String jobId1 = slaRegBean1.getId();
        slaRegBean1.setExpectedEnd(sdf.parse("2013-03-07"));
        slaRegBean1.setExpectedStart(sdf.parse("2012-03-07"));
        slaCalcMemory.addRegistration(jobId1, slaRegBean1);
        SLACalcStatus calc1 = slaCalcMemory.get(jobId1);
        calc1.setEventProcessed(1);
        calc1.setSLAStatus(SLAEvent.SLAStatus.IN_PROCESS);
        calc1.setJobStatus(WorkflowAction.Status.RUNNING.name());
        calc1.setLastModifiedTime(new Date());
        SLASummaryBean slaSummaryBean = new SLASummaryBean(calc1);

        SLASummaryQueryExecutor.getInstance().executeUpdate(SLASummaryQuery.UPDATE_SLA_SUMMARY_ALL, slaSummaryBean);

        // Simulate a lost success event
        WorkflowActionBean wab = new WorkflowActionBean();
        wab.setId(jobId1);
        wab.setStatus(WorkflowAction.Status.OK);
        wab.setStartTime(sdf.parse("2012-02-07"));
        wab.setEndTime(sdf.parse("2013-02-07"));
        WorkflowActionInsertJPAExecutor wfInsertCmd = new WorkflowActionInsertJPAExecutor(wab);
        jpaService.execute(wfInsertCmd);

        slaCalcMemory = new SLACalculatorMemory();
        slaCalcMemory.init(Services.get().getConf());

        // As job succeeded, it should not be in memory
        assertEquals(0, slaCalcMemory.size());
        SLASummaryBean slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId1);
        assertEquals("job@1", slaSummary.getId());
        assertEquals(8, slaSummary.getEventProcessed());
        assertEquals(AppType.WORKFLOW_ACTION, slaSummary.getAppType());
        assertEquals("OK", slaSummary.getJobStatus());
        assertEquals(SLAEvent.SLAStatus.MET, slaSummary.getSLAStatus());
        assertEquals(sdf.parse("2012-02-07"), slaSummary.getActualStart());
        assertEquals(sdf.parse("2013-02-07"), slaSummary.getActualEnd());
        assertEquals(sdf.parse("2013-02-07").getTime() - sdf.parse("2012-02-07").getTime(),
                slaSummary.getActualDuration());

    }

    @Test
    public void testCoordinatorActionSLAStatusOnRestart() throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd");
        SLACalculatorMemory slaCalcMemory = new SLACalculatorMemory();
        slaCalcMemory.init(Services.get().getConf());
        SLARegistrationBean slaRegBean1 = _createSLARegistration("job@1", AppType.COORDINATOR_ACTION);
        String jobId1 = slaRegBean1.getId();
        slaRegBean1.setExpectedEnd(sdf.parse("2013-03-07"));
        slaRegBean1.setExpectedStart(sdf.parse("2012-03-07"));
        slaCalcMemory.addRegistration(jobId1, slaRegBean1);
        SLACalcStatus calc1 = slaCalcMemory.get(jobId1);
        calc1.setEventProcessed(1);
        calc1.setSLAStatus(SLAEvent.SLAStatus.IN_PROCESS);
        calc1.setJobStatus(WorkflowAction.Status.RUNNING.name());
        calc1.setLastModifiedTime(new Date());
        SLASummaryBean slaSummaryBean = new SLASummaryBean(calc1);

        SLASummaryQueryExecutor.getInstance().executeUpdate(SLASummaryQuery.UPDATE_SLA_SUMMARY_ALL, slaSummaryBean);

        // Simulate a lost failed event
        CoordinatorActionBean cab = new CoordinatorActionBean();
        cab.setId(jobId1);
        cab.setStatus(CoordinatorAction.Status.FAILED);
        cab.setLastModifiedTime(sdf.parse("2013-02-07"));
        cab.setExternalId("wf_job");
        CoordActionInsertJPAExecutor caInsertCmd = new CoordActionInsertJPAExecutor(cab);
        jpaService.execute(caInsertCmd);
        WorkflowJobBean wjb = new WorkflowJobBean();
        wjb.setId("wf_job");
        wjb.setStartTime(sdf.parse("2012-02-07"));
        wjb.setLastModifiedTime(new Date());
        WorkflowJobQueryExecutor.getInstance().insert(wjb);

        slaCalcMemory = new SLACalculatorMemory();
        slaCalcMemory.init(Services.get().getConf());

        // As job succeeded, it should not be in memory
        assertEquals(0, slaCalcMemory.size());
        SLASummaryBean slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId1);

        assertEquals("job@1", slaSummary.getId());
        assertEquals(8, slaSummary.getEventProcessed());
        assertEquals(AppType.COORDINATOR_ACTION, slaSummary.getAppType());
        assertEquals("FAILED", slaSummary.getJobStatus());
        assertEquals(SLAEvent.SLAStatus.MISS, slaSummary.getSLAStatus());
        assertEquals(sdf.parse("2012-02-07"), slaSummary.getActualStart());
        assertEquals(sdf.parse("2013-02-07"), slaSummary.getActualEnd());
        assertEquals(sdf.parse("2013-02-07").getTime() - sdf.parse("2012-02-07").getTime(),
                slaSummary.getActualDuration());
    }

    @Test
    public void testSLAEvents1() throws Exception {
        SLACalculatorMemory slaCalcMemory = new SLACalculatorMemory();
        EventHandlerService ehs = Services.get().get(EventHandlerService.class);
        slaCalcMemory.init(Services.get().getConf());
        WorkflowJobBean job1 = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);
        SLARegistrationBean slaRegBean = _createSLARegistration(job1.getId(), AppType.WORKFLOW_JOB);
        slaRegBean.setExpectedStart(new Date(System.currentTimeMillis() - 1 * 1 * 3600 * 1000)); // 1 hour
        slaRegBean.setExpectedDuration(2 * 3600 * 1000);
        slaRegBean.setExpectedEnd(new Date(System.currentTimeMillis() - 1 * 1 * 3600 * 1000)); // 1 hour
        String jobId = slaRegBean.getId();
        slaCalcMemory.addRegistration(jobId, slaRegBean);
        assertEquals(1, slaCalcMemory.size());
        SLASummaryBean slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId);
        assertEquals(SLAStatus.NOT_STARTED, slaSummary.getSLAStatus());
        assertEquals("PREP", slaSummary.getJobStatus());
        slaCalcMemory.updateJobSla(jobId);
        assertEquals(2, ehs.getEventQueue().size());
        slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId);
        // both start miss and end miss (101)
        assertEquals(5, slaSummary.getEventProcessed());
        assertEquals(SLAEvent.EventStatus.END_MISS, slaSummary.getEventStatus());
        assertEquals(SLAStatus.MISS, slaSummary.getSLAStatus());

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        assertEquals(SLAStatus.MISS, slaSummary.getSLAStatus());
        slaCalcMemory.addJobStatus(jobId, WorkflowJob.Status.RUNNING.toString(), EventStatus.STARTED,
                sdf.parse("2012-01-01"), null);
        slaCalcMemory.addJobStatus(jobId, WorkflowJob.Status.SUSPENDED.toString(), EventStatus.SUSPEND,
                sdf.parse("2012-01-01"), null);
        slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId);
        assertEquals(WorkflowJob.Status.SUSPENDED.toString(), slaSummary.getJobStatus());
        assertEquals(5, slaSummary.getEventProcessed());
        slaCalcMemory.addJobStatus(jobId, WorkflowJob.Status.SUCCEEDED.toString(), EventStatus.SUCCESS,
                sdf.parse("2012-01-01"), sdf.parse("2012-01-02"));

        assertEquals(3, ehs.getEventQueue().size());
        slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId);
        // All events processed and actual times stored (1000)
        assertEquals(8, slaSummary.getEventProcessed());
        assertEquals(SLAStatus.MISS, slaSummary.getSLAStatus());
        assertEquals(WorkflowJob.Status.SUCCEEDED.toString(), slaSummary.getJobStatus());
        assertEquals(SLAEvent.EventStatus.DURATION_MISS, slaSummary.getEventStatus());
        assertEquals(sdf.parse("2012-01-01").getTime(), slaSummary.getActualStart().getTime());
        assertEquals(sdf.parse("2012-01-02").getTime(), slaSummary.getActualEnd().getTime());
        assertEquals(sdf.parse("2012-01-02").getTime() - sdf.parse("2012-01-01").getTime(),
                slaSummary.getActualDuration());
        assertEquals(0, slaCalcMemory.size());
    }

    @Test
    public void testSLAEvents2() throws Exception {
        SLACalculatorMemory slaCalcMemory = new SLACalculatorMemory();
        EventHandlerService ehs = Services.get().get(EventHandlerService.class);
        slaCalcMemory.init(Services.get().getConf());

        WorkflowJobBean job1 = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);
        SLARegistrationBean slaRegBean = _createSLARegistration(job1.getId(), AppType.WORKFLOW_JOB);
        slaRegBean.setExpectedStart(new Date(System.currentTimeMillis() - 1 * 1 * 3600 * 1000));
        slaRegBean.setExpectedEnd(new Date(System.currentTimeMillis() + 2 * 1 * 3600 * 1000));

        String jobId = slaRegBean.getId();
        slaCalcMemory.addRegistration(jobId, slaRegBean);
        assertEquals(1, slaCalcMemory.size());
        slaCalcMemory.updateJobSla(jobId);
        SLASummaryBean slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId);
        // Duration bit should be processed as expected duration is not set
        assertEquals(3, slaSummary.getEventProcessed());
        // check only start event in queue
        assertEquals(1, ehs.getEventQueue().size());
        ehs.getEventQueue().clear();

        // set back to 1, to make duration event not processed
        slaSummary.setEventProcessed(1);
        SLASummaryQueryExecutor.getInstance().executeUpdate(SLASummaryQuery.UPDATE_SLA_SUMMARY_ALL, slaSummary);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        slaCalcMemory.addJobStatus(jobId, WorkflowJob.Status.SUCCEEDED.toString(), EventStatus.SUCCESS,
                sdf.parse("2012-01-01"), sdf.parse("2012-01-02"));
        slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId);
        // all should be processed
        assertEquals(8, slaSummary.getEventProcessed());
        // check only end event is in queue
        assertEquals(1, ehs.getEventQueue().size());
        ehs.getEventQueue().clear();

        slaSummary.setEventProcessed(1);
        SLASummaryQueryExecutor.getInstance().executeUpdate(SLASummaryQuery.UPDATE_SLA_SUMMARY_ALL, slaSummary);

        slaRegBean = _createSLARegistration("job-2", AppType.WORKFLOW_JOB);
        slaRegBean.setExpectedStart(new Date(System.currentTimeMillis() - 1 * 1 * 3600 * 1000));
        slaRegBean.setExpectedEnd(new Date(System.currentTimeMillis() + 2 * 1 * 3600 * 1000));

        jobId = slaRegBean.getId();
        slaCalcMemory.addRegistration(jobId, slaRegBean);
        assertEquals(1, slaCalcMemory.size());

        slaCalcMemory.addJobStatus(jobId, WorkflowJob.Status.KILLED.toString(), EventStatus.FAILURE, null,
                sdf.parse("2012-01-02"));
        slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId);
        // Actual start null, so all events processed
        assertEquals(8, slaSummary.getEventProcessed());
        assertEquals(1, ehs.getEventQueue().size());
        assertNull(slaSummary.getActualStart());
        assertEquals(sdf.parse("2012-01-02"), slaSummary.getActualEnd());
        assertEquals(SLAStatus.MISS, slaSummary.getSLAStatus());
        assertEquals(SLAEvent.EventStatus.END_MISS, slaSummary.getEventStatus());
    }

    @Test
    public void testDuplicateStartMiss() throws Exception {
        // test start-miss
        EventHandlerService ehs = Services.get().get(EventHandlerService.class);
        SLACalculatorMemory slaCalcMemory = new SLACalculatorMemory();
        slaCalcMemory.init(Services.get().getConf());
        WorkflowJobBean job1 = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);
        SLARegistrationBean slaRegBean = _createSLARegistration(job1.getId(), AppType.WORKFLOW_JOB);
        Date startTime = new Date(System.currentTimeMillis() - 1 * 1 * 3600 * 1000); // 1 hour back
        slaRegBean.setExpectedStart(startTime);
        slaRegBean.setExpectedDuration(3600 * 1000);
        slaRegBean.setExpectedEnd(new Date(System.currentTimeMillis() + 1 * 1 * 3600 * 1000)); // 1 hour ahead
        String jobId = slaRegBean.getId();
        slaCalcMemory.addRegistration(slaRegBean.getId(), slaRegBean);
        slaCalcMemory.updateJobSla(jobId);
        SLASummaryBean slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId);

        assertEquals(1, slaSummary.getEventProcessed());
        assertEquals(SLAStatus.NOT_STARTED, slaSummary.getSLAStatus());
        slaCalcMemory.addJobStatus(jobId, WorkflowJob.Status.RUNNING.toString(), EventStatus.STARTED,
                new Date(System.currentTimeMillis()), null);
        slaCalcMemory.updateJobSla(jobId);
        slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId);

        assertEquals(1, slaSummary.getEventProcessed());
        assertEquals(SLAStatus.IN_PROCESS, slaSummary.getSLAStatus());
        assertEquals(WorkflowJob.Status.RUNNING.toString(), slaSummary.getJobStatus());
        assertEquals(1, ehs.getEventQueue().size());
    }

    @Test
    public void testDuplicateEndMiss() throws Exception {
        EventHandlerService ehs = Services.get().get(EventHandlerService.class);
        SLACalculatorMemory slaCalcMemory = new SLACalculatorMemory();
        slaCalcMemory.init(Services.get().getConf());
        WorkflowJobBean job1 = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        SLARegistrationBean slaRegBean = _createSLARegistration(job1.getId(), AppType.WORKFLOW_JOB);
        Date startTime = new Date(System.currentTimeMillis() + 1 * 1 * 3600 * 1000); // 1 hour ahead
        slaRegBean.setExpectedStart(startTime);
        slaRegBean.setExpectedDuration(3600 * 1000);
        slaRegBean.setExpectedEnd(new Date(System.currentTimeMillis() - 1 * 1 * 3600 * 1000)); // 1 hour back
        String jobId = slaRegBean.getId();
        slaCalcMemory.addRegistration(slaRegBean.getId(), slaRegBean);
        slaCalcMemory.updateJobSla(jobId);
        SLASummaryBean slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId);
        slaRegBean = SLARegistrationQueryExecutor.getInstance().get(SLARegQuery.GET_SLA_REG_ALL, jobId);
        assertNotNull(slaRegBean.getCreatedTimestamp());
        assertEquals(slaRegBean.getCreatedTimestamp(), slaSummary.getCreatedTimestamp());
        // Only end sla should be processed (100)
        assertEquals(4, slaSummary.getEventProcessed());
        slaCalcMemory.updateJobSla(jobId);
        slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId);
        assertEquals(4, slaSummary.getEventProcessed());
        assertEquals(SLAStatus.MISS, slaSummary.getSLAStatus());
        slaCalcMemory.addJobStatus(jobId, WorkflowJob.Status.SUCCEEDED.toString(), EventStatus.SUCCESS,
                new Date(System.currentTimeMillis()), new Date(System.currentTimeMillis() + 1 * 1 * 3600 * 1000));
        slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId);

        // Only Duration sla should be processed as end is already processed
        // (110)
        assertEquals(6, slaSummary.getEventProcessed());
        assertEquals(SLAStatus.MISS, slaSummary.getSLAStatus());
        // Recieve start event
        assertTrue(slaCalcMemory.addJobStatus(jobId, WorkflowJob.Status.RUNNING.toString(), EventStatus.STARTED,
                new Date(System.currentTimeMillis()), new Date(System.currentTimeMillis() + 1 * 1 * 3600 * 1000)));
        slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId);
        // Start event received so all bits should be processed (111)
        assertEquals(8, slaSummary.getEventProcessed());
        assertEquals(SLAStatus.MISS, slaSummary.getSLAStatus());
        assertEquals(0, slaCalcMemory.size());
        assertEquals(3, ehs.getEventQueue().size());

    }

    public void testSLAHistorySet() throws Exception {
            EventHandlerService ehs = Services.get().get(EventHandlerService.class);
            SLACalculatorMemory slaCalcMemory = new SLACalculatorMemory();
            slaCalcMemory.init(Services.get().getConf());
            WorkflowJobBean job1 = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);
            SLARegistrationBean slaRegBean = _createSLARegistration(job1.getId(), AppType.WORKFLOW_JOB);
            Date startTime = new Date(System.currentTimeMillis() - 1 * 1 * 3600 * 1000);
            slaRegBean.setExpectedStart(startTime); // 1 hour back
            slaRegBean.setExpectedDuration(1000);
            slaRegBean.setExpectedEnd(new Date(System.currentTimeMillis() - 1 * 1 * 3600 * 1000));
            String jobId = slaRegBean.getId();
            slaCalcMemory.addRegistration(slaRegBean.getId(), slaRegBean);
            slaCalcMemory.updateJobSla(jobId);
            slaCalcMemory.addJobStatus(jobId, WorkflowJob.Status.RUNNING.toString(), EventStatus.STARTED, new Date(
                    System.currentTimeMillis() - 3600 * 1000), null);
            slaCalcMemory.updateJobSla(jobId);
            SLASummaryBean slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId);
            // The actual end times are not stored, but sla's processed so (111)
            assertEquals(7, slaSummary.getEventProcessed());
            // Moved from map to history set
            assertEquals(0, slaCalcMemory.size());
            // Add terminal state event so actual end time is stored
            slaCalcMemory.addJobStatus(jobId, WorkflowJob.Status.SUCCEEDED.toString(), EventStatus.SUCCESS, new Date(
                    System.currentTimeMillis() - 3600 * 1000), new Date(System.currentTimeMillis()));
            slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId);
            // The actual times are stored, so event processed(1000)
            assertEquals(8, slaSummary.getEventProcessed());
            assertEquals(3, ehs.getEventQueue().size());
    }

    private SLARegistrationBean _createSLARegistration(String jobId, AppType appType) {
        SLARegistrationBean bean = new SLARegistrationBean();
        bean.setId(jobId);
        bean.setAppType(appType);
        return bean;
    }

    public void testHistoryPurge() throws Exception{
        EventHandlerService ehs = Services.get().get(EventHandlerService.class);
        SLACalculatorMemory slaCalcMemory = new SLACalculatorMemory();
        slaCalcMemory.init(Services.get().getConf());
        WorkflowJobBean job1 = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);
        SLARegistrationBean slaRegBean = _createSLARegistration(job1.getId(), AppType.WORKFLOW_JOB);
        Date startTime = new Date(System.currentTimeMillis() - 1 * 1 * 3600 * 1000);
        slaRegBean.setExpectedStart(startTime); // 1 hour back
        slaRegBean.setExpectedDuration(1000);
        slaRegBean.setExpectedEnd(new Date(System.currentTimeMillis() - 1 * 1 * 3600 * 1000));
        String jobId = slaRegBean.getId();
        slaCalcMemory.addRegistration(slaRegBean.getId(), slaRegBean);
        slaCalcMemory.updateJobSla(jobId);
        slaCalcMemory.addJobStatus(jobId, WorkflowJob.Status.RUNNING.toString(), EventStatus.STARTED, new Date(
                System.currentTimeMillis() - 3600 * 1000), null);
        slaCalcMemory.updateJobSla(jobId);
        SLASummaryBean slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId);
        // The actual end times are not stored, but sla's processed so (111)
        assertEquals(7, slaSummary.getEventProcessed());
        assertTrue(slaCalcMemory.isJobIdInHistorySet(job1.getId()));
        job1.setStatusStr("SUCCEEDED");
        job1.setLastModifiedTime(new Date());
        WorkflowJobQueryExecutor.getInstance().executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_MODTIME, job1);
        slaCalcMemory.new HistoryPurgeWorker().run();
        assertFalse(slaCalcMemory.isJobIdInHistorySet(job1.getId()));
    }

    public void testErrorLoggingWithJobIdPrefix() throws Exception {
        SLAService slaService = Services.get().get(SLAService.class);
        SLACalculator slaCalculator = slaService.getSLACalculator();
        slaCalculator.addRegistration("dummy-id", _createSLARegistration("dummy-id", AppType.WORKFLOW_JOB));

        TestLogAppender appender = getTestLogAppender();
        Logger logger = Logger.getLogger(SLACalculatorMemory.class);
        logger.addAppender(appender);
        logger.setLevel(Level.ERROR);
        try {
            slaService.runSLAWorker();
        }
        finally {
            logger.removeAppender(appender);
        }

        List<LoggingEvent> log = appender.getLog();
        LoggingEvent firstLogEntry = log.get(0);
        assertEquals(Level.ERROR, firstLogEntry.getLevel());
        assertTrue(firstLogEntry.getMessage().toString().contains("JOB[dummy-id]"));
        assertEquals("org.apache.oozie.sla.SLACalculatorMemory", firstLogEntry.getLoggerName());

    }

}
