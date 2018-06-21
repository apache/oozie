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
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.oozie.AppType;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.event.JobEvent.EventStatus;
import org.apache.oozie.client.event.SLAEvent;
import org.apache.oozie.client.event.SLAEvent.SLAStatus;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.BatchQueryExecutor.UpdateEntry;
import org.apache.oozie.executor.jpa.CoordActionInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.SLARegistrationQueryExecutor;
import org.apache.oozie.executor.jpa.SLARegistrationQueryExecutor.SLARegQuery;
import org.apache.oozie.executor.jpa.SLASummaryQueryExecutor;
import org.apache.oozie.executor.jpa.SLASummaryQueryExecutor.SLASummaryQuery;
import org.apache.oozie.executor.jpa.WorkflowActionInsertJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor.WorkflowJobQuery;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.InstrumentationService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.service.SLAService;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.JobUtils;
import org.apache.oozie.util.Pair;
import org.apache.oozie.util.db.AlwaysFailingHSQLDriverMapper;
import org.apache.oozie.util.db.FailingHSQLDBDriverWrapper;
import org.apache.oozie.util.db.FailingDBHelperForTest;
import org.apache.oozie.workflow.WorkflowInstance;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

public class TestSLACalculatorMemory extends XDataTestCase {
    private Services services;
    private JPAService jpaService;
    private Instrumentation instrumentation;

    @Override
    @Before
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        Configuration conf = services.get(ConfigurationService.class).getConf();
        conf.set(Services.CONF_SERVICE_EXT_CLASSES, "org.apache.oozie.service.EventHandlerService," +
                "org.apache.oozie.sla.service.SLAService," +
                "org.apache.oozie.service.InstrumentationService");
        conf.setInt(SLAService.CONF_SLA_CHECK_INTERVAL, 600);
        services.init();
        jpaService = services.get(JPAService.class);
        instrumentation = services.get(InstrumentationService.class).get();
    }

    @Override
    @After
    protected void tearDown() throws Exception {
        services.destroy();
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
        slaCalcMemory.init(Services.get().get(ConfigurationService.class).getConf());
        SLARegistrationBean slaRegBean1 = _createSLARegistration("job-1-W", AppType.WORKFLOW_JOB);
        String jobId1 = slaRegBean1.getId();
        SLARegistrationBean slaRegBean2 = _createSLARegistration("job-2-W", AppType.WORKFLOW_JOB);
        String jobId2 = slaRegBean2.getId();
        SLARegistrationBean slaRegBean3 = _createSLARegistration("job-3-W", AppType.WORKFLOW_JOB);
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

        Date startTime = new Date(System.currentTimeMillis() - 1 * 1 * 3600 * 1000); // 1 hour back
        Date endTime = new Date(System.currentTimeMillis() + 2 * 1 * 3600 * 1000); // 1 hour back

        slaRegBean3.setExpectedStart(startTime);
        slaRegBean3.setExpectedEnd(endTime);

        slaCalcMemory.addRegistration(jobId1, slaRegBean1);
        slaCalcMemory.addRegistration(jobId2, slaRegBean2);
        slaCalcMemory.addRegistration(jobId3, slaRegBean3);

        slaCalcMemory.updateAllSlaStatus();
        SLACalcStatus calc1 = slaCalcMemory.get(jobId1);
        SLACalcStatus calc2 = slaCalcMemory.get(jobId2);
        SLACalcStatus calc3 = slaCalcMemory.get(jobId3);


        calc1.setEventStatus(SLAEvent.EventStatus.END_MISS);
        calc1.setSLAStatus(SLAEvent.SLAStatus.MISS);
        calc1.setJobStatus(WorkflowJob.Status.FAILED.toString());
        // set last modified time 5 days back
        Date lastModifiedTime = new Date(System.currentTimeMillis() - 5*24*60*60*1000);
        calc1.setLastModifiedTime(lastModifiedTime);

        List<UpdateEntry> updateList = new ArrayList<UpdateEntry>();
        WorkflowJobBean wf1 = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW, jobId1);
        wf1.setId(jobId1);
        wf1.setStatus(WorkflowJob.Status.SUCCEEDED);
        wf1.setStartTime(sdf.parse("2011-03-09"));
        wf1.setEndTime(sdf.parse("2011-03-10"));
        wf1.setLastModifiedTime(new Date());

        WorkflowJobBean wf2 = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW, jobId2);
        wf2.setId(jobId2);
        wf2.setStatus(WorkflowJob.Status.RUNNING);
        wf2.setStartTime(sdf.parse("2011-03-09"));
        wf2.setEndTime(null);
        wf2.setLastModifiedTime(new Date());

        WorkflowJobBean wf3 = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW, jobId3);
        wf3.setId(jobId3);
        wf3.setStatus(WorkflowJob.Status.RUNNING);
        wf3.setStartTime(startTime);
        wf3.setEndTime(null);
        wf3.setLastModifiedTime(new Date());

        updateList.add(new UpdateEntry<WorkflowJobQuery>(WorkflowJobQuery.UPDATE_WORKFLOW, wf1));
        updateList.add(new UpdateEntry<WorkflowJobQuery>(WorkflowJobQuery.UPDATE_WORKFLOW, wf2));
        updateList.add(new UpdateEntry<WorkflowJobQuery>(WorkflowJobQuery.UPDATE_WORKFLOW, wf3));
        updateList.add(new UpdateEntry<SLASummaryQuery>(SLASummaryQuery.UPDATE_SLA_SUMMARY_ALL,
                new SLASummaryBean(calc2)));
        updateList.add(new UpdateEntry<SLASummaryQuery>(SLASummaryQuery.UPDATE_SLA_SUMMARY_ALL,
                new SLASummaryBean(calc3)));

        BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(null, updateList, null);

        slaCalcMemory = new SLACalculatorMemory();
        slaCalcMemory.init(Services.get().get(ConfigurationService.class).getConf());
        slaCalcMemory.updateAllSlaStatus();

        SLACalcStatus calc = new SLACalcStatus(SLASummaryQueryExecutor.getInstance().get(
                SLASummaryQuery.GET_SLA_SUMMARY, jobId1), SLARegistrationQueryExecutor.getInstance().get(
                SLARegQuery.GET_SLA_REG_ON_RESTART, jobId1));
        assertEquals("job-1-W", calc.getId());
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
        assertEquals(SLAEvent.EventStatus.END_MISS, calc1.getEventStatus());
        assertEquals(SLAEvent.SLAStatus.MISS, calc1.getSLAStatus());
        assertEquals(WorkflowJob.Status.FAILED.toString(), calc1.getJobStatus());
        assertEquals(lastModifiedTime, calc1.getLastModifiedTime());

        calc2 = new SLACalcStatus(SLASummaryQueryExecutor.getInstance().get(
                SLASummaryQuery.GET_SLA_SUMMARY, jobId2), SLARegistrationQueryExecutor.getInstance().get(
                SLARegQuery.GET_SLA_REG_ON_RESTART, jobId2));


        assertEquals(8, calc.getEventProcessed());
        assertEquals(7, calc2.getEventProcessed());
        // jobId2 should be in history set as eventprocessed is 7 (111)
        //job3 will be in slamap
        assertEquals(1, slaCalcMemory.size()); // 1 out of 3 jobs in map
        WorkflowJobBean wf = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW, jobId3);
        wf.setId(jobId3);
        wf.setStatus(WorkflowJob.Status.SUCCEEDED);
        wf.setEndTime(endTime);
        wf.setStartTime(startTime);
        WorkflowJobQueryExecutor.getInstance().executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_INSTANCE_MOD_START_END, wf);

        slaCalcMemory.addJobStatus(jobId3, WorkflowJob.Status.SUCCEEDED.toString(), EventStatus.SUCCESS,
                startTime, endTime);

        SLASummaryBean slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId3);
        assertEquals(8, slaSummary.getEventProcessed());
        assertEquals(startTime, slaSummary.getActualStart());
        assertEquals(endTime, slaSummary.getActualEnd());
        assertEquals(WorkflowJob.Status.SUCCEEDED.toString(), slaSummary.getJobStatus());
    }


    @Test
    public void testWorkflowJobSLAStatusOnRestart() throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd");
        SLACalculatorMemory slaCalcMemory = new SLACalculatorMemory();
        slaCalcMemory.init(Services.get().get(ConfigurationService.class).getConf());
        SLARegistrationBean slaRegBean1 = _createSLARegistration("job-1-W", AppType.WORKFLOW_JOB);
        final String jobId1 = slaRegBean1.getId();
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
        slaCalcMemory.init(Services.get().get(ConfigurationService.class).getConf());
        slaCalcMemory.updateAllSlaStatus();

        slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId1);
        assertEquals("job-1-W", slaSummary.getId());
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
        slaCalcMemory.init(Services.get().get(ConfigurationService.class).getConf());
        slaCalcMemory.updateAllSlaStatus();

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

        SLAService slaService = Services.get().get(SLAService.class);
        slaService.startSLAWorker();
        slaService.addStatusEvent(jobId1, "RUNNING", null, null, null);
        waitFor(60 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId1)
                        .getEventProcessed() == 7;
            }
        });

        slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, slaSummaryBean.getId());
        //since job is already running and it's a old job
        assertEquals(7, slaSummary.getEventProcessed());
        assertEquals("RUNNING", slaSummary.getJobStatus());
        assertEquals(sdf.parse("2012-02-07"), slaSummary.getActualStart());
        assertNull(slaSummary.getActualEnd());
        assertEquals(-1, slaSummary.getActualDuration());
        assertEquals(SLAEvent.SLAStatus.MISS, slaSummary.getSLAStatus());
    }

    @Test
    public void testWorkflowActionSLAStatusOnRestart() throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd");
        SLACalculatorMemory slaCalcMemory = new SLACalculatorMemory();
        slaCalcMemory.init(Services.get().get(ConfigurationService.class).getConf());
        SLARegistrationBean slaRegBean1 = _createSLARegistration("job-W@1", AppType.WORKFLOW_ACTION);
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
        slaCalcMemory.init(Services.get().get(ConfigurationService.class).getConf());
        slaCalcMemory.updateAllSlaStatus();
        assertEquals(0, slaCalcMemory.size());
        SLASummaryBean slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId1);
        assertEquals("job-W@1", slaSummary.getId());
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
        slaCalcMemory.init(Services.get().get(ConfigurationService.class).getConf());
        SLARegistrationBean slaRegBean1 = _createSLARegistration("job-C@1", AppType.COORDINATOR_ACTION);
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
        cab.setExternalId("wf_job-W");
        CoordActionInsertJPAExecutor caInsertCmd = new CoordActionInsertJPAExecutor(cab);
        jpaService.execute(caInsertCmd);
        WorkflowJobBean wjb = new WorkflowJobBean();
        wjb.setId("wf_job-W");
        wjb.setStartTime(sdf.parse("2012-02-07"));
        wjb.setLastModifiedTime(new Date());
        WorkflowJobQueryExecutor.getInstance().insert(wjb);

        slaCalcMemory = new SLACalculatorMemory();
        slaCalcMemory.init(Services.get().get(ConfigurationService.class).getConf());
        slaCalcMemory.updateAllSlaStatus();
        // As job succeeded, it should not be in memory
        assertEquals(0, slaCalcMemory.size());
        SLASummaryBean slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId1);

        assertEquals("job-C@1", slaSummary.getId());
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
    public void testEventMissOnRestart() throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd");
        SLACalculatorMemory slaCalcMemory = new SLACalculatorMemory();
        slaCalcMemory.init(Services.get().get(ConfigurationService.class).getConf());
        CoordinatorActionBean coordAction = new CoordinatorActionBean();
        coordAction.setId("coordActionId-C@1");
        coordAction.setStatus(CoordinatorAction.Status.RUNNING);
        coordAction.setLastModifiedTime(sdf.parse("2013-02-07"));
        CoordActionInsertJPAExecutor caInsertCmd = new CoordActionInsertJPAExecutor(coordAction);
        jpaService.execute(caInsertCmd);

        CoordinatorActionBean coordAction2 = new CoordinatorActionBean();
        coordAction2.setId("coordActionId-C@2");
        coordAction2.setStatus(CoordinatorAction.Status.RUNNING);
        coordAction2.setLastModifiedTime(sdf.parse("2013-02-07"));
        caInsertCmd = new CoordActionInsertJPAExecutor(coordAction2);
        jpaService.execute(caInsertCmd);

        SLARegistrationBean slaRegBean1 = _createSLARegistration("coordActionId-C@1", AppType.COORDINATOR_ACTION);
        String jobId1 = slaRegBean1.getId();
        slaRegBean1.setExpectedEnd(sdf.parse("2013-03-07"));
        slaRegBean1.setExpectedStart(sdf.parse("2012-03-07"));
        slaRegBean1.setExpectedDuration(100000); // long duration;
        slaCalcMemory.addRegistration(jobId1, slaRegBean1);
        slaCalcMemory.updateAllSlaStatus();

        SLARegistrationBean slaRegBean2 = _createSLARegistration("coordActionId-C@2", AppType.COORDINATOR_ACTION);
        String jobId2 = slaRegBean2.getId();
        slaRegBean2.setExpectedStart(new Date(System.currentTimeMillis() + 1 * 1 * 3600 * 1000)); // 1 hour
        slaRegBean2.setExpectedEnd(new Date(System.currentTimeMillis() + 2 * 1 * 3600 * 1000)); // 2 hour
        slaRegBean2.setExpectedDuration(100000); // long duration;
        slaCalcMemory.addRegistration(jobId2, slaRegBean2);
        slaCalcMemory.updateAllSlaStatus();
        assertEquals(2, slaCalcMemory.size());

        SLASummaryBean slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId1);
        SLASummaryBean slaSummary2 = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId2);

        assertEquals("coordActionId-C@1", slaSummary.getId());
        assertEquals(5, slaSummary.getEventProcessed());
        assertEquals(-1, slaSummary.getActualDuration());

        assertEquals("coordActionId-C@2", slaSummary2.getId());
        assertEquals(0, slaSummary2.getEventProcessed());
        assertEquals(-1, slaSummary2.getActualDuration());

        coordAction.setStatusStr("FAILED");
        coordAction2.setStatusStr("FAILED");
        CoordActionQueryExecutor.getInstance().executeUpdate(CoordActionQuery.UPDATE_COORD_ACTION_FOR_START, coordAction);
        CoordActionQueryExecutor.getInstance().executeUpdate(CoordActionQuery.UPDATE_COORD_ACTION_FOR_START, coordAction2);

        slaCalcMemory.init(Services.get().get(ConfigurationService.class).getConf());
        slaCalcMemory.updateAllSlaStatus();

        slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId1);
        slaSummary2 = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId2);

        assertEquals("coordActionId-C@1", slaSummary.getId());
        assertEquals(8, slaSummary.getEventProcessed());
        assertEquals("coordActionId-C@2", slaSummary2.getId());
        assertEquals(8, slaSummary2.getEventProcessed());

    }
    @Test
    public void testSLAEvents1() throws Exception {
        SLACalculatorMemory slaCalcMemory = new SLACalculatorMemory();
        EventHandlerService ehs = Services.get().get(EventHandlerService.class);
        slaCalcMemory.init(Services.get().get(ConfigurationService.class).getConf());
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

        job1.setStatusStr(WorkflowJob.Status.SUSPENDED.toString());
        job1.setLastModifiedTime(new Date());
        WorkflowJobQueryExecutor.getInstance().executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_MODTIME, job1);

        slaCalcMemory.addJobStatus(jobId, WorkflowJob.Status.SUSPENDED.toString(), EventStatus.SUSPEND,
                sdf.parse("2012-01-01"), null);
        slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId);
        assertEquals(WorkflowJob.Status.SUSPENDED.toString(), slaSummary.getJobStatus());

        assertEquals(5, slaSummary.getEventProcessed());
        job1.setStatusStr(WorkflowJob.Status.SUCCEEDED.toString());
        job1.setLastModifiedTime(new Date());
        job1.setStartTime(sdf.parse("2012-01-01"));
        job1.setEndTime(sdf.parse("2012-01-02"));
        WorkflowJobQueryExecutor.getInstance().executeUpdate(
                WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_INSTANCE_MOD_START_END, job1);

        slaCalcMemory.addJobStatus(jobId, WorkflowJob.Status.SUCCEEDED.toString(), EventStatus.SUCCESS,
                sdf.parse("2012-01-01"), sdf.parse("2012-01-02"));

        assertEquals(3, ehs.getEventQueue().size());
        slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId);
        // All events processed and actual times stored (1000)
        assertEquals(8, slaSummary.getEventProcessed());
        assertEquals(SLAStatus.MET, slaSummary.getSLAStatus());
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
        slaCalcMemory.init(Services.get().get(ConfigurationService.class).getConf());

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
        job1.setStatus(WorkflowJob.Status.SUCCEEDED);
        job1.setStartTime(sdf.parse("2012-01-01"));
        job1.setEndTime(sdf.parse("2012-01-02"));
        WorkflowJobQueryExecutor.getInstance().executeUpdate(
                WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_INSTANCE_MOD_START_END, job1);

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
        WorkflowJobBean job2 = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);

        slaRegBean = _createSLARegistration(job2.getId(), AppType.WORKFLOW_JOB);
        slaRegBean.setExpectedStart(new Date(System.currentTimeMillis() - 1 * 1 * 3600 * 1000));
        slaRegBean.setExpectedEnd(new Date(System.currentTimeMillis() + 2 * 1 * 3600 * 1000));

        jobId = slaRegBean.getId();
        slaCalcMemory.addRegistration(jobId, slaRegBean);
        assertEquals(1, slaCalcMemory.size());
        job2.setStatus(WorkflowJob.Status.KILLED);
        job2.setEndTime(sdf.parse("2012-01-02"));
        WorkflowJobQueryExecutor.getInstance().executeUpdate(
                WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_INSTANCE_MOD_START_END, job2);

        slaCalcMemory.addJobStatus(job2.getId(), WorkflowJob.Status.KILLED.toString(), EventStatus.FAILURE, null,
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
        slaCalcMemory.init(Services.get().get(ConfigurationService.class).getConf());
        WorkflowJobBean job1 = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);
        SLARegistrationBean slaRegBean = _createSLARegistration(job1.getId(), AppType.WORKFLOW_JOB);
        Date startTime = new Date(System.currentTimeMillis() - 1 * 1 * 3600 * 1000); // 1 hour back
        slaRegBean.setExpectedStart(startTime);
        slaRegBean.setExpectedDuration(2* 3600 * 1000); //to avoid duration miss
        slaRegBean.setExpectedEnd(new Date(System.currentTimeMillis() + 1 * 1 * 3600 * 1000)); // 1 hour ahead
        String jobId = slaRegBean.getId();
        slaCalcMemory.addRegistration(slaRegBean.getId(), slaRegBean);
        slaCalcMemory.updateJobSla(jobId);
        SLASummaryBean slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId);

        assertEquals(1, slaSummary.getEventProcessed());
        assertEquals(SLAStatus.NOT_STARTED, slaSummary.getSLAStatus());
        job1.setStatus(WorkflowJob.Status.RUNNING);
        job1.setStartTime(startTime);
        WorkflowJobQueryExecutor.getInstance().executeUpdate(
                WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_INSTANCE_MOD_START_END, job1);

        slaCalcMemory.addJobStatus(jobId, WorkflowJob.Status.RUNNING.toString(), EventStatus.STARTED,
                new Date(System.currentTimeMillis()), null);

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
        slaCalcMemory.init(Services.get().get(ConfigurationService.class).getConf());
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

        job1.setId(job1.getId());
        job1.setStatus(WorkflowJob.Status.SUCCEEDED);
        job1.setStartTime(new Date(System.currentTimeMillis()));
        job1.setEndTime(new Date(System.currentTimeMillis() + 1 * 1 * 3600 * 1000));
        WorkflowJobQueryExecutor.getInstance().executeUpdate(
                WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_INSTANCE_MOD_START_END, job1);

        slaCalcMemory.addJobStatus(jobId, WorkflowJob.Status.SUCCEEDED.toString(), EventStatus.SUCCESS,
                new Date(System.currentTimeMillis()), new Date(System.currentTimeMillis() + 1 * 1 * 3600 * 1000));
        slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId);

        // Only Duration sla should be processed as end is already processed
        // (110)
        assertEquals(8, slaSummary.getEventProcessed());
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
            slaCalcMemory.init(Services.get().get(ConfigurationService.class).getConf());
            WorkflowJobBean job1 = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);
            SLARegistrationBean slaRegBean = _createSLARegistration(job1.getId(), AppType.WORKFLOW_JOB);
            Date startTime = new Date(System.currentTimeMillis() - 1 * 1 * 3600 * 1000);
            slaRegBean.setExpectedStart(startTime); // 1 hour back
            slaRegBean.setExpectedDuration(1000);
            slaRegBean.setExpectedEnd(new Date(System.currentTimeMillis() - 1 * 1 * 3600 * 1000));
            String jobId = slaRegBean.getId();
            slaCalcMemory.addRegistration(slaRegBean.getId(), slaRegBean);
            slaCalcMemory.updateJobSla(jobId);

            job1.setId(job1.getId());
            job1.setStatus(WorkflowJob.Status.RUNNING);
            job1.setStartTime(new Date(System.currentTimeMillis() - 3600 * 1000));
            WorkflowJobQueryExecutor.getInstance().executeUpdate(
                    WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_INSTANCE_MOD_START_END, job1);

            slaCalcMemory.addJobStatus(jobId, WorkflowJob.Status.RUNNING.toString(), EventStatus.STARTED, new Date(
                    System.currentTimeMillis() - 3600 * 1000), null);
            SLASummaryBean slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId);
            // The actual end times are not stored, but sla's processed so (111)
            assertEquals(7, slaSummary.getEventProcessed());
            // Moved from map to history set
            assertEquals(0, slaCalcMemory.size());
            // Add terminal state event so actual end time is stored

            job1.setId(job1.getId());
            job1.setStatus(WorkflowJob.Status.SUCCEEDED);
            job1.setEndTime(new Date(System.currentTimeMillis() - 3600 * 1000));
            job1.setStartTime(new Date(System.currentTimeMillis()));
            WorkflowJobQueryExecutor.getInstance().executeUpdate(
                    WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_INSTANCE_MOD_START_END, job1);

            WorkflowJobQueryExecutor.getInstance().executeUpdate(
                WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_INSTANCE_MOD_START_END, job1);

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
        SLACalculatorMemory slaCalcMemory = new SLACalculatorMemory();
        slaCalcMemory.init(Services.get().get(ConfigurationService.class).getConf());
        WorkflowJobBean job1 = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);
        SLARegistrationBean slaRegBean = _createSLARegistration(job1.getId(), AppType.WORKFLOW_JOB);
        Date startTime = new Date(System.currentTimeMillis() - 1 * 1 * 3600 * 1000);
        slaRegBean.setExpectedStart(startTime); // 1 hour back
        slaRegBean.setExpectedDuration(1000);
        slaRegBean.setExpectedEnd(new Date(System.currentTimeMillis() - 1 * 1 * 3600 * 1000));
        String jobId = slaRegBean.getId();
        slaCalcMemory.addRegistration(slaRegBean.getId(), slaRegBean);
        slaCalcMemory.updateJobSla(jobId);
        job1.setStatusStr("RUNNING");
        job1.setLastModifiedTime(new Date());
        job1.setStartTime(startTime);
        WorkflowJobQueryExecutor.getInstance().executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_INSTANCE_MOD_START_END, job1);

        slaCalcMemory.addJobStatus(jobId, WorkflowJob.Status.RUNNING.toString(), EventStatus.STARTED, new Date(
                System.currentTimeMillis() - 3600 * 1000), null);
        SLASummaryBean slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId);
        // The actual end times are not stored, but sla's processed so (111)
        assertEquals(7, slaSummary.getEventProcessed());
        assertTrue(slaCalcMemory.isJobIdInHistorySet(job1.getId()));
        job1.setStatusStr("SUCCEEDED");
        job1.setLastModifiedTime(new Date());
        job1.setStartTime(startTime);
        WorkflowJobQueryExecutor.getInstance().executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_INSTANCE_MOD_START_END, job1);
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

    @SuppressWarnings("serial")
    public void testDisablingAlertsEvents() throws Exception {
        SLAService slaService = Services.get().get(SLAService.class);
        EventHandlerService ehs = Services.get().get(EventHandlerService.class);
        SLACalculator slaCalculator = slaService.getSLACalculator();
        // create dummy sla records and coord action records
        String id1 = _setupSlaMap(slaCalculator, "00020-1234567-wrkf-C", 1);
        String id2 = _setupSlaMap(slaCalculator, "00020-1234567-wrkf-C", 2);

        SLACalcStatus slaCalcObj1 = slaCalculator.get(id1);
        assertFalse(slaCalcObj1.getSLAConfigMap().containsKey(OozieClient.SLA_DISABLE_ALERT));
        SLACalcStatus slaCalcObj2 = slaCalculator.get(id2);
        assertFalse(slaCalcObj2.getSLAConfigMap().containsKey(OozieClient.SLA_DISABLE_ALERT));
        slaCalculator.updateAllSlaStatus();
        assertTrue(ehs.getEventQueue().size() > 0);

        // check that SLACalculator sends no event
        ehs.getEventQueue().clear();
        SLASummaryBean persistentSla = new SLASummaryBean(slaCalcObj1);
        // reset eventProcessed for the sla calc objects
        persistentSla.setEventProcessed(0);
        SLASummaryQueryExecutor.getInstance().executeUpdate(SLASummaryQuery.UPDATE_SLA_SUMMARY_EVENTPROCESSED,
                persistentSla);
        persistentSla = new SLASummaryBean(slaCalcObj2);
        persistentSla.setEventProcessed(0);
        SLASummaryQueryExecutor.getInstance().executeUpdate(SLASummaryQuery.UPDATE_SLA_SUMMARY_EVENTPROCESSED,
                persistentSla);
        // CASE I : list of sla ids, no new params
        slaService.enableChildJobAlert(Arrays.asList(id1, id2));
        slaCalculator.updateAllSlaStatus();
        assertTrue(ehs.getEventQueue().isEmpty());

        // CASE II : ALL
        _setupSlaMap(slaCalculator, "00020-1234567-wrkf-C", 3);
        _setupSlaMap(slaCalculator, "00020-1234567-wrkf-C", 4);
        slaCalculator.enableChildJobAlert(Arrays.asList("00020-1234567-wrkf-C"));
        slaCalculator.updateAllSlaStatus();
        assertFalse(ehs.getEventQueue().isEmpty());

        // CASE III : resume w/ new params
        final String id5 = _setupSlaMap(slaCalculator, "00020-1234567-wrkf-C", 5);
        Date now = new Date();
        now.setTime(now.getTime() - 10 * 60 * 1000);
       final  String newParams = RestConstants.SLA_NOMINAL_TIME + "=" + DateUtils.formatDateOozieTZ(now) + ";"
                + RestConstants.SLA_SHOULD_END + "=5";
        slaCalculator.changeDefinition(new ArrayList<Pair<String,Map<String,String>>>(){
            {
            add(new Pair<String,Map<String,String>>(id5, JobUtils.parseChangeValue(newParams)));
            }
        });

        slaCalculator.updateAllSlaStatus();
        assertTrue(ehs.getEventQueue().size() > 0);

    }

    private String _setupSlaMap(SLACalculator slaCalculator, String id, int actionNum) throws Exception {
        CoordinatorActionBean action = addRecordToCoordActionTable(id, actionNum,
                CoordinatorAction.Status.TIMEDOUT, "coord-action-get.xml", 0);
        action.setExternalId(null);
        CoordActionQueryExecutor.getInstance().executeUpdate(CoordActionQuery.UPDATE_COORD_ACTION_FOR_START, action);
        SLARegistrationBean slaRegBean = _createSLARegistration(action.getId(), AppType.COORDINATOR_ACTION);
        Date startTime = new Date(System.currentTimeMillis() - 2 * 3600 * 1000);
        slaRegBean.setExpectedStart(startTime); // 2 hours back
        slaRegBean.setExpectedDuration(1000);
        slaRegBean.setExpectedEnd(new Date(System.currentTimeMillis() - 1 * 3600 * 1000)); // 1 hr back
        slaRegBean.setParentId(id);
        slaCalculator.addRegistration(slaRegBean.getId(), slaRegBean);
        return action.getId();
    }
    @Test
    public void testEventOutOfOrder() throws Exception {
        SLACalculatorMemory slaCalcMemory = new SLACalculatorMemory();
        slaCalcMemory.init(Services.get().get(ConfigurationService.class).getConf());
        WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        SLARegistrationBean slaRegBean = _createSLARegistration(wfJob.getId(), AppType.WORKFLOW_JOB);
        Date startTime = new Date(System.currentTimeMillis() + 1 * 1 * 3600 * 1000); // 1 hour ahead
        slaRegBean.setExpectedStart(startTime);
        slaRegBean.setExpectedDuration(3600 * 1000);
        slaRegBean.setExpectedEnd(new Date(System.currentTimeMillis() - 1 * 1 * 3600 * 1000)); // 1 hour back
        String jobId = slaRegBean.getId();
        slaCalcMemory.addRegistration(slaRegBean.getId(), slaRegBean);
        slaCalcMemory.updateJobSla(jobId);
        SLASummaryBean slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId);
        slaRegBean = SLARegistrationQueryExecutor.getInstance().get(SLARegQuery.GET_SLA_REG_ALL, jobId);
        assertEquals(slaSummary.getJobStatus(), WorkflowInstance.Status.RUNNING.toString());

        wfJob.setStatus(WorkflowJob.Status.SUCCEEDED);
        wfJob.setEndTime(new Date(System.currentTimeMillis() + 1 * 1 * 3600 * 1000));
        WorkflowJobQueryExecutor.getInstance().executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_INSTANCE_MOD_END, wfJob);
        slaCalcMemory.addJobStatus(jobId, WorkflowJob.Status.SUCCEEDED.toString(), EventStatus.SUCCESS,
                new Date(System.currentTimeMillis()), new Date(System.currentTimeMillis() + 1 * 1 * 3600 * 1000));
        slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId);
        assertEquals(slaSummary.getJobStatus(), WorkflowInstance.Status.SUCCEEDED.toString());

        slaCalcMemory.addJobStatus(jobId, WorkflowJob.Status.RUNNING.toString(), EventStatus.SUCCESS,
                new Date(System.currentTimeMillis()), new Date(System.currentTimeMillis() + 1 * 1 * 3600 * 1000));

        slaSummary = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId);
        assertEquals(slaSummary.getJobStatus(), WorkflowInstance.Status.SUCCEEDED.toString());
    }

    public void testWFEndNotCoord() throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd");
        SLACalculatorMemory slaCalcMemory = new SLACalculatorMemory();
        slaCalcMemory.init(Services.get().get(ConfigurationService.class).getConf());
        SLARegistrationBean slaRegBean = _createSLARegistration("job-C@1", AppType.COORDINATOR_ACTION);
        String coordActionId = slaRegBean.getId();
        slaRegBean.setExpectedEnd(sdf.parse("2013-03-07"));
        slaRegBean.setExpectedStart(sdf.parse("2012-03-07"));
        slaCalcMemory.addRegistration(coordActionId, slaRegBean);
        SLACalcStatus calc1 = slaCalcMemory.get(coordActionId);
        calc1.setEventProcessed(1);
        calc1.setSLAStatus(SLAEvent.SLAStatus.IN_PROCESS);
        calc1.setJobStatus(WorkflowAction.Status.RUNNING.name());
        calc1.setLastModifiedTime(new Date());
        SLASummaryBean slaSummaryBean = new SLASummaryBean(calc1);

        SLASummaryQueryExecutor.getInstance().executeUpdate(SLASummaryQuery.UPDATE_SLA_SUMMARY_ALL, slaSummaryBean);

        // Simulate a lost failed event
        CoordinatorActionBean coordAction = new CoordinatorActionBean();
        coordAction.setId(coordActionId);
        coordAction.setStatus(CoordinatorAction.Status.RUNNING);
        coordAction.setLastModifiedTime(sdf.parse("2013-02-07"));
        coordAction.setExternalId("wf_job-W");
        CoordActionInsertJPAExecutor caInsertCmd = new CoordActionInsertJPAExecutor(coordAction);
        jpaService.execute(caInsertCmd);
        WorkflowJobBean wjb = new WorkflowJobBean();
        wjb.setId("wf_job-W");
        wjb.setStartTime(sdf.parse("2012-02-07"));
        wjb.setLastModifiedTime(new Date());
        wjb.setStatus(WorkflowJob.Status.SUCCEEDED);
        WorkflowJobQueryExecutor.getInstance().insert(wjb);

        calc1 = slaCalcMemory.get(coordActionId);
        slaCalcMemory.updateJobSla(coordActionId);
        slaSummaryBean = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, coordActionId);
        //cord action is running and wf job is completed
        assertEquals(slaSummaryBean.getJobStatus(), WorkflowInstance.Status.RUNNING.name());

        coordAction.setStatus(CoordinatorAction.Status.SUCCEEDED);
        CoordActionQueryExecutor.getInstance().executeUpdate(CoordActionQuery.UPDATE_COORD_ACTION_STATUS_PENDING_TIME, coordAction);

        slaCalcMemory.addJobStatus(coordActionId, WorkflowJob.Status.SUCCEEDED.toString(), EventStatus.SUCCESS,
                sdf.parse("2012-02-07"), sdf.parse("2012-03-07"));

        slaSummaryBean = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, coordActionId);
        assertEquals(slaSummaryBean.getJobStatus(), WorkflowInstance.Status.SUCCEEDED.toString());
    }

    public void testSingleAddUpdateRemoveInstrumentedCorrectly() throws Exception {
        SLACalculatorMemory slaCalcMemory = new SLACalculatorMemory();
        slaCalcMemory.init(Services.get().get(ConfigurationService.class).getConf());

        WorkflowJobBean job1 = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);
        SLARegistrationBean slaRegBean = _createSLARegistration(job1.getId(), AppType.WORKFLOW_JOB);
        Date startTime = new Date(System.currentTimeMillis() - 1 * 1 * 3600 * 1000);
        slaRegBean.setExpectedStart(startTime); // 1 hour back
        slaRegBean.setExpectedDuration(1000);
        slaRegBean.setExpectedEnd(new Date(System.currentTimeMillis() - 1 * 1 * 3600 * 1000));
        String jobId = slaRegBean.getId();
        slaCalcMemory.addRegistration(slaRegBean.getId(), slaRegBean);

        long slaMapSize = instrumentation.getCounters().get(SLACalculatorMemory.INSTRUMENTATION_GROUP).
                get(SLACalculatorMemory.SLA_MAP).getValue();

        assertEquals("SLA map size after add should be 1", 1, slaMapSize);

        slaCalcMemory.updateJobSla(jobId);

        slaMapSize = instrumentation.getCounters().get(SLACalculatorMemory.INSTRUMENTATION_GROUP).
                get(SLACalculatorMemory.SLA_MAP).getValue();
        assertEquals("SLA map size after update should be 1", 1, slaMapSize);

        slaCalcMemory.removeRegistration(jobId);

        slaMapSize = instrumentation.getCounters().get(SLACalculatorMemory.INSTRUMENTATION_GROUP).
                get(SLACalculatorMemory.SLA_MAP).getValue();
        assertEquals("SLA map size after remove should be 0", 0, slaMapSize);
    }

    public void testAddMultipleRestartRemoveMultipleInstrumentedCorrectly() throws Exception {
        SLACalculatorMemory slaCalcMemory = new SLACalculatorMemory();
        slaCalcMemory.init(Services.get().get(ConfigurationService.class).getConf());
        SLARegistrationBean slaRegBean1 = _createSLARegistration("job-1-W", AppType.WORKFLOW_JOB);
        String jobId1 = slaRegBean1.getId();
        SLARegistrationBean slaRegBean2 = _createSLARegistration("job-2-W", AppType.WORKFLOW_JOB);
        String jobId2 = slaRegBean2.getId();
        SLARegistrationBean slaRegBean3 = _createSLARegistration("job-3-W", AppType.WORKFLOW_JOB);
        String jobId3 = slaRegBean3.getId();
        List<String> idList = new ArrayList<String>();
        idList.add(slaRegBean1.getId());
        idList.add(slaRegBean2.getId());
        idList.add(slaRegBean3.getId());
        createWorkflow(idList);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd");
        slaRegBean1.setAppName("app-name");
        slaRegBean1.setExpectedDuration(10000);
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

        Date startTime = new Date(System.currentTimeMillis() - 1 * 1 * 3600 * 1000); // 1 hour back
        Date endTime = new Date(System.currentTimeMillis() + 2 * 1 * 3600 * 1000); // 1 hour back

        slaRegBean3.setExpectedStart(startTime);
        slaRegBean3.setExpectedEnd(endTime);

        slaCalcMemory.addRegistration(jobId1, slaRegBean1);
        slaCalcMemory.addRegistration(jobId2, slaRegBean2);
        slaCalcMemory.addRegistration(jobId3, slaRegBean3);

        long slaMapSize = instrumentation.getCounters().get(SLACalculatorMemory.INSTRUMENTATION_GROUP).
                get(SLACalculatorMemory.SLA_MAP).getValue();

        assertEquals("SLA map size after add all should be 3", 3, slaMapSize);

        slaCalcMemory.updateAllSlaStatus();

        slaMapSize = instrumentation.getCounters().get(SLACalculatorMemory.INSTRUMENTATION_GROUP).
                get(SLACalculatorMemory.SLA_MAP).getValue();
        assertEquals("SLA map size after update all should be 2. An instance of SLACalcStatus was removed", 2, slaMapSize);

        slaCalcMemory.removeRegistration(jobId1);
        slaCalcMemory.removeRegistration(jobId2);
        slaCalcMemory.removeRegistration(jobId3);

        slaMapSize = instrumentation.getCounters().get(SLACalculatorMemory.INSTRUMENTATION_GROUP).
                get(SLACalculatorMemory.SLA_MAP).getValue();
        assertEquals("SLA map size after remove all should be 0", 0, slaMapSize);
    }


    public void testWhenSLARegistrationIsAddedBeanIsStoredCorrectly() throws Exception {
        SLACalculatorMemory slaCalcMemory = new SLACalculatorMemory();
        slaCalcMemory.init(Services.get().get(ConfigurationService.class).getConf());
        SLARegistrationBean slaRegBean = _createSLARegistration("job-1-W", AppType.WORKFLOW_JOB);
        slaCalcMemory.addRegistration(slaRegBean.getId(), slaRegBean);
        Assert.assertNotNull(slaCalcMemory.get(slaRegBean.getId()));
        Assert.assertEquals(slaRegBean, slaCalcMemory.get(slaRegBean.getId()).getSLARegistrationBean());
    }

    public void testWhenSLARegistrationIsAddedAndAllDBCallsAreDisruptedBeanIsNotStored() throws Exception {
        SLACalculatorMemory slaCalcMemory = new SLACalculatorMemory();
        slaCalcMemory.init(Services.get().get(ConfigurationService.class).getConf());
        SLARegistrationBean slaRegBean = _createSLARegistration("job-1-W", AppType.WORKFLOW_JOB);

        try {
            FailingDBHelperForTest.setDbPredicate(new SLARegistrationInsertUpdatePredicate());
            prepareFailingDB();
            slaCalcMemory.addRegistration(slaRegBean.getId(), slaRegBean);
            fail("Expected JPAExecutorException not thrown");
        } catch (JPAExecutorException ex) {
            Assert.assertNull(slaCalcMemory.get(slaRegBean.getId()));
        } finally {
            FailingDBHelperForTest.resetDbPredicate();
            System.clearProperty(FailingHSQLDBDriverWrapper.USE_FAILING_DRIVER);
        }

    }

    public void testWhenSLARegistrationIsUpdatedBeanIsStoredCorrectly() throws Exception {
        SLACalculatorMemory slaCalcMemory = new SLACalculatorMemory();
        slaCalcMemory.init(Services.get().get(ConfigurationService.class).getConf());
        final String jobId = "job-1-W";
        SLARegistrationBean slaRegBean = _createSLARegistration(jobId, AppType.WORKFLOW_JOB);
        SLARegistrationBean slaRegBean2 = _createSLARegistration(jobId, AppType.WORKFLOW_JOB);

        addAndUpdateRegistration(slaCalcMemory, jobId, slaRegBean, slaRegBean2);

        Assert.assertNotNull(slaCalcMemory.get(jobId));
        Assert.assertEquals("The updated SLA registration bean should be in the cache",
                slaRegBean2, slaCalcMemory.get(jobId).getSLARegistrationBean());
    }

    public void testWhenSLARegistrationIsUpdatedAndAllDBCallsAreDisruptedBeanIsNotStored() throws Exception {
        SLACalculatorMemory slaCalcMemory = new SLACalculatorMemory();
        slaCalcMemory.init(Services.get().get(ConfigurationService.class).getConf());
        final String jobId = "job-1-W";
        SLARegistrationBean slaRegBean = _createSLARegistration(jobId, AppType.WORKFLOW_JOB);
        SLARegistrationBean slaRegBean2 = _createSLARegistration(jobId, AppType.WORKFLOW_JOB);
        final int expectedDuration = 1000;
        slaRegBean2.setExpectedDuration(expectedDuration);

        try {
            addAndUpdateRegistrationWithDBCrushSimulation(slaCalcMemory, jobId, slaRegBean, slaRegBean2);
            fail("Expected JPAExecutorException not thrown");
        } catch (JPAExecutorException ex) {
            Assert.assertNotNull(slaCalcMemory.get(slaRegBean.getId()));
            // the update failed
            Assert.assertEquals(slaRegBean, slaCalcMemory.get(jobId).getSLARegistrationBean());
        } finally {
            FailingDBHelperForTest.resetDbPredicate();
            System.clearProperty(FailingHSQLDBDriverWrapper.USE_FAILING_DRIVER);
        }
    }

    public void testWhenSLARegistrationExistsWithoutSLASummaryUpdateSLARetries() throws Exception {
        final SLACalculatorMemory slaCalculatorMemory = new SLACalculatorMemory();
        slaCalculatorMemory.init(Services.get().get(ConfigurationService.class).getConf());
        final String jobId = "job-1-W";
        final SLARegistrationBean slaRegistration = _createSLARegistration(jobId, AppType.WORKFLOW_JOB);
        slaCalculatorMemory.addRegistration(jobId, slaRegistration);

        updateJobSlaFailing(slaCalculatorMemory, jobId,
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Assert.assertNotNull("after first update, SLACalcStatus should still be present",
                                    slaCalculatorMemory.get(slaRegistration.getId()));
                            Assert.assertEquals("updating SLA_REGISTRATION should have been failed",
                                    slaRegistration,
                                    slaCalculatorMemory.get(jobId).getSLARegistrationBean());
                            Assert.assertEquals("SLACalcStatus.retryCount should have been increased",
                                    1, slaCalculatorMemory.get(jobId).getRetryCount());
                        } catch (JPAExecutorException ignored) {
                        }
                    }
                });

        updateJobSlaFailing(slaCalculatorMemory, jobId,
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Assert.assertNotNull("after second update, SLACalcStatus should still be present",
                                    slaCalculatorMemory.get(slaRegistration.getId()));
                            Assert.assertEquals("updating SLA_REGISTRATION should have been failed",
                                    slaRegistration,
                                    slaCalculatorMemory.get(jobId).getSLARegistrationBean());
                            Assert.assertEquals("SLACalcStatus.retryCount should have been increased",
                                    2, slaCalculatorMemory.get(jobId).getRetryCount());
                        } catch (JPAExecutorException ignored) {
                        }
                    }
                }
        );

        updateJobSlaFailing(slaCalculatorMemory, jobId,
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Assert.assertNotNull("after third update, SLACalcStatus should still be present",
                                    slaCalculatorMemory.get(slaRegistration.getId()));
                            Assert.assertEquals("updating SLA_REGISTRATION should have been failed",
                                    slaRegistration,
                                    slaCalculatorMemory.get(jobId).getSLARegistrationBean());
                            Assert.assertEquals("SLACalcStatus.retryCount should have been increased",
                                    3, slaCalculatorMemory.get(jobId).getRetryCount());
                        } catch (JPAExecutorException ignored) {
                        }
                    }
                }
        );

        updateJobSlaFailing(slaCalculatorMemory, jobId,
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Assert.assertNull("after fourth update, SLACalcStatus should no more be present",
                                    slaCalculatorMemory.get(slaRegistration.getId()));
                        } catch (JPAExecutorException ignored) {
                        }
                    }
                }
        );
    }

    private void updateJobSlaFailing(final SLACalculatorMemory slaCalculatorMemory,
                                     final String jobId,
                                     final Runnable assertsWhenFailing)
            throws Exception {
        try {
            FailingDBHelperForTest.setDbPredicate(new SLASummarySelectPredicate(1));
            prepareFailingDB();

            slaCalculatorMemory.updateJobSla(jobId);
        }
        catch (final JPAExecutorException e) {
            assertsWhenFailing.run();
        }
        finally {
            FailingDBHelperForTest.resetDbPredicate();
            System.clearProperty(FailingHSQLDBDriverWrapper.USE_FAILING_DRIVER);
        }
    }

    private void addAndUpdateRegistration(final SLACalculatorMemory slaCalcMemory, final String jobId,
                                          final SLARegistrationBean slaRegBean, final SLARegistrationBean slaRegBean2)
            throws JPAExecutorException {
        slaCalcMemory.addRegistration(jobId, slaRegBean);
        slaCalcMemory.updateRegistration(jobId, slaRegBean2);
    }

    private void addAndUpdateRegistrationWithDBCrushSimulation(final SLACalculatorMemory slaCalcMemory, final String jobId,
                                                               final SLARegistrationBean slaRegBean,
                                                               final SLARegistrationBean slaRegBean2) throws Exception {
        slaCalcMemory.addRegistration(jobId, slaRegBean);
        FailingDBHelperForTest.setDbPredicate(new SLARegistrationInsertUpdatePredicate());
        prepareFailingDB();
        slaCalcMemory.updateRegistration(jobId, slaRegBean2);
    }

    private void prepareFailingDB() throws ServiceException {
        System.setProperty(FailingHSQLDBDriverWrapper.USE_FAILING_DRIVER, Boolean.TRUE.toString());
        Configuration conf = services.get(ConfigurationService.class).getConf();
        conf.set(JPAService.CONF_DRIVER, AlwaysFailingHSQLDriverMapper.class.getCanonicalName());
        conf.setInt(JPAService.MAX_RETRY_COUNT, 2);
        jpaService.destroy();
        jpaService.init(services);
    }

    static class SLARegistrationInsertUpdatePredicate extends DmlPredicate {
        SLARegistrationInsertUpdatePredicate() {
            super("SLA_REGISTRATION", Sets.newHashSet("INSERT INTO ", "UPDATE "));
        }
    }

    static class SLASummarySelectPredicate extends DmlPredicate {
        private int remainingSuccessfulAttempts;
        SLASummarySelectPredicate(final int remainingSuccessfulAttempts) {
            super("SLA_SUMMARY", Sets.newHashSet("SELECT "));
            this.remainingSuccessfulAttempts = remainingSuccessfulAttempts;
        }

        @Override
        public boolean apply(@Nullable String input) {
            if (super.apply(input)) {
                if (remainingSuccessfulAttempts <= 0) {
                    return true;
                }
                else {
                    remainingSuccessfulAttempts--;
                    return false;
                }
            }
            else {
                return false;
            }
        }
    }

    static class DmlPredicate implements com.google.common.base.Predicate<String> {
        private final String tableName;
        private final Set<String> operationPrefixes;

        DmlPredicate(final String tableName, final Set<String> operationPrefixes) {
            this.tableName = tableName;
            this.operationPrefixes = operationPrefixes;
        }

        @Override
        public boolean apply(@Nullable String input) {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(input));
            boolean operationMatch = false;
            for (String s: operationPrefixes) {
                if (input.startsWith(s)) {
                    operationMatch = true;
                    break;
                }
            }
            return operationMatch && input.toUpperCase().contains(tableName);
        }
    }
}
