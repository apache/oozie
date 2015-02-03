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

package org.apache.oozie.executor.jpa;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.WorkflowsInfo;
import org.apache.oozie.XException;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor.WorkflowJobQuery;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.workflow.WorkflowInstance;

public class TestWorkflowsJobGetJPAExecutor extends XDataTestCase {
    Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testWfJobsGet() throws Exception {
        WorkflowJobBean workflowJob = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);
        addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);
        _testGetWFInfos();
        _testGetWFInfoForId(workflowJob);
        System.out.println("testWfJobsGet Successful");
    }

    private void _testGetWFInfos() throws JPAExecutorException {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        WorkflowsJobGetJPAExecutor wfGetCmd = new WorkflowsJobGetJPAExecutor(filter, 1, 1);
        WorkflowsInfo wfInfo = jpaService.execute(wfGetCmd);
        System.out.println("got WorkflowsInfo " + wfInfo.getLen());
        List<WorkflowJobBean> wfBeans = wfInfo.getWorkflows();
        assertEquals(1, wfBeans.size());

        filter = new HashMap<String, List<String>>();
        wfGetCmd = new WorkflowsJobGetJPAExecutor(filter, 1, 2);
        wfInfo = jpaService.execute(wfGetCmd);
        wfBeans = wfInfo.getWorkflows();
        assertEquals(2, wfBeans.size());
    }

    private void _testGetWFInfoForId(WorkflowJobBean wfBean) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        wfBean.setParentId("test-parent-C");
        WorkflowJobQueryExecutor.getInstance().executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW_PARENT_MODIFIED, wfBean);
        List<String> jobIdList = new ArrayList<String>();
        jobIdList.add(wfBean.getId());
        filter.put(OozieClient.FILTER_ID, jobIdList);
        WorkflowsJobGetJPAExecutor wfGetCmd = new WorkflowsJobGetJPAExecutor(filter, 1, 1);
        WorkflowsInfo wfInfo = jpaService.execute(wfGetCmd);
        assertNotNull(wfInfo);
        assertEquals(wfInfo.getWorkflows().size(), 1);
        WorkflowJobBean retBean = wfInfo.getWorkflows().get(0);
        assertEquals(wfBean.getId(), retBean.getId());
        assertEquals(wfBean.getAppName(), retBean.getAppName());
        assertEquals(wfBean.getStatusStr(), retBean.getStatusStr());
        assertEquals(wfBean.getRun(), retBean.getRun());
        assertEquals(wfBean.getUser(), retBean.getUser());
        assertEquals(wfBean.getGroup(), retBean.getGroup());
        assertEquals(wfBean.getCreatedTime(), retBean.getCreatedTime());
        assertEquals(wfBean.getStartTime(), retBean.getStartTime());
        assertEquals(wfBean.getLastModifiedTime(), retBean.getLastModifiedTime());
        assertEquals(wfBean.getEndTime(), retBean.getEndTime());
        assertEquals(wfBean.getExternalId(), retBean.getExternalId());
        assertEquals(wfBean.getParentId(), retBean.getParentId());
    }

    public void testWfJobsGetWithCreatedTime() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        Date createdTime1 = DateUtils.parseDateUTC("2012-01-01T10:00Z");
        Date createdTime2 = DateUtils.parseDateUTC("2012-02-01T10:00Z");
        Date createdTime3 = DateUtils.parseDateUTC("2012-03-01T10:00Z");
        Date createdTime4 = DateUtils.parseDateUTC("2012-04-01T10:00Z");
        Date createdTime5 = org.apache.commons.lang.time.DateUtils.addHours(new Date(), -5);
        WorkflowJobBean wrkJob = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);
        wrkJob.setCreatedTime(createdTime1);
        WorkflowJobQueryExecutor.getInstance().executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW, wrkJob);
        wrkJob = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);
        wrkJob.setCreatedTime(createdTime2);
        WorkflowJobQueryExecutor.getInstance().executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW, wrkJob);
        wrkJob = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);
        wrkJob.setCreatedTime(createdTime3);
        WorkflowJobQueryExecutor.getInstance().executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW, wrkJob);
        wrkJob = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);
        wrkJob.setCreatedTime(createdTime4);
        WorkflowJobQueryExecutor.getInstance().executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW, wrkJob);
        WorkflowJobQueryExecutor.getInstance().executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW, wrkJob);
        wrkJob = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);
        wrkJob.setCreatedTime(createdTime5);
        WorkflowJobQueryExecutor.getInstance().executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW, wrkJob);

        // filter 2012-02-01T00:00Z <= createdTimeStamp <= 2012-03-02T00:00Z
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        List<String> timeList = new ArrayList<String>();
        timeList.add("2012-02-01T00:00Z");
        filter.put(OozieClient.FILTER_CREATED_TIME_START, timeList);
        timeList = new ArrayList<String>();
        timeList.add("2012-03-02T00:00Z");
        filter.put(OozieClient.FILTER_CREATED_TIME_END, timeList);
        WorkflowsJobGetJPAExecutor wfGetCmd = new WorkflowsJobGetJPAExecutor(filter, 1, 10);
        WorkflowsInfo wfInfo = jpaService.execute(wfGetCmd);
        assertNotNull(wfInfo);
        assertEquals(2, wfInfo.getWorkflows().size());
        for(WorkflowJobBean jobBean: wfInfo.getWorkflows()) {
            assertTrue(jobBean.getCreatedTime().equals(createdTime2) || jobBean.getCreatedTime().equals(createdTime3));
        }

        //createdTime specified in wrong format
        filter = new HashMap<String, List<String>>();
        timeList = new ArrayList<String>();
        timeList.add("2012-02-01T00:00"); // not correct ISO8601_UTC_MASK format
        filter.put(OozieClient.FILTER_CREATED_TIME_START, timeList);
        wfGetCmd = new WorkflowsJobGetJPAExecutor(filter, 1, 10);
        try{
            wfInfo = jpaService.execute(wfGetCmd);
        } catch (XException e) {
            assertEquals(ErrorCode.E0302, e.getErrorCode());
        }

        // filter 2012-04-01T00:00Z <= createdTimeStamp
        filter = new HashMap<String, List<String>>();
        timeList = new ArrayList<String>();
        timeList.add("2012-04-01T00:00Z");
        filter.put(OozieClient.FILTER_CREATED_TIME_START, timeList);
        wfGetCmd = new WorkflowsJobGetJPAExecutor(filter, 1, 10);
        wfInfo = jpaService.execute(wfGetCmd);
        assertNotNull(wfInfo);
        assertEquals(2, wfInfo.getWorkflows().size());
        for(WorkflowJobBean jobBean: wfInfo.getWorkflows()) {
            assertTrue(jobBean.getCreatedTime().equals(createdTime4) || jobBean.getCreatedTime().equals(createdTime5));
        }

        // filter 2012-02-01T00:00Z >= createdTimeStamp
        filter = new HashMap<String, List<String>>();
        timeList = new ArrayList<String>();
        timeList.add("2012-02-01T00:00Z");
        filter.put(OozieClient.FILTER_CREATED_TIME_END, timeList);
        wfGetCmd = new WorkflowsJobGetJPAExecutor(filter, 1, 10);
        wfInfo = jpaService.execute(wfGetCmd);
        assertNotNull(wfInfo);
        assertEquals(1, wfInfo.getWorkflows().size());
        assertTrue(wfInfo.getWorkflows().get(0).getCreatedTime().equals(createdTime1));

        // when createdTimeStamp specified multiple times
        filter = new HashMap<String, List<String>>();
        timeList = new ArrayList<String>();
        timeList.add("2012-02-01T00:00Z");
        timeList.add("2012-04-01T00:00Z");
        filter.put(OozieClient.FILTER_CREATED_TIME_END, timeList);
        wfGetCmd = new WorkflowsJobGetJPAExecutor(filter, 1, 10);
        try{
            wfInfo = jpaService.execute(wfGetCmd);
        } catch (XException e) {
            assertEquals(ErrorCode.E0302, e.getErrorCode());
        }

        // filter createdTimeStamp > currentTime - 2 days
        filter = new HashMap<String, List<String>>();
        timeList = new ArrayList<String>();
        timeList.add("-2d");
        filter.put(OozieClient.FILTER_CREATED_TIME_START, timeList);
        wfGetCmd = new WorkflowsJobGetJPAExecutor(filter, 1, 10);
        wfInfo = jpaService.execute(wfGetCmd);
        assertNotNull(wfInfo);
        assertEquals(1, wfInfo.getWorkflows().size());
        assertTrue(wfInfo.getWorkflows().get(0).getCreatedTime().equals(createdTime5));

        // filter createdTimeStamp > currentTime - 10 hour
        filter = new HashMap<String, List<String>>();
        timeList = new ArrayList<String>();
        timeList.add("-10h");
        filter.put(OozieClient.FILTER_CREATED_TIME_START, timeList);
        wfGetCmd = new WorkflowsJobGetJPAExecutor(filter, 1, 10);
        wfInfo = jpaService.execute(wfGetCmd);
        assertNotNull(wfInfo);
        assertEquals(1, wfInfo.getWorkflows().size());
        assertTrue(wfInfo.getWorkflows().get(0).getCreatedTime().equals(createdTime5));

        // filter createdTimeStamp > currentTime - 600 min
        filter = new HashMap<String, List<String>>();
        timeList = new ArrayList<String>();
        timeList.add("-600m");
        filter.put(OozieClient.FILTER_CREATED_TIME_START, timeList);
        wfGetCmd = new WorkflowsJobGetJPAExecutor(filter, 1, 10);
        wfInfo = jpaService.execute(wfGetCmd);
        assertNotNull(wfInfo);
        assertEquals(1, wfInfo.getWorkflows().size());
        assertTrue(wfInfo.getWorkflows().get(0).getCreatedTime().equals(createdTime5));

        // filter createdTimeStamp < currentTime - 3 days
        filter = new HashMap<String, List<String>>();
        timeList = new ArrayList<String>();
        timeList.add("-3d");
        filter.put(OozieClient.FILTER_CREATED_TIME_END, timeList);
        wfGetCmd = new WorkflowsJobGetJPAExecutor(filter, 1, 10);
        wfInfo = jpaService.execute(wfGetCmd);
        assertNotNull(wfInfo);
        assertEquals(4, wfInfo.getWorkflows().size());
        for (WorkflowJobBean jobBean : wfInfo.getWorkflows()) {
            assertTrue(jobBean.getCreatedTime().equals(createdTime1) || jobBean.getCreatedTime().equals(createdTime2)
                    || jobBean.getCreatedTime().equals(createdTime3) || jobBean.getCreatedTime().equals(createdTime4));
        }

        // filter createdTimeStamp < currentTime - 2 hours
        filter = new HashMap<String, List<String>>();
        timeList = new ArrayList<String>();
        timeList.add("-2h");
        filter.put(OozieClient.FILTER_CREATED_TIME_END, timeList);
        wfGetCmd = new WorkflowsJobGetJPAExecutor(filter, 1, 10);
        wfInfo = jpaService.execute(wfGetCmd);
        assertNotNull(wfInfo);
        assertEquals(5, wfInfo.getWorkflows().size());
        for (WorkflowJobBean jobBean : wfInfo.getWorkflows()) {
            assertTrue(jobBean.getCreatedTime().equals(createdTime1) || jobBean.getCreatedTime().equals(createdTime2)
                    || jobBean.getCreatedTime().equals(createdTime3) || jobBean.getCreatedTime().equals(createdTime4)
                    || jobBean.getCreatedTime().equals(createdTime5));
        }

        // filter createdTimeStamp < currentTime - 60 min
        filter = new HashMap<String, List<String>>();
        timeList = new ArrayList<String>();
        timeList.add("-60m");
        filter.put(OozieClient.FILTER_CREATED_TIME_END, timeList);
        wfGetCmd = new WorkflowsJobGetJPAExecutor(filter, 1, 10);
        wfInfo = jpaService.execute(wfGetCmd);
        assertNotNull(wfInfo);
        assertEquals(5, wfInfo.getWorkflows().size());
        for (WorkflowJobBean jobBean : wfInfo.getWorkflows()) {
            assertTrue(jobBean.getCreatedTime().equals(createdTime1) || jobBean.getCreatedTime().equals(createdTime2)
                    || jobBean.getCreatedTime().equals(createdTime3) || jobBean.getCreatedTime().equals(createdTime4)
                    || jobBean.getCreatedTime().equals(createdTime5));
        }

        // when offset is in wrong format
        filter = new HashMap<String, List<String>>();
        timeList = new ArrayList<String>();
        timeList.add("-5M");
        filter.put(OozieClient.FILTER_CREATED_TIME_END, timeList);
        wfGetCmd = new WorkflowsJobGetJPAExecutor(filter, 1, 10);
        try{
            wfInfo = jpaService.execute(wfGetCmd);
        } catch (XException e) {
            assertEquals(ErrorCode.E0302, e.getErrorCode());
        }

     // when offset is in wrong format
        filter = new HashMap<String, List<String>>();
        timeList = new ArrayList<String>();
        timeList.add("5D");
        filter.put(OozieClient.FILTER_CREATED_TIME_END, timeList);
        wfGetCmd = new WorkflowsJobGetJPAExecutor(filter, 1, 10);
        try{
            wfInfo = jpaService.execute(wfGetCmd);
        } catch (XException e) {
            assertEquals(ErrorCode.E0302, e.getErrorCode());
        }
    }
}
