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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.WorkflowsInfo;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor.WorkflowJobQuery;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.workflow.WorkflowException;
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
}
