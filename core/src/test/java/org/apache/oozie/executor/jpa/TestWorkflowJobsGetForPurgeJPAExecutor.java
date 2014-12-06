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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor.WorkflowJobQuery;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.LiteWorkflowStoreService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.lite.EndNodeDef;
import org.apache.oozie.workflow.lite.LiteWorkflowApp;
import org.apache.oozie.workflow.lite.StartNodeDef;

public class TestWorkflowJobsGetForPurgeJPAExecutor extends XDataTestCase {
    private static final long DAY_IN_MS = 24 * 60 * 60 * 1000;
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

    public void testWfJobsGetForPurgeWithParent() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        WorkflowJobBean job1 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean job2 = addRecordToWfJobTable(WorkflowJob.Status.FAILED, WorkflowInstance.Status.FAILED);
        WorkflowJobBean job3 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        job3.setParentId(job1.getId());
        job3.setLastModifiedTime(new Date());
        WorkflowJobQueryExecutor.getInstance().executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW_PARENT_MODIFIED, job3);

        List<String> list = new ArrayList<String>();
        list.addAll(jpaService.execute(new WorkflowJobsGetForPurgeJPAExecutor(1, 20)));
        // job3 shouldn't be in the list because it has a parent
        checkWorkflows(list, job1.getId(), job2.getId());
    }

    public void testWfJobsGetForPurgeTooMany() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        WorkflowJobBean job1 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean job2 = addRecordToWfJobTable(WorkflowJob.Status.FAILED, WorkflowInstance.Status.FAILED);
        WorkflowJobBean job3 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean job4 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
        WorkflowJobBean job5 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);

        List<String> list = new ArrayList<String>();
        // Get the first 3
        list.addAll(jpaService.execute(new WorkflowJobsGetForPurgeJPAExecutor(1, 3)));
        assertEquals(3, list.size());
        // Get the next 3 (though there's only 2 more)
        list.addAll(jpaService.execute(new WorkflowJobsGetForPurgeJPAExecutor(1, 3, 3)));
        assertEquals(5, list.size());
        checkWorkflows(list, job1.getId(), job2.getId(), job3.getId(), job4.getId(), job5.getId());
    }

    @Override
    protected WorkflowJobBean addRecordToWfJobTable(WorkflowJob.Status jobStatus, WorkflowInstance.Status instanceStatus) throws Exception {
        WorkflowApp app =
            new LiteWorkflowApp("testApp", "<workflow-app/>",
                new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "end")).
                    addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));
        Configuration conf = new Configuration();
        Path appUri = new Path(getAppPath(), "workflow.xml");
        conf.set(OozieClient.APP_PATH, appUri.toString());
        conf.set(OozieClient.LOG_TOKEN, "testToken");
        conf.set(OozieClient.USER_NAME, getTestUser());

        WorkflowJobBean wfBean = createWorkflow(app, conf, jobStatus, instanceStatus);
        Timestamp startTS = new Timestamp(System.currentTimeMillis() - (3 * DAY_IN_MS));
        Timestamp endTS = new Timestamp(System.currentTimeMillis() - (2 * DAY_IN_MS));
        wfBean.setStartTime(DateUtils.toDate(startTS));
        wfBean.setEndTime(DateUtils.toDate(endTS));
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            WorkflowJobInsertJPAExecutor wfInsertCmd = new WorkflowJobInsertJPAExecutor(wfBean);
            jpaService.execute(wfInsertCmd);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test wf job record to table");
            throw je;
        }
        return wfBean;
    }

    private void checkWorkflows(List<String> wfs, String... wfJobIDs) {
        assertEquals(wfJobIDs.length, wfs.size());
        Arrays.sort(wfJobIDs);
        Collections.sort(wfs);

        for (int i = 0; i < wfJobIDs.length; i++) {
            assertEquals(wfJobIDs[i], wfs.get(i));
        }
    }
}
