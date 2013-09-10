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

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor.WorkflowJobQuery;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.workflow.WorkflowInstance;

public class TestWorkflowJobQueryExecutor extends XDataTestCase {
    Services services;
    JPAService jpaService;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        jpaService = Services.get().get(JPAService.class);
        cleanUpDBTables();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testGetQuery() throws Exception {
        EntityManager em = jpaService.getEntityManager();
        WorkflowJobBean bean = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);

        // UPDATE_WORKFLOW
        Query query = WorkflowJobQueryExecutor.getInstance().getUpdateQuery(WorkflowJobQuery.UPDATE_WORKFLOW, bean, em);
        assertEquals(query.getParameterValue("appName"), bean.getAppName());
        assertEquals(query.getParameterValue("appPath"), bean.getAppPath());
        assertEquals(query.getParameterValue("conf"), bean.getConfBlob());
        assertEquals(query.getParameterValue("groupName"), bean.getGroup());
        assertEquals(query.getParameterValue("run"), bean.getRun());
        assertEquals(query.getParameterValue("user"), bean.getUser());
        assertEquals(query.getParameterValue("createdTime"), bean.getCreatedTimestamp());
        assertEquals(query.getParameterValue("endTime"), bean.getEndTimestamp());
        assertEquals(query.getParameterValue("externalId"), bean.getExternalId());
        assertEquals(query.getParameterValue("lastModTime"), bean.getLastModifiedTimestamp());
        assertEquals(query.getParameterValue("logToken"), bean.getLogToken());
        assertEquals(query.getParameterValue("protoActionConf"), bean.getProtoActionConfBlob());
        assertEquals(query.getParameterValue("slaXml"), bean.getSlaXmlBlob());
        assertEquals(query.getParameterValue("startTime"), bean.getStartTimestamp());
        assertEquals(query.getParameterValue("status"), bean.getStatus().toString());
        assertEquals(query.getParameterValue("wfInstance"), bean.getWfInstanceBlob());
        assertEquals(query.getParameterValue("id"), bean.getId());

        // UPDATE_WORKFLOW_MODTIME
        query = WorkflowJobQueryExecutor.getInstance().getUpdateQuery(WorkflowJobQuery.UPDATE_WORKFLOW_MODTIME, bean,
                em);
        assertEquals(query.getParameterValue("lastModTime"), bean.getLastModifiedTimestamp());
        assertEquals(query.getParameterValue("id"), bean.getId());

        // UPDATE_WORKFLOW_STATUS_MODTIME:
        query = WorkflowJobQueryExecutor.getInstance().getUpdateQuery(WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_MODTIME,
                bean, em);
        assertEquals(query.getParameterValue("status"), bean.getStatus().toString());
        assertEquals(query.getParameterValue("lastModTime"), bean.getLastModifiedTimestamp());
        assertEquals(query.getParameterValue("id"), bean.getId());

        // UPDATE_WORKFLOW_PARENT_MODIFIED
        query = WorkflowJobQueryExecutor.getInstance().getUpdateQuery(WorkflowJobQuery.UPDATE_WORKFLOW_PARENT_MODIFIED,
                bean, em);
        assertEquals(query.getParameterValue("parentId"), bean.getParentId());
        assertEquals(query.getParameterValue("lastModTime"), bean.getLastModifiedTimestamp());
        assertEquals(query.getParameterValue("id"), bean.getId());

        // UPDATE_WORKFLOW_STATUS_INSTANCE_MODIFIED
        query = WorkflowJobQueryExecutor.getInstance().getUpdateQuery(
                WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_INSTANCE_MODIFIED, bean, em);
        assertEquals(query.getParameterValue("status"), bean.getStatus().toString());
        assertEquals(query.getParameterValue("wfInstance"), bean.getWfInstanceBlob());
        assertEquals(query.getParameterValue("lastModTime"), bean.getLastModifiedTimestamp());
        assertEquals(query.getParameterValue("id"), bean.getId());

        // UPDATE_WORKFLOW_STATUS_INSTANCE_MOD_END
        query = WorkflowJobQueryExecutor.getInstance().getUpdateQuery(
                WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_INSTANCE_MOD_END, bean, em);
        assertEquals(query.getParameterValue("status"), bean.getStatus().toString());
        assertEquals(query.getParameterValue("wfInstance"), bean.getWfInstanceBlob());
        assertEquals(query.getParameterValue("lastModTime"), bean.getLastModifiedTimestamp());
        assertEquals(query.getParameterValue("endTime"), bean.getEndTimestamp());
        assertEquals(query.getParameterValue("id"), bean.getId());

        // UPDATE_WORKFLOW_STATUS_INSTANCE_MOD_END
        query = WorkflowJobQueryExecutor.getInstance().getUpdateQuery(
                WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_INSTANCE_MOD_END, bean, em);
        assertEquals(query.getParameterValue("status"), bean.getStatus().toString());
        assertEquals(query.getParameterValue("wfInstance"), bean.getWfInstanceBlob());
        assertEquals(query.getParameterValue("lastModTime"), bean.getLastModifiedTimestamp());
        assertEquals(query.getParameterValue("endTime"), bean.getEndTimestamp());
        assertEquals(query.getParameterValue("id"), bean.getId());

        // UPDATE_WORKFLOW_STATUS_INSTANCE_MOD_START_END
        query = WorkflowJobQueryExecutor.getInstance().getUpdateQuery(
                WorkflowJobQuery.UPDATE_WORKFLOW_STATUS_INSTANCE_MOD_START_END, bean, em);
        assertEquals(query.getParameterValue("status"), bean.getStatus().toString());
        assertEquals(query.getParameterValue("wfInstance"), bean.getWfInstanceBlob());
        assertEquals(query.getParameterValue("lastModTime"), bean.getLastModifiedTimestamp());
        assertEquals(query.getParameterValue("startTime"), bean.getStartTimestamp());
        assertEquals(query.getParameterValue("endTime"), bean.getEndTimestamp());
        assertEquals(query.getParameterValue("id"), bean.getId());

        // UPDATE_WORKFLOW_RERUN
        query = WorkflowJobQueryExecutor.getInstance().getUpdateQuery(WorkflowJobQuery.UPDATE_WORKFLOW_RERUN, bean, em);
        assertEquals(query.getParameterValue("appName"), bean.getAppName());
        assertEquals(query.getParameterValue("protoActionConf"), bean.getProtoActionConfBlob());
        assertEquals(query.getParameterValue("appPath"), bean.getAppPath());
        assertEquals(query.getParameterValue("conf"), bean.getConfBlob());
        assertEquals(query.getParameterValue("logToken"), bean.getLogToken());
        assertEquals(query.getParameterValue("user"), bean.getUser());
        assertEquals(query.getParameterValue("group"), bean.getGroup());
        assertEquals(query.getParameterValue("externalId"), bean.getExternalId());
        assertEquals(query.getParameterValue("endTime"), bean.getEndTimestamp());
        assertEquals(query.getParameterValue("run"), bean.getRun());
        assertEquals(query.getParameterValue("status"), bean.getStatus().toString());
        assertEquals(query.getParameterValue("wfInstance"), bean.getWfInstanceBlob());
        assertEquals(query.getParameterValue("lastModTime"), bean.getLastModifiedTimestamp());
        assertEquals(query.getParameterValue("id"), bean.getId());

        // GET_WORKFLOW
        query = WorkflowJobQueryExecutor.getInstance().getSelectQuery(WorkflowJobQuery.GET_WORKFLOW, em, bean.getId());
        assertEquals(query.getParameterValue("id"), bean.getId());
    }

    public void testExecuteUpdate() throws Throwable {

        try {
            WorkflowJobBean bean = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);
            bean.setStatus(WorkflowJob.Status.RUNNING);
            WorkflowJobQueryExecutor.getInstance().executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW, bean);
            WorkflowJobBean bean2 = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW,
                    bean.getId());
            assertEquals(bean2.getStatus(), WorkflowJob.Status.RUNNING);
        }
        catch (Throwable e) {
            // TODO Auto-generated catch block
            System.out.println("Debug: encountered exception");
            e.printStackTrace();
            throw e;
        }
    }

    public void testInsert() throws Throwable {
        try {
            WorkflowJobBean bean = new WorkflowJobBean();
            bean.setId("test-oozie-wrk");
            bean.setAppName("test");
            bean.setUser("oozie");
            WorkflowJobQueryExecutor.getInstance().insert(bean);
            WorkflowJobBean retBean = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW,
                    "test-oozie-wrk");
            assertNotNull(retBean);
            assertEquals(retBean.getAppName(), "test");
            assertEquals(retBean.getUser(), "oozie");
        }
        catch (Throwable e) {
            System.out.println("Debug: encountered exception testinsert");
            // TODO Auto-generated catch block
            e.printStackTrace();
            throw e;
        }
    }

    public void testGet() throws Exception {
        // TODO
    }

    public void testGetList() throws Exception {
        // TODO
    }
}
