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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.HashSet;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorEngine;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor.WorkflowJobQuery;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
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
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testGetUpdateQuery() throws Exception {
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
    }

    public void testExecuteUpdate() throws Exception {

        WorkflowJobBean bean = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);
        bean.setStatus(WorkflowJob.Status.RUNNING);
        WorkflowJobQueryExecutor.getInstance().executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW, bean);
        WorkflowJobBean bean2 = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW, bean.getId());
        assertEquals(bean2.getStatus(), WorkflowJob.Status.RUNNING);
    }

    public void testGetSelectQuery() throws Exception {
        EntityManager em = jpaService.getEntityManager();
        WorkflowJobBean bean = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);

        // GET_WORKFLOW
        Query query = WorkflowJobQueryExecutor.getInstance().getSelectQuery(WorkflowJobQuery.GET_WORKFLOW, em, bean.getId());
        assertEquals(query.getParameterValue("id"), bean.getId());

        // GET_WORKFLOW_SUSPEND
        query = WorkflowJobQueryExecutor.getInstance().getSelectQuery(WorkflowJobQuery.GET_WORKFLOW_SUSPEND, em, bean.getId());
        assertEquals(query.getParameterValue("id"), bean.getId());
    }

    public void testInsert() throws Exception {
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

    public void testGet() throws Exception {
        WorkflowJobBean bean = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        bean.setStartTime(new Date(System.currentTimeMillis() - 10));
        bean.setEndTime(new Date());
        WorkflowJobQueryExecutor.getInstance().executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW, bean);
        WorkflowJobBean retBean;
        // GET_WORKFLOW_STARTTIME
        retBean = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW_STARTTIME, bean.getId());
        assertEquals(bean.getId(), retBean.getId());
        assertEquals(bean.getStartTime().getTime(), retBean.getStartTime().getTime());
        assertNull(retBean.getWorkflowInstance());
        assertNull(retBean.getProtoActionConf());
        assertNull(retBean.getSlaXml());
        assertNull(retBean.getConf());
        // GET_WORKFLOW_START_END_TIME
        retBean = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW_START_END_TIME, bean.getId());
        assertEquals(bean.getId(), retBean.getId());
        assertEquals(bean.getStartTime().getTime(), retBean.getStartTime().getTime());
        assertEquals(bean.getEndTime().getTime(), retBean.getEndTime().getTime());
        assertNull(retBean.getWorkflowInstance());
        assertNull(retBean.getProtoActionConf());
        assertNull(retBean.getSlaXml());
        assertNull(retBean.getConf());
        // GET_WORKFLOW_USER_GROUP
        retBean = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW_USER_GROUP, bean.getId());
        assertEquals(bean.getUser(), retBean.getUser());
        assertEquals(bean.getGroup(), retBean.getGroup());
        assertNull(retBean.getWorkflowInstance());
        assertNull(retBean.getProtoActionConf());
        assertNull(retBean.getSlaXml());
        assertNull(retBean.getConf());

        // GET_WORKFLOW_SUSPEND
        retBean = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW_SUSPEND, bean.getId());
        assertEquals(bean.getId(), retBean.getId());
        assertEquals(bean.getUser(), retBean.getUser());
        assertEquals(bean.getGroup(), retBean.getGroup());
        assertEquals(bean.getAppName(), retBean.getAppName());
        assertEquals(bean.getStatusStr(), retBean.getStatusStr());
        assertEquals(bean.getParentId(), retBean.getParentId());
        assertEquals(bean.getLogToken(), retBean.getLogToken());
        assertEquals(ByteBuffer.wrap(bean.getWfInstanceBlob().getBytes()).getInt(),
                ByteBuffer.wrap(retBean.getWfInstanceBlob().getBytes()).getInt());
        assertEquals(bean.getStartTime().getTime(), retBean.getStartTime().getTime());
        assertEquals(bean.getEndTime().getTime(), retBean.getEndTime().getTime());
        assertNull(retBean.getProtoActionConf());
        assertNull(retBean.getSlaXml());
        assertNull(retBean.getConf());

        // GET_WORKFLOW_ACTION_OP
        retBean = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW_ACTION_OP, bean.getId());
        assertEquals(bean.getId(), retBean.getId());
        assertEquals(bean.getUser(), retBean.getUser());
        assertEquals(bean.getGroup(), retBean.getGroup());
        assertEquals(bean.getAppName(), retBean.getAppName());
        assertEquals(bean.getAppPath(), retBean.getAppPath());
        assertEquals(bean.getStatusStr(), retBean.getStatusStr());
        assertEquals(bean.getRun(), retBean.getRun());
        assertEquals(bean.getParentId(), retBean.getParentId());
        assertEquals(bean.getLogToken(), retBean.getLogToken());
        assertEquals(ByteBuffer.wrap(bean.getWfInstanceBlob().getBytes()).getInt(),
                ByteBuffer.wrap(retBean.getWfInstanceBlob().getBytes()).getInt());
        assertEquals(bean.getProtoActionConf(), retBean.getProtoActionConf());
        assertNull(retBean.getSlaXml());
        assertNull(retBean.getConf());

        //GET_WORKFLOW_RERUN
        retBean = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW_RERUN, bean.getId());
        assertEquals(bean.getId(), retBean.getId());
        assertEquals(bean.getUser(), retBean.getUser());
        assertEquals(bean.getGroup(), retBean.getGroup());
        assertEquals(bean.getAppName(), retBean.getAppName());
        assertEquals(bean.getStatusStr(), retBean.getStatusStr());
        assertEquals(bean.getRun(), retBean.getRun());
        assertEquals(bean.getLogToken(), retBean.getLogToken());
        assertEquals(ByteBuffer.wrap(bean.getWfInstanceBlob().getBytes()).getInt(),
                ByteBuffer.wrap(retBean.getWfInstanceBlob().getBytes()).getInt());
        assertNull(retBean.getProtoActionConf());
        assertNull(retBean.getSlaXml());
        assertNull(retBean.getConf());

        //GET_WORKFLOW_DEFINITION
        retBean = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW_DEFINITION, bean.getId());
        assertEquals(bean.getId(), retBean.getId());
        assertEquals(bean.getUser(), retBean.getUser());
        assertEquals(bean.getGroup(), retBean.getGroup());
        assertEquals(bean.getAppName(), retBean.getAppName());
        assertEquals(bean.getLogToken(), retBean.getLogToken());
        assertEquals(ByteBuffer.wrap(bean.getWfInstanceBlob().getBytes()).getInt(),
                ByteBuffer.wrap(retBean.getWfInstanceBlob().getBytes()).getInt());
        assertNull(retBean.getProtoActionConf());
        assertNull(retBean.getSlaXml());
        assertNull(retBean.getConf());

        // GET_WORKFLOW_KILL
        retBean = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW_KILL, bean.getId());
        assertEquals(bean.getId(), retBean.getId());
        assertEquals(bean.getUser(), retBean.getUser());
        assertEquals(bean.getGroup(), retBean.getGroup());
        assertEquals(bean.getAppName(), retBean.getAppName());
        assertEquals(bean.getAppPath(), retBean.getAppPath());
        assertEquals(bean.getStatusStr(), retBean.getStatusStr());
        assertEquals(bean.getParentId(), retBean.getParentId());
        assertEquals(bean.getStartTime().getTime(), retBean.getStartTime().getTime());
        assertEquals(bean.getEndTime().getTime(), retBean.getEndTime().getTime());
        assertEquals(bean.getLogToken(), retBean.getLogToken());
        assertEquals(ByteBuffer.wrap(bean.getWfInstanceBlob().getBytes()).getInt(),
                ByteBuffer.wrap(retBean.getWfInstanceBlob().getBytes()).getInt());
        assertEquals(bean.getSlaXml(), retBean.getSlaXml());
        assertNull(retBean.getProtoActionConf());
        assertNull(retBean.getConf());

        // GET_WORKFLOW_RESUME
        retBean = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW_RESUME, bean.getId());
        assertEquals(bean.getId(), retBean.getId());
        assertEquals(bean.getUser(), retBean.getUser());
        assertEquals(bean.getGroup(), retBean.getGroup());
        assertEquals(bean.getAppName(), retBean.getAppName());
        assertEquals(bean.getAppPath(), retBean.getAppPath());
        assertEquals(bean.getStatusStr(), retBean.getStatusStr());
        assertEquals(bean.getParentId(), retBean.getParentId());
        assertEquals(bean.getStartTime().getTime(), retBean.getStartTime().getTime());
        assertEquals(bean.getEndTime().getTime(), retBean.getEndTime().getTime());
        assertEquals(bean.getLogToken(), retBean.getLogToken());
        assertEquals(ByteBuffer.wrap(bean.getWfInstanceBlob().getBytes()).getInt(),
                ByteBuffer.wrap(retBean.getWfInstanceBlob().getBytes()).getInt());
        assertEquals(bean.getProtoActionConf(), retBean.getProtoActionConf());
        assertNull(retBean.getConf());
        assertNull(retBean.getSlaXml());

        // GET_WORKFLOW_STATUS
        retBean = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW_STATUS, bean.getId());
        assertEquals(bean.getId(), retBean.getId());
        assertEquals(bean.getStatus(), retBean.getStatus());
    }

    public void testGetList() throws Exception {
        // GET_WORKFLOWS_PARENT_COORD_RERUN
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, null, null, false,
                false, 1);
        WorkflowJobBean wfJob1 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                coordJob.getId() + "@2");
        CoordinatorActionBean coordAction1 = addRecordToCoordActionTable(coordJob.getId(), 2,
                CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", wfJob1.getId(), "SUCCEEDED", 0);
        // second wf after rerunning coord action, having same parent id
        WorkflowJobBean wfJob2 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                coordJob.getId() + "@2");
        final CoordinatorEngine ce = new CoordinatorEngine(getTestUser());
        List<WorkflowJobBean> wfsForRerun = ce.getReruns(coordAction1.getId());
        assertEquals(2, wfsForRerun.size());
        assertEquals(wfJob1.getId(), wfsForRerun.get(0).getId());
        assertEquals(wfJob2.getId(), wfsForRerun.get(1).getId());

        // GET_COMPLETED_COORD_WORKFLOWS_OLDER_THAN
        coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, null, null, false,
                false, 1);
        wfJob1 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                coordJob.getId() + "@1");
        wfJob1.setEndTime(DateUtils.parseDateOozieTZ("2009-12-18T03:00Z"));
        WorkflowJobQueryExecutor.getInstance().executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW, wfJob1);
        wfJob2 = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED,
                coordJob.getId() + "@2");
        wfJob2.setEndTime(DateUtils.parseDateOozieTZ("2009-12-18T03:00Z"));
        WorkflowJobQueryExecutor.getInstance().executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW, wfJob2);
        long olderthan = 30;
        List<WorkflowJobBean> jobBeans = WorkflowJobQueryExecutor.getInstance().getList(
                WorkflowJobQuery.GET_COMPLETED_COORD_WORKFLOWS_OLDER_THAN, olderthan,
                0, 10);

        HashSet<String> jobIds = new HashSet<String>(Arrays.asList(wfJob1.getId(), wfJob2.getId()));
        assertEquals(2, jobBeans.size());
        assertTrue(jobIds.contains(jobBeans.get(0).getId()));
        assertTrue(jobIds.contains(jobBeans.get(1).getId()));
    }
}
