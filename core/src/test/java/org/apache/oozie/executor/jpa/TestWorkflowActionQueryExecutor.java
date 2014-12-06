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

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor.WorkflowActionQuery;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.workflow.WorkflowInstance;

public class TestWorkflowActionQueryExecutor extends XDataTestCase {
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
        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean bean = addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.PREP);

        // UPDATE_ACTION
        Query query = WorkflowActionQueryExecutor.getInstance().getUpdateQuery(WorkflowActionQuery.UPDATE_ACTION, bean,
                em);
        assertEquals(query.getParameterValue("conf"), bean.getConfBlob());
        assertEquals(query.getParameterValue("consoleUrl"), bean.getConsoleUrl());
        assertEquals(query.getParameterValue("data"), bean.getDataBlob());
        assertEquals(query.getParameterValue("stats"), bean.getStatsBlob());
        assertEquals(query.getParameterValue("externalChildIDs"), bean.getExternalChildIDsBlob());
        assertEquals(query.getParameterValue("errorCode"), bean.getErrorCode());
        assertEquals(query.getParameterValue("errorMessage"), bean.getErrorMessage());
        assertEquals(query.getParameterValue("externalId"), bean.getExternalId());
        assertEquals(query.getParameterValue("externalStatus"), bean.getExternalStatus());
        assertEquals(query.getParameterValue("name"), bean.getName());
        assertEquals(query.getParameterValue("cred"), bean.getCred());
        assertEquals(query.getParameterValue("retries"), bean.getRetries());
        assertEquals(query.getParameterValue("trackerUri"), bean.getTrackerUri());
        assertEquals(query.getParameterValue("transition"), bean.getTransition());
        assertEquals(query.getParameterValue("type"), bean.getType());
        assertEquals(query.getParameterValue("endTime"), bean.getEndTimestamp());
        assertEquals(query.getParameterValue("executionPath"), bean.getExecutionPath());
        assertEquals(query.getParameterValue("lastCheckTime"), bean.getLastCheckTimestamp());
        assertEquals(query.getParameterValue("logToken"), bean.getLogToken());
        assertEquals(query.getParameterValue("pending"), bean.getPending());
        assertEquals(query.getParameterValue("pendingAge"), bean.getPendingAge());
        assertEquals(query.getParameterValue("signalValue"), bean.getSignalValue());
        assertEquals(query.getParameterValue("slaXml"), bean.getSlaXmlBlob());
        assertEquals(query.getParameterValue("startTime"), bean.getStartTimestamp());
        assertEquals(query.getParameterValue("status"), bean.getStatus().toString());
        assertEquals(query.getParameterValue("wfId"), bean.getWfId());
        assertEquals(query.getParameterValue("id"), bean.getId());

        // UPDATE_ACTION_FOR_LAST_CHECKED_TIME
        query = WorkflowActionQueryExecutor.getInstance().getUpdateQuery(WorkflowActionQuery.UPDATE_ACTION, bean, em);
        assertEquals(query.getParameterValue("lastCheckTime"), bean.getLastCheckTimestamp());
        assertEquals(query.getParameterValue("id"), bean.getId());

        // UPDATE_ACTION_PENDING
        query = WorkflowActionQueryExecutor.getInstance().getUpdateQuery(WorkflowActionQuery.UPDATE_ACTION_PENDING,
                bean, em);
        assertEquals(query.getParameterValue("pending"), bean.getPending());
        assertEquals(query.getParameterValue("pendingAge"), bean.getPendingAgeTimestamp());
        assertEquals(query.getParameterValue("id"), bean.getId());

        // UPDATE_ACTION_STATUS_PENDING
        query = WorkflowActionQueryExecutor.getInstance().getUpdateQuery(
                WorkflowActionQuery.UPDATE_ACTION_STATUS_PENDING, bean, em);
        assertEquals(query.getParameterValue("pending"), bean.getPending());
        assertEquals(query.getParameterValue("pendingAge"), bean.getPendingAgeTimestamp());
        assertEquals(query.getParameterValue("status"), bean.getStatus().toString());
        assertEquals(query.getParameterValue("id"), bean.getId());
        // UPDATE_ACTION_PENDING_TRANS
        query = WorkflowActionQueryExecutor.getInstance().getUpdateQuery(
                WorkflowActionQuery.UPDATE_ACTION_PENDING_TRANS, bean, em);
        assertEquals(query.getParameterValue("pending"), bean.getPending());
        assertEquals(query.getParameterValue("pendingAge"), bean.getPendingAgeTimestamp());
        assertEquals(query.getParameterValue("transition"), bean.getTransition());
        assertEquals(query.getParameterValue("id"), bean.getId());
        // UPDATE_ACTION_PENDING_TRANS_ERROR
        query = WorkflowActionQueryExecutor.getInstance().getUpdateQuery(
                WorkflowActionQuery.UPDATE_ACTION_PENDING_TRANS_ERROR, bean, em);
        assertEquals(query.getParameterValue("pending"), bean.getPending());
        assertEquals(query.getParameterValue("pendingAge"), bean.getPendingAgeTimestamp());
        assertEquals(query.getParameterValue("transition"), bean.getTransition());
        assertEquals(query.getParameterValue("errorCode"), bean.getErrorCode());
        assertEquals(query.getParameterValue("errorMessage"), bean.getErrorMessage());
        assertEquals(query.getParameterValue("id"), bean.getId());
        // UPDATE_ACTION_START
        query = WorkflowActionQueryExecutor.getInstance().getUpdateQuery(WorkflowActionQuery.UPDATE_ACTION_START, bean,
                em);
        assertEquals(query.getParameterValue("startTime"), bean.getStartTimestamp());
        assertEquals(query.getParameterValue("externalChildIDs"), bean.getExternalChildIDsBlob());
        assertEquals(query.getParameterValue("conf"), bean.getConfBlob());
        assertEquals(query.getParameterValue("errorCode"), bean.getErrorCode());
        assertEquals(query.getParameterValue("errorMessage"), bean.getErrorMessage());
        assertEquals(query.getParameterValue("externalId"), bean.getExternalId());
        assertEquals(query.getParameterValue("trackerUri"), bean.getTrackerUri());
        assertEquals(query.getParameterValue("consoleUrl"), bean.getConsoleUrl());
        assertEquals(query.getParameterValue("lastCheckTime"), bean.getLastCheckTimestamp());
        assertEquals(query.getParameterValue("status"), bean.getStatus().toString());
        assertEquals(query.getParameterValue("externalStatus"), bean.getExternalStatus());
        assertEquals(query.getParameterValue("data"), bean.getDataBlob());
        assertEquals(query.getParameterValue("retries"), bean.getRetries());
        assertEquals(query.getParameterValue("pending"), bean.getPending());
        assertEquals(query.getParameterValue("pendingAge"), bean.getPendingAgeTimestamp());
        assertEquals(query.getParameterValue("userRetryCount"), bean.getUserRetryCount());
        assertEquals(query.getParameterValue("id"), bean.getId());
        // UPDATE_ACTION_CHECK
        query = WorkflowActionQueryExecutor.getInstance().getUpdateQuery(WorkflowActionQuery.UPDATE_ACTION_CHECK, bean,
                em);
        assertEquals(query.getParameterValue("externalChildIDs"), bean.getExternalChildIDsBlob());
        assertEquals(query.getParameterValue("externalStatus"), bean.getExternalStatus());
        assertEquals(query.getParameterValue("status"), bean.getStatus().toString());
        assertEquals(query.getParameterValue("data"), bean.getDataBlob());
        assertEquals(query.getParameterValue("pending"), bean.getPending());
        assertEquals(query.getParameterValue("errorCode"), bean.getErrorCode());
        assertEquals(query.getParameterValue("errorMessage"), bean.getErrorMessage());
        assertEquals(query.getParameterValue("lastCheckTime"), bean.getLastCheckTimestamp());
        assertEquals(query.getParameterValue("retries"), bean.getRetries());
        assertEquals(query.getParameterValue("pendingAge"), bean.getPendingAgeTimestamp());
        assertEquals(query.getParameterValue("startTime"), bean.getStartTimestamp());
        assertEquals(query.getParameterValue("stats"), bean.getStatsBlob());
        assertEquals(query.getParameterValue("userRetryCount"), bean.getUserRetryCount());
        assertEquals(query.getParameterValue("id"), bean.getId());
        // UPDATE_ACTION_END
        query = WorkflowActionQueryExecutor.getInstance().getUpdateQuery(WorkflowActionQuery.UPDATE_ACTION_END, bean,
                em);
        assertEquals(query.getParameterValue("errorCode"), bean.getErrorCode());
        assertEquals(query.getParameterValue("errorMessage"), bean.getErrorMessage());
        assertEquals(query.getParameterValue("retries"), bean.getRetries());
        assertEquals(query.getParameterValue("endTime"), bean.getEndTimestamp());
        assertEquals(query.getParameterValue("status"), bean.getStatus().toString());
        assertEquals(query.getParameterValue("retries"), bean.getRetries());
        assertEquals(query.getParameterValue("pending"), bean.getPending());
        assertEquals(query.getParameterValue("pendingAge"), bean.getPendingAgeTimestamp());
        assertEquals(query.getParameterValue("signalValue"), bean.getSignalValue());
        assertEquals(query.getParameterValue("userRetryCount"), bean.getUserRetryCount());
        assertEquals(query.getParameterValue("externalStatus"), bean.getExternalStatus());
        assertEquals(query.getParameterValue("stats"), bean.getStatsBlob());
        assertEquals(query.getParameterValue("id"), bean.getId());
        em.close();
    }

    public void testExecuteUpdate() throws Exception {

        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean bean = addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.PREP);
        bean.setStatus(WorkflowAction.Status.RUNNING);
        bean.setName("test-name");
        WorkflowActionQueryExecutor.getInstance().executeUpdate(WorkflowActionQuery.UPDATE_ACTION, bean);
        WorkflowActionBean retBean = WorkflowActionQueryExecutor.getInstance().get(WorkflowActionQuery.GET_ACTION,
                bean.getId());
        assertEquals("test-name", retBean.getName());
        assertEquals(retBean.getStatus(), WorkflowAction.Status.RUNNING);
    }

    public void testGet() throws Exception {
        WorkflowActionBean bean = addRecordToWfActionTable("workflowId", "testAction", WorkflowAction.Status.PREP, "",
                true);
        WorkflowActionBean retBean;

        //GET_ACTION_ID_TYPE_LASTCHECK
        retBean = WorkflowActionQueryExecutor.getInstance().get(WorkflowActionQuery.GET_ACTION_ID_TYPE_LASTCHECK,
                bean.getId());
        assertEquals(bean.getId(), retBean.getId());
        assertEquals(bean.getType(), retBean.getType());
        assertEquals(bean.getLastCheckTime(), retBean.getLastCheckTime());

        //GET_ACTION_FAIL
        retBean = WorkflowActionQueryExecutor.getInstance().get(WorkflowActionQuery.GET_ACTION_FAIL, bean.getId());
        assertEquals(bean.getId(), retBean.getId());
        assertEquals(bean.getJobId(), retBean.getJobId());
        assertEquals(bean.getName(), retBean.getName());
        assertEquals(bean.getStatusStr(), retBean.getStatusStr());
        assertEquals(bean.getPending(), retBean.getPending());
        assertEquals(bean.getType(), retBean.getType());
        assertEquals(bean.getLogToken(), retBean.getLogToken());
        assertEquals(bean.getTransition(), retBean.getTransition());
        assertEquals(bean.getErrorCode(), retBean.getErrorCode());
        assertEquals(bean.getErrorMessage(), retBean.getErrorMessage());
        assertNull(retBean.getConf());
        assertNull(retBean.getSlaXml());
        assertNull(retBean.getData());
        assertNull(retBean.getStats());
        assertNull(retBean.getExternalChildIDs());

        //GET_ACTION_SIGNAL
        retBean = WorkflowActionQueryExecutor.getInstance().get(WorkflowActionQuery.GET_ACTION_SIGNAL, bean.getId());
        assertEquals(bean.getId(), retBean.getId());
        assertEquals(bean.getJobId(), retBean.getJobId());
        assertEquals(bean.getName(), retBean.getName());
        assertEquals(bean.getStatusStr(), retBean.getStatusStr());
        assertEquals(bean.getPending(), retBean.getPending());
        assertEquals(bean.getPendingAge().getTime(), retBean.getPendingAge().getTime());
        assertEquals(bean.getType(), retBean.getType());
        assertEquals(bean.getLogToken(), retBean.getLogToken());
        assertEquals(bean.getTransition(), retBean.getTransition());
        assertEquals(bean.getErrorCode(), retBean.getErrorCode());
        assertEquals(bean.getErrorMessage(), retBean.getErrorMessage());
        assertEquals(bean.getExecutionPath(), retBean.getExecutionPath());
        assertEquals(bean.getSignalValue(), retBean.getSignalValue());
        assertEquals(bean.getSlaXml(), retBean.getSlaXml());
        assertNull(retBean.getConf());
        assertNull(retBean.getData());
        assertNull(retBean.getStats());
        assertNull(retBean.getExternalChildIDs());

        // GET_ACTION_CHECK
        retBean = WorkflowActionQueryExecutor.getInstance().get(WorkflowActionQuery.GET_ACTION_CHECK, bean.getId());
        assertEquals(bean.getId(), retBean.getId());
        assertEquals(bean.getJobId(), retBean.getJobId());
        assertEquals(bean.getName(), retBean.getName());
        assertEquals(bean.getStatusStr(), retBean.getStatusStr());
        assertEquals(bean.getPending(), retBean.getPending());
        assertEquals(bean.getPendingAge().getTime(), retBean.getPendingAge().getTime());
        assertEquals(bean.getType(), retBean.getType());
        assertEquals(bean.getLogToken(), retBean.getLogToken());
        assertEquals(bean.getTransition(), retBean.getTransition());
        assertEquals(bean.getRetries(), retBean.getRetries());
        assertEquals(bean.getUserRetryCount(), retBean.getUserRetryCount());
        assertEquals(bean.getUserRetryMax(), retBean.getUserRetryMax());
        assertEquals(bean.getUserRetryInterval(), retBean.getUserRetryInterval());
        assertEquals(bean.getTrackerUri(), retBean.getTrackerUri());
        assertEquals(bean.getStartTime().getTime(), retBean.getStartTime().getTime());
        assertEquals(bean.getEndTime().getTime(), retBean.getEndTime().getTime());
        assertEquals(bean.getLastCheckTime().getTime(), retBean.getLastCheckTime().getTime());
        assertEquals(bean.getErrorCode(), retBean.getErrorCode());
        assertEquals(bean.getErrorMessage(), retBean.getErrorMessage());
        assertEquals(bean.getExternalId(), retBean.getExternalId());
        assertEquals(bean.getExternalStatus(), retBean.getExternalStatus());
        assertEquals(bean.getExternalChildIDs(), retBean.getExternalChildIDs());
        assertEquals(bean.getConf(), retBean.getConf());
        assertNull(retBean.getData());
        assertNull(retBean.getStats());
        assertNull(retBean.getSlaXml());

        // GET_ACTION_END
        retBean = WorkflowActionQueryExecutor.getInstance().get(WorkflowActionQuery.GET_ACTION_END, bean.getId());
        assertEquals(bean.getId(), retBean.getId());
        assertEquals(bean.getJobId(), retBean.getJobId());
        assertEquals(bean.getName(), retBean.getName());
        assertEquals(bean.getStatusStr(), retBean.getStatusStr());
        assertEquals(bean.getPending(), retBean.getPending());
        assertEquals(bean.getPendingAge().getTime(), retBean.getPendingAge().getTime());
        assertEquals(bean.getType(), retBean.getType());
        assertEquals(bean.getLogToken(), retBean.getLogToken());
        assertEquals(bean.getTransition(), retBean.getTransition());
        assertEquals(bean.getRetries(), retBean.getRetries());
        assertEquals(bean.getTrackerUri(), retBean.getTrackerUri());
        assertEquals(bean.getUserRetryCount(), retBean.getUserRetryCount());
        assertEquals(bean.getUserRetryMax(), retBean.getUserRetryMax());
        assertEquals(bean.getUserRetryInterval(), retBean.getUserRetryInterval());
        assertEquals(bean.getExternalId(), retBean.getExternalId());
        assertEquals(bean.getExternalStatus(), retBean.getExternalStatus());
        assertEquals(bean.getExternalChildIDs(), retBean.getExternalChildIDs());
        assertEquals(bean.getStartTime().getTime(), retBean.getStartTime().getTime());
        assertEquals(bean.getEndTime().getTime(), retBean.getEndTime().getTime());
        assertEquals(bean.getErrorCode(), retBean.getErrorCode());
        assertEquals(bean.getErrorMessage(), retBean.getErrorMessage());
        assertEquals(bean.getConf(), retBean.getConf());
        assertEquals(bean.getData(), retBean.getData());
        assertEquals(bean.getStats(), retBean.getStats());
        assertNull(retBean.getSlaXml());

        //GET_ACTION_COMPLETED
        retBean = WorkflowActionQueryExecutor.getInstance().get(WorkflowActionQuery.GET_ACTION_COMPLETED, bean.getId());
        assertEquals(bean.getId(), retBean.getId());
        assertEquals(bean.getJobId(), retBean.getJobId());
        assertEquals(bean.getStatusStr(), retBean.getStatusStr());
        assertEquals(bean.getType(), retBean.getType());
        assertEquals(bean.getLogToken(), retBean.getLogToken());
        assertNull(retBean.getSlaXml());
        assertNull(retBean.getConf());
        assertNull(retBean.getData());
        assertNull(retBean.getStats());
        assertNull(retBean.getExternalChildIDs());

        // GET_ACTION (entire obj)
        retBean = WorkflowActionQueryExecutor.getInstance().get(WorkflowActionQuery.GET_ACTION, bean.getId());
        assertEquals(bean.getId(), retBean.getId());
        assertEquals(bean.getJobId(), retBean.getJobId());
        assertEquals(bean.getName(), retBean.getName());
        assertEquals(bean.getStatusStr(), retBean.getStatusStr());
        assertEquals(bean.getPending(), retBean.getPending());
        assertEquals(bean.getPendingAge().getTime(), retBean.getPendingAge().getTime());
        assertEquals(bean.getType(), retBean.getType());
        assertEquals(bean.getLogToken(), retBean.getLogToken());
        assertEquals(bean.getTransition(), retBean.getTransition());
        assertEquals(bean.getRetries(), retBean.getRetries());
        assertEquals(bean.getUserRetryCount(), retBean.getUserRetryCount());
        assertEquals(bean.getUserRetryMax(), retBean.getUserRetryMax());
        assertEquals(bean.getUserRetryInterval(), retBean.getUserRetryInterval());
        assertEquals(bean.getStartTime().getTime(), retBean.getStartTime().getTime());
        assertEquals(bean.getEndTime().getTime(), retBean.getEndTime().getTime());
        assertEquals(bean.getCreatedTime().getTime(), retBean.getCreatedTime().getTime());
        assertEquals(bean.getLastCheckTime().getTime(), retBean.getLastCheckTime().getTime());
        assertEquals(bean.getErrorCode(), retBean.getErrorCode());
        assertEquals(bean.getErrorMessage(), retBean.getErrorMessage());
        assertEquals(bean.getExecutionPath(), retBean.getExecutionPath());
        assertEquals(bean.getSignalValue(), retBean.getSignalValue());
        assertEquals(bean.getCred(), retBean.getCred());
        assertEquals(bean.getConf(), retBean.getConf());
        assertEquals(bean.getSlaXml(), retBean.getSlaXml());
        assertEquals(bean.getData(), retBean.getData());
        assertEquals(bean.getStats(), retBean.getStats());
        assertEquals(bean.getExternalChildIDs(), retBean.getExternalChildIDs());
    }

    public void testGetList() throws Exception {
        addRecordToWfActionTable("wrkflow","1", WorkflowAction.Status.RUNNING, true);
        addRecordToWfActionTable("wrkflow","2", WorkflowAction.Status.RUNNING, true);
        addRecordToWfActionTable("wrkflow","3", WorkflowAction.Status.RUNNING, true);
        addRecordToWfActionTable("wrkflow","4", WorkflowAction.Status.PREP, true);
        addRecordToWfActionTable("wrkflow","5", WorkflowAction.Status.FAILED, true);
        addRecordToWfActionTable("wrkflow","6", WorkflowAction.Status.FAILED, false);
        //GET_RUNNING_ACTIONS
        List<WorkflowActionBean> retList = WorkflowActionQueryExecutor.getInstance().getList(
                WorkflowActionQuery.GET_RUNNING_ACTIONS, 0);
        assertEquals(3, retList.size());
        for(WorkflowActionBean bean : retList){
            assertTrue(bean.getId().equals("wrkflow@1") || bean.getId().equals("wrkflow@2") || bean.getId().equals("wrkflow@3"));
        }
        //GET_PENDING_ACTIONS
        sleep(10);
        long olderThan = 1;
        retList = WorkflowActionQueryExecutor.getInstance().getList(
                WorkflowActionQuery.GET_PENDING_ACTIONS, olderThan);
        assertEquals(2, retList.size());
        for(WorkflowActionBean bean : retList){
            assertTrue(bean.getId().equals("wrkflow@4") || bean.getId().equals("wrkflow@5"));
        }
        olderThan = 10000;
        retList = WorkflowActionQueryExecutor.getInstance().getList(
                WorkflowActionQuery.GET_PENDING_ACTIONS, olderThan);
        assertEquals(0, retList.size());

    }

    public void testInsert() throws Exception {
        WorkflowActionBean bean = new WorkflowActionBean();
        bean.setId("test-oozie-action");
        bean.setName("test");
        WorkflowJobQueryExecutor.getInstance().insert(bean);
        WorkflowActionBean retBean = WorkflowActionQueryExecutor.getInstance().get(WorkflowActionQuery.GET_ACTION,
                "test-oozie-action");
        assertEquals(retBean.getName(), "test");
    }
}
