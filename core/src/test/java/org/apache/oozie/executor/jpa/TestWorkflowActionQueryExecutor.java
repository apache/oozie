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
        cleanUpDBTables();
    }

    @Override
    protected void tearDown() throws Exception {
        System.out.println("Debug: In teardown");
        new Throwable().printStackTrace();
        try {
            services.destroy();
            super.tearDown();
        }
        catch (Exception e) {
            System.out.println("Debug: exception In teardown");
            e.printStackTrace();
            throw e;
        }
    }

    public void testGetQuery() throws Exception {
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
        assertEquals(query.getParameterValue("id"), bean.getId());

        // UPDATE_ACTION_STATUS_PENDING
        query = WorkflowActionQueryExecutor.getInstance().getUpdateQuery(
                WorkflowActionQuery.UPDATE_ACTION_STATUS_PENDING, bean, em);
        assertEquals(query.getParameterValue("pending"), bean.getPending());
        assertEquals(query.getParameterValue("status"), bean.getStatus().toString());
        assertEquals(query.getParameterValue("id"), bean.getId());
        // UPDATE_ACTION_PENDING_TRANS
        query = WorkflowActionQueryExecutor.getInstance().getUpdateQuery(
                WorkflowActionQuery.UPDATE_ACTION_PENDING_TRANS, bean, em);
        assertEquals(query.getParameterValue("pending"), bean.getPending());
        assertEquals(query.getParameterValue("transition"), bean.getTransition());
        assertEquals(query.getParameterValue("id"), bean.getId());
        // UPDATE_ACTION_PENDING_TRANS_ERROR
        query = WorkflowActionQueryExecutor.getInstance().getUpdateQuery(
                WorkflowActionQuery.UPDATE_ACTION_PENDING_TRANS_ERROR, bean, em);
        assertEquals(query.getParameterValue("pending"), bean.getPending());
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
        WorkflowActionQueryExecutor.getInstance().executeUpdate(WorkflowActionQuery.UPDATE_ACTION, bean);
        WorkflowActionBean retBean = WorkflowActionQueryExecutor.getInstance().get(WorkflowActionQuery.GET_ACTION,
                bean.getId());
        assertEquals(retBean.getStatus(), WorkflowAction.Status.RUNNING);
    }

    public void testGet() throws Exception {
        // TODO
    }

    public void testGetList() throws Exception {
        // TODO
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
