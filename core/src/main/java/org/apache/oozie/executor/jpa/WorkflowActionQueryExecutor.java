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

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;

import com.google.common.annotations.VisibleForTesting;

/**
 * Query Executor that provides API to run query for Workflow Action
 */
public class WorkflowActionQueryExecutor extends
        QueryExecutor<WorkflowActionBean, WorkflowActionQueryExecutor.WorkflowActionQuery> {

    public enum WorkflowActionQuery {
        UPDATE_ACTION,
        UPDATE_ACTION_FOR_LAST_CHECKED_TIME,
        UPDATE_ACTION_START,
        UPDATE_ACTION_CHECK,
        UPDATE_ACTION_END,
        UPDATE_ACTION_PENDING,
        UPDATE_ACTION_STATUS_PENDING,
        UPDATE_ACTION_PENDING_TRANS,
        UPDATE_ACTION_PENDING_TRANS_ERROR,
        GET_ACTION
    };

    private static WorkflowActionQueryExecutor instance = new WorkflowActionQueryExecutor();
    private static JPAService jpaService;

    private WorkflowActionQueryExecutor() {
        Services services = Services.get();
        if (services != null) {
            jpaService = services.get(JPAService.class);
        }
    }

    public static QueryExecutor<WorkflowActionBean, WorkflowActionQuery> getInstance() {
        if (instance == null) {
            // It will not be null in normal execution. Required for testcase as
            // they reinstantiate JPAService everytime
            instance = new WorkflowActionQueryExecutor();
        }
        return WorkflowActionQueryExecutor.instance;
    }

    @Override
    public Query getUpdateQuery(WorkflowActionQuery namedQuery, WorkflowActionBean actionBean, EntityManager em)
            throws JPAExecutorException {
        Query query = em.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case UPDATE_ACTION:
                query.setParameter("conf", actionBean.getConfBlob());
                query.setParameter("consoleUrl", actionBean.getConsoleUrl());
                query.setParameter("data", actionBean.getDataBlob());
                query.setParameter("stats", actionBean.getStatsBlob());
                query.setParameter("externalChildIDs", actionBean.getExternalChildIDsBlob());
                query.setParameter("errorCode", actionBean.getErrorCode());
                query.setParameter("errorMessage", actionBean.getErrorMessage());
                query.setParameter("externalId", actionBean.getExternalId());
                query.setParameter("externalStatus", actionBean.getExternalStatus());
                query.setParameter("name", actionBean.getName());
                query.setParameter("cred", actionBean.getCred());
                query.setParameter("retries", actionBean.getRetries());
                query.setParameter("trackerUri", actionBean.getTrackerUri());
                query.setParameter("transition", actionBean.getTransition());
                query.setParameter("type", actionBean.getType());
                query.setParameter("endTime", actionBean.getEndTimestamp());
                query.setParameter("executionPath", actionBean.getExecutionPath());
                query.setParameter("lastCheckTime", actionBean.getLastCheckTimestamp());
                query.setParameter("logToken", actionBean.getLogToken());
                query.setParameter("pending", actionBean.getPending());
                query.setParameter("pendingAge", actionBean.getPendingAgeTimestamp());
                query.setParameter("signalValue", actionBean.getSignalValue());
                query.setParameter("slaXml", actionBean.getSlaXmlBlob());
                query.setParameter("startTime", actionBean.getStartTimestamp());
                query.setParameter("status", actionBean.getStatusStr());
                query.setParameter("wfId", actionBean.getWfId());
                query.setParameter("id", actionBean.getId());
                break;
            case UPDATE_ACTION_FOR_LAST_CHECKED_TIME:
                query.setParameter("lastCheckTime", actionBean.getLastCheckTimestamp());
                query.setParameter("id", actionBean.getId());
                break;
            case UPDATE_ACTION_PENDING:
                query.setParameter("pending", actionBean.getPending());
                query.setParameter("id", actionBean.getId());
                break;
            case UPDATE_ACTION_STATUS_PENDING:
                query.setParameter("status", actionBean.getStatus().toString());
                query.setParameter("pending", actionBean.getPending());
                query.setParameter("id", actionBean.getId());
                break;
            case UPDATE_ACTION_PENDING_TRANS:
                query.setParameter("transition", actionBean.getTransition());
                query.setParameter("pending", actionBean.getPending());
                query.setParameter("id", actionBean.getId());
                break;
            case UPDATE_ACTION_PENDING_TRANS_ERROR:
                query.setParameter("transition", actionBean.getTransition());
                query.setParameter("pending", actionBean.getPending());
                query.setParameter("errorCode", actionBean.getErrorCode());
                query.setParameter("errorMessage", actionBean.getErrorMessage());
                query.setParameter("id", actionBean.getId());
                break;
            case UPDATE_ACTION_START:
                query.setParameter("startTime", actionBean.getStartTimestamp());
                query.setParameter("externalChildIDs", actionBean.getExternalChildIDsBlob());
                query.setParameter("conf", actionBean.getConfBlob());
                query.setParameter("errorCode", actionBean.getErrorCode());
                query.setParameter("errorMessage", actionBean.getErrorMessage());
                query.setParameter("externalId", actionBean.getExternalId());
                query.setParameter("trackerUri", actionBean.getTrackerUri());
                query.setParameter("consoleUrl", actionBean.getConsoleUrl());
                query.setParameter("lastCheckTime", actionBean.getLastCheckTimestamp());
                query.setParameter("status", actionBean.getStatus().toString());
                query.setParameter("externalStatus", actionBean.getExternalStatus());
                query.setParameter("data", actionBean.getDataBlob());
                query.setParameter("retries", actionBean.getRetries());
                query.setParameter("pending", actionBean.getPending());
                query.setParameter("pendingAge", actionBean.getPendingAgeTimestamp());
                query.setParameter("userRetryCount", actionBean.getUserRetryCount());
                query.setParameter("id", actionBean.getId());
                break;
            case UPDATE_ACTION_CHECK:
                query.setParameter("externalChildIDs", actionBean.getExternalChildIDsBlob());
                query.setParameter("externalStatus", actionBean.getExternalStatus());
                query.setParameter("status", actionBean.getStatus().toString());
                query.setParameter("data", actionBean.getDataBlob());
                query.setParameter("pending", actionBean.getPending());
                query.setParameter("errorCode", actionBean.getErrorCode());
                query.setParameter("errorMessage", actionBean.getErrorMessage());
                query.setParameter("lastCheckTime", actionBean.getLastCheckTimestamp());
                query.setParameter("retries", actionBean.getRetries());
                query.setParameter("pendingAge", actionBean.getPendingAgeTimestamp());
                query.setParameter("startTime", actionBean.getStartTimestamp());
                query.setParameter("stats", actionBean.getStatsBlob());
                query.setParameter("userRetryCount", actionBean.getUserRetryCount());
                query.setParameter("id", actionBean.getId());
                break;
            case UPDATE_ACTION_END:
                query.setParameter("errorCode", actionBean.getErrorCode());
                query.setParameter("errorMessage", actionBean.getErrorMessage());
                query.setParameter("retries", actionBean.getRetries());
                query.setParameter("status", actionBean.getStatus().toString());
                query.setParameter("endTime", actionBean.getEndTimestamp());
                query.setParameter("pending", actionBean.getPending());
                query.setParameter("pendingAge", actionBean.getPendingAgeTimestamp());
                query.setParameter("signalValue", actionBean.getSignalValue());
                query.setParameter("userRetryCount", actionBean.getUserRetryCount());
                query.setParameter("externalStatus", actionBean.getExternalStatus());
                query.setParameter("stats", actionBean.getStatsBlob());
                query.setParameter("id", actionBean.getId());
                break;
            default:
                throw new JPAExecutorException(ErrorCode.E0603, "QueryExecutor cannot set parameters for "
                        + namedQuery.name());
        }
        return query;
    }

    @Override
    public Query getSelectQuery(WorkflowActionQuery namedQuery, EntityManager em, Object... parameters)
            throws JPAExecutorException {
        Query query = em.createNamedQuery(namedQuery.name());
        WorkflowActionQuery waQuery = (WorkflowActionQuery) namedQuery;
        switch (waQuery) {
            case GET_ACTION:
                query.setParameter("id", parameters[0]);
                break;
            default:
                throw new JPAExecutorException(ErrorCode.E0603, "QueryExecutor cannot set parameters for "
                        + waQuery.name());
        }
        return query;
    }

    @Override
    public int executeUpdate(WorkflowActionQuery namedQuery, WorkflowActionBean actionBean) throws JPAExecutorException {
        EntityManager em = jpaService.getEntityManager();
        Query query = getUpdateQuery(namedQuery, actionBean, em);
        int ret = jpaService.executeUpdate(namedQuery.name(), query, em);
        return ret;
    }

    @Override
    public WorkflowActionBean get(WorkflowActionQuery namedQuery, Object... parameters) throws JPAExecutorException {
        EntityManager em = jpaService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        WorkflowActionBean bean = null;
        bean = (WorkflowActionBean) jpaService.executeGet(namedQuery.name(), query, em);
        if (bean == null) {
            throw new JPAExecutorException(ErrorCode.E0605, query.toString());
        }
        return bean;
    }

    @Override
    public List<WorkflowActionBean> getList(WorkflowActionQuery namedQuery, Object... parameters)
            throws JPAExecutorException {
        EntityManager em = jpaService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        List<WorkflowActionBean> beanList = (List<WorkflowActionBean>) jpaService.executeGetList(namedQuery.name(),
                query, em);
        if (beanList == null || beanList.size() == 0) {
            throw new JPAExecutorException(ErrorCode.E0605, query.toString());
        }
        return beanList;
    }

    @VisibleForTesting
    public static void destroy() {
        if (instance != null) {
            jpaService = null;
            instance = null;
        }
    }
}
