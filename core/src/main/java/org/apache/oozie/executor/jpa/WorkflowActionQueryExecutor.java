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
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.StringBlob;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.DateUtils;

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
        GET_ACTION,
        GET_ACTION_ID_TYPE_LASTCHECK,
        GET_ACTION_FAIL,
        GET_ACTION_SIGNAL,
        GET_ACTION_CHECK,
        GET_ACTION_END,
        GET_ACTION_COMPLETED,
        GET_RUNNING_ACTIONS,
        GET_PENDING_ACTIONS,
        GET_ACTIONS_FOR_WORKFLOW_RERUN
    };

    private static WorkflowActionQueryExecutor instance = new WorkflowActionQueryExecutor();

    private WorkflowActionQueryExecutor() {
    }

    public static QueryExecutor<WorkflowActionBean, WorkflowActionQuery> getInstance() {
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
                query.setParameter("pendingAge", actionBean.getPendingAgeTimestamp());
                query.setParameter("id", actionBean.getId());
                break;
            case UPDATE_ACTION_STATUS_PENDING:
                query.setParameter("status", actionBean.getStatus().toString());
                query.setParameter("pending", actionBean.getPending());
                query.setParameter("pendingAge", actionBean.getPendingAgeTimestamp());
                query.setParameter("id", actionBean.getId());
                break;
            case UPDATE_ACTION_PENDING_TRANS:
                query.setParameter("transition", actionBean.getTransition());
                query.setParameter("pending", actionBean.getPending());
                query.setParameter("pendingAge", actionBean.getPendingAgeTimestamp());
                query.setParameter("id", actionBean.getId());
                break;
            case UPDATE_ACTION_PENDING_TRANS_ERROR:
                query.setParameter("transition", actionBean.getTransition());
                query.setParameter("pending", actionBean.getPending());
                query.setParameter("pendingAge", actionBean.getPendingAgeTimestamp());
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
        switch (namedQuery) {
            case GET_ACTION:
            case GET_ACTION_ID_TYPE_LASTCHECK:
            case GET_ACTION_FAIL:
            case GET_ACTION_SIGNAL:
            case GET_ACTION_CHECK:
            case GET_ACTION_END:
            case GET_ACTION_COMPLETED:
                query.setParameter("id", parameters[0]);
                break;
            case GET_RUNNING_ACTIONS:
                Timestamp ts = new Timestamp(System.currentTimeMillis() - (Integer) parameters[0] * 1000);
                query.setParameter("lastCheckTime", ts);
                break;
            case GET_PENDING_ACTIONS:
                Long minimumPendingAgeSecs = (Long) parameters[0];
                Timestamp pts = new Timestamp(System.currentTimeMillis() - minimumPendingAgeSecs * 1000);
                Timestamp createdTimeInterval = new Timestamp((Long) parameters[1]);
                query.setParameter("pendingAge", pts);
                query.setParameter("createdTime", createdTimeInterval);
                break;
            case GET_ACTIONS_FOR_WORKFLOW_RERUN:
                query.setParameter("wfId", parameters[0]);
                break;
            default:
                throw new JPAExecutorException(ErrorCode.E0603, "QueryExecutor cannot set parameters for "
                        + namedQuery.name());
        }
        return query;
    }

    @Override
    public int executeUpdate(WorkflowActionQuery namedQuery, WorkflowActionBean actionBean) throws JPAExecutorException {
        JPAService jpaService = Services.get().get(JPAService.class);
        EntityManager em = jpaService.getEntityManager();
        Query query = getUpdateQuery(namedQuery, actionBean, em);
        int ret = jpaService.executeUpdate(namedQuery.name(), query, em);
        return ret;
    }

    private WorkflowActionBean constructBean(WorkflowActionQuery namedQuery, Object ret) throws JPAExecutorException {
        WorkflowActionBean bean;
        Object[] arr;
        switch (namedQuery) {
            case GET_ACTION:
                bean = (WorkflowActionBean) ret;
                break;
            case GET_ACTION_ID_TYPE_LASTCHECK:
                bean = new WorkflowActionBean();
                arr = (Object[]) ret;
                bean.setId((String) arr[0]);
                bean.setType((String) arr[1]);
                bean.setLastCheckTime(DateUtils.toDate((Timestamp) arr[2]));
                break;
            case GET_ACTION_FAIL:
                bean = new WorkflowActionBean();
                arr = (Object[]) ret;
                bean.setId((String) arr[0]);
                bean.setJobId((String) arr[1]);
                bean.setName((String) arr[2]);
                bean.setStatusStr((String) arr[3]);
                bean.setPending((Integer) arr[4]);
                bean.setType((String) arr[5]);
                bean.setLogToken((String) arr[6]);
                bean.setTransition((String) arr[7]);
                bean.setErrorInfo((String) arr[8], (String) arr[9]);
                break;
            case GET_ACTION_SIGNAL:
                bean = new WorkflowActionBean();
                arr = (Object[]) ret;
                bean.setId((String) arr[0]);
                bean.setJobId((String) arr[1]);
                bean.setName((String) arr[2]);
                bean.setStatusStr((String) arr[3]);
                bean.setPending((Integer) arr[4]);
                bean.setPendingAge(DateUtils.toDate((Timestamp) arr[5]));
                bean.setType((String) arr[6]);
                bean.setLogToken((String) arr[7]);
                bean.setTransition((String) arr[8]);
                bean.setErrorInfo((String) arr[9], (String) arr[10]);
                bean.setExecutionPath((String) arr[11]);
                bean.setSignalValue((String) arr[12]);
                bean.setSlaXmlBlob((StringBlob) arr[13]);
                bean.setExternalId((String) arr[14]);
                break;
            case GET_ACTION_CHECK:
                bean = new WorkflowActionBean();
                arr = (Object[]) ret;
                bean.setId((String) arr[0]);
                bean.setJobId((String) arr[1]);
                bean.setName((String) arr[2]);
                bean.setStatusStr((String) arr[3]);
                bean.setPending((Integer) arr[4]);
                bean.setPendingAge(DateUtils.toDate((Timestamp) arr[5]));
                bean.setType((String) arr[6]);
                bean.setLogToken((String) arr[7]);
                bean.setTransition((String) arr[8]);
                bean.setRetries((Integer) arr[9]);
                bean.setUserRetryCount((Integer) arr[10]);
                bean.setUserRetryMax((Integer) arr[11]);
                bean.setUserRetryInterval((Integer) arr[12]);
                bean.setTrackerUri((String) arr[13]);
                bean.setStartTime(DateUtils.toDate((Timestamp) arr[14]));
                bean.setEndTime(DateUtils.toDate((Timestamp) arr[15]));
                bean.setLastCheckTime(DateUtils.toDate((Timestamp) arr[16]));
                bean.setErrorInfo((String) arr[17], (String) arr[18]);
                bean.setExternalId((String) arr[19]);
                bean.setExternalStatus((String) arr[20]);
                bean.setExternalChildIDsBlob((StringBlob) arr[21]);
                bean.setConfBlob((StringBlob) arr[22]);
                break;
            case GET_ACTION_END:
                bean = new WorkflowActionBean();
                arr = (Object[]) ret;
                bean.setId((String) arr[0]);
                bean.setJobId((String) arr[1]);
                bean.setName((String) arr[2]);
                bean.setStatusStr((String) arr[3]);
                bean.setPending((Integer) arr[4]);
                bean.setPendingAge(DateUtils.toDate((Timestamp) arr[5]));
                bean.setType((String) arr[6]);
                bean.setLogToken((String) arr[7]);
                bean.setTransition((String) arr[8]);
                bean.setRetries((Integer) arr[9]);
                bean.setTrackerUri((String) arr[10]);
                bean.setUserRetryCount((Integer) arr[11]);
                bean.setUserRetryMax((Integer) arr[12]);
                bean.setUserRetryInterval((Integer) arr[13]);
                bean.setStartTime(DateUtils.toDate((Timestamp) arr[14]));
                bean.setEndTime(DateUtils.toDate((Timestamp) arr[15]));
                bean.setErrorInfo((String) arr[16], (String) arr[17]);
                bean.setExternalId((String) arr[18]);
                bean.setExternalStatus((String) arr[19]);
                bean.setExternalChildIDsBlob((StringBlob) arr[20]);
                bean.setConfBlob((StringBlob) arr[21]);
                bean.setDataBlob((StringBlob) arr[22]);
                bean.setStatsBlob((StringBlob) arr[23]);
                break;
            case GET_ACTION_COMPLETED:
                bean = new WorkflowActionBean();
                arr = (Object[]) ret;
                bean.setId((String) arr[0]);
                bean.setJobId((String) arr[1]);
                bean.setStatusStr((String) arr[2]);
                bean.setType((String) arr[3]);
                bean.setLogToken((String) arr[4]);
                break;
            case GET_RUNNING_ACTIONS:
                bean = new WorkflowActionBean();
                bean.setId((String)ret);
                break;
            case GET_PENDING_ACTIONS:
                bean = new WorkflowActionBean();
                arr = (Object[]) ret;
                bean.setId((String) arr[0]);
                bean.setJobId((String) arr[1]);
                bean.setStatusStr((String) arr[2]);
                bean.setType((String) arr[3]);
                bean.setPendingAge(DateUtils.toDate((Timestamp) arr[4]));
                break;
            case GET_ACTIONS_FOR_WORKFLOW_RERUN:
                bean = new WorkflowActionBean();
                arr = (Object[]) ret;
                bean.setId((String) arr[0]);
                bean.setName((String) arr[1]);
                bean.setStatusStr((String) arr[2]);
                bean.setEndTime(DateUtils.toDate((Timestamp) arr[3]));
                bean.setType((String) arr[4]);
                break;
            default:
                throw new JPAExecutorException(ErrorCode.E0603, "QueryExecutor cannot construct action bean for "
                        + namedQuery.name());
        }
        return bean;
    }

    @Override
    public WorkflowActionBean get(WorkflowActionQuery namedQuery, Object... parameters) throws JPAExecutorException {
        JPAService jpaService = Services.get().get(JPAService.class);
        EntityManager em = jpaService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        Object ret = jpaService.executeGet(namedQuery.name(), query, em);
        if (ret == null) {
            throw new JPAExecutorException(ErrorCode.E0605, query.toString());
        }
        WorkflowActionBean bean = constructBean(namedQuery, ret);
        return bean;
    }

    @Override
    public List<WorkflowActionBean> getList(WorkflowActionQuery namedQuery, Object... parameters)
            throws JPAExecutorException {
        JPAService jpaService = Services.get().get(JPAService.class);
        EntityManager em = jpaService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        List<?> retList = (List<?>) jpaService.executeGetList(namedQuery.name(), query, em);
        List<WorkflowActionBean> beanList = new ArrayList<WorkflowActionBean>();
        if (retList != null) {
            for (Object ret : retList) {
                beanList.add(constructBean(namedQuery, ret));
            }
        }
        return beanList;
    }

    @Override
    public Object getSingleValue(WorkflowActionQuery namedQuery, Object... parameters) throws JPAExecutorException {
        throw new UnsupportedOperationException();
    }
}
