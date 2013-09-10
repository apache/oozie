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
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;

import com.google.common.annotations.VisibleForTesting;

/**
 * Query Executor that provides API to run query for Workflow Job
 */
public class WorkflowJobQueryExecutor extends QueryExecutor<WorkflowJobBean, WorkflowJobQueryExecutor.WorkflowJobQuery> {

    public enum WorkflowJobQuery {
        UPDATE_WORKFLOW,
        UPDATE_WORKFLOW_MODTIME,
        UPDATE_WORKFLOW_STATUS_MODTIME,
        UPDATE_WORKFLOW_PARENT_MODIFIED,
        UPDATE_WORKFLOW_STATUS_INSTANCE_MODIFIED,
        UPDATE_WORKFLOW_STATUS_INSTANCE_MOD_END,
        UPDATE_WORKFLOW_STATUS_INSTANCE_MOD_START_END,
        UPDATE_WORKFLOW_RERUN,
        GET_WORKFLOW,
        DELETE_WORKFLOW
    };

    private static WorkflowJobQueryExecutor instance = new WorkflowJobQueryExecutor();
    private static JPAService jpaService;

    private WorkflowJobQueryExecutor() {
        Services services = Services.get();
        if (services != null) {
            jpaService = services.get(JPAService.class);
        }
    }

    public static QueryExecutor<WorkflowJobBean, WorkflowJobQueryExecutor.WorkflowJobQuery> getInstance() {
        if (instance == null) {
            // It will not be null in normal execution. Required for test as
            // they reinstantiate JPAService everytime
            instance = new WorkflowJobQueryExecutor();
        }
        return WorkflowJobQueryExecutor.instance;
    }

    @Override
    public Query getUpdateQuery(WorkflowJobQuery namedQuery, WorkflowJobBean wfBean, EntityManager em)
            throws JPAExecutorException {

        Query query = em.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case UPDATE_WORKFLOW:
                query.setParameter("appName", wfBean.getAppName());
                query.setParameter("appPath", wfBean.getAppPath());
                query.setParameter("conf", wfBean.getConfBlob());
                query.setParameter("groupName", wfBean.getGroup());
                query.setParameter("run", wfBean.getRun());
                query.setParameter("user", wfBean.getUser());
                query.setParameter("createdTime", wfBean.getCreatedTimestamp());
                query.setParameter("endTime", wfBean.getEndTimestamp());
                query.setParameter("externalId", wfBean.getExternalId());
                query.setParameter("lastModTime", wfBean.getLastModifiedTimestamp());
                query.setParameter("logToken", wfBean.getLogToken());
                query.setParameter("protoActionConf", wfBean.getProtoActionConfBlob());
                query.setParameter("slaXml", wfBean.getSlaXmlBlob());
                query.setParameter("startTime", wfBean.getStartTimestamp());
                query.setParameter("status", wfBean.getStatusStr());
                query.setParameter("wfInstance", wfBean.getWfInstanceBlob());
                query.setParameter("id", wfBean.getId());
                break;
            case UPDATE_WORKFLOW_MODTIME:
                query.setParameter("lastModTime", wfBean.getLastModifiedTimestamp());
                query.setParameter("id", wfBean.getId());
                break;
            case UPDATE_WORKFLOW_STATUS_MODTIME:
                query.setParameter("status", wfBean.getStatus().toString());
                query.setParameter("lastModTime", wfBean.getLastModifiedTimestamp());
                query.setParameter("id", wfBean.getId());
                break;
            case UPDATE_WORKFLOW_PARENT_MODIFIED:
                query.setParameter("parentId", wfBean.getParentId());
                query.setParameter("lastModTime", wfBean.getLastModifiedTimestamp());
                query.setParameter("id", wfBean.getId());
                break;
            case UPDATE_WORKFLOW_STATUS_INSTANCE_MODIFIED:
                query.setParameter("status", wfBean.getStatus().toString());
                query.setParameter("wfInstance", wfBean.getWfInstanceBlob());
                query.setParameter("lastModTime", wfBean.getLastModifiedTimestamp());
                query.setParameter("id", wfBean.getId());
                break;
            case UPDATE_WORKFLOW_STATUS_INSTANCE_MOD_END:
                query.setParameter("status", wfBean.getStatus().toString());
                query.setParameter("wfInstance", wfBean.getWfInstanceBlob());
                query.setParameter("lastModTime", wfBean.getLastModifiedTimestamp());
                query.setParameter("endTime", wfBean.getEndTimestamp());
                query.setParameter("id", wfBean.getId());
                break;
            case UPDATE_WORKFLOW_STATUS_INSTANCE_MOD_START_END:
                query.setParameter("status", wfBean.getStatus().toString());
                query.setParameter("wfInstance", wfBean.getWfInstanceBlob());
                query.setParameter("lastModTime", wfBean.getLastModifiedTimestamp());
                query.setParameter("startTime", wfBean.getStartTimestamp());
                query.setParameter("endTime", wfBean.getEndTimestamp());
                query.setParameter("id", wfBean.getId());
                break;
            case UPDATE_WORKFLOW_RERUN:
                query.setParameter("appName", wfBean.getAppName());
                query.setParameter("protoActionConf", wfBean.getProtoActionConfBlob());
                query.setParameter("appPath", wfBean.getAppPath());
                query.setParameter("conf", wfBean.getConfBlob());
                query.setParameter("logToken", wfBean.getLogToken());
                query.setParameter("user", wfBean.getUser());
                query.setParameter("group", wfBean.getGroup());
                query.setParameter("externalId", wfBean.getExternalId());
                query.setParameter("endTime", wfBean.getEndTimestamp());
                query.setParameter("run", wfBean.getRun());
                query.setParameter("status", wfBean.getStatus().toString());
                query.setParameter("wfInstance", wfBean.getWfInstanceBlob());
                query.setParameter("lastModTime", wfBean.getLastModifiedTimestamp());
                query.setParameter("id", wfBean.getId());
                break;
            default:
                throw new JPAExecutorException(ErrorCode.E0603, "QueryExecutor cannot set parameters for "
                        + namedQuery.name());
        }
        return query;
    }

    @Override
    public Query getSelectQuery(WorkflowJobQuery namedQuery, EntityManager em, Object... parameters)
            throws JPAExecutorException {
        Query query = em.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case GET_WORKFLOW:
                query.setParameter("id", parameters[0]);
                break;
            default:
                throw new JPAExecutorException(ErrorCode.E0603, "QueryExecutor cannot set parameters for "
                        + namedQuery.name());
        }
        return query;
    }

    @Override
    public int executeUpdate(WorkflowJobQuery namedQuery, WorkflowJobBean jobBean) throws JPAExecutorException {
        EntityManager em = jpaService.getEntityManager();
        Query query = getUpdateQuery(namedQuery, jobBean, em);
        int ret = jpaService.executeUpdate(namedQuery.name(), query, em);
        return ret;
    }

    @Override
    public WorkflowJobBean get(WorkflowJobQuery namedQuery, Object... parameters) throws JPAExecutorException {
        EntityManager em = jpaService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        WorkflowJobBean bean = (WorkflowJobBean) jpaService.executeGet(namedQuery.name(), query, em);
        if (bean == null) {
            throw new JPAExecutorException(ErrorCode.E0604, query.toString());
        }
        return bean;
    }

    @Override
    public List<WorkflowJobBean> getList(WorkflowJobQuery namedQuery, Object... parameters) throws JPAExecutorException {
        EntityManager em = jpaService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        List<WorkflowJobBean> beanList = (List<WorkflowJobBean>) jpaService
                .executeGetList(namedQuery.name(), query, em);
        if (beanList == null || beanList.size() == 0) {
            throw new JPAExecutorException(ErrorCode.E0604, query.toString());
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
