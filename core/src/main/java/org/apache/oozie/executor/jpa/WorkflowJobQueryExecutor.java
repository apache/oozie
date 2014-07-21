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

import org.apache.oozie.BinaryBlob;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.StringBlob;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.DateUtils;

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
        GET_WORKFLOW_STARTTIME,
        GET_WORKFLOW_USER_GROUP,
        GET_WORKFLOW_SUSPEND,
        GET_WORKFLOW_ACTION_OP,
        GET_WORKFLOW_RERUN,
        GET_WORKFLOW_DEFINITION,
        GET_WORKFLOW_KILL,
        GET_WORKFLOW_RESUME,
        GET_WORKFLOW_STATUS,
        GET_WORKFLOWS_PARENT_COORD_RERUN
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
            case GET_WORKFLOW_STARTTIME:
            case GET_WORKFLOW_USER_GROUP:
            case GET_WORKFLOW_SUSPEND:
            case GET_WORKFLOW_ACTION_OP:
            case GET_WORKFLOW_RERUN:
            case GET_WORKFLOW_DEFINITION:
            case GET_WORKFLOW_KILL:
            case GET_WORKFLOW_RESUME:
            case GET_WORKFLOW_STATUS:
                query.setParameter("id", parameters[0]);
                break;
            case GET_WORKFLOWS_PARENT_COORD_RERUN:
                query.setParameter("parentId", parameters[0]);
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

    private WorkflowJobBean constructBean(WorkflowJobQuery namedQuery, Object ret, Object... parameters)
            throws JPAExecutorException {
        WorkflowJobBean bean;
        Object[] arr;
        switch (namedQuery) {
            case GET_WORKFLOW:
                bean = (WorkflowJobBean) ret;
                break;
            case GET_WORKFLOW_STARTTIME:
                bean = new WorkflowJobBean();
                arr = (Object[]) ret;
                bean.setId((String) arr[0]);
                bean.setStartTime(DateUtils.toDate((Timestamp) arr[1]));
                break;
            case GET_WORKFLOW_USER_GROUP:
                bean = new WorkflowJobBean();
                arr = (Object[]) ret;
                bean.setUser((String) arr[0]);
                bean.setGroup((String) arr[1]);
                break;
            case GET_WORKFLOW_SUSPEND:
                bean = new WorkflowJobBean();
                arr = (Object[]) ret;
                bean.setId((String) arr[0]);
                bean.setUser((String) arr[1]);
                bean.setGroup((String) arr[2]);
                bean.setAppName((String) arr[3]);
                bean.setStatusStr((String) arr[4]);
                bean.setParentId((String) arr[5]);
                bean.setStartTime(DateUtils.toDate((Timestamp) arr[6]));
                bean.setEndTime(DateUtils.toDate((Timestamp) arr[7]));
                bean.setLogToken((String) arr[8]);
                bean.setWfInstanceBlob((BinaryBlob) (arr[9]));
                break;
            case GET_WORKFLOW_ACTION_OP:
                bean = new WorkflowJobBean();
                arr = (Object[]) ret;
                bean.setId((String) arr[0]);
                bean.setUser((String) arr[1]);
                bean.setGroup((String) arr[2]);
                bean.setAppName((String) arr[3]);
                bean.setAppPath((String) arr[4]);
                bean.setStatusStr((String) arr[5]);
                bean.setParentId((String) arr[6]);
                bean.setLogToken((String) arr[7]);
                bean.setWfInstanceBlob((BinaryBlob) (arr[8]));
                bean.setProtoActionConfBlob((StringBlob) arr[9]);
                break;
            case GET_WORKFLOW_RERUN:
                bean = new WorkflowJobBean();
                arr = (Object[]) ret;
                bean.setId((String) arr[0]);
                bean.setUser((String) arr[1]);
                bean.setGroup((String) arr[2]);
                bean.setAppName((String) arr[3]);
                bean.setStatusStr((String) arr[4]);
                bean.setRun((Integer) arr[5]);
                bean.setLogToken((String) arr[6]);
                bean.setWfInstanceBlob((BinaryBlob) (arr[7]));
                break;
            case GET_WORKFLOW_DEFINITION:
                bean = new WorkflowJobBean();
                arr = (Object[]) ret;
                bean.setId((String) arr[0]);
                bean.setUser((String) arr[1]);
                bean.setGroup((String) arr[2]);
                bean.setAppName((String) arr[3]);
                bean.setLogToken((String) arr[4]);
                bean.setWfInstanceBlob((BinaryBlob) (arr[5]));
                break;
            case GET_WORKFLOW_KILL:
                bean = new WorkflowJobBean();
                arr = (Object[]) ret;
                bean.setId((String) arr[0]);
                bean.setUser((String) arr[1]);
                bean.setGroup((String) arr[2]);
                bean.setAppName((String) arr[3]);
                bean.setAppPath((String) arr[4]);
                bean.setStatusStr((String) arr[5]);
                bean.setParentId((String) arr[6]);
                bean.setStartTime(DateUtils.toDate((Timestamp) arr[7]));
                bean.setEndTime(DateUtils.toDate((Timestamp) arr[8]));
                bean.setLogToken((String) arr[9]);
                bean.setWfInstanceBlob((BinaryBlob) (arr[10]));
                bean.setSlaXmlBlob((StringBlob) arr[11]);
                break;
            case GET_WORKFLOW_RESUME:
                bean = new WorkflowJobBean();
                arr = (Object[]) ret;
                bean.setId((String) arr[0]);
                bean.setUser((String) arr[1]);
                bean.setGroup((String) arr[2]);
                bean.setAppName((String) arr[3]);
                bean.setAppPath((String) arr[4]);
                bean.setStatusStr((String) arr[5]);
                bean.setParentId((String) arr[6]);
                bean.setStartTime(DateUtils.toDate((Timestamp) arr[7]));
                bean.setEndTime(DateUtils.toDate((Timestamp) arr[8]));
                bean.setLogToken((String) arr[9]);
                bean.setWfInstanceBlob((BinaryBlob) (arr[10]));
                bean.setProtoActionConfBlob((StringBlob) arr[11]);
                break;
            case GET_WORKFLOW_STATUS:
                bean = new WorkflowJobBean();
                bean.setId((String) parameters[0]);
                bean.setStatusStr((String) ret);
                break;
            case GET_WORKFLOWS_PARENT_COORD_RERUN:
                bean = new WorkflowJobBean();
                arr = (Object[]) ret;
                bean.setId((String) arr[0]);
                bean.setStatusStr((String) arr[1]);
                bean.setStartTime(DateUtils.toDate((Timestamp) arr[2]));
                bean.setEndTime(DateUtils.toDate((Timestamp) arr[3]));
                break;
            default:
                throw new JPAExecutorException(ErrorCode.E0603, "QueryExecutor cannot construct job bean for "
                        + namedQuery.name());
        }
        return bean;
    }

    @Override
    public WorkflowJobBean get(WorkflowJobQuery namedQuery, Object... parameters) throws JPAExecutorException {
        EntityManager em = jpaService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        Object ret = jpaService.executeGet(namedQuery.name(), query, em);
        if (ret == null) {
            throw new JPAExecutorException(ErrorCode.E0604, query.toString());
        }
        WorkflowJobBean bean = constructBean(namedQuery, ret, parameters);
        return bean;
    }

    @Override
    public List<WorkflowJobBean> getList(WorkflowJobQuery namedQuery, Object... parameters) throws JPAExecutorException {
        EntityManager em = jpaService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        List<?> retList = (List<?>) jpaService.executeGetList(namedQuery.name(), query, em);
        List<WorkflowJobBean> beanList = new ArrayList<WorkflowJobBean>();
        if (retList != null) {
            for (Object ret : retList) {
                beanList.add(constructBean(namedQuery, ret, parameters));
            }
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
