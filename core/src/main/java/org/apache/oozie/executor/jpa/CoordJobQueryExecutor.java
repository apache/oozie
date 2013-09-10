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

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;

import com.google.common.annotations.VisibleForTesting;

/**
 * Query Executor that provides API to run query for Coordinator Job
 */

public class CoordJobQueryExecutor extends QueryExecutor<CoordinatorJobBean, CoordJobQueryExecutor.CoordJobQuery> {

    public enum CoordJobQuery {
        UPDATE_COORD_JOB,
        UPDATE_COORD_JOB_STATUS,
        UPDATE_COORD_JOB_BUNDLEID,
        UPDATE_COORD_JOB_APPNAMESPACE,
        UPDATE_COORD_JOB_STATUS_PENDING,
        UPDATE_COORD_JOB_BUNDLEID_APPNAMESPACE_PAUSETIME,
        UPDATE_COORD_JOB_STATUS_MODTIME,
        UPDATE_COORD_JOB_STATUS_PENDING_MODTIME,
        UPDATE_COORD_JOB_LAST_MODIFIED_TIME,
        UPDATE_COORD_JOB_STATUS_PENDING_TIME,
        UPDATE_COORD_JOB_MATERIALIZE,
        UPDATE_COORD_JOB_CHANGE,
        GET_COORD_JOB
    };

    private static CoordJobQueryExecutor instance = new CoordJobQueryExecutor();
    private static JPAService jpaService;

    private CoordJobQueryExecutor() {
        Services services = Services.get();
        if (services != null) {
            jpaService = services.get(JPAService.class);
        }
    }

    public static CoordJobQueryExecutor getInstance() {
        if (instance == null) {
            // It will not be null in normal execution. Required for testcase as
            // they reinstantiate JPAService everytime
            instance = new CoordJobQueryExecutor();
        }
        return CoordJobQueryExecutor.instance;
    }

    @Override
    public Query getUpdateQuery(CoordJobQuery namedQuery, CoordinatorJobBean cjBean, EntityManager em)
            throws JPAExecutorException {
        Query query = em.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case UPDATE_COORD_JOB:
                query.setParameter("appName", cjBean.getAppName());
                query.setParameter("appPath", cjBean.getAppPath());
                query.setParameter("concurrency", cjBean.getConcurrency());
                query.setParameter("conf", cjBean.getConfBlob());
                query.setParameter("externalId", cjBean.getExternalId());
                query.setParameter("frequency", cjBean.getFrequency());
                query.setParameter("lastActionNumber", cjBean.getLastActionNumber());
                query.setParameter("timeOut", cjBean.getTimeout());
                query.setParameter("timeZone", cjBean.getTimeZone());
                query.setParameter("createdTime", cjBean.getCreatedTimestamp());
                query.setParameter("endTime", cjBean.getEndTimestamp());
                query.setParameter("execution", cjBean.getExecution());
                query.setParameter("jobXml", cjBean.getJobXmlBlob());
                query.setParameter("lastAction", cjBean.getLastActionTimestamp());
                query.setParameter("lastModifiedTime", cjBean.getLastModifiedTimestamp());
                query.setParameter("nextMaterializedTime", cjBean.getNextMaterializedTimestamp());
                query.setParameter("origJobXml", cjBean.getOrigJobXmlBlob());
                query.setParameter("slaXml", cjBean.getSlaXmlBlob());
                query.setParameter("startTime", cjBean.getStartTimestamp());
                query.setParameter("status", cjBean.getStatus().toString());
                query.setParameter("timeUnit", cjBean.getTimeUnitStr());
                query.setParameter("id", cjBean.getId());
                break;
            case UPDATE_COORD_JOB_STATUS:
                query.setParameter("status", cjBean.getStatus().toString());
                query.setParameter("id", cjBean.getId());
                break;
            case UPDATE_COORD_JOB_BUNDLEID:
                query.setParameter("bundleId", cjBean.getBundleId());
                query.setParameter("id", cjBean.getId());
                break;
            case UPDATE_COORD_JOB_APPNAMESPACE:
                query.setParameter("appNamespace", cjBean.getAppNamespace());
                query.setParameter("id", cjBean.getId());
                break;
            case UPDATE_COORD_JOB_STATUS_PENDING:
                query.setParameter("status", cjBean.getStatus().toString());
                query.setParameter("pending", cjBean.isPending() ? 1 : 0);
                query.setParameter("id", cjBean.getId());
                break;
            case UPDATE_COORD_JOB_BUNDLEID_APPNAMESPACE_PAUSETIME:
                query.setParameter("bundleId", cjBean.getBundleId());
                query.setParameter("appNamespace", cjBean.getAppNamespace());
                query.setParameter("pauseTime", cjBean.getPauseTimestamp());
                query.setParameter("id", cjBean.getId());
                break;
            case UPDATE_COORD_JOB_STATUS_MODTIME:
                query.setParameter("status", cjBean.getStatus().toString());
                query.setParameter("lastModifiedTime", cjBean.getLastModifiedTimestamp());
                query.setParameter("id", cjBean.getId());
                break;
            case UPDATE_COORD_JOB_STATUS_PENDING_MODTIME:
                query.setParameter("status", cjBean.getStatus().toString());
                query.setParameter("pending", cjBean.isPending() ? 1 : 0);
                query.setParameter("lastModifiedTime", cjBean.getLastModifiedTimestamp());
                query.setParameter("id", cjBean.getId());
                break;
            case UPDATE_COORD_JOB_LAST_MODIFIED_TIME:
                query.setParameter("lastModifiedTime", cjBean.getLastModifiedTimestamp());
                query.setParameter("id", cjBean.getId());
                break;
            case UPDATE_COORD_JOB_STATUS_PENDING_TIME:
                query.setParameter("status", cjBean.getStatus().toString());
                query.setParameter("pending", cjBean.isPending() ? 1 : 0);
                query.setParameter("doneMaterialization", cjBean.isDoneMaterialization() ? 1 : 0);
                query.setParameter("lastModifiedTime", cjBean.getLastModifiedTimestamp());
                query.setParameter("suspendedTime", cjBean.getSuspendedTimestamp());
                query.setParameter("id", cjBean.getId());
                break;
            case UPDATE_COORD_JOB_MATERIALIZE:
                query.setParameter("status", cjBean.getStatus().toString());
                query.setParameter("pending", cjBean.isPending() ? 1 : 0);
                query.setParameter("doneMaterialization", cjBean.isDoneMaterialization() ? 1 : 0);
                query.setParameter("lastActionTime", cjBean.getLastActionTimestamp());
                query.setParameter("lastActionNumber", cjBean.getLastActionNumber());
                query.setParameter("nextMatdTime", cjBean.getNextMaterializedTimestamp());
                query.setParameter("id", cjBean.getId());
                break;
            case UPDATE_COORD_JOB_CHANGE:
                query.setParameter("endTime", cjBean.getEndTimestamp());
                query.setParameter("status", cjBean.getStatus().toString());
                query.setParameter("pending", cjBean.isPending() ? 1 : 0);
                query.setParameter("doneMaterialization", cjBean.isDoneMaterialization() ? 1 : 0);
                query.setParameter("concurrency", cjBean.getConcurrency());
                query.setParameter("pauseTime", cjBean.getPauseTimestamp());
                query.setParameter("lastActionNumber", cjBean.getLastActionNumber());
                query.setParameter("lastActionTime", cjBean.getLastActionTimestamp());
                query.setParameter("nextMatdTime", cjBean.getNextMaterializedTimestamp());
                query.setParameter("id", cjBean.getId());
                break;
            default:
                throw new JPAExecutorException(ErrorCode.E0603, "QueryExecutor cannot set parameters for "
                        + namedQuery.name());
        }
        return query;
    }

    @Override
    public Query getSelectQuery(CoordJobQuery namedQuery, EntityManager em, Object... parameters)
            throws JPAExecutorException {
        Query query = em.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case GET_COORD_JOB:
                query.setParameter("id", parameters[0]);
                break;
            default:
                throw new JPAExecutorException(ErrorCode.E0603, "QueryExecutor cannot set parameters for "
                        + namedQuery.name());
        }
        return query;
    }

    @Override
    public int executeUpdate(CoordJobQuery namedQuery, CoordinatorJobBean jobBean) throws JPAExecutorException {
        EntityManager em = jpaService.getEntityManager();
        Query query = getUpdateQuery(namedQuery, jobBean, em);
        int ret = jpaService.executeUpdate(namedQuery.name(), query, em);
        return ret;
    }

    @Override
    public CoordinatorJobBean get(CoordJobQuery namedQuery, Object... parameters) throws JPAExecutorException {
        EntityManager em = jpaService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        @SuppressWarnings("unchecked")
        CoordinatorJobBean bean = (CoordinatorJobBean) jpaService.executeGet(namedQuery.name(), query, em);
        if (bean == null) {
            throw new JPAExecutorException(ErrorCode.E0604, query.toString());
        }
        return bean;
    }

    @Override
    public List<CoordinatorJobBean> getList(CoordJobQuery namedQuery, Object... parameters) throws JPAExecutorException {
        EntityManager em = jpaService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        @SuppressWarnings("unchecked")
        List<CoordinatorJobBean> beanList = (List<CoordinatorJobBean>) jpaService.executeGetList(namedQuery.name(),
                query, em);
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
