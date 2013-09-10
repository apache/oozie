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

import java.util.Date;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.Query;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;

import com.google.common.annotations.VisibleForTesting;

/**
 * Query Executor that provides API to run query for Coordinator Action
 */
public class CoordActionQueryExecutor extends
        QueryExecutor<CoordinatorActionBean, CoordActionQueryExecutor.CoordActionQuery> {

    public enum CoordActionQuery {
        UPDATE_COORD_ACTION,
        UPDATE_COORD_ACTION_STATUS_PENDING_TIME,
        UPDATE_COORD_ACTION_FOR_INPUTCHECK,
        UPDATE_COORD_ACTION_FOR_PUSH_INPUTCHECK,
        UPDATE_COORD_ACTION_DEPENDENCIES,
        UPDATE_COORD_ACTION_FOR_START,
        UPDATE_COORD_ACTION_FOR_MODIFIED_DATE,
        UPDATE_COORD_ACTION_RERUN,
        GET_COORD_ACTION
    };

    private static CoordActionQueryExecutor instance = new CoordActionQueryExecutor();
    private static JPAService jpaService;

    private CoordActionQueryExecutor() {
        Services services = Services.get();
        if (services != null) {
            jpaService = services.get(JPAService.class);
        }
    }

    public static QueryExecutor<CoordinatorActionBean, CoordActionQueryExecutor.CoordActionQuery> getInstance() {
        if (instance == null) {
            // It will not be null in normal execution. Required for testcase as
            // they reinstantiate JPAService everytime
            instance = new CoordActionQueryExecutor();
        }
        return CoordActionQueryExecutor.instance;
    }

    @Override
    public Query getUpdateQuery(CoordActionQuery namedQuery, CoordinatorActionBean actionBean, EntityManager em)
            throws JPAExecutorException {

        Query query = em.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case UPDATE_COORD_ACTION:
                query.setParameter("actionNumber", actionBean.getActionNumber());
                query.setParameter("actionXml", actionBean.getActionXmlBlob());
                query.setParameter("consoleUrl", actionBean.getConsoleUrl());
                query.setParameter("createdConf", actionBean.getCreatedConfBlob());
                query.setParameter("errorCode", actionBean.getErrorCode());
                query.setParameter("errorMessage", actionBean.getErrorMessage());
                query.setParameter("externalStatus", actionBean.getExternalStatus());
                query.setParameter("missingDependencies", actionBean.getMissingDependenciesBlob());
                query.setParameter("runConf", actionBean.getRunConfBlob());
                query.setParameter("timeOut", actionBean.getTimeOut());
                query.setParameter("trackerUri", actionBean.getTrackerUri());
                query.setParameter("type", actionBean.getType());
                query.setParameter("createdTime", actionBean.getCreatedTimestamp());
                query.setParameter("externalId", actionBean.getExternalId());
                query.setParameter("jobId", actionBean.getJobId());
                query.setParameter("lastModifiedTime", new Date());
                query.setParameter("nominalTime", actionBean.getNominalTimestamp());
                query.setParameter("slaXml", actionBean.getSlaXmlBlob());
                query.setParameter("status", actionBean.getStatus().toString());
                query.setParameter("id", actionBean.getId());
                break;

            case UPDATE_COORD_ACTION_STATUS_PENDING_TIME:
                query.setParameter("status", actionBean.getStatus().toString());
                query.setParameter("pending", actionBean.getPending());
                query.setParameter("lastModifiedTime", new Date());
                query.setParameter("id", actionBean.getId());
                break;

            case UPDATE_COORD_ACTION_FOR_INPUTCHECK:
                query.setParameter("status", actionBean.getStatus().toString());
                query.setParameter("lastModifiedTime", new Date());
                query.setParameter("actionXml", actionBean.getActionXmlBlob());
                query.setParameter("missingDependencies", actionBean.getMissingDependenciesBlob());
                query.setParameter("id", actionBean.getId());
                break;

            case UPDATE_COORD_ACTION_FOR_PUSH_INPUTCHECK:
                query.setParameter("status", actionBean.getStatus().toString());
                query.setParameter("lastModifiedTime", new Date());
                query.setParameter("actionXml", actionBean.getActionXmlBlob());
                query.setParameter("pushMissingDependencies", actionBean.getPushMissingDependenciesBlob());
                query.setParameter("id", actionBean.getId());
                break;

            case UPDATE_COORD_ACTION_DEPENDENCIES:
                query.setParameter("missingDependencies", actionBean.getMissingDependenciesBlob());
                query.setParameter("pushMissingDependencies", actionBean.getPushMissingDependenciesBlob());
                query.setParameter("id", actionBean.getId());
                break;

            case UPDATE_COORD_ACTION_FOR_START:
                query.setParameter("status", actionBean.getStatus().toString());
                query.setParameter("lastModifiedTime", new Date());
                query.setParameter("runConf", actionBean.getRunConfBlob());
                query.setParameter("externalId", actionBean.getExternalId());
                query.setParameter("pending", actionBean.getPending());
                query.setParameter("errorCode", actionBean.getErrorCode());
                query.setParameter("errorMessage", actionBean.getErrorMessage());
                query.setParameter("id", actionBean.getId());
                break;

            case UPDATE_COORD_ACTION_FOR_MODIFIED_DATE:
                query.setParameter("lastModifiedTime", actionBean.getLastModifiedTimestamp());
                query.setParameter("id", actionBean.getId());
                break;

            case UPDATE_COORD_ACTION_RERUN:
                query.setParameter("actionXml", actionBean.getActionXmlBlob());
                query.setParameter("status", actionBean.getStatusStr());
                query.setParameter("externalId", actionBean.getExternalId());
                query.setParameter("externalStatus", actionBean.getExternalStatus());
                query.setParameter("rerunTime", actionBean.getRerunTimestamp());
                query.setParameter("lastModifiedTime", actionBean.getLastModifiedTimestamp());
                query.setParameter("createdTime", actionBean.getCreatedTimestamp());
                query.setParameter("id", actionBean.getId());
                break;

            default:
                throw new JPAExecutorException(ErrorCode.E0603, "QueryExecutor cannot set parameters for "
                        + namedQuery.name());
        }
        return query;
    }

    @Override
    public Query getSelectQuery(CoordActionQuery namedQuery, EntityManager em, Object... parameters)
            throws JPAExecutorException {
        Query query = em.createNamedQuery(namedQuery.name());
        CoordActionQuery caQuery = (CoordActionQuery) namedQuery;
        switch (caQuery) {
            case GET_COORD_ACTION:
                query.setParameter("id", parameters[0]);
                break;
            default:
                throw new JPAExecutorException(ErrorCode.E0603, "QueryExecutor cannot set parameters for "
                        + caQuery.name());
        }
        return query;
    }

    @Override
    public int executeUpdate(CoordActionQuery namedQuery, CoordinatorActionBean jobBean) throws JPAExecutorException {
        EntityManager em = jpaService.getEntityManager();
        Query query = getUpdateQuery(namedQuery, jobBean, em);
        int ret = jpaService.executeUpdate(namedQuery.name(), query, em);
        return ret;
    }

    @Override
    public CoordinatorActionBean get(CoordActionQuery namedQuery, Object... parameters) throws JPAExecutorException {
        EntityManager em = jpaService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        CoordinatorActionBean bean = (CoordinatorActionBean) jpaService.executeGet(namedQuery.name(), query, em);
        if (bean == null) {
            throw new JPAExecutorException(ErrorCode.E0605, query.toString());
        }
        return bean;
    }

    @Override
    public List<CoordinatorActionBean> getList(CoordActionQuery namedQuery, Object... parameters)
            throws JPAExecutorException {
        EntityManager em = jpaService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        List<CoordinatorActionBean> beanList = (List<CoordinatorActionBean>) jpaService.executeGetList(
                namedQuery.name(), query, em);
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
