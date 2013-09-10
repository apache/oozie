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

import java.util.HashMap;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.NamedQuery;
import javax.persistence.Query;

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;

import com.google.common.annotations.VisibleForTesting;

/**
 * Query Executor that provides API to run query for Bundle Job
 */
public class BundleJobQueryExecutor extends QueryExecutor<BundleJobBean, BundleJobQueryExecutor.BundleJobQuery> {

    public enum BundleJobQuery {
        UPDATE_BUNDLE_JOB,
        UPDATE_BUNDLE_JOB_STATUS,
        UPDATE_BUNDLE_JOB_STATUS_PENDING,
        UPDATE_BUNDLE_JOB_STATUS_PENDING_MODTIME,
        UPDATE_BUNDLE_JOB_STATUS_PENDING_SUSP_MOD_TIME,
        UPDATE_BUNDLE_JOB_STATUS_PAUSE_ENDTIME,
        UPDATE_BUNDLE_JOB_PAUSE_KICKOFF,
        GET_BUNDLE_JOB
    };

    private static BundleJobQueryExecutor instance = new BundleJobQueryExecutor();
    private static JPAService jpaService;

    private BundleJobQueryExecutor() {
        Services services = Services.get();
        if (services != null) {
            jpaService = services.get(JPAService.class);
        }
    }

    public static QueryExecutor<BundleJobBean, BundleJobQueryExecutor.BundleJobQuery> getInstance() {
        if (instance == null) {
            // It will not be null in normal execution. Required for testcase as
            // they reinstantiate JPAService everytime
            instance = new BundleJobQueryExecutor();
        }
        return BundleJobQueryExecutor.instance;
    }

    @Override
    public Query getUpdateQuery(BundleJobQuery namedQuery, BundleJobBean bjBean, EntityManager em)
            throws JPAExecutorException {
        Query query = em.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case UPDATE_BUNDLE_JOB:
                query.setParameter("appName", bjBean.getAppName());
                query.setParameter("appPath", bjBean.getAppPath());
                query.setParameter("conf", bjBean.getConfBlob());
                query.setParameter("timeOut", bjBean.getTimeout());
                query.setParameter("createdTimestamp", bjBean.getCreatedTimestamp());
                query.setParameter("endTimestamp", bjBean.getEndTimestamp());
                query.setParameter("jobXml", bjBean.getJobXmlBlob());
                query.setParameter("lastModifiedTimestamp", bjBean.getLastModifiedTimestamp());
                query.setParameter("origJobXml", bjBean.getOrigJobXmlBlob());
                query.setParameter("startTimestamp", bjBean.getstartTimestamp());
                query.setParameter("status", bjBean.getStatus().toString());
                query.setParameter("timeUnit", bjBean.getTimeUnit());
                query.setParameter("pending", bjBean.isPending() ? 1 : 0);
                query.setParameter("id", bjBean.getId());
                break;
            case UPDATE_BUNDLE_JOB_STATUS:
                query.setParameter("status", bjBean.getStatus().toString());
                query.setParameter("lastModifiedTimestamp", bjBean.getLastModifiedTimestamp());
                query.setParameter("pending", bjBean.getPending());
                query.setParameter("id", bjBean.getId());
                break;
            case UPDATE_BUNDLE_JOB_STATUS_PENDING:
                query.setParameter("status", bjBean.getStatus().toString());
                query.setParameter("pending", bjBean.getPending());
                query.setParameter("id", bjBean.getId());
                break;
            case UPDATE_BUNDLE_JOB_STATUS_PENDING_SUSP_MOD_TIME:
                query.setParameter("status", bjBean.getStatus().toString());
                query.setParameter("lastModifiedTime", bjBean.getLastModifiedTimestamp());
                query.setParameter("pending", bjBean.getPending());
                query.setParameter("suspendedTime", bjBean.getSuspendedTimestamp());
                query.setParameter("id", bjBean.getId());
                break;
            case UPDATE_BUNDLE_JOB_STATUS_PENDING_MODTIME:
                query.setParameter("status", bjBean.getStatus().toString());
                query.setParameter("lastModifiedTime", bjBean.getLastModifiedTimestamp());
                query.setParameter("pending", bjBean.getPending());
                query.setParameter("id", bjBean.getId());
                break;
            case UPDATE_BUNDLE_JOB_STATUS_PAUSE_ENDTIME:
                query.setParameter("status", bjBean.getStatus().toString());
                query.setParameter("pauseTime", bjBean.getPauseTimestamp());
                query.setParameter("endTime", bjBean.getEndTimestamp());
                query.setParameter("id", bjBean.getId());
                break;
            case UPDATE_BUNDLE_JOB_PAUSE_KICKOFF:
                query.setParameter("pauseTime", bjBean.getPauseTimestamp());
                query.setParameter("kickoffTime", bjBean.getKickoffTimestamp());
                query.setParameter("id", bjBean.getId());
                break;
            default:
                throw new JPAExecutorException(ErrorCode.E0603, "QueryExecutor cannot set parameters for "
                        + namedQuery.name());
        }
        return query;
    }

    @Override
    public Query getSelectQuery(BundleJobQuery namedQuery, EntityManager em, Object... parameters)
            throws JPAExecutorException {
        Query query = em.createNamedQuery(namedQuery.name());
        BundleJobQuery bjQuery = (BundleJobQuery) namedQuery;
        switch (bjQuery) {
            case GET_BUNDLE_JOB:
                query.setParameter("id", parameters[0]);
                break;
            default:
                throw new JPAExecutorException(ErrorCode.E0603, "QueryExecutor cannot set parameters for "
                        + bjQuery.name());
        }
        return query;
    }

    @Override
    public int executeUpdate(BundleJobQuery namedQuery, BundleJobBean jobBean) throws JPAExecutorException {
        EntityManager em = jpaService.getEntityManager();
        Query query = getUpdateQuery(namedQuery, jobBean, em);
        int ret = jpaService.executeUpdate(namedQuery.name(), query, em);
        return ret;
    }

    @Override
    public BundleJobBean get(BundleJobQuery namedQuery, Object... parameters) throws JPAExecutorException {
        EntityManager em = jpaService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        BundleJobBean bean = (BundleJobBean) jpaService.executeGet(namedQuery.name(), query, em);
        if (bean == null) {
            throw new JPAExecutorException(ErrorCode.E0604, query.toString());
        }
        return bean;
    }

    @Override
    public List<BundleJobBean> getList(BundleJobQuery namedQuery, Object... parameters) throws JPAExecutorException {
        // TODO
        return null;
    }

    @VisibleForTesting
    public static void destroy() {
        if (instance != null) {
            jpaService = null;
            instance = null;
        }
    }
}
