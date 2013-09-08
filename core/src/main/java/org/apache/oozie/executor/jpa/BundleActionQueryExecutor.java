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
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;

import com.google.common.annotations.VisibleForTesting;

/**
 * Query Executor that provides API to run query for Bundle Action
 */
public class BundleActionQueryExecutor extends
        QueryExecutor<BundleActionBean, BundleActionQueryExecutor.BundleActionQuery> {

    public enum BundleActionQuery {
        UPDATE_BUNDLE_ACTION_PENDING_MODTIME,
        UPDATE_BUNDLE_ACTION_STATUS_PENDING_MODTIME,
        UPDATE_BUNDLE_ACTION_STATUS_PENDING_MODTIME_COORDID,
        GET_BUNDLE_ACTION
    };

    private static BundleActionQueryExecutor instance = new BundleActionQueryExecutor();
    private static JPAService jpaService;

    private BundleActionQueryExecutor() {
        Services services = Services.get();
        if (services != null) {
            jpaService = services.get(JPAService.class);
        }
    }

    public static QueryExecutor<BundleActionBean, BundleActionQueryExecutor.BundleActionQuery> getInstance() {
        if (instance == null) {
            // It will not be null in normal execution. Required for testcase as
            // they reinstantiate JPAService everytime
            instance = new BundleActionQueryExecutor();
        }
        return BundleActionQueryExecutor.instance;
    }

    @Override
    public Query getUpdateQuery(BundleActionQuery namedQuery, BundleActionBean baBean, EntityManager em)
            throws JPAExecutorException {

        Query query = em.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case UPDATE_BUNDLE_ACTION_PENDING_MODTIME:
                query.setParameter("lastModifiedTimestamp", baBean.getLastModifiedTimestamp());
                query.setParameter("pending", baBean.getPending());
                query.setParameter("bundleActionId", baBean.getBundleActionId());
                break;
            case UPDATE_BUNDLE_ACTION_STATUS_PENDING_MODTIME:
                query.setParameter("status", baBean.getStatusStr());
                query.setParameter("lastModifiedTimestamp", baBean.getLastModifiedTimestamp());
                query.setParameter("pending", baBean.getPending());
                query.setParameter("bundleActionId", baBean.getBundleActionId());
                break;
            case UPDATE_BUNDLE_ACTION_STATUS_PENDING_MODTIME_COORDID:
                query.setParameter("status", baBean.getStatusStr());
                query.setParameter("lastModifiedTimestamp", baBean.getLastModifiedTimestamp());
                query.setParameter("pending", baBean.getPending());
                query.setParameter("coordId", baBean.getCoordId());
                query.setParameter("bundleActionId", baBean.getBundleActionId());
                break;
            default:
                throw new JPAExecutorException(ErrorCode.E0603, "QueryExecutor cannot set parameters for "
                        + namedQuery.name());
        }
        return query;
    }

    @Override
    public Query getSelectQuery(BundleActionQuery namedQuery, EntityManager em, Object... parameters)
            throws JPAExecutorException {
        Query query = em.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case GET_BUNDLE_ACTION:
                query.setParameter("bundleActionId", parameters[0]);
                break;
            default:
                throw new JPAExecutorException(ErrorCode.E0603, "QueryExecutor cannot set parameters for "
                        + namedQuery.name());
        }
        return query;
    }

    @Override
    public int executeUpdate(BundleActionQuery namedQuery, BundleActionBean jobBean) throws JPAExecutorException {
        EntityManager em = jpaService.getEntityManager();
        Query query = getUpdateQuery(namedQuery, jobBean, em);
        int ret = jpaService.executeUpdate(namedQuery.name(), query, em);
        return ret;
    }

    @Override
    public BundleActionBean get(BundleActionQuery namedQuery, Object... parameters) throws JPAExecutorException {
        EntityManager em = jpaService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        BundleActionBean bean = (BundleActionBean) jpaService.executeGet(namedQuery.name(), query, em);
        if (bean == null) {
            throw new JPAExecutorException(ErrorCode.E0604, query.toString());
        }
        return bean;
    }

    @Override
    public List<BundleActionBean> getList(BundleActionQuery namedQuery, Object... parameters)
            throws JPAExecutorException {
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
