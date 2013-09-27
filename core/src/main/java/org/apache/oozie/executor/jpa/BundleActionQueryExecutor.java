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
import java.util.Date;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.Query;
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor.WorkflowActionQuery;
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
        GET_BUNDLE_ACTION,
        GET_BUNDLE_ACTIONS_FOR_BUNDLE,
        GET_BUNDLE_ACTIONS_BY_LAST_MODIFIED_TIME,
        GET_BUNDLE_WAITING_ACTIONS_OLDER_THAN,
        GET_BUNDLE_ACTION_STATUS_PENDING_FOR_BUNDLE
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
                query.setParameter("lastModifiedTime", baBean.getLastModifiedTimestamp());
                query.setParameter("pending", baBean.getPending());
                query.setParameter("bundleActionId", baBean.getBundleActionId());
                break;
            case UPDATE_BUNDLE_ACTION_STATUS_PENDING_MODTIME:
                query.setParameter("status", baBean.getStatusStr());
                query.setParameter("lastModifiedTime", baBean.getLastModifiedTimestamp());
                query.setParameter("pending", baBean.getPending());
                query.setParameter("bundleActionId", baBean.getBundleActionId());
                break;
            case UPDATE_BUNDLE_ACTION_STATUS_PENDING_MODTIME_COORDID:
                query.setParameter("status", baBean.getStatusStr());
                query.setParameter("lastModifiedTime", baBean.getLastModifiedTimestamp());
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
            case GET_BUNDLE_ACTIONS_FOR_BUNDLE:
                query.setParameter("bundleId", parameters[0]);
                break;
            case GET_BUNDLE_ACTIONS_BY_LAST_MODIFIED_TIME:
                query.setParameter("lastModifiedTime", new Timestamp(((Date)parameters[0]).getTime()));
                break;
            case GET_BUNDLE_WAITING_ACTIONS_OLDER_THAN:
                Timestamp ts = new Timestamp(System.currentTimeMillis() - (Long)parameters[0] * 1000);
                query.setParameter("lastModifiedTime", ts);
                break;
            case GET_BUNDLE_ACTION_STATUS_PENDING_FOR_BUNDLE:
                query.setParameter("bundleId", parameters[0]);
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
        Object ret = jpaService.executeGet(namedQuery.name(), query, em);
        if (ret == null) {
            throw new JPAExecutorException(ErrorCode.E0604, query.toString());
        }
        BundleActionBean bean = constructBean(namedQuery, ret);
        return bean;
    }

    private BundleActionBean constructBean(BundleActionQuery namedQuery, Object ret) throws JPAExecutorException {
        BundleActionBean bean;
        Object[] arr;
        switch (namedQuery) {
            case GET_BUNDLE_ACTION:
            case GET_BUNDLE_ACTIONS_FOR_BUNDLE:
                bean = (BundleActionBean) ret;
                break;
            case GET_BUNDLE_ACTIONS_BY_LAST_MODIFIED_TIME:
                bean = new BundleActionBean();
                bean.setBundleId((String) ret);
                break;
            case GET_BUNDLE_WAITING_ACTIONS_OLDER_THAN:
                bean = new BundleActionBean();
                arr = (Object[]) ret;
                bean.setBundleActionId((String) arr[0]);
                bean.setBundleId((String) arr[1]);
                bean.setStatusStr((String) arr[2]);
                bean.setCoordId((String) arr[3]);
                bean.setCoordName((String) arr[4]);
                break;
            case GET_BUNDLE_ACTION_STATUS_PENDING_FOR_BUNDLE:
                bean = new BundleActionBean();
                arr = (Object[]) ret;
                bean.setCoordId((String) arr[0]);
                bean.setStatusStr((String) arr[1]);
                bean.setPending((Integer) arr[2]);
                break;
            default:
                throw new JPAExecutorException(ErrorCode.E0603, "QueryExecutor cannot construct action bean for "
                        + namedQuery.name());
        }
        return bean;
    }

    @Override
    public List<BundleActionBean> getList(BundleActionQuery namedQuery, Object... parameters)
            throws JPAExecutorException {
        EntityManager em = jpaService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        List<?> retList = (List<?>) jpaService.executeGetList(namedQuery.name(), query, em);
        List<BundleActionBean> beanList = new ArrayList<BundleActionBean>();
        if (retList != null) {
            for (Object ret : retList) {
                beanList.add(constructBean(namedQuery, ret));
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
