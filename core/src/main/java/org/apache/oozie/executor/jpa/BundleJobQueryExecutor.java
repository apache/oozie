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

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.StringBlob;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.DateUtils;

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
        GET_BUNDLE_JOB,
        GET_BUNDLE_JOB_STATUS,
        GET_BUNDLE_JOB_ID_STATUS_PENDING_MODTIME,
        GET_BUNDLE_JOB_ID_JOBXML_CONF,
        GET_BUNDLE_IDS_FOR_STATUS_TRANSIT
    };

    private static BundleJobQueryExecutor instance = new BundleJobQueryExecutor();

    private BundleJobQueryExecutor() {
    }

    public static QueryExecutor<BundleJobBean, BundleJobQueryExecutor.BundleJobQuery> getInstance() {
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
                query.setParameter("createdTime", bjBean.getCreatedTimestamp());
                query.setParameter("endTime", bjBean.getEndTimestamp());
                query.setParameter("jobXml", bjBean.getJobXmlBlob());
                query.setParameter("lastModifiedTime", bjBean.getLastModifiedTimestamp());
                query.setParameter("origJobXml", bjBean.getOrigJobXmlBlob());
                query.setParameter("startTime", bjBean.getstartTimestamp());
                query.setParameter("status", bjBean.getStatus().toString());
                query.setParameter("timeUnit", bjBean.getTimeUnit());
                query.setParameter("pending", bjBean.isPending() ? 1 : 0);
                query.setParameter("id", bjBean.getId());
                break;
            case UPDATE_BUNDLE_JOB_STATUS:
                query.setParameter("status", bjBean.getStatus().toString());
                query.setParameter("lastModifiedTime", bjBean.getLastModifiedTimestamp());
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
        switch (namedQuery) {
            case GET_BUNDLE_JOB:
            case GET_BUNDLE_JOB_ID_STATUS_PENDING_MODTIME:
            case GET_BUNDLE_JOB_ID_JOBXML_CONF:
            case GET_BUNDLE_JOB_STATUS:
                query.setParameter("id", parameters[0]);
                break;
            case GET_BUNDLE_IDS_FOR_STATUS_TRANSIT:
                query.setParameter("lastModifiedTime", DateUtils.convertDateToTimestamp((Date)parameters[0]));
                break;
            default:
                throw new JPAExecutorException(ErrorCode.E0603, "QueryExecutor cannot set parameters for "
                        + namedQuery.name());
        }
        return query;
    }

    @Override
    public int executeUpdate(BundleJobQuery namedQuery, BundleJobBean jobBean) throws JPAExecutorException {
        JPAService jpaService = Services.get().get(JPAService.class);
        EntityManager em = jpaService.getEntityManager();
        Query query = getUpdateQuery(namedQuery, jobBean, em);
        int ret = jpaService.executeUpdate(namedQuery.name(), query, em);
        return ret;
    }

    @Override
    public BundleJobBean get(BundleJobQuery namedQuery, Object... parameters) throws JPAExecutorException {
        JPAService jpaService = Services.get().get(JPAService.class);
        EntityManager em = jpaService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        Object ret = jpaService.executeGet(namedQuery.name(), query, em);
        if (ret == null) {
            throw new JPAExecutorException(ErrorCode.E0604, query.toString());
        }
        BundleJobBean bean = constructBean(namedQuery, ret, parameters);
        return bean;
    }

    @Override
    public List<BundleJobBean> getList(BundleJobQuery namedQuery, Object... parameters) throws JPAExecutorException {
        JPAService jpaService = Services.get().get(JPAService.class);
        EntityManager em = jpaService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        List<?> retList = (List<?>) jpaService.executeGetList(namedQuery.name(), query, em);
        List<BundleJobBean> beanList = new ArrayList<BundleJobBean>();
        if (retList != null) {
            for (Object ret : retList) {
                beanList.add(constructBean(namedQuery, ret));
            }
        }
        return beanList;
    }

    private BundleJobBean constructBean(BundleJobQuery namedQuery, Object ret, Object... parameters)
            throws JPAExecutorException {
        BundleJobBean bean;
        Object[] arr;
        switch (namedQuery) {
            case GET_BUNDLE_JOB:
                bean = (BundleJobBean) ret;
                break;
            case GET_BUNDLE_JOB_STATUS:
                bean = new BundleJobBean();
                bean.setId((String) parameters[0]);
                bean.setStatus((String) ret);
                break;
            case GET_BUNDLE_JOB_ID_STATUS_PENDING_MODTIME:
                bean = new BundleJobBean();
                arr = (Object[]) ret;
                bean.setId((String) arr[0]);
                bean.setStatus((String) arr[1]);
                bean.setPending((Integer) arr[2]);
                bean.setLastModifiedTime(DateUtils.toDate((Timestamp) arr[3]));
                break;
            case GET_BUNDLE_JOB_ID_JOBXML_CONF:
                bean = new BundleJobBean();
                arr = (Object[]) ret;
                bean.setId((String) arr[0]);
                bean.setJobXmlBlob((StringBlob) arr[1]);
                bean.setConfBlob((StringBlob) arr[2]);
                break;

            case GET_BUNDLE_IDS_FOR_STATUS_TRANSIT:
                bean = new BundleJobBean();
                bean.setId((String) ret);
                break;

            default:
                throw new JPAExecutorException(ErrorCode.E0603, "QueryExecutor cannot construct job bean for "
                        + namedQuery.name());
        }
        return bean;
    }

    @Override
    public Object getSingleValue(BundleJobQuery namedQuery, Object... parameters) throws JPAExecutorException {
        throw new UnsupportedOperationException();
    }
}
