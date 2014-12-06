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
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.SLASummaryBean;

import com.google.common.annotations.VisibleForTesting;

/**
 * Query Executor for SLA Event
 *
 */
public class SLASummaryQueryExecutor extends QueryExecutor<SLASummaryBean, SLASummaryQueryExecutor.SLASummaryQuery> {

    public enum SLASummaryQuery {
        UPDATE_SLA_SUMMARY_FOR_STATUS_ACTUAL_TIMES,
        UPDATE_SLA_SUMMARY_ALL,
        GET_SLA_SUMMARY
    };

    private static SLASummaryQueryExecutor instance = new SLASummaryQueryExecutor();
    private static JPAService jpaService;

    private SLASummaryQueryExecutor() {
        Services services = Services.get();
        if (services != null) {
            jpaService = Services.get().get(JPAService.class);
        }
    }

    public static QueryExecutor<SLASummaryBean, SLASummaryQueryExecutor.SLASummaryQuery> getInstance() {
        if (instance == null) {
            instance = new SLASummaryQueryExecutor();
        }
        return SLASummaryQueryExecutor.instance;
    }

    @Override
    public Query getUpdateQuery(SLASummaryQuery namedQuery, SLASummaryBean bean, EntityManager em)
            throws JPAExecutorException {
        Query query = em.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case UPDATE_SLA_SUMMARY_FOR_STATUS_ACTUAL_TIMES:
                query.setParameter("jobId", bean.getId());
                query.setParameter("slaStatus", bean.getSLAStatus().toString());
                query.setParameter("lastModifiedTS", bean.getLastModifiedTimestamp());
                query.setParameter("eventStatus", bean.getEventStatusString());
                query.setParameter("jobStatus", bean.getJobStatus());
                query.setParameter("eventProcessed", bean.getEventProcessed());
                query.setParameter("actualStartTS", bean.getActualStartTimestamp());
                query.setParameter("actualEndTS", bean.getActualEndTimestamp());
                query.setParameter("actualDuration", bean.getActualDuration());
                break;
            case UPDATE_SLA_SUMMARY_ALL:
                query.setParameter("appName", bean.getAppName());
                query.setParameter("appType", bean.getAppType().toString());
                query.setParameter("nominalTime", bean.getNominalTimestamp());
                query.setParameter("expectedStartTime", bean.getExpectedStartTimestamp());
                query.setParameter("expectedEndTime", bean.getExpectedEndTimestamp());
                query.setParameter("expectedDuration", bean.getExpectedDuration());
                query.setParameter("jobStatus", bean.getJobStatus());
                query.setParameter("slaStatus", bean.getSLAStatusString());
                query.setParameter("eventStatus", bean.getEventStatusString());
                query.setParameter("lastModTime", bean.getLastModifiedTimestamp());
                query.setParameter("user", bean.getUser());
                query.setParameter("parentId", bean.getParentId());
                query.setParameter("eventProcessed", bean.getEventProcessed());
                query.setParameter("actualDuration", bean.getActualDuration());
                query.setParameter("actualEndTS", bean.getActualEndTimestamp());
                query.setParameter("actualStartTS", bean.getActualStartTimestamp());
                query.setParameter("jobId", bean.getId());
                break;
            default:
                throw new JPAExecutorException(ErrorCode.E0603, "QueryExecutor cannot set parameters for "
                        + namedQuery.name());
        }
        return query;
    }

    @Override
    public Query getSelectQuery(SLASummaryQuery namedQuery, EntityManager em, Object... parameters)
            throws JPAExecutorException {
        Query query = em.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case GET_SLA_SUMMARY:
                query.setParameter("id", parameters[0]);
                break;
        }
        return query;
    }

    @Override
    public int executeUpdate(SLASummaryQuery namedQuery, SLASummaryBean jobBean) throws JPAExecutorException {
        EntityManager em = jpaService.getEntityManager();
        Query query = getUpdateQuery(namedQuery, jobBean, em);
        int ret = jpaService.executeUpdate(namedQuery.name(), query, em);
        return ret;
    }

    @Override
    public SLASummaryBean get(SLASummaryQuery namedQuery, Object... parameters) throws JPAExecutorException {
        EntityManager em = jpaService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        @SuppressWarnings("unchecked")
        SLASummaryBean bean = (SLASummaryBean) jpaService.executeGet(namedQuery.name(), query, em);
        return bean;
    }

    @Override
    public List<SLASummaryBean> getList(SLASummaryQuery namedQuery, Object... parameters) throws JPAExecutorException {
        EntityManager em = jpaService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        @SuppressWarnings("unchecked")
        List<SLASummaryBean> beanList = (List<SLASummaryBean>) jpaService.executeGetList(namedQuery.name(), query, em);
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
