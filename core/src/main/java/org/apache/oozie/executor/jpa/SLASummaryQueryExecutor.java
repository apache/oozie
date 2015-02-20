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
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.SLASummaryBean;

/**
 * Query Executor for SLA Event
 *
 */
public class SLASummaryQueryExecutor extends QueryExecutor<SLASummaryBean, SLASummaryQueryExecutor.SLASummaryQuery> {

    public enum SLASummaryQuery {
        UPDATE_SLA_SUMMARY_FOR_STATUS_ACTUAL_TIMES,
        UPDATE_SLA_SUMMARY_FOR_ACTUAL_TIMES,
        UPDATE_SLA_SUMMARY_ALL,
        UPDATE_SLA_SUMMARY_EVENTPROCESSED,
        UPDATE_SLA_SUMMARY_FOR_EXPECTED_TIMES,
        UPDATE_SLA_SUMMARY_LAST_MODIFIED_TIME,
        GET_SLA_SUMMARY,
        GET_SLA_SUMMARY_EVENTPROCESSED,
        GET_SLA_SUMMARY_EVENTPROCESSED_LAST_MODIFIED
    };

    private static SLASummaryQueryExecutor instance = new SLASummaryQueryExecutor();

    private SLASummaryQueryExecutor() {
    }

    public static QueryExecutor<SLASummaryBean, SLASummaryQueryExecutor.SLASummaryQuery> getInstance() {
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
            case UPDATE_SLA_SUMMARY_FOR_ACTUAL_TIMES:
                query.setParameter("jobId", bean.getId());
                query.setParameter("eventProcessed", bean.getEventProcessed());
                query.setParameter("actualStartTS", bean.getActualStartTimestamp());
                query.setParameter("actualEndTS", bean.getActualEndTimestamp());
                query.setParameter("actualDuration", bean.getActualDuration());
                query.setParameter("lastModifiedTS", bean.getLastModifiedTimestamp());
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
            case UPDATE_SLA_SUMMARY_FOR_EXPECTED_TIMES:
                query.setParameter("nominalTime", bean.getNominalTimestamp());
                query.setParameter("expectedStartTime", bean.getExpectedStartTimestamp());
                query.setParameter("expectedEndTime", bean.getExpectedEndTimestamp());
                query.setParameter("expectedDuration", bean.getExpectedDuration());
                query.setParameter("lastModTime", bean.getLastModifiedTimestamp());
                query.setParameter("jobId", bean.getId());
                break;

            case UPDATE_SLA_SUMMARY_EVENTPROCESSED:
                query.setParameter("eventProcessed", bean.getEventProcessed());
                query.setParameter("jobId", bean.getId());
                break;
            case UPDATE_SLA_SUMMARY_LAST_MODIFIED_TIME:
                query.setParameter("lastModifiedTS", bean.getLastModifiedTime());
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
            case GET_SLA_SUMMARY_EVENTPROCESSED:
            case GET_SLA_SUMMARY_EVENTPROCESSED_LAST_MODIFIED:
                query.setParameter("id", parameters[0]);
                break;
        }
        return query;
    }

    @Override
    public int executeUpdate(SLASummaryQuery namedQuery, SLASummaryBean jobBean) throws JPAExecutorException {
        JPAService jpaService = Services.get().get(JPAService.class);
        EntityManager em = jpaService.getEntityManager();
        Query query = getUpdateQuery(namedQuery, jobBean, em);
        int ret = jpaService.executeUpdate(namedQuery.name(), query, em);
        return ret;
    }

    @Override
    public SLASummaryBean get(SLASummaryQuery namedQuery, Object... parameters) throws JPAExecutorException {
        JPAService jpaService = Services.get().get(JPAService.class);
        EntityManager em = jpaService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        Object ret = jpaService.executeGet(namedQuery.name(), query, em);
        if (ret == null && !namedQuery.equals(SLASummaryQuery.GET_SLA_SUMMARY)) {
            throw new JPAExecutorException(ErrorCode.E0604, query.toString());
        }
        SLASummaryBean bean = constructBean(namedQuery, ret, parameters);
        return bean;
    }

    @Override
    public List<SLASummaryBean> getList(SLASummaryQuery namedQuery, Object... parameters) throws JPAExecutorException {
        JPAService jpaService = Services.get().get(JPAService.class);
        EntityManager em = jpaService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        @SuppressWarnings("unchecked")
        List<SLASummaryBean> beanList = (List<SLASummaryBean>) jpaService.executeGetList(namedQuery.name(), query, em);
        return beanList;
    }

    @Override
    public Object getSingleValue(SLASummaryQuery namedQuery, Object... parameters) throws JPAExecutorException {
        JPAService jpaService = Services.get().get(JPAService.class);
        EntityManager em = jpaService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        Object ret = jpaService.executeGet(namedQuery.name(), query, em);
        if (ret == null) {
            throw new JPAExecutorException(ErrorCode.E0604, query.toString());
        }
        return ret;
    }

    private SLASummaryBean constructBean(SLASummaryQuery namedQuery, Object ret, Object... parameters)
            throws JPAExecutorException {
        SLASummaryBean bean;
        switch (namedQuery) {
            case GET_SLA_SUMMARY:
                bean = (SLASummaryBean) ret;
                break;
            case GET_SLA_SUMMARY_EVENTPROCESSED:
                bean = new SLASummaryBean();
                bean.setEventProcessed(((Byte)ret).intValue());
                break;
            case GET_SLA_SUMMARY_EVENTPROCESSED_LAST_MODIFIED:
                Object[] arr = (Object[]) ret;
                bean = new SLASummaryBean();
                bean.setEventProcessed((Byte)arr[0]);
                bean.setLastModifiedTime((Timestamp)arr[1]);

                break;

            default:
                throw new JPAExecutorException(ErrorCode.E0603, "QueryExecutor cannot construct job bean for "
                        + namedQuery.name());
        }
        return bean;
    }

}
