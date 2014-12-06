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
import org.apache.oozie.sla.SLARegistrationBean;

import com.google.common.annotations.VisibleForTesting;

/**
 * Query Executor for SLA Event
 *
 */
public class SLARegistrationQueryExecutor extends QueryExecutor<SLARegistrationBean, SLARegistrationQueryExecutor.SLARegQuery> {

    public enum SLARegQuery {
        UPDATE_SLA_REG_ALL
    };

    private static SLARegistrationQueryExecutor instance = new SLARegistrationQueryExecutor();
    private static JPAService jpaService;

    private SLARegistrationQueryExecutor() {
        Services services = Services.get();
        if (services != null) {
            jpaService = Services.get().get(JPAService.class);
        }
    }

    public static QueryExecutor<SLARegistrationBean, SLARegistrationQueryExecutor.SLARegQuery> getInstance() {
        if (instance == null) {
            instance = new SLARegistrationQueryExecutor();
        }
        return SLARegistrationQueryExecutor.instance;
    }

    @Override
    public Query getUpdateQuery(SLARegQuery namedQuery, SLARegistrationBean bean, EntityManager em)
            throws JPAExecutorException {

        Query query = em.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case UPDATE_SLA_REG_ALL:
                query.setParameter("jobId", bean.getId());
                query.setParameter("nominalTime", bean.getNominalTimestamp());
                query.setParameter("expectedStartTime", bean.getExpectedStartTimestamp());
                query.setParameter("expectedEndTime", bean.getExpectedEndTimestamp());
                query.setParameter("expectedDuration", bean.getExpectedDuration());
                query.setParameter("slaConfig", bean.getSlaConfig());
                query.setParameter("notificationMsg", bean.getNotificationMsg());
                query.setParameter("upstreamApps", bean.getUpstreamApps());
                query.setParameter("appType", bean.getAppType().toString());
                query.setParameter("appName", bean.getAppName());
                query.setParameter("user", bean.getUser());
                query.setParameter("parentId", bean.getParentId());
                query.setParameter("jobData", bean.getJobData());
                break;
            default:
                throw new JPAExecutorException(ErrorCode.E0603, "QueryExecutor cannot set parameters for "
                        + namedQuery.name());
        }
        return query;
    }

    @Override
    public Query getSelectQuery(SLARegQuery namedQuery, EntityManager em, Object... parameters)
            throws JPAExecutorException {
        // TODO
        return null;
    }

    @Override
    public int executeUpdate(SLARegQuery namedQuery, SLARegistrationBean jobBean) throws JPAExecutorException {
        EntityManager em = jpaService.getEntityManager();
        Query query = getUpdateQuery(namedQuery, jobBean, em);
        int ret = jpaService.executeUpdate(namedQuery.name(), query, em);
        return ret;
    }

    @Override
    public SLARegistrationBean get(SLARegQuery namedQuery, Object... parameters) throws JPAExecutorException {
        EntityManager em = jpaService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        @SuppressWarnings("unchecked")
        SLARegistrationBean bean = (SLARegistrationBean) jpaService.executeGet(namedQuery.name(), query, em);
        return bean;
    }

    @Override
    public List<SLARegistrationBean> getList(SLARegQuery namedQuery, Object... parameters) throws JPAExecutorException {
        EntityManager em = jpaService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        @SuppressWarnings("unchecked")
        List<SLARegistrationBean> beanList = (List<SLARegistrationBean>) jpaService.executeGetList(namedQuery.name(),
                query, em);
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
