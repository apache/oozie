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

import org.apache.oozie.ErrorCode;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.SLARegistrationBean;

/**
 * Query Executor for SLA Event
 *
 */
public class SLARegistrationQueryExecutor extends QueryExecutor<SLARegistrationBean, SLARegistrationQueryExecutor.SLARegQuery> {

    public enum SLARegQuery {
        UPDATE_SLA_REG_ALL,
        UPDATE_SLA_CONFIG,
        UPDATE_SLA_EXPECTED_VALUE,
        GET_SLA_REG_ALL,
        GET_SLA_EXPECTED_VALUE_CONFIG,
        GET_SLA_REG_FOR_PARENT_ID,
        GET_SLA_REG_ON_RESTART,
        GET_SLA_CONFIGS
    };

    private static SLARegistrationQueryExecutor instance = new SLARegistrationQueryExecutor();

    private SLARegistrationQueryExecutor() {
    }

    public static QueryExecutor<SLARegistrationBean, SLARegistrationQueryExecutor.SLARegQuery> getInstance() {
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
            case UPDATE_SLA_EXPECTED_VALUE:
                query.setParameter("jobId", bean.getId());
                query.setParameter("expectedStartTime", bean.getExpectedStartTimestamp());
                query.setParameter("expectedEndTime", bean.getExpectedEndTimestamp());
                query.setParameter("expectedDuration", bean.getExpectedDuration());
                break;
            case UPDATE_SLA_CONFIG:
                query.setParameter("jobId", bean.getId());
                query.setParameter("slaConfig", bean.getSlaConfig());
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
        Query query = em.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case GET_SLA_REG_ALL:
            case GET_SLA_REG_ON_RESTART:
                query.setParameter("id", parameters[0]);
                break;
            case GET_SLA_CONFIGS:
                query.setParameter("ids", parameters[0]);
                break;
            case GET_SLA_EXPECTED_VALUE_CONFIG:
                query.setParameter("id", parameters[0]);
                break;
            case GET_SLA_REG_FOR_PARENT_ID:
                query.setParameter("parentId", parameters[0]);
                break;

            default:
                throw new JPAExecutorException(ErrorCode.E0603, "QueryExecutor cannot set parameters for "
                        + namedQuery.name());
        }
        return query;
    }

    @Override
    public int executeUpdate(SLARegQuery namedQuery, SLARegistrationBean jobBean) throws JPAExecutorException {
        JPAService jpaService = Services.get().get(JPAService.class);
        EntityManager em = jpaService.getEntityManager();
        Query query = getUpdateQuery(namedQuery, jobBean, em);
        int ret = jpaService.executeUpdate(namedQuery.name(), query, em);
        return ret;
    }

    @Override
    public SLARegistrationBean get(SLARegQuery namedQuery, Object... parameters) throws JPAExecutorException {
        JPAService jpaService = Services.get().get(JPAService.class);
        EntityManager em = jpaService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        Object ret = jpaService.executeGet(namedQuery.name(), query, em);
        if (ret == null && !namedQuery.equals(SLARegQuery.GET_SLA_REG_ALL)) {
            throw new JPAExecutorException(ErrorCode.E0604, query.toString());
        }
        SLARegistrationBean bean = constructBean(namedQuery, ret, parameters);
        return bean;
    }

    @Override
    public List<SLARegistrationBean> getList(SLARegQuery namedQuery, Object... parameters) throws JPAExecutorException {
        JPAService jpaService = Services.get().get(JPAService.class);
        EntityManager em = jpaService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        List<?> retList = (List<?>) jpaService.executeGetList(namedQuery.name(), query, em);
        List<SLARegistrationBean> beanList = new ArrayList<SLARegistrationBean>();
        if (retList != null) {
            for (Object ret : retList) {
                beanList.add(constructBean(namedQuery, ret));
            }
        }
        return beanList;
    }

    private SLARegistrationBean constructBean(SLARegQuery namedQuery, Object ret, Object... parameters)
            throws JPAExecutorException {
        SLARegistrationBean bean;
        Object[] arr;
        switch (namedQuery) {
            case GET_SLA_REG_ALL:
                bean = (SLARegistrationBean) ret;
                if(bean != null) {
                    bean.setSlaConfig(bean.getSlaConfig());
                }
                break;
            case GET_SLA_REG_ON_RESTART:
                bean = new SLARegistrationBean();
                arr = (Object[]) ret;
                bean.setNotificationMsg((String) arr[0]);
                bean.setUpstreamApps((String) arr[1]);
                bean.setSlaConfig((String) arr[2]);
                bean.setJobData((String) arr[3]);
                break;
            case GET_SLA_CONFIGS:
                bean = new SLARegistrationBean();
                arr = (Object[]) ret;
                bean.setId((String) arr[0]);
                bean.setSlaConfig((String) arr[1]);
                break;
            case GET_SLA_EXPECTED_VALUE_CONFIG:
                bean = new SLARegistrationBean();
                arr = (Object[]) ret;
                bean.setId((String) arr[0]);
                bean.setSlaConfig((String) arr[1]);
                bean.setExpectedStart((Timestamp)arr[2]);
                bean.setExpectedEnd((Timestamp)arr[3]);
                bean.setExpectedDuration((Long)arr[4]);
                bean.setNominalTime((Timestamp)arr[5]);
                break;
            case GET_SLA_REG_FOR_PARENT_ID:
                bean = new SLARegistrationBean();
                arr = (Object[]) ret;
                bean.setId((String) arr[0]);
                bean.setSlaConfig((String) arr[1]);
                break;
            default:
                throw new JPAExecutorException(ErrorCode.E0603, "QueryExecutor cannot construct job bean for "
                        + namedQuery.name());
        }
        return bean;
    }

    @Override
    public Object getSingleValue(SLARegQuery namedQuery, Object... parameters) throws JPAExecutorException {
        throw new UnsupportedOperationException();
    }
}
