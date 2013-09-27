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

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.StringBlob;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.DateUtils;

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
        GET_COORD_JOB,
        GET_COORD_JOB_USER_APPNAME,
        GET_COORD_JOB_INPUT_CHECK,
        GET_COORD_JOB_ACTION_READY,
        GET_COORD_JOB_ACTION_KILL,
        GET_COORD_JOB_MATERIALIZE,
        GET_COORD_JOB_SUSPEND_KILL,
        GET_COORD_JOB_STATUS_PARENTID
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
                query.setParameter("appNamespace", cjBean.getAppNamespace());
                query.setParameter("bundleId", cjBean.getBundleId());
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
            case GET_COORD_JOB_USER_APPNAME:
            case GET_COORD_JOB_INPUT_CHECK:
            case GET_COORD_JOB_ACTION_READY:
            case GET_COORD_JOB_ACTION_KILL:
            case GET_COORD_JOB_MATERIALIZE:
            case GET_COORD_JOB_SUSPEND_KILL:
            case GET_COORD_JOB_STATUS_PARENTID:
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

    private CoordinatorJobBean constructBean(CoordJobQuery namedQuery, Object ret, Object... parameters)
            throws JPAExecutorException {
        CoordinatorJobBean bean;
        Object[] arr;
        switch (namedQuery) {
            case GET_COORD_JOB:
                bean = (CoordinatorJobBean) ret;
                break;
            case GET_COORD_JOB_USER_APPNAME:
                bean = new CoordinatorJobBean();
                arr = (Object[]) ret;
                bean.setUser((String) arr[0]);
                bean.setAppName((String) arr[1]);
                break;
            case GET_COORD_JOB_INPUT_CHECK:
                bean = new CoordinatorJobBean();
                arr = (Object[]) ret;
                bean.setUser((String) arr[0]);
                bean.setAppName((String) arr[1]);
                bean.setStatusStr((String) arr[2]);
                bean.setAppNamespace((String) arr[3]);
                break;
            case GET_COORD_JOB_ACTION_READY:
                bean = new CoordinatorJobBean();
                arr = (Object[]) ret;
                bean.setId((String) arr[0]);
                bean.setUser((String) arr[1]);
                bean.setGroup((String) arr[2]);
                bean.setAppName((String) arr[3]);
                bean.setStatusStr((String) arr[4]);
                bean.setExecution((String) arr[5]);
                bean.setConcurrency((Integer) arr[6]);
                break;
            case GET_COORD_JOB_ACTION_KILL:
                bean = new CoordinatorJobBean();
                arr = (Object[]) ret;
                bean.setId((String) arr[0]);
                bean.setUser((String) arr[1]);
                bean.setGroup((String) arr[2]);
                bean.setAppName((String) arr[3]);
                bean.setStatusStr((String) arr[4]);
                break;
            case GET_COORD_JOB_MATERIALIZE:
                bean = new CoordinatorJobBean();
                arr = (Object[]) ret;
                bean.setId((String) arr[0]);
                bean.setUser((String) arr[1]);
                bean.setGroup((String) arr[2]);
                bean.setAppName((String) arr[3]);
                bean.setStatusStr((String) arr[4]);
                bean.setFrequency((String) arr[5]);
                bean.setMatThrottling((Integer) arr[6]);
                bean.setTimeout((Integer) arr[7]);
                bean.setTimeZone((String) arr[8]);
                bean.setStartTime(DateUtils.toDate((Timestamp) arr[9]));
                bean.setEndTime(DateUtils.toDate((Timestamp) arr[10]));
                bean.setPauseTime(DateUtils.toDate((Timestamp) arr[11]));
                bean.setNextMaterializedTime(DateUtils.toDate((Timestamp) arr[12]));
                bean.setLastActionTime(DateUtils.toDate((Timestamp) arr[13]));
                bean.setLastActionNumber((Integer) arr[14]);
                bean.setDoneMaterialization((Integer) arr[15]);
                bean.setBundleId((String) arr[16]);
                bean.setConfBlob((StringBlob) arr[17]);
                bean.setJobXmlBlob((StringBlob) arr[18]);
                break;
            case GET_COORD_JOB_SUSPEND_KILL:
                bean = new CoordinatorJobBean();
                arr = (Object[]) ret;
                bean.setId((String) arr[0]);
                bean.setUser((String) arr[1]);
                bean.setGroup((String) arr[2]);
                bean.setAppName((String) arr[3]);
                bean.setStatusStr((String) arr[4]);
                bean.setBundleId((String) arr[5]);
                bean.setAppNamespace((String) arr[6]);
                bean.setDoneMaterialization((Integer) arr[7]);
                break;
            case GET_COORD_JOB_STATUS_PARENTID:
                bean = new CoordinatorJobBean();
                arr = (Object[]) ret;
                bean.setId((String) parameters[0]);
                bean.setStatusStr((String) arr[0]);
                bean.setBundleId((String) arr[1]);
                break;
            default:
                throw new JPAExecutorException(ErrorCode.E0603, "QueryExecutor cannot construct job bean for "
                        + namedQuery.name());
        }
        return bean;
    }

    @Override
    public CoordinatorJobBean get(CoordJobQuery namedQuery, Object... parameters) throws JPAExecutorException {
        EntityManager em = jpaService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        Object ret = jpaService.executeGet(namedQuery.name(), query, em);
        if (ret == null) {
            throw new JPAExecutorException(ErrorCode.E0604, query.toString());
        }
        CoordinatorJobBean bean = constructBean(namedQuery, ret, parameters);
        return bean;
    }

    @Override
    public List<CoordinatorJobBean> getList(CoordJobQuery namedQuery, Object... parameters) throws JPAExecutorException {
        EntityManager em = jpaService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        List<?> retList = (List<?>) jpaService.executeGetList(namedQuery.name(), query, em);
        List<CoordinatorJobBean> beanList = new ArrayList<CoordinatorJobBean>();
        if (retList != null) {
            for (Object ret : retList) {
                beanList.add(constructBean(namedQuery, ret, parameters));
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
