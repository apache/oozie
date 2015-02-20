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

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.StringBlob;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.DateUtils;

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
        UPDATE_COORD_JOB_CONF,
        UPDATE_COORD_JOB_XML,
        GET_COORD_JOB,
        GET_COORD_JOB_USER_APPNAME,
        GET_COORD_JOB_INPUT_CHECK,
        GET_COORD_JOB_ACTION_READY,
        GET_COORD_JOB_ACTION_KILL,
        GET_COORD_JOB_MATERIALIZE,
        GET_COORD_JOB_SUSPEND_KILL,
        GET_COORD_JOB_STATUS,
        GET_COORD_JOB_STATUS_PARENTID,
        GET_COORD_JOBS_CHANGED,
        GET_COORD_JOBS_OLDER_FOR_MATERIALIZATION,
        GET_COORD_FOR_ABANDONEDCHECK,
        GET_COORD_IDS_FOR_STATUS_TRANSIT,
        GET_COORD_JOBS_FOR_BUNDLE_BY_APPNAME_ID,
        GET_COORD_JOBS_WITH_PARENT_ID,
        GET_COORD_JOB_CONF,
        GET_COORD_JOB_XML
    };

    private static CoordJobQueryExecutor instance = new CoordJobQueryExecutor();

    private CoordJobQueryExecutor() {
    }

    public static CoordJobQueryExecutor getInstance() {
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
                query.setParameter("matThrottling", cjBean.getMatThrottling());
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
                query.setParameter("lastModifiedTime", cjBean.getLastModifiedTimestamp());
                query.setParameter("id", cjBean.getId());
                break;
            case UPDATE_COORD_JOB_CONF:
                query.setParameter("conf", cjBean.getConfBlob());
                query.setParameter("id", cjBean.getId());
                break;
            case UPDATE_COORD_JOB_XML:
                query.setParameter("jobXml", cjBean.getJobXmlBlob());
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
            case GET_COORD_JOB_STATUS:
            case GET_COORD_JOB_STATUS_PARENTID:
            case GET_COORD_JOB_CONF:
            case GET_COORD_JOB_XML:
                query.setParameter("id", parameters[0]);
                break;
            case GET_COORD_JOBS_CHANGED:
                query.setParameter("lastModifiedTime", new Timestamp(((Date)parameters[0]).getTime()));
                break;
            case GET_COORD_JOBS_OLDER_FOR_MATERIALIZATION:
                query.setParameter("matTime", new Timestamp(((Date)parameters[0]).getTime()));
                int limit = (Integer) parameters[1];
                if (limit > 0) {
                    query.setMaxResults(limit);
                }
                break;
            case GET_COORD_FOR_ABANDONEDCHECK:
                query.setParameter(1, (Integer) parameters[0]);
                query.setParameter(2, (Timestamp) parameters[1]);
                break;

            case GET_COORD_IDS_FOR_STATUS_TRANSIT:
                query.setParameter("lastModifiedTime", new Timestamp(((Date) parameters[0]).getTime()));
                break;
            case GET_COORD_JOBS_FOR_BUNDLE_BY_APPNAME_ID:
                query.setParameter("appName", parameters[0]);
                query.setParameter("bundleId", parameters[1]);
                break;
            case GET_COORD_JOBS_WITH_PARENT_ID:
                query.setParameter("parentId", parameters[0]);
                break;
            default:
                throw new JPAExecutorException(ErrorCode.E0603, "QueryExecutor cannot set parameters for "
                        + namedQuery.name());
        }
        return query;
    }

    @Override
    public int executeUpdate(CoordJobQuery namedQuery, CoordinatorJobBean jobBean) throws JPAExecutorException {
        JPAService jpaService = Services.get().get(JPAService.class);
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
                bean.setExecution((String) arr[4]);
                bean.setFrequency((String) arr[5]);
                bean.setTimeUnitStr((String) arr[6]);
                bean.setTimeZone((String) arr[7]);
                bean.setEndTime(DateUtils.toDate((Timestamp) arr[8]));
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
                bean.setAppNamespace((String) arr[19]);
                bean.setTimeUnitStr((String) arr[20]);
                bean.setExecution((String) arr[21]);
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
            case GET_COORD_JOB_STATUS:
                bean = new CoordinatorJobBean();
                bean.setId((String) parameters[0]);
                bean.setStatusStr((String) ret);
                break;
            case GET_COORD_JOB_STATUS_PARENTID:
                bean = new CoordinatorJobBean();
                arr = (Object[]) ret;
                bean.setId((String) parameters[0]);
                bean.setStatusStr((String) arr[0]);
                bean.setBundleId((String) arr[1]);
                break;
            case GET_COORD_JOBS_CHANGED:
                bean = (CoordinatorJobBean) ret;
                break;
            case GET_COORD_JOBS_OLDER_FOR_MATERIALIZATION:
                bean = new CoordinatorJobBean();
                bean.setId((String) ret);
                break;
            case GET_COORD_JOBS_FOR_BUNDLE_BY_APPNAME_ID:
                bean = new CoordinatorJobBean();
                bean.setId((String) ret);
                break;
            case GET_COORD_JOBS_WITH_PARENT_ID:
                bean = new CoordinatorJobBean();
                bean.setId((String) ret);
                break;
            case GET_COORD_FOR_ABANDONEDCHECK:
                bean = new CoordinatorJobBean();
                arr = (Object[]) ret;
                bean.setId((String) arr[0]);
                bean.setUser((String) arr[1]);
                bean.setGroup((String) arr[2]);
                bean.setAppName((String) arr[3]);
                break;
            case GET_COORD_IDS_FOR_STATUS_TRANSIT:
                bean = new CoordinatorJobBean();
                bean.setId((String) ret);
                break;
            case GET_COORD_JOB_CONF:
                bean = new CoordinatorJobBean();
                bean.setConfBlob((StringBlob) ret);
                break;
            case GET_COORD_JOB_XML:
                bean = new CoordinatorJobBean();
                bean.setJobXmlBlob((StringBlob) ret);
                break;

            default:
                throw new JPAExecutorException(ErrorCode.E0603, "QueryExecutor cannot construct job bean for "
                        + namedQuery.name());
        }
        return bean;
    }

    @Override
    public CoordinatorJobBean get(CoordJobQuery namedQuery, Object... parameters) throws JPAExecutorException {
        JPAService jpaService = Services.get().get(JPAService.class);
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
        JPAService jpaService = Services.get().get(JPAService.class);
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

    @Override
    public Object getSingleValue(CoordJobQuery namedQuery, Object... parameters) throws JPAExecutorException {
        throw new UnsupportedOperationException();
    }
}
