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
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.rest.BulkResponseImpl;
import org.apache.oozie.BulkResponseInfo;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.StringBlob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ParamChecker;

/**
 * The query executor class for bulk monitoring queries i.e. debugging bundle ->
 * coord actions directly
 */
public class BulkJPAExecutor implements JPAExecutor<BulkResponseInfo> {
    private Map<String, List<String>> bulkFilter;
    // defaults
    private int start = 1;
    private int len = 50;
    private enum PARAM_TYPE {
        ID, NAME
    }

    public BulkJPAExecutor(Map<String, List<String>> bulkFilter, int start, int len) {
        ParamChecker.notNull(bulkFilter, "bulkFilter");
        this.bulkFilter = bulkFilter;
        this.start = start;
        this.len = len;
    }

    @Override
    public String getName() {
        return "BulkJPAExecutor";
    }

    @Override
    public BulkResponseInfo execute(EntityManager em) throws JPAExecutorException {
        List<BulkResponseImpl> responseList = new ArrayList<BulkResponseImpl>();
        Map<String, Timestamp> actionTimes = new HashMap<String, Timestamp>();

        try {
            // Lightweight Query 1 on Bundle level to fetch the bundle job(s)
            // corresponding to names or ids
            List<BundleJobBean> bundleBeans = bundleQuery(em);

            // Join query between coordinator job and coordinator action tables
            // to get entries for specific bundleId only
            String conditions = actionQuery(em, bundleBeans, actionTimes, responseList);

            // Query to get the count of records
            long total = countQuery(conditions, em, bundleBeans, actionTimes);

            BulkResponseInfo bulk = new BulkResponseInfo(responseList, start, len, total);
            return bulk;
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
    }

    /**
     * build the bundle level query to get bundle beans for the specified ids or appnames
     * @param em
     * @return List BundleJobBeans
     * @throws JPAExecutorException
     */
    @SuppressWarnings("unchecked")
    private List<BundleJobBean> bundleQuery(EntityManager em) throws JPAExecutorException {
        Query q = em.createNamedQuery("BULK_MONITOR_BUNDLE_QUERY");
        StringBuilder bundleQuery = new StringBuilder(q.toString());

        StringBuilder whereClause = null;
        List<String> bundles = bulkFilter.get(BulkResponseImpl.BULK_FILTER_BUNDLE);
        if (bundles != null) {
            PARAM_TYPE type = getParamType(bundles.get(0), 'B');
            if (type == PARAM_TYPE.NAME) {
                whereClause = inClause(bundles, "appName", 'b');
            }
            else if (type == PARAM_TYPE.ID) {
                whereClause = inClause(bundles, "id", 'b');
            }

            // Query: select <columns> from BundleJobBean b where b.id IN (...) _or_ b.appName IN (...)
            bundleQuery.append(whereClause.replace(whereClause.indexOf("AND"), whereClause.indexOf("AND") + 3, "WHERE"));
            List<Object[]> bundleObjs = (List<Object[]>) em.createQuery(bundleQuery.toString()).getResultList();
            if (bundleObjs.isEmpty()) {
                throw new JPAExecutorException(ErrorCode.E0603, "No entries found for given bundle(s)");
            }

            List<BundleJobBean> bundleBeans = new ArrayList<BundleJobBean>();
            for (Object[] bundleElem : bundleObjs) {
                bundleBeans.add(constructBundleBean(bundleElem));
            }
            return bundleBeans;
        }
        return null;
    }

    /**
     * Validate and determine whether passed param is job-id or appname
     * @param id
     * @param job
     * @return PARAM_TYPE
     */
    private PARAM_TYPE getParamType(String id, char job) {
        Pattern p = Pattern.compile("\\d{7}-\\d{15}-" + Services.get().getSystemId() + "-" + job);
        Matcher m = p.matcher(id);
        if (m.matches()) {
            return PARAM_TYPE.ID;
        }
        return PARAM_TYPE.NAME;
    }

    /**
     * Compose the coord action level query comprising bundle id/appname filter and coord action
     * status filter (if specified) and start-time or nominal-time filter (if specified)
     * @param em
     * @param bundles
     * @param times
     * @param responseList
     * @return Query string
     * @throws ParseException
     */
    @SuppressWarnings("unchecked")
    private String actionQuery(EntityManager em, List<BundleJobBean> bundles,
            Map<String, Timestamp> times, List<BulkResponseImpl> responseList) throws ParseException {
        Query q = em.createNamedQuery("BULK_MONITOR_ACTIONS_QUERY");
        StringBuilder getActions = new StringBuilder(q.toString());
        int offset = getActions.indexOf("ORDER");
        StringBuilder conditionClause = new StringBuilder();

        List<String> coords = bulkFilter.get(BulkResponseImpl.BULK_FILTER_COORD);
        // Query: Select <columns> from CoordinatorActionBean a, CoordinatorJobBean c WHERE a.jobId = c.id
        // AND c.bundleId = :bundleId AND c.appName/id IN (...)
        if (coords != null) {
            PARAM_TYPE type = getParamType(coords.get(0), 'C');
            if (type == PARAM_TYPE.NAME) {
                conditionClause.append(inClause(bulkFilter.get(BulkResponseImpl.BULK_FILTER_COORD), "appName", 'c'));
            }
            else if (type == PARAM_TYPE.ID) {
                conditionClause.append(inClause(bulkFilter.get(BulkResponseImpl.BULK_FILTER_COORD), "id", 'c'));
            }
        }
        // Query: Select <columns> from CoordinatorActionBean a, CoordinatorJobBean c WHERE a.jobId = c.id
        // AND c.bundleId = :bundleId AND c.appName/id IN (...) AND a.statusStr IN (...)
        conditionClause.append(statusClause(bulkFilter.get(BulkResponseImpl.BULK_FILTER_STATUS)));
        offset = getActions.indexOf("ORDER");
        getActions.insert(offset - 1, conditionClause);

        // Query: Select <columns> from CoordinatorActionBean a, CoordinatorJobBean c WHERE a.jobId = c.id
        // AND c.bundleId = :bundleId AND c.appName/id IN (...) AND a.statusStr IN (...)
        // AND a.createdTimestamp >= startCreated _or_ a.createdTimestamp <= endCreated
        // AND a.nominalTimestamp >= startNominal _or_ a.nominalTimestamp <= endNominal
        timesClause(getActions, offset, times);
        q = em.createQuery(getActions.toString());
        Iterator<Entry<String, Timestamp>> iter = times.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<String, Timestamp> time = iter.next();
            q.setParameter(time.getKey(), time.getValue());
        }
        // pagination
        q.setFirstResult(start - 1);
        q.setMaxResults(len);
        // repeatedly execute above query for each bundle
        for (BundleJobBean bundle : bundles) {
            q.setParameter("bundleId", bundle.getId());
            List<Object[]> response = q.getResultList();
            for (Object[] r : response) {
                BulkResponseImpl br = getResponseFromObject(bundle, r);
                responseList.add(br);
            }
        }
        return q.toString();
    }

    /**
     * Get total number of records for use with offset and len in API
     * @param clause
     * @param em
     * @param bundles
     * @return total count of coord actions
     */
    private long countQuery(String clause, EntityManager em, List<BundleJobBean> bundles, Map<String, Timestamp> times) {
        Query q = em.createNamedQuery("BULK_MONITOR_COUNT_QUERY");
        StringBuilder getTotal = new StringBuilder(q.toString() + " ");
        // Query: select COUNT(a) from CoordinatorActionBean a, CoordinatorJobBean c
        // get entire WHERE clause from above i.e. actionQuery() for all conditions on coordinator job
        // and action status and times
        getTotal.append(clause.substring(clause.indexOf("WHERE"), clause.indexOf("ORDER")));
        int offset = getTotal.indexOf("bundleId");
        List<String> bundleIds = new ArrayList<String>();
        for (BundleJobBean bundle : bundles) {
            bundleIds.add(bundle.getId());
        }
        // Query: select COUNT(a) from CoordinatorActionBean a, CoordinatorJobBean c WHERE ...
        // AND c.bundleId IN (... list of bundle ids) i.e. replace single :bundleId with list
        getTotal = getTotal.replace(offset - 6, offset + 20, inClause(bundleIds, "bundleId", 'c').toString());
        q = em.createQuery(getTotal.toString());
        Iterator<Entry<String, Timestamp>> iter = times.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<String, Timestamp> time = iter.next();
            q.setParameter(time.getKey(), time.getValue());
        }
        long total = ((Long) q.getSingleResult()).longValue();
        return total;
    }

    // Form the where clause to filter by coordinator appname/id
    private StringBuilder inClause(List<String> values, String col, char type) {
        StringBuilder sb = new StringBuilder();
        boolean firstVal = true;
        for (String name : nullToEmpty(values)) {
            if (firstVal) {
                sb.append(" AND " + type + "." + col + " IN (\'" + name + "\'");
                firstVal = false;
            }
            else {
                sb.append(",\'" + name + "\'");
            }
        }
        if (!firstVal) {
            sb.append(") ");
        }
        return sb;
    }

    // Form the where clause to filter by coord action status
    private StringBuilder statusClause(List<String> statuses) {
        StringBuilder sb = inClause(statuses, "statusStr", 'a');
        if (sb.length() == 0) { // statuses was null. adding default
            sb.append(" AND a.statusStr IN ('KILLED', 'FAILED') ");
        }
        return sb;
    }

    private void timesClause(StringBuilder sb, int offset, Map<String, Timestamp> eachTime) throws ParseException {
        Timestamp ts = null;
        List<String> times = bulkFilter.get(BulkResponseImpl.BULK_FILTER_START_CREATED_EPOCH);
        if (times != null) {
            ts = new Timestamp(DateUtils.parseDateUTC(times.get(0)).getTime());
            sb.insert(offset - 1, " AND a.createdTimestamp >= :startCreated");
            eachTime.put("startCreated", ts);
        }
        times = bulkFilter.get(BulkResponseImpl.BULK_FILTER_END_CREATED_EPOCH);
        if (times != null) {
            ts = new Timestamp(DateUtils.parseDateUTC(times.get(0)).getTime());
            sb.insert(offset - 1, " AND a.createdTimestamp <= :endCreated");
            eachTime.put("endCreated", ts);
        }
        times = bulkFilter.get(BulkResponseImpl.BULK_FILTER_START_NOMINAL_EPOCH);
        if (times != null) {
            ts = new Timestamp(DateUtils.parseDateUTC(times.get(0)).getTime());
            sb.insert(offset - 1, " AND a.nominalTimestamp >= :startNominal");
            eachTime.put("startNominal", ts);
        }
        times = bulkFilter.get(BulkResponseImpl.BULK_FILTER_END_NOMINAL_EPOCH);
        if (times != null) {
            ts = new Timestamp(DateUtils.parseDateUTC(times.get(0)).getTime());
            sb.insert(offset - 1, " AND a.nominalTimestamp <= :endNominal");
            eachTime.put("endNominal", ts);
        }
    }

    private BulkResponseImpl getResponseFromObject(BundleJobBean bundleBean, Object arr[]) {
        BulkResponseImpl bean = new BulkResponseImpl();
        CoordinatorJobBean coordBean = new CoordinatorJobBean();
        CoordinatorActionBean actionBean = new CoordinatorActionBean();
        if (arr[0] != null) {
            actionBean.setId((String) arr[0]);
        }
        if (arr[1] != null) {
            actionBean.setActionNumber((Integer) arr[1]);
        }
        if (arr[2] != null) {
            actionBean.setErrorCode((String) arr[2]);
        }
        if (arr[3] != null) {
            actionBean.setErrorMessage((String) arr[3]);
        }
        if (arr[4] != null) {
            actionBean.setExternalId((String) arr[4]);
        }
        if (arr[5] != null) {
            actionBean.setExternalStatus((String) arr[5]);
        }
        if (arr[6] != null) {
            actionBean.setStatus(CoordinatorAction.Status.valueOf((String) arr[6]));
        }
        if (arr[7] != null) {
            actionBean.setCreatedTime(DateUtils.toDate((Timestamp) arr[7]));
        }
        if (arr[8] != null) {
            actionBean.setNominalTime(DateUtils.toDate((Timestamp) arr[8]));
        }
        if (arr[9] != null) {
            actionBean.setMissingDependenciesBlob((StringBlob) arr[9]);
        }
        if (arr[10] != null) {
            coordBean.setId((String) arr[10]);
            actionBean.setJobId((String) arr[10]);
        }
        if (arr[11] != null) {
            coordBean.setAppName((String) arr[11]);
        }
        if (arr[12] != null) {
            coordBean.setStatus(CoordinatorJob.Status.valueOf((String) arr[12]));
        }
        bean.setBundle(bundleBean);
        bean.setCoordinator(coordBean);
        bean.setAction(actionBean);
        return bean;
    }

    private BundleJobBean constructBundleBean(Object[] barr) throws JPAExecutorException {
        BundleJobBean bean = new BundleJobBean();
        if (barr[0] != null) {
            bean.setId((String) barr[0]);
        }
        else {
            throw new JPAExecutorException(ErrorCode.E0603,
                    "bundleId returned by query is null - cannot retrieve bulk results");
        }
        if (barr[1] != null) {
            bean.setAppName((String) barr[1]);
        }
        if (barr[2] != null) {
            bean.setStatus(BundleJob.Status.valueOf((String) barr[2]));
        }
        if (barr[3] != null) {
            bean.setUser((String) barr[3]);
        }
        return bean;
    }

    // null safeguard
    public static List<String> nullToEmpty(List<String> input) {
        return input == null ? Collections.<String> emptyList() : input;
    }

}
