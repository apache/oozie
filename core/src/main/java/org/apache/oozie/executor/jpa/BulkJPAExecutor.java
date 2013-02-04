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
import org.apache.oozie.client.CoordinatorAction;
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

    public BulkJPAExecutor(Map<String, List<String>> bulkFilter, int start, int len) {
        ParamChecker.notNull(bulkFilter, "bulkFilter");
        this.bulkFilter = bulkFilter;
        this.start = start;
        this.len = len;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "BulkJPAExecutor";
    }

    /*
     * (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.EntityManager)
     */
    @Override
    public BulkResponseInfo execute(EntityManager em) throws JPAExecutorException {
        List<BulkResponseImpl> responseList = new ArrayList<BulkResponseImpl>();
        Map<String, Timestamp> actionTimes = new HashMap<String, Timestamp>();

        try {
            // Lightweight Query 1 on Bundle level to fetch the bundle job
            // corresponding to name
            BundleJobBean bundleBean = bundleQuery(em);

            // Join query between coordinator job and coordinator action tables
            // to get entries for specific bundleId only
            String conditions = actionQuery(em, bundleBean, actionTimes, responseList);

            // Query to get the count of records
            long total = countQuery(conditions, em, bundleBean, actionTimes);

            BulkResponseInfo bulk = new BulkResponseInfo(responseList, start, len, total);
            return bulk;
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private BundleJobBean bundleQuery(EntityManager em) throws JPAExecutorException {
        BundleJobBean bundleBean = new BundleJobBean();
        String bundleName = bulkFilter.get(BulkResponseImpl.BULK_FILTER_BUNDLE_NAME).get(0);
        Query q = em.createNamedQuery("BULK_MONITOR_BUNDLE_QUERY");
        q.setParameter("appName", bundleName);
        List<Object[]> bundles = (List<Object[]>) q.getResultList();
        if (bundles.isEmpty()) {
            throw new JPAExecutorException(ErrorCode.E0603, "No bundle entries found for bundle name: "
                    + bundleName);
        }
        if (bundles.size() > 1) { // more than one bundles running with same
                                  // name - ERROR. Fail fast
            throw new JPAExecutorException(ErrorCode.E0603, "Non-unique bundles present for same bundle name: "
                    + bundleName);
        }
        bundleBean = getBeanForBundleJob(bundles.get(0), bundleName);
        return bundleBean;
    }

    @SuppressWarnings("unchecked")
    private String actionQuery(EntityManager em, BundleJobBean bundleBean,
            Map<String, Timestamp> times, List<BulkResponseImpl> responseList) throws ParseException {
        Query q = em.createNamedQuery("BULK_MONITOR_ACTIONS_QUERY");
        StringBuilder getActions = new StringBuilder(q.toString());
        StringBuilder conditionClause = new StringBuilder();
        conditionClause.append(coordNamesClause(bulkFilter.get(BulkResponseImpl.BULK_FILTER_COORD_NAME)));
        conditionClause.append(statusClause(bulkFilter.get(BulkResponseImpl.BULK_FILTER_STATUS)));
        int offset = getActions.indexOf("ORDER");
        getActions.insert(offset - 1, conditionClause);
        timesClause(getActions, offset, times);
        q = em.createQuery(getActions.toString());
        Iterator<Entry<String, Timestamp>> iter = times.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<String, Timestamp> time = iter.next();
            q.setParameter(time.getKey(), time.getValue());
        }
        q.setParameter("bundleId", bundleBean.getId());
        // pagination
        q.setFirstResult(start - 1);
        q.setMaxResults(len);

        List<Object[]> response = q.getResultList();
        for (Object[] r : response) {
            BulkResponseImpl br = getResponseFromObject(bundleBean, r);
            responseList.add(br);
        }
        return q.toString();
    }

    private long countQuery(String clause, EntityManager em, BundleJobBean bundleBean, Map<String, Timestamp> times) {
        Query q = em.createNamedQuery("BULK_MONITOR_COUNT_QUERY");
        StringBuilder getTotal = new StringBuilder(q.toString() + " ");
        getTotal.append(clause.substring(clause.indexOf("WHERE"), clause.indexOf("ORDER")));
        q = em.createQuery(getTotal.toString());
        q.setParameter("bundleId", bundleBean.getId());
        Iterator<Entry<String, Timestamp>> iter = times.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<String, Timestamp> time = iter.next();
            q.setParameter(time.getKey(), time.getValue());
        }
        long total = ((Long) q.getSingleResult()).longValue();
        return total;
    }

    // Form the where clause to filter by coordinator names
    private StringBuilder coordNamesClause(List<String> coordNames) {
        StringBuilder sb = new StringBuilder();
        boolean firstVal = true;
        for (String name : nullToEmpty(coordNames)) {
            if (firstVal) {
                sb.append(" AND c.appName IN (\'" + name + "\'");
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
        StringBuilder sb = new StringBuilder();
        boolean firstVal = true;
        for (String status : nullToEmpty(statuses)) {
            if (firstVal) {
                sb.append(" AND a.status IN (\'" + status + "\'");
                firstVal = false;
            }
            else {
                sb.append(",\'" + status + "\'");
            }
        }
        if (!firstVal) {
            sb.append(") ");
        }
        else { // statuses was null. adding default
            sb.append(" AND a.status IN ('KILLED', 'FAILED') ");
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
            actionBean.setMissingDependencies((String) arr[9]);
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

    private BundleJobBean getBeanForBundleJob(Object[] barr, String name) throws JPAExecutorException {
        BundleJobBean bean = new BundleJobBean();
        if (barr[0] != null) {
            bean.setId((String) barr[0]);
        }
        else {
            throw new JPAExecutorException(ErrorCode.E0603,
                    "bundleId returned by query is null - cannot retrieve bulk results");
        }
        bean.setAppName(name);
        if (barr[1] != null) {
            bean.setStatus(BundleJob.Status.valueOf((String) barr[1]));
        }
        return bean;
    }

    // null safeguard
    public static List<String> nullToEmpty(List<String> input) {
        return input == null ? Collections.<String> emptyList() : input;
    }

}
