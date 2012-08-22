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
import java.util.List;
import java.util.Map;
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
 * Load the coordinators for specified bundle in the Coordinator job bean
 */
public class BulkJPAExecutor implements JPAExecutor<BulkResponseInfo> {
    private Map<String, List<String>> bulkFilter;
    // defaults
    private int start = 1;
    private int len = 50;

    public static final String bundleQuery = "SELECT b.id, b.status FROM BundleJobBean b WHERE b.appName = :appName";
    // Join query
    public static final String actionQuery = "SELECT a.id, a.actionNumber, a.errorCode, a.errorMessage, a.externalId, " +
            "a.externalStatus, a.status, a.createdTimestamp, a.nominalTimestamp, a.missingDependencies, " +
            "c.id, c.appName, c.status FROM CoordinatorActionBean a, CoordinatorJobBean c " +
            "WHERE a.jobId = c.id AND c.bundleId = :bundleId ORDER BY a.jobId, a.createdTimestamp";
    public static final String countQuery = "SELECT COUNT(a) FROM CoordinatorActionBean a, CoordinatorJobBean c ";

    public BulkJPAExecutor(Map<String, List<String>> bulkFilter, int start, int len) {
        ParamChecker.notNull(bulkFilter, "bulkFilter");
        this.bulkFilter = bulkFilter;
        this.start = start;
        this.len = len;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "BulkJPAExecutor";
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.EntityManager)
     */
    @Override
    @SuppressWarnings("unchecked")
    public BulkResponseInfo execute(EntityManager em) throws JPAExecutorException {
        BundleJobBean bundleBean = new BundleJobBean();

        List<BulkResponseImpl> responseList = new ArrayList<BulkResponseImpl>();

        try {

            // Lightweight Query 1 on Bundle level to fetch the bundle job params corresponding to name
            String bundleName = bulkFilter.get(BulkResponseImpl.BULK_FILTER_BUNDLE_NAME).get(0);
            Query q = em.createQuery(bundleQuery);
            q.setParameter("appName", bundleName);
            List<Object[]> bundles = (List<Object[]>) q.getResultList();
            if(bundles.isEmpty()) {
                throw new JPAExecutorException(ErrorCode.E0603, "No bundle entries found for bundle name: " + bundleName);
            }
            if(bundles.size() > 1) { // more than one bundles running with same name - ERROR. Fail fast
                throw new JPAExecutorException(ErrorCode.E0603, "Non-unique bundles present for same bundle name: " + bundleName);
            }
            bundleBean = getBeanForBundleJob(bundles.get(0), bundleName);

            //Join query between coordinator job and coordinator action tables to get entries for specific bundleId only
            StringBuilder conditionClause = new StringBuilder();
            conditionClause.append(coordNamesClause(bulkFilter.get(BulkResponseImpl.BULK_FILTER_COORD_NAME)));
            conditionClause.append(statusClause(bulkFilter.get(BulkResponseImpl.BULK_FILTER_STATUS)));
            conditionClause.append(timesClause());
            StringBuilder getActions = new StringBuilder(actionQuery);
            int offset = getActions.indexOf("ORDER");
            getActions.insert(offset-1, conditionClause);
            q = em.createQuery(getActions.toString());
            q.setParameter("bundleId", bundleBean.getId());
            // pagination
            q.setFirstResult(start - 1);
            q.setMaxResults(len);

            List<Object[]> response = q.getResultList();
            for (Object[] r : response) {
                BulkResponseImpl br = getResponseFromObject(bundleBean, r);
                responseList.add(br);
            }

            StringBuilder getTotal = new StringBuilder(countQuery);
            String query = q.toString();
            getTotal.append(query.substring(query.indexOf("WHERE"), query.indexOf("ORDER")));
            Query qTotal = em.createQuery(getTotal.toString());
            qTotal.setParameter("bundleId", bundleBean.getId());
            long total = ((Long) qTotal.getSingleResult()).longValue();

            BulkResponseInfo bulk = new BulkResponseInfo(responseList, start, len , total);
            return bulk;
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e);
        }
    }

    // Form the where clause to filter by coordinator names
    private StringBuilder coordNamesClause(List<String> coordNames) {
        StringBuilder sb = new StringBuilder();
        boolean firstVal = true;
        for (String name : nullToEmpty(coordNames)) {
            if(firstVal) {
                sb.append(" AND c.appName IN (\'" + name + "\'");
                firstVal = false;
            } else {
                sb.append(",\'" + name + "\'");
            }
        }
        if(!firstVal) {
            sb.append(") ");
        }
        return sb;
    }

    // Form the where clause to filter by coord action status
    private StringBuilder statusClause(List<String> statuses) {
        StringBuilder sb = new StringBuilder();
        boolean firstVal = true;
        for (String status : nullToEmpty(statuses)) {
            if(firstVal) {
                sb.append(" AND a.status IN (\'" + status + "\'");
                firstVal = false;
            } else {
                sb.append(",\'" + status + "\'");
            }
        }
        if(!firstVal) {
            sb.append(") ");
        } else { // statuses was null. adding default
            sb.append(" AND a.status IN ('KILLED', 'FAILED') ");
        }
        return sb;
    }

    private StringBuilder timesClause() throws ParseException {
        StringBuilder sb = new StringBuilder();
        List<String> times = bulkFilter.get(BulkResponseImpl.BULK_FILTER_START_CREATED_EPOCH);
        if(times != null) {
            sb.append(" AND a.createdTimestamp >= '" + new Timestamp(DateUtils.parseDateUTC(times.get(0)).getTime()) + "'");
        }
        times = bulkFilter.get(BulkResponseImpl.BULK_FILTER_END_CREATED_EPOCH);
        if(times != null) {
            sb.append(" AND a.createdTimestamp <= '" + new Timestamp(DateUtils.parseDateUTC(times.get(0)).getTime()) + "'");
        }
        times = bulkFilter.get(BulkResponseImpl.BULK_FILTER_START_NOMINAL_EPOCH);
        if(times != null) {
            sb.append(" AND a.nominalTimestamp >= '" + new Timestamp(DateUtils.parseDateUTC(times.get(0)).getTime()) + "'");
        }
        times = bulkFilter.get(BulkResponseImpl.BULK_FILTER_END_NOMINAL_EPOCH);
        if(times != null) {
            sb.append(" AND a.nominalTimestamp <= '" + new Timestamp(DateUtils.parseDateUTC(times.get(0)).getTime()) + "'");
        }
        return sb;
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

    private BundleJobBean getBeanForBundleJob(Object[] barr, String name) throws Exception {
        BundleJobBean bean = new BundleJobBean();
        if (barr[0] != null) {
            bean.setId((String) barr[0]);
        } else {
            throw new JPAExecutorException(ErrorCode.E0603, "bundleId returned by query is null - cannot retrieve bulk results");
        }
        bean.setAppName(name);
        if (barr[1] != null) {
            bean.setStatus(BundleJob.Status.valueOf((String) barr[1]));
        }
        return bean;
    }

    // null safeguard
    public static List<String> nullToEmpty(List<String> input) {
        return input == null ? Collections.<String>emptyList() : input;
    }

}
