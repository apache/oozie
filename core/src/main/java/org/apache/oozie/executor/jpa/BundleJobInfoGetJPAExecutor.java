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
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.BundleJobInfo;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.BundleJob.Timeunit;
import org.apache.oozie.store.StoreStatusFilter;
import org.apache.oozie.util.ParamChecker;
import org.apache.openjpa.persistence.OpenJPAPersistence;
import org.apache.openjpa.persistence.OpenJPAQuery;
import org.apache.openjpa.persistence.jdbc.FetchDirection;
import org.apache.openjpa.persistence.jdbc.JDBCFetchPlan;
import org.apache.openjpa.persistence.jdbc.LRSSizeAlgorithm;
import org.apache.openjpa.persistence.jdbc.ResultSetType;

/**
 * Load the BundleInfo and return it.
 */
public class BundleJobInfoGetJPAExecutor implements JPAExecutor<BundleJobInfo> {

    private Map<String, List<String>> filter;
    private int start = 1;
    private int len = 50;

    /**
     * The constructor for BundleJobInfoGetJPAExecutor
     *
     * @param filter the filter string
     * @param start start location for paging
     * @param len total length to get
     */
    public BundleJobInfoGetJPAExecutor(Map<String, List<String>> filter, int start, int len) {
        ParamChecker.notNull(filter, "filter");
        this.filter = filter;
        this.start = start;
        this.len = len;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "BundleJobInfoGetJPAExecutor";
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.EntityManager)
     */
    @Override
    @SuppressWarnings("unchecked")
    public BundleJobInfo execute(EntityManager em) throws JPAExecutorException {
        List<String> orArray = new ArrayList<String>();
        List<String> colArray = new ArrayList<String>();
        List<String> valArray = new ArrayList<String>();
        StringBuilder sb = new StringBuilder("");

        StoreStatusFilter.filter(filter, orArray, colArray, valArray, sb, StoreStatusFilter.bundleSeletStr,
                                 StoreStatusFilter.bundleCountStr);

        int realLen = 0;

        Query q = null;
        Query qTotal = null;
        if (orArray.size() == 0) {
            q = em.createNamedQuery("GET_BUNDLE_JOBS_COLUMNS");
            q.setFirstResult(start - 1);
            q.setMaxResults(len);
            qTotal = em.createNamedQuery("GET_BUNDLE_JOBS_COUNT");
        }
        else {
            StringBuilder sbTotal = new StringBuilder(sb);
            sb.append(" order by w.createdTimestamp desc ");
            q = em.createQuery(sb.toString());
            q.setFirstResult(start - 1);
            q.setMaxResults(len);
            qTotal = em.createQuery(sbTotal.toString().replace(StoreStatusFilter.bundleSeletStr,
                                                                          StoreStatusFilter.bundleCountStr));
        }

        for (int i = 0; i < orArray.size(); i++) {
            q.setParameter(colArray.get(i), valArray.get(i));
            qTotal.setParameter(colArray.get(i), valArray.get(i));
        }

        OpenJPAQuery kq = OpenJPAPersistence.cast(q);
        JDBCFetchPlan fetch = (JDBCFetchPlan) kq.getFetchPlan();
        fetch.setFetchBatchSize(20);
        fetch.setResultSetType(ResultSetType.SCROLL_INSENSITIVE);
        fetch.setFetchDirection(FetchDirection.FORWARD);
        fetch.setLRSSizeAlgorithm(LRSSizeAlgorithm.LAST);
        List<?> resultList = q.getResultList();
        List<Object[]> objectArrList = (List<Object[]>) resultList;
        List<BundleJobBean> bundleBeansList = new ArrayList<BundleJobBean>();

        for (Object[] arr : objectArrList) {
            BundleJobBean bean = getBeanForBundleJobFromArray(arr);
            bundleBeansList.add(bean);
        }

        realLen = ((Long) qTotal.getSingleResult()).intValue();

        return new BundleJobInfo(bundleBeansList, start, len, realLen);
    }

    private BundleJobBean getBeanForBundleJobFromArray(Object[] arr) {

        BundleJobBean bean = new BundleJobBean();
        bean.setId((String) arr[0]);
        if (arr[1] != null) {
            bean.setAppName((String) arr[1]);
        }
        if (arr[2] != null) {
            bean.setAppPath((String) arr[2]);
        }
        if (arr[3] != null) {
            bean.setConf((String) arr[3]);
        }
        if (arr[4] != null) {
            bean.setStatus(Job.Status.valueOf((String) arr[4]));
        }
        if (arr[5] != null) {
            bean.setKickoffTime((Timestamp) arr[5]);
        }
        if (arr[6] != null) {
            bean.setStartTime((Timestamp) arr[6]);
        }
        if (arr[7] != null) {
            bean.setEndTime((Timestamp) arr[7]);
        }
        if (arr[8] != null) {
            bean.setPauseTime((Timestamp) arr[8]);
        }
        if (arr[9] != null) {
            bean.setCreatedTime((Timestamp) arr[9]);
        }
        if (arr[10] != null) {
            bean.setUser((String) arr[10]);
        }
        if (arr[11] != null) {
            bean.setGroup((String) arr[11]);
        }
        if (arr[12] != null) {
            bean.setTimeUnit(Timeunit.valueOf((String) arr[12]));
        }
        if (arr[13] != null) {
            bean.setTimeOut((Integer) arr[13]);
        }

        return bean;
    }
}
