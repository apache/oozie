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

import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.WorkflowsInfo;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob.Status;
import org.apache.oozie.util.XLog;
import org.apache.openjpa.persistence.OpenJPAPersistence;
import org.apache.openjpa.persistence.OpenJPAQuery;
import org.apache.openjpa.persistence.jdbc.FetchDirection;
import org.apache.openjpa.persistence.jdbc.JDBCFetchPlan;
import org.apache.openjpa.persistence.jdbc.LRSSizeAlgorithm;
import org.apache.openjpa.persistence.jdbc.ResultSetType;

public class WorkflowsJobGetJPAExecutor implements JPAExecutor<WorkflowsInfo> {

    private static final String seletStr = "Select w.id, w.appName, w.status, w.run, w.user, w.group, w.createdTimestamp, "
        + "w.startTimestamp, w.lastModifiedTimestamp, w.endTimestamp, w.externalId from WorkflowJobBean w";
    private static final String countStr = "Select count(w) from WorkflowJobBean w";

    private final Map<String, List<String>> filter;
    private final int start;
    private final int len;

    /**
     * This JPA Executor gets the workflows info for the range.
     *
     * @param filter
     * @param start
     * @param len
     */
    public WorkflowsJobGetJPAExecutor(Map<String, List<String>> filter, int start, int len) {
        this.filter = filter;
        this.start = start;
        this.len = len;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.EntityManager)
     */
    @SuppressWarnings("unchecked")
    @Override
    public WorkflowsInfo execute(EntityManager em) throws JPAExecutorException {
        List<String> orArray = new ArrayList<String>();
        List<String> colArray = new ArrayList<String>();
        List<String> valArray = new ArrayList<String>();
        StringBuilder sb = new StringBuilder("");
        boolean isStatus = false;
        boolean isAppName = false;
        boolean isUser = false;
        boolean isEnabled = false;
        boolean isId = false;
        int index = 0;
        for (Map.Entry<String, List<String>> entry : filter.entrySet()) {
            String colName = null;
            String colVar = null;
            if (entry.getKey().equals(OozieClient.FILTER_GROUP)) {
                XLog.getLog(getClass()).warn("Filter by 'group' is not supported anymore");
            } else {
                if (entry.getKey().equals(OozieClient.FILTER_STATUS)) {
                    List<String> values = filter.get(OozieClient.FILTER_STATUS);
                    colName = "status";
                    for (int i = 0; i < values.size(); i++) {
                        colVar = "status";
                        colVar = colVar + index;
                        if (!isEnabled && !isStatus) {
                            sb.append(seletStr).append(" where w.status IN (:status" + index);
                            isStatus = true;
                            isEnabled = true;
                        }
                        else {
                            if (isEnabled && !isStatus) {
                                sb.append(" and w.status IN (:status" + index);
                                isStatus = true;
                            }
                            else {
                                if (isStatus) {
                                    sb.append(", :status" + index);
                                }
                            }
                        }
                        if (i == values.size() - 1) {
                            sb.append(")");
                        }
                        index++;
                        valArray.add(values.get(i));
                        orArray.add(colName);
                        colArray.add(colVar);
                    }
                }
                else {
                    if (entry.getKey().equals(OozieClient.FILTER_NAME)) {
                        List<String> values = filter.get(OozieClient.FILTER_NAME);
                        colName = "appName";
                        for (int i = 0; i < values.size(); i++) {
                            colVar = "appName";
                            colVar = colVar + index;
                            if (!isEnabled && !isAppName) {
                                sb.append(seletStr).append(" where w.appName IN (:appName" + index);
                                isAppName = true;
                                isEnabled = true;
                            }
                            else {
                                if (isEnabled && !isAppName) {
                                    sb.append(" and w.appName IN (:appName" + index);
                                    isAppName = true;
                                }
                                else {
                                    if (isAppName) {
                                        sb.append(", :appName" + index);
                                    }
                                }
                            }
                            if (i == values.size() - 1) {
                                sb.append(")");
                            }
                            index++;
                            valArray.add(values.get(i));
                            orArray.add(colName);
                            colArray.add(colVar);
                        }
                    }
                    else {
                        if (entry.getKey().equals(OozieClient.FILTER_USER)) {
                            List<String> values = filter.get(OozieClient.FILTER_USER);
                            colName = "user";
                            for (int i = 0; i < values.size(); i++) {
                                colVar = "user";
                                colVar = colVar + index;
                                if (!isEnabled && !isUser) {
                                    sb.append(seletStr).append(" where w.user IN (:user" + index);
                                    isUser = true;
                                    isEnabled = true;
                                }
                                else {
                                    if (isEnabled && !isUser) {
                                        sb.append(" and w.user IN (:user" + index);
                                        isUser = true;
                                    }
                                    else {
                                        if (isUser) {
                                            sb.append(", :user" + index);
                                        }
                                    }
                                }
                                if (i == values.size() - 1) {
                                    sb.append(")");
                                }
                                index++;
                                valArray.add(values.get(i));
                                orArray.add(colName);
                                colArray.add(colVar);
                            }
                        }
                    }
                    if (entry.getKey().equals(OozieClient.FILTER_ID)) {
                        List<String> values = filter.get(OozieClient.FILTER_ID);
                        colName = "id";
                        for (int i = 0; i < values.size(); i++) {
                            colVar = "id";
                            colVar = colVar + index;
                            if (!isEnabled && !isId) {
                                sb.append(seletStr).append(" where w.id IN (:id" + index);
                                isId = true;
                                isEnabled = true;
                            }
                            else {
                                if (isEnabled && !isId) {
                                    sb.append(" and w.id IN (:id" + index);
                                    isId = true;
                                }
                                else {
                                    if (isId) {
                                        sb.append(", :id" + index);
                                    }
                                }
                            }
                            if (i == values.size() - 1) {
                                sb.append(")");
                            }
                            index++;
                            valArray.add(values.get(i));
                            orArray.add(colName);
                            colArray.add(colVar);
                        }
                    }
                }
            }
        }

        int realLen = 0;

        Query q = null;
        Query qTotal = null;
        if (orArray.size() == 0) {
            q = em.createNamedQuery("GET_WORKFLOWS_COLUMNS");
            q.setFirstResult(start - 1);
            q.setMaxResults(len);
            qTotal = em.createNamedQuery("GET_WORKFLOWS_COUNT");
        }
        else {
            if (orArray.size() > 0) {
                StringBuilder sbTotal = new StringBuilder(sb);
                sb.append(" order by w.startTimestamp desc ");
                q = em.createQuery(sb.toString());
                q.setFirstResult(start - 1);
                q.setMaxResults(len);
                qTotal = em.createQuery(sbTotal.toString().replace(seletStr, countStr));
                for (int i = 0; i < orArray.size(); i++) {
                    q.setParameter(colArray.get(i), valArray.get(i));
                    qTotal.setParameter(colArray.get(i), valArray.get(i));
                }
            }
        }

        OpenJPAQuery kq = OpenJPAPersistence.cast(q);
        JDBCFetchPlan fetch = (JDBCFetchPlan) kq.getFetchPlan();
        fetch.setFetchBatchSize(20);
        fetch.setResultSetType(ResultSetType.SCROLL_INSENSITIVE);
        fetch.setFetchDirection(FetchDirection.FORWARD);
        fetch.setLRSSizeAlgorithm(LRSSizeAlgorithm.LAST);
        List<?> resultList = q.getResultList();
        List<Object[]> objectArrList = (List<Object[]>) resultList;
        List<WorkflowJobBean> wfBeansList = new ArrayList<WorkflowJobBean>();

        for (Object[] arr : objectArrList) {
            WorkflowJobBean ww = getBeanForWorkflowFromArray(arr);
            wfBeansList.add(ww);
        }

        realLen = ((Long) qTotal.getSingleResult()).intValue();

        return new WorkflowsInfo(wfBeansList, start, len, realLen);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "WorkflowsJobGetJPAExecutor";
    }

    private WorkflowJobBean getBeanForWorkflowFromArray(Object[] arr) {

        WorkflowJobBean wfBean = new WorkflowJobBean();
        wfBean.setId((String) arr[0]);
        if (arr[1] != null) {
            wfBean.setAppName((String) arr[1]);
        }
        if (arr[2] != null) {
            wfBean.setStatus(Status.valueOf((String) arr[2]));
        }
        if (arr[3] != null) {
            wfBean.setRun((Integer) arr[3]);
        }
        if (arr[4] != null) {
            wfBean.setUser((String) arr[4]);
        }
        if (arr[5] != null) {
            wfBean.setGroup((String) arr[5]);
        }
        if (arr[6] != null) {
            wfBean.setCreatedTime((Timestamp) arr[6]);
        }
        if (arr[7] != null) {
            wfBean.setStartTime((Timestamp) arr[7]);
        }
        if (arr[8] != null) {
            wfBean.setLastModifiedTime((Timestamp) arr[8]);
        }
        if (arr[9] != null) {
            wfBean.setEndTime((Timestamp) arr[9]);
        }
        if (arr[10] != null) {
            wfBean.setExternalId((String) arr[10]);
        }
        return wfBean;
    }
}
