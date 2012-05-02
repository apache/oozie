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

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ParamChecker;

/**
 * Load coordinator actions by start and len (a subset) for a coordinator job.
 */
public class CoordJobGetActionsSubsetJPAExecutor implements JPAExecutor<List<CoordinatorActionBean>> {

    private String coordJobId = null;
    private int start = 1;
    private int len = 50;
    private List<String> filterList;

    public CoordJobGetActionsSubsetJPAExecutor(String coordJobId) {
        ParamChecker.notNull(coordJobId, "coordJobId");
        this.coordJobId = coordJobId;
    }

    public CoordJobGetActionsSubsetJPAExecutor(String coordJobId, List<String> filterList, int start, int len) {
        this(coordJobId);
        ParamChecker.notNull(filterList, "filterList");
        this.filterList = filterList;
        this.start = start;
        this.len = len;
    }

    @Override
    public String getName() {
        return "CoordJobGetActionsSubsetJPAExecutor";
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<CoordinatorActionBean> execute(EntityManager em) throws JPAExecutorException {
        List<CoordinatorActionBean> actionList = new ArrayList<CoordinatorActionBean>();
        try {
            if (!Services.get().getConf()
                    .getBoolean(CoordActionGetForInfoJPAExecutor.COORD_GET_ALL_COLS_FOR_ACTION, false)) {
                Query q = em.createNamedQuery("GET_ACTIONS_FOR_COORD_JOB_ORDER_BY_NOMINAL_TIME");
                q = setQueryParameters(q, em);
                List<Object[]> actions = q.getResultList();

                for (Object[] a : actions) {
                    CoordinatorActionBean aa = getBeanForRunningCoordAction(a);
                    actionList.add(aa);
                }
            } else {
                Query q = em.createNamedQuery("GET_ALL_COLS_FOR_ACTIONS_FOR_COORD_JOB_ORDER_BY_NOMINAL_TIME");
                q = setQueryParameters(q, em);
                List<CoordinatorActionBean> caActions = q.getResultList();

                for (CoordinatorActionBean a : caActions) {
                    CoordinatorActionBean aa = getBeanForCoordAction(a);
                    actionList.add(aa);
                }
            }
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e);
        }
        return actionList;
    }

    private Query setQueryParameters(Query q, EntityManager em){
        if (!filterList.isEmpty()) {
            // Add the filter clause
            String query = q.toString();
            StringBuilder sbTotal = new StringBuilder(query);
            int offset = query.lastIndexOf("order");
            // Get the 'where' clause for status filters
            StringBuilder statusClause = getStatusClause(filterList);
            // Insert 'where' before 'order by'
            sbTotal.insert(offset, statusClause);
            q = em.createQuery(sbTotal.toString());
        }
        q.setParameter("jobId", coordJobId);
        q.setFirstResult(start - 1);
        q.setMaxResults(len);
        return q;
    }

    // Form the where clause to filter by status values
    private StringBuilder getStatusClause(List<String> filterList) {
        StringBuilder sb = new StringBuilder();
        boolean isStatus = false;
        for (String statusVal : filterList) {
            if (!isStatus) {
                sb.append(" and a.status IN (\'" + statusVal + "\'");
                isStatus = true;
            }
            else {
                sb.append(",\'" + statusVal + "\'");
            }
        }
        sb.append(") ");
        return sb;
    }

    private CoordinatorActionBean getBeanForCoordAction(CoordinatorActionBean a){
        if (a != null) {
            CoordinatorActionBean action = new CoordinatorActionBean();
            action.setId(a.getId());
            action.setActionNumber(a.getActionNumber());
            action.setActionXml(a.getActionXml());
            action.setConsoleUrl(a.getConsoleUrl());
            action.setCreatedConf(a.getCreatedConf());
            action.setExternalStatus(a.getExternalStatus());
            action.setMissingDependencies(a.getMissingDependencies());
            action.setRunConf(a.getRunConf());
            action.setTimeOut(a.getTimeOut());
            action.setTrackerUri(a.getTrackerUri());
            action.setType(a.getType());
            action.setCreatedTime(a.getCreatedTime());
            action.setExternalId(a.getExternalId());
            action.setJobId(a.getJobId());
            action.setLastModifiedTime(a.getLastModifiedTime());
            action.setNominalTime(a.getNominalTime());
            action.setSlaXml(a.getSlaXml());
            action.setStatus(a.getStatus());
            return action;
        }
        return null;
    }

    private CoordinatorActionBean getBeanForRunningCoordAction(Object arr[]) {
        CoordinatorActionBean bean = new CoordinatorActionBean();
        if (arr[0] != null) {
            bean.setId((String) arr[0]);
        }
        if (arr[1] != null) {
            bean.setActionNumber((Integer) arr[1]);
        }
        if (arr[2] != null) {
            bean.setConsoleUrl((String) arr[2]);
        }
        if (arr[3] != null) {
            bean.setErrorCode((String) arr[3]);
        }
        if (arr[4] != null) {
            bean.setErrorMessage((String) arr[4]);
        }
        if (arr[5] != null) {
            bean.setExternalId((String) arr[5]);
        }
        if (arr[6] != null) {
            bean.setExternalStatus((String) arr[6]);
        }
        if (arr[7] != null) {
            bean.setJobId((String) arr[7]);
        }
        if (arr[8] != null) {
            bean.setTrackerUri((String) arr[8]);
        }
        if (arr[9] != null) {
            bean.setCreatedTime(DateUtils.toDate((Timestamp) arr[9]));
        }
        if (arr[10] != null) {
            bean.setNominalTime(DateUtils.toDate((Timestamp) arr[10]));
        }
        if (arr[11] != null) {
            bean.setStatus(CoordinatorAction.Status.valueOf((String) arr[11]));
        }
        if (arr[12] != null) {
            bean.setLastModifiedTime(DateUtils.toDate((Timestamp) arr[12]));
        }
        if (arr[13] != null) {
            bean.setMissingDependencies((String) arr[13]);
        }
        if (arr[14] != null) {
            bean.setTimeOut((Integer) arr[14]);
        }
        return bean;

    }

}
