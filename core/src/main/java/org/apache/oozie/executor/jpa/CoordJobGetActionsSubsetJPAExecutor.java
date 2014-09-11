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

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorEngine.FILTER_COMPARATORS;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.StringBlob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.Pair;
import org.apache.oozie.util.ParamChecker;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Load coordinator actions by offset and len (a subset) for a coordinator job.
 */
public class CoordJobGetActionsSubsetJPAExecutor implements JPAExecutor<List<CoordinatorActionBean>> {

    private String coordJobId = null;
    private int offset = 1;
    private int len = 50;
    private boolean desc = false;
    private Map<Pair<String, FILTER_COMPARATORS>, List<Object>> filterMap;

    public CoordJobGetActionsSubsetJPAExecutor(String coordJobId) {
        ParamChecker.notNull(coordJobId, "coordJobId");
        this.coordJobId = coordJobId;
    }

    public CoordJobGetActionsSubsetJPAExecutor(String coordJobId, Map<Pair<String, FILTER_COMPARATORS>, List<Object>> filterMap,
            int offset, int len, boolean desc) {
        this(coordJobId);
        this.filterMap = filterMap;
        this.offset = offset;
        this.len = len;
        this.desc = desc;
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
                actionList = q.getResultList();
            }
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
        return actionList;
    }

    private Query setQueryParameters(Query q, EntityManager em){
        Map<String, Object> params = null;
        if (filterMap != null) {
            // Add the filter clause
            String query = q.toString();
            StringBuilder sbTotal = new StringBuilder(query);
            int offset = query.lastIndexOf("order");
            // Get the 'where' clause for status filters
            StringBuilder statusClause = new StringBuilder();
            params = getWhereClause(statusClause, filterMap);
            // Insert 'where' before 'order by'
            sbTotal.insert(offset, statusClause);
            q = em.createQuery(sbTotal.toString());
        }
        if (desc) {
            q = em.createQuery(q.toString().concat(" desc"));
        }
        if (params != null) {
            for (String pname : params.keySet()) {
                q.setParameter(pname, params.get(pname));
            }
        }
        q.setParameter("jobId", coordJobId);
        q.setFirstResult(offset - 1);
        q.setMaxResults(len);
        return q;
    }

    // Form the where clause to filter by status values
    private Map<String, Object> getWhereClause(StringBuilder sb, Map<Pair<String, FILTER_COMPARATORS>,
        List<Object>> filterMap) {
        Map<String, Object> params = new HashMap<String, Object>();
        int pcnt= 1;
        for (Entry<Pair<String, FILTER_COMPARATORS>, List<Object>> filter : filterMap.entrySet()) {
            String field = filter.getKey().getFist();
            FILTER_COMPARATORS comp = filter.getKey().getSecond();
            String sqlField;
            if (field.equals(OozieClient.FILTER_STATUS)) {
                sqlField = "a.statusStr";
            } else if (field.equals(OozieClient.FILTER_NOMINAL_TIME)) {
                sqlField = "a.nominalTimestamp";
            } else {
                throw new IllegalArgumentException("Invalid filter key " + field);
            }

            sb.append(" and ").append(sqlField).append(" ");
            switch (comp) {
            case EQUALS:
                sb.append("IN (");
                params.putAll(appendParams(sb, filter.getValue(), pcnt));
                sb.append(")");
                break;

            case NOT_EQUALS:
                sb.append("NOT IN (");
                params.putAll(appendParams(sb, filter.getValue(), pcnt));
                sb.append(")");
                break;

            case GREATER:
            case GREATER_EQUAL:
            case LESSTHAN:
            case LESSTHAN_EQUAL:
                if (filter.getValue().size() != 1) {
                    throw new IllegalArgumentException(field + comp.getSign() + " can't have more than 1 values");
                }

                sb.append(comp.getSign()).append(" ");
                params.putAll(appendParams(sb, filter.getValue(), pcnt));
                break;
            }

            pcnt += filter.getValue().size();
        }
        sb.append(" ");
        return params;
    }

    private Map<String, Object> appendParams(StringBuilder sb, List<Object> value, int sindex) {
        Map<String, Object> params = new HashMap<String, Object>();
        boolean first = true;
        for (Object val : value) {
            String pname = "p" + sindex++;
            params.put(pname, val);
            if (!first) {
                sb.append(", ");
            }
            sb.append(':').append(pname);
            first = false;
        }
        return params;
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
            bean.setMissingDependenciesBlob((StringBlob) arr[13]);
        }
        if (arr[14] != null) {
            bean.setPushMissingDependenciesBlob((StringBlob) arr[14]);
        }
        if (arr[15] != null) {
            bean.setTimeOut((Integer) arr[15]);
        }
        return bean;

    }

}
