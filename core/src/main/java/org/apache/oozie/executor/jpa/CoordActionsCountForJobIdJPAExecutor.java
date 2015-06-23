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

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.coord.CoordUtils;
import org.apache.oozie.CoordinatorEngine.FILTER_COMPARATORS;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.Pair;
import org.apache.oozie.util.ParamChecker;

import java.util.List;
import java.util.Map;

/**
 * Load the number of running actions for a coordinator job.
 */
public class CoordActionsCountForJobIdJPAExecutor implements JPAExecutor<Integer> {

    private String coordJobId = null;
    private Map<Pair<String, FILTER_COMPARATORS>, List<Object>> filterMap;

    public CoordActionsCountForJobIdJPAExecutor(String coordJobId, Map<Pair<String, FILTER_COMPARATORS>, List<Object>> filterMap) {
        ParamChecker.notNull(coordJobId, "coordJobId");
        this.coordJobId = coordJobId;
        this.filterMap = filterMap;
    }

    @Override
    public String getName() {
        return "CoordActionsCountForJobIdJPAExecutor";
    }

    @Override
    public Integer execute(EntityManager em) throws JPAExecutorException {
        try {
            Query q = em.createNamedQuery("GET_COORD_ACTIONS_COUNT_BY_JOBID");
            q = setQueryParameters(q, em);
            Long count = (Long) q.getSingleResult();
            return Integer.valueOf(count.intValue());
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
    }

    private Query setQueryParameters(Query q, EntityManager em){
        Map<String, Object> params = null;
        if (filterMap != null) {
            // Add the filter clause
            String query = q.toString();
            StringBuilder sbTotal = new StringBuilder(query);
            // Get the 'where' clause for status filters
            StringBuilder statusClause = new StringBuilder();
            params = CoordUtils.getWhereClause(statusClause, filterMap);
            sbTotal.insert(sbTotal.length(), statusClause);
            q = em.createQuery(sbTotal.toString());
        }
        if (params != null) {
            for (String pname : params.keySet()) {
                q.setParameter(pname, params.get(pname));
            }
        }
        q.setParameter("jobId", coordJobId);
        return q;
    }

}
