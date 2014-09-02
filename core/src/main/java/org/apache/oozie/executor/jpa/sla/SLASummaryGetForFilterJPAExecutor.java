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

package org.apache.oozie.executor.jpa.sla;

import java.sql.Timestamp;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.executor.jpa.JPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.sla.SLASummaryBean;

/**
 * Load the list of SLASummaryBean (for dashboard) and return the list.
 */
public class SLASummaryGetForFilterJPAExecutor implements JPAExecutor<List<SLASummaryBean>> {

    private static final String selectStr = "SELECT OBJECT(s) FROM SLASummaryBean s WHERE ";

    private SLASummaryFilter filter;
    private int numMaxResults;


    public SLASummaryGetForFilterJPAExecutor(SLASummaryFilter filter, int numMaxResults) {
        this.filter = filter;
        this.numMaxResults = numMaxResults;
    }

    @Override
    public String getName() {
        return "SLASummaryGetForFilterJPAExecutor";
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<SLASummaryBean> execute(EntityManager em) throws JPAExecutorException {
        List<SLASummaryBean> ssBean;
        StringBuilder sb = new StringBuilder(selectStr);
        Map<String, Object> queryParams = new LinkedHashMap<String, Object>();
        boolean firstCondition = true;
        if (filter.getJobId() != null) {
            firstCondition = false;
            if (filter.getParentId() != null) {
                sb.append("(s.jobId = :jobId OR s.parentId = :parentId)");
                queryParams.put("jobId", filter.getJobId());
                queryParams.put("parentId", filter.getParentId());
            }
            else {
                sb.append("s.jobId = :jobId");
                queryParams.put("jobId", filter.getJobId());
            }
        }
        if (filter.getParentId() != null && filter.getJobId() == null) {
            firstCondition = false;
            sb.append("s.parentId = :parentId");
            queryParams.put("parentId", filter.getParentId());
        }
        if (filter.getAppName() != null && filter.getJobId() == null && filter.getParentId() == null) {
            firstCondition = false;
            sb.append("s.appName = :appName");
            queryParams.put("appName", filter.getAppName());
        }
        if (filter.getNominalStart() != null) {
            if (firstCondition) {
                firstCondition = false;
            }
            else {
                sb.append(" AND ");
            }
            sb.append("s.nominalTimeTS >= :nominalTimeStart");
            queryParams.put("nominalTimeStart", new Timestamp(filter.getNominalStart().getTime()));
        }

        if (filter.getNominalEnd() != null) {
            if (firstCondition) {
                firstCondition = false;
            }
            else {
                sb.append(" AND ");
            }
            sb.append("s.nominalTimeTS <= :nominalTimeEnd");
            queryParams.put("nominalTimeEnd", new Timestamp(filter.getNominalEnd().getTime()));
        }

        sb.append(" ORDER BY s.nominalTimeTS");
        try {
            Query q = em.createQuery(sb.toString());
            for (Map.Entry<String, Object> entry : queryParams.entrySet()) {
                q.setParameter(entry.getKey(), entry.getValue());
            }
            q.setMaxResults(numMaxResults);
            ssBean = (List<SLASummaryBean>) q.getResultList();
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
        return ssBean;
    }

    public static class SLASummaryFilter {

        private String appName;
        private String jobId;
        private String parentId;
        private Date nominalStart;
        private Date nominalEnd;

        public SLASummaryFilter() {
        }

        public String getAppName() {
            return appName;
        }

        public void setAppName(String appName) {
            this.appName = appName;
        }

        public String getJobId() {
            return jobId;
        }

        public void setJobId(String jobId) {
            this.jobId = jobId;
        }

        public String getParentId() {
            return parentId;
        }

        public void setParentId(String parentId) {
            this.parentId = parentId;
        }

        public Date getNominalStart() {
            return nominalStart;
        }

        public void setNominalStart(Date nominalStart) {
            this.nominalStart = nominalStart;
        }

        public Date getNominalEnd() {
            return nominalEnd;
        }

        public void setNominalEnd(Date nominalEnd) {
            this.nominalEnd = nominalEnd;
        }

    }

}
