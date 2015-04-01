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
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.event.SLAEvent;
import org.apache.oozie.client.event.SLAEvent.EventStatus;
import org.apache.oozie.client.event.SLAEvent.SLAStatus;
import org.apache.oozie.executor.jpa.JPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.sla.SLASummaryBean;
import org.apache.oozie.util.XLog;

/**
 * Load the list of SLASummaryBean (for dashboard) and return the list.
 */
public class SLASummaryGetForFilterJPAExecutor implements JPAExecutor<List<SLASummaryBean>> {

    private static final String selectStr = "SELECT OBJECT(s) FROM SLASummaryBean s WHERE ";
    private static final String bundleIdQuery = "SELECT a.coordId FROM BundleActionBean a WHERE a.bundleId=:bundleId";
    private static final String bundleNameQuery = "SELECT a.coordId FROM BundleActionBean a WHERE a.bundleId in "
            + "(SELECT b.id from BundleJobBean b WHERE b.appName=:appName)";
    private SLASummaryFilter filter;
    private int numMaxResults;
    private XLog LOG = XLog.getLog(getClass());


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
        List<SLASummaryBean> ssBean = null;
        StringBuilder sb = new StringBuilder(selectStr);
        Map<String, Object> queryParams = new LinkedHashMap<String, Object>();
        boolean firstCondition = true;
        boolean jobExists = true;
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
        if (filter.getBundleId() != null || filter.getBundleName() != null) {
            firstCondition = false;
            Query bq;
            List<Object> returnList;
            try {
                if (filter.getBundleId() != null) {
                    bq = em.createQuery(bundleIdQuery);
                    bq.setParameter("bundleId", filter.getBundleId());
                }
                else {
                    bq = em.createQuery(bundleNameQuery);
                    bq.setParameter("appName", filter.getBundleName());
                }
                bq.setMaxResults(numMaxResults);
                returnList = (List<Object>) bq.getResultList();
            }
            catch (Exception e) {
                throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
            }
            StringBuilder sub = null;
            int ind = 0;
            if(returnList.size() == 0) {
                jobExists = false;
            }
            for (Object obj : returnList) {
                String coordId = (String) obj;
                if (sub == null) {
                    sub = new StringBuilder();
                    sub.append("s.parentId in (:parentId").append(ind);
                }
                else {
                    sub.append(",:parentId").append(ind);
                }
                queryParams.put("parentId" + ind, coordId);
                ind++;
            }
            if(sub != null) {
                sub.append(")");
                sb.append(sub.toString());
            }
        }
        if (filter.getAppName() != null) {
            if (firstCondition ){
                firstCondition = false;
            } else {
                sb.append(" AND ");
            }
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

        if (filter.getEventStatus() != null) {
            processEventStatusFilter(filter, queryParams,sb,firstCondition);
        }

        if (filter.getSLAStatus() != null) {
            StringBuilder sub = null;
            int ind = 0;
            if (firstCondition) {
                firstCondition = false;
            }
            else {
                sb.append(" AND ");
            }
            for (SLAStatus status : filter.getSLAStatus()) {
                if (sub == null) {
                    sub = new StringBuilder();
                    sub.append("s.slaStatus in (:slaStatus").append(ind);
                }
                else {
                    sub.append(",:slaStatus").append(ind);
                }
                queryParams.put("slaStatus" + ind, status.toString());
                ind++;
            }
            if(sub != null) {
                sub.append(")");
                sb.append(sub.toString());
            }
        }

        if (jobExists) {
            sb.append(" ORDER BY s.nominalTimeTS");
            LOG.debug("Query String: " + sb.toString());
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
        }
        return ssBean;
    }

    private void processEventStatusFilter(SLASummaryFilter filter, Map<String, Object> queryParams, StringBuilder sb,
            boolean firstCondition) {
        if (firstCondition) {
            firstCondition = false;
        }
        else {
            sb.append(" AND ");
        }
        List<EventStatus> eventStatusList = filter.getEventStatus();
        int ind = 0;
        Timestamp currentTime = new Timestamp(new Date().getTime());
        for (EventStatus status : eventStatusList) {
            if (ind > 0) {
                sb.append(" OR ");
            }
            if (status.equals(EventStatus.START_MET)) {
                sb.append("(s.expectedStartTS IS NOT NULL AND s.actualStartTS IS NOT NULL ").append(
                        " AND s.expectedStartTS >= s.actualStartTS)");
            }
            else if (status.equals(EventStatus.START_MISS)) {
                sb.append("((s.expectedStartTS IS NOT NULL AND s.actualStartTS IS NOT NULL ")
                        .append(" AND s.expectedStartTS <= s.actualStartTS) ")
                        .append("OR (s.expectedStartTS IS NOT NULL AND s.actualStartTS IS NULL ")
                        .append(" AND s.expectedStartTS <= :currentTimeStamp))");
                queryParams.put("currentTimeStamp",currentTime);
            }
            else if (status.equals(EventStatus.DURATION_MET)) {
                sb.append("(s.expectedDuration <> -1 AND s.actualDuration <> -1 ").append(
                        " AND s.expectedDuration >= s.actualDuration) ");
            }

            else if (status.equals(EventStatus.DURATION_MISS)) {
                sb.append("((s.expectedDuration <> -1 AND s.actualDuration <> -1 ")
                        .append("AND s.expectedDuration < s.actualDuration) ")
                        .append("OR s.eventStatus = 'DURATION_MISS')");
            }
            else if (status.equals(EventStatus.END_MET)) {
                sb.append("(s.expectedEndTS IS NOT NULL AND s.actualEndTS IS NOT NULL ").append(
                        " AND s.expectedEndTS <= s.actualEndTS) ");
            }
            else if (status.equals(EventStatus.END_MISS)) {
                sb.append("((s.expectedEndTS IS NOT NULL AND s.actualEndTS IS NOT NULL ")
                        .append("AND s.expectedEndTS <= s.actualEndTS) ")
                        .append("OR (s.expectedEndTS IS NOT NULL AND s.actualEndTS IS NULL ")
                        .append("AND s.expectedEndTS <= :currentTimeStamp))");
                queryParams.put("currentTimeStamp",currentTime);
            }
            ind++;
        }
    }

    public static class SLASummaryFilter {

        private String appName;
        private String jobId;
        private String parentId;
        private String bundleId;
        private String bundleName;
        private List<SLAEvent.EventStatus> eventStatus;
        private List<SLAEvent.SLAStatus> slaStatus;
        private static String EventStatusSep = ",";
        private static String SLAStatusSep = ",";
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

        public String getBundleId(){
            return this.bundleId;
        }

        public void setBundleId(String bundleId) {
            this.bundleId = bundleId;
        }

        public String getBundleName(){
            return this.bundleName;
        }

        public void setBundleName(String name){
            this.bundleName = name;
        }

        public List<EventStatus> getEventStatus() {
            return this.eventStatus;
        }

        public void setEventStatus(String str) {
            if (this.eventStatus == null) {
                this.eventStatus = new ArrayList<EventStatus>();
            }
            String[] statusArr = str.split(EventStatusSep);
            for (String s : statusArr) {
                this.eventStatus.add(SLAEvent.EventStatus.valueOf(s));
            }
        }

        public List<SLAStatus> getSLAStatus() {
            return this.slaStatus;
        }

        public void setSLAStatus(String str) {
            if (this.slaStatus == null) {
                this.slaStatus = new ArrayList<SLAStatus>();
            }
            String[] statusArr = str.split(SLAStatusSep);
            for (String s : statusArr) {
                this.slaStatus.add(SLAEvent.SLAStatus.valueOf(s));
            }
        }
    }

}
