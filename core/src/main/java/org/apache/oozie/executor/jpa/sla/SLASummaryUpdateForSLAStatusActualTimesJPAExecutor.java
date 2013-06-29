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

import java.util.Date;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.executor.jpa.JPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.sla.SLASummaryBean;
import org.apache.oozie.util.ParamChecker;

/**
 * Update the slaStatus, eventStatus, eventProcessed, jobStatus, actualStart,
 * actualEnd, actualDuration and lastModifiedTime of SLAsummaryBean
 */
public class SLASummaryUpdateForSLAStatusActualTimesJPAExecutor implements JPAExecutor<Void> {

    private SLASummaryBean slaSummaryBean = null;

    public SLASummaryUpdateForSLAStatusActualTimesJPAExecutor(SLASummaryBean slaSummaryBean) {
        ParamChecker.notNull(slaSummaryBean, "slaSummaryBean");
        this.slaSummaryBean = slaSummaryBean;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.
     * EntityManager)
     */
    @Override
    public Void execute(EntityManager em) throws JPAExecutorException {
        try {
            Query q = em.createNamedQuery("UPDATE_SLA_SUMMARY_FOR_STATUS_ACTUAL_TIMES");
            q.setParameter("jobId", slaSummaryBean.getId());
            q.setParameter("slaStatus", slaSummaryBean.getSLAStatusString());
            q.setParameter("lastModifiedTS", new Date());
            q.setParameter("eventStatus", slaSummaryBean.getEventStatusString());
            q.setParameter("jobStatus", slaSummaryBean.getJobStatus());
            q.setParameter("eventProcessed", slaSummaryBean.getEventProcessed());
            q.setParameter("actualStartTS", slaSummaryBean.getActualStart());
            q.setParameter("actualEndTS", slaSummaryBean.getActualEnd());
            q.setParameter("actualDuration", slaSummaryBean.getActualDuration());
            q.executeUpdate();
            return null;
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "SLASummaryUpdateForSLAStatusActualTimesJPAExecutor";
    }
}
