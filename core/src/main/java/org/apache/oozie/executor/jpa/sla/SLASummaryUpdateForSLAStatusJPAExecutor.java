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
 * Update the slaStatus, eventStatus, eventProcessed and lastModifiedTime of SLAsummaryBean
 */
public class SLASummaryUpdateForSLAStatusJPAExecutor implements JPAExecutor<Void> {

    private SLASummaryBean slaSummaryBean = null;

    public SLASummaryUpdateForSLAStatusJPAExecutor(SLASummaryBean slaSummaryBean) {
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
            Query q = em.createNamedQuery("UPDATE_SLA_SUMMARY_FOR_SLA_STATUS");

            q.setParameter("jobId", slaSummaryBean.getJobId());
            q.setParameter("slaStatus", slaSummaryBean.getSLAStatusString());
            q.setParameter("lastModifiedTS", new Date());
            q.setParameter("eventStatus", slaSummaryBean.getEventStatusString());
            q.setParameter("eventProcessed", slaSummaryBean.getEventProcessed());
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
        return "SLASummaryUpdateForSLAStatusJPAExecutor";
    }
}
