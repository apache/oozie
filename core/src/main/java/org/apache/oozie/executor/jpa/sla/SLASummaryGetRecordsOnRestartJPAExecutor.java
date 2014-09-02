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
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.executor.jpa.JPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.sla.SLASummaryBean;

/**
 * Load the list of SLASummaryBean when Oozie restarts and return the list.
 */
public class SLASummaryGetRecordsOnRestartJPAExecutor implements JPAExecutor<List<SLASummaryBean>> {

    private int days;
    public SLASummaryGetRecordsOnRestartJPAExecutor (int days) {
        this.days = days;
    }

    @Override
    public String getName() {
        return "SLASummaryGetRecordsOnRestartJPAExecutor";
    }

    @SuppressWarnings("unchecked")
	@Override
    public List<SLASummaryBean> execute(EntityManager em) throws JPAExecutorException {
        List<SLASummaryBean> ssBean;
        try {
            Query q = em.createNamedQuery("GET_SLA_SUMMARY_RECORDS_RESTART");
            Timestamp ts = new Timestamp(System.currentTimeMillis() - days * 24 * 60 * 60 * 1000);
            q.setParameter("lastModifiedTime", ts);
            ssBean = q.getResultList();
            return ssBean;
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
    }

}
