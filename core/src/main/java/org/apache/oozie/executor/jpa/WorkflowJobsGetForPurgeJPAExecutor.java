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
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowJobBean;

/**
 * Load the list of completed WorkflowJob for purge ready.
 */
public class WorkflowJobsGetForPurgeJPAExecutor implements JPAExecutor<List<WorkflowJobBean>> {

    private static final long DAY_IN_MS = 24 * 60 * 60 * 1000;
    private long olderThanDays;
    private int limit;

    public WorkflowJobsGetForPurgeJPAExecutor(long olderThanDays, int limit) {
        this.olderThanDays = olderThanDays;
        this.limit = limit;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "WorkflowJobsGetForPurgeJPAExecutor";
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.EntityManager)
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<WorkflowJobBean> execute(EntityManager em) throws JPAExecutorException {
        List<WorkflowJobBean> workflows = null;
        try {
            Timestamp maxEndTime = new Timestamp(System.currentTimeMillis() - (olderThanDays * DAY_IN_MS));
            Query jobQ = em.createNamedQuery("GET_COMPLETED_WORKFLOWS_OLDER_THAN");
            jobQ.setParameter("endTime", maxEndTime);
            jobQ.setMaxResults(limit);
            workflows = jobQ.getResultList();
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
        return workflows;
    }

}
