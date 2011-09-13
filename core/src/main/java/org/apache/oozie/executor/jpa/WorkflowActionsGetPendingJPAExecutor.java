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
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.util.ParamChecker;

public class WorkflowActionsGetPendingJPAExecutor implements JPAExecutor<List<WorkflowActionBean>>{
    private long minimumPendingAgeSecs = 0;

    public WorkflowActionsGetPendingJPAExecutor(final long minimumPendingAgeSecs) {
        ParamChecker.notNull(minimumPendingAgeSecs, "minimumPendingAgeSecs");
        this.minimumPendingAgeSecs = minimumPendingAgeSecs;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "WorkflowActionsGetPendingJPAExecutor";
    }

    /**
     * Load All the actions that are pending for more than given time.
     *
     * @param minimumPendingAgeSecs Minimum Pending age in seconds
     * @return List of action beans
     * @throws JPAExecutorException
     */
    @Override
    public List<WorkflowActionBean> execute(EntityManager em) throws JPAExecutorException {
        Timestamp ts = new Timestamp(System.currentTimeMillis() - minimumPendingAgeSecs * 1000);
        List<WorkflowActionBean> actionList = null;
        try {
            Query q = em.createNamedQuery("GET_PENDING_ACTIONS");
            q.setParameter("pendingAge", ts);
            actionList = q.getResultList();
        }
        catch (IllegalStateException e) {
            throw new JPAExecutorException(ErrorCode.E0601, e.getMessage(), e);
        }
        return actionList;
    }
}
