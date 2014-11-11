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

import org.apache.oozie.ErrorCode;
import org.apache.oozie.FaultInjection;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.Collection;

/**
 * Delete WF job, its list of actions and return the number of actions that were deleted.
 */
public class WorkflowJobsDeleteJPAExecutor implements JPAExecutor<Integer> {

    private Collection<String> deleteList;

    /**
     * Initialize the JPAExecutor using the delete list of WorkflowJobBeans
     * @param deleteList
     */
    public WorkflowJobsDeleteJPAExecutor(Collection<String> deleteList) {
        this.deleteList = deleteList;
    }

    public WorkflowJobsDeleteJPAExecutor() {
    }

    /**
     * Sets the delete list for WorkflowJobBeans
     *
     * @param deleteList
     */
    public void setDeleteList(Collection<String> deleteList) {
        this.deleteList = deleteList;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "WorkflowJobsDeleteJPAExecutor";
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.
     * EntityManager)
     */
    @Override
    public Integer execute(EntityManager em) throws JPAExecutorException {
        int actionsDeleted = 0;
        try {
            // Only used by test cases to check for rollback of transaction
            FaultInjection.activate("org.apache.oozie.command.SkipCommitFaultInjection");
            if (deleteList != null && !deleteList.isEmpty()) {
                // Delete the WF job
                Query q = em.createNamedQuery("DELETE_WORKFLOW");
                q.setParameter("id", deleteList);
                q.executeUpdate();
                // Delete the actions for this WF job
                Query g = em.createNamedQuery("DELETE_ACTIONS_FOR_WORKFLOW");
                g.setParameter("wfId", deleteList);
                actionsDeleted = g.executeUpdate();
            }
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
        return actionsDeleted;
    }
}
