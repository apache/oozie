/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.executor.jpa;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.ParamChecker;

/**
 * Load the list of CoordinatorAction for a CoordJob and return the list.
 */
public class CoordActionsGetForJobJPAExecutor implements JPAExecutor<List<CoordinatorActionBean>> {

    private String coordJobId = null;
    private int numResults = 50;
    private String executionOrder = "FIFO";

    public CoordActionsGetForJobJPAExecutor(String coordJobId) {
        ParamChecker.notNull(coordJobId, "coordJobId");
        this.coordJobId = coordJobId;
    }

    public CoordActionsGetForJobJPAExecutor(String coordJobId, int numResults, String executionOrder) {
        this(coordJobId);
        ParamChecker.notNull(executionOrder, "executionOrder");
        this.numResults = numResults;
        this.executionOrder = executionOrder;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "CoordActionsGetForJobJPAExecutor";
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.EntityManager)
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<CoordinatorActionBean> execute(EntityManager em) throws JPAExecutorException {
        List<CoordinatorActionBean> caBeans;
        try {
            Query q;
            // check if executionOrder is FIFO, LIFO, or LAST_ONLY
            if (executionOrder.equalsIgnoreCase("FIFO")) {
                q = em.createNamedQuery("GET_COORD_ACTIONS_FOR_JOB_FIFO");
            }
            else {
                q = em.createNamedQuery("GET_COORD_ACTIONS_FOR_JOB_LIFO");
            }
            q.setParameter("jobId", coordJobId);
            if (executionOrder.equalsIgnoreCase("LAST_ONLY")) {
                q.setMaxResults(1);
            }
            else {
                if (numResults > 0) {
                    q.setMaxResults(numResults);
                }
            }
            caBeans = q.getResultList();
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e);
        }
        return caBeans;
    }
}