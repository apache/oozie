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

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;

/**
 * Load the list of CoordinatorJob with the passed in parentId
 */
public class CoordJobsGetFromParentIdJPAExecutor implements JPAExecutor<List<String>> {

    private String parentId;
    private int limit;
    private int offset;

    public CoordJobsGetFromParentIdJPAExecutor(String parentId, int limit) {
        this(parentId, 0, limit);
    }

    public CoordJobsGetFromParentIdJPAExecutor(String parentId, int offset, int limit) {
        this.parentId = parentId;
        this.offset = offset;
        this.limit = limit;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "CoordJobsGetFromParentIdJPAExecutor";
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.EntityManager)
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<String> execute(EntityManager em) throws JPAExecutorException {
        List<String> coords = null;
        try {
            Query jobQ = em.createNamedQuery("GET_COORD_JOBS_WITH_PARENT_ID");
            jobQ.setParameter("parentId", parentId);
            jobQ.setMaxResults(limit);
            jobQ.setFirstResult(offset);
            coords = jobQ.getResultList();
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
        return coords;
    }

}
