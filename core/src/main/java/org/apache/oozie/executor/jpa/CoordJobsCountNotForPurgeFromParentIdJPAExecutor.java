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

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;

/**
 * Count the number of Coordinator children of a parent bundle that are not ready to be purged
 */
public class CoordJobsCountNotForPurgeFromParentIdJPAExecutor implements JPAExecutor<Long> {

    private static final long DAY_IN_MS = 24 * 60 * 60 * 1000;
    private long olderThanDays;
    private String parentId;

    public CoordJobsCountNotForPurgeFromParentIdJPAExecutor(long olderThanDays, String parentId) {
        this.olderThanDays = olderThanDays;
        this.parentId = parentId;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "CoordJobsCountNotForPurgeFromParentIdJPAExecutor";
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.EntityManager)
     */
    @Override
    @SuppressWarnings("unchecked")
    public Long execute(EntityManager em) throws JPAExecutorException {
        Long count = 0L;
        try {
            Timestamp lastModTm = new Timestamp(System.currentTimeMillis() - (olderThanDays * DAY_IN_MS));
            Query jobQ = em.createNamedQuery("GET_COORD_COUNT_WITH_PARENT_ID_NOT_READY_FOR_PURGE");
            jobQ.setParameter("parentId", parentId);
            jobQ.setParameter("lastModTime", lastModTm);
            count = (Long) jobQ.getSingleResult();
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
        return count;
    }

}
