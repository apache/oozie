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

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.ParamChecker;

/**
 * Load the number of pending actions for a status for a coordinator job.
 */
public class CoordActionsPendingFalseStatusCountGetJPAExecutor implements JPAExecutor<Integer> {

    private String coordJobId = null;
    private String status = null;

    public CoordActionsPendingFalseStatusCountGetJPAExecutor(String coordJobId, String status) {
        ParamChecker.notNull(coordJobId, "coordJobId");
        ParamChecker.notNull(status, "status");
        this.coordJobId = coordJobId;
        this.status = status;
    }

    @Override
    public String getName() {
        return "CoordActionsPendingFalseStatusCountGetJPAExecutor";
    }

    @Override
    public Integer execute(EntityManager em) throws JPAExecutorException {
        try {
            Query q = em.createNamedQuery("GET_COORD_ACTIONS_PENDING_FALSE_STATUS_COUNT");
            q.setParameter("jobId", coordJobId);
            q.setParameter("status", status);
            Long count = (Long) q.getSingleResult();
            return Integer.valueOf(count.intValue());
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
    }

}
