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

public class CoordJobGetActionRunningCountForRangeJPAExecutor implements JPAExecutor<Long> {

    private String jobId = null;
    private String startAction, endAction;

    public CoordJobGetActionRunningCountForRangeJPAExecutor(String jobId, String startAction, String endAction) {
        ParamChecker.notNull(jobId, "jobId");
        this.jobId = jobId;
        this.startAction = startAction;
        this.endAction = endAction;
    }

    @Override
    public String getName() {
        return "CoordJobGetActionIdsForDateRangeJPAExecutor";
    }

    @Override
    public Long execute(EntityManager em) throws JPAExecutorException {
        try {
            Query q = em.createNamedQuery("GET_COORD_ACTIONS_COUNT_RUNNING_FOR_RANGE");
            q.setParameter("jobId", jobId);
            q.setParameter("startAction", startAction);
            q.setParameter("endAction", endAction);
            Long count = (Long) q.getSingleResult();
            return count;
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
    }

}
