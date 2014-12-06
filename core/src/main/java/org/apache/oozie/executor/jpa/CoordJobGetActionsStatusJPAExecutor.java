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

import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.util.ParamChecker;

/**
 * Get the status of Coordinator actions for a given Coordinator job
 */
public class CoordJobGetActionsStatusJPAExecutor implements JPAExecutor<List<CoordinatorAction.Status>> {

    private String coordJobId = null;

    public CoordJobGetActionsStatusJPAExecutor(String coordJobId) {
        ParamChecker.notNull(coordJobId, "coordJobId");
        this.coordJobId = coordJobId;
    }

    @Override
    public String getName() {
        return "CoordJobGetActionsStatusJPAExecutor";
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<CoordinatorAction.Status> execute(EntityManager em) throws JPAExecutorException {
        try {
            Query q = em.createNamedQuery("GET_COORD_ACTIONS_STATUS_UNIGNORED");
            q.setParameter("jobId", coordJobId);
            List<String> coordStatusResultList = q.getResultList();
            List<CoordinatorAction.Status> coordStatus = new ArrayList<CoordinatorAction.Status>();
            for (String a : coordStatusResultList) {
                coordStatus.add(CoordinatorAction.Status.valueOf(a));
            }
            return coordStatus;
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
    }
}
