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

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.List;

public class CoordActionsGetFromCoordJobIdJPAExecutor implements JPAExecutor<List<String>> {

    private String coordId;
    private int limit;
    private int offset;

    public CoordActionsGetFromCoordJobIdJPAExecutor(String coordId, int offset, int limit) {
        this.coordId = coordId;
        this.offset = offset;
        this.limit = limit;
    }

    @Override
    public String getName() {
        return "CoordActionsGetFromCoordJobIdJPAExecutor";
    }

    @Override
    public List<String> execute(EntityManager em) throws JPAExecutorException {
        List<String> actions = null;
        try {
            Query jobQ = em.createNamedQuery("GET_COORD_ACTIONS_FOR_COORDINATOR");
            jobQ.setParameter("jobId", coordId);
            jobQ.setMaxResults(limit);
            jobQ.setFirstResult(offset);
            actions = jobQ.getResultList();
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
        return actions;
    }
}
