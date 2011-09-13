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
import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.ParamChecker;

public class CoordActionsGetReadyGroupbyJobIDJPAExecutor implements JPAExecutor<List<String>>{
    private long checkAgeSecs = 0;

    public CoordActionsGetReadyGroupbyJobIDJPAExecutor(final long checkAgeSecs) {
        ParamChecker.notNull(checkAgeSecs, "checkAgeSecs");
        this.checkAgeSecs = checkAgeSecs;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "CoordActionsGetReadyGroupbyJobIDJPAExecutor";
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.EntityManager)
     */
    @Override
    public List<String> execute(EntityManager em) throws JPAExecutorException {
        List<String> jobids = new ArrayList<String>();
        try {
            Query q = em.createNamedQuery("GET_READY_ACTIONS_GROUP_BY_JOBID");
            Timestamp ts = new Timestamp(System.currentTimeMillis() - checkAgeSecs * 1000);
            q.setParameter(1, ts);
            List<Object[]> list = q.getResultList();

            for (Object[] arr : list) {
                if (arr != null && arr[0] != null) {
                    jobids.add((String) arr[0]);
                }
            }

            return jobids;
        }
        catch (IllegalStateException e) {
            throw new JPAExecutorException(ErrorCode.E0601, e.getMessage(), e);
        }
    }

}
