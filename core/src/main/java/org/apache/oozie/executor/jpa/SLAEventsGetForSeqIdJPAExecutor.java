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
import org.apache.oozie.SLAEventBean;

/**
 * Load the list of SLAEventBean for a seqId and return the list.
 */
public class SLAEventsGetForSeqIdJPAExecutor implements JPAExecutor<List<SLAEventBean>> {

    private long seqId = 0;
    private int limitLen = 100; // Default
    private long[] lastSeqId;

    public SLAEventsGetForSeqIdJPAExecutor(long seqId, int limitLen, long[] lastSeqId) {
        this.seqId = seqId;
        this.limitLen = limitLen;
        this.lastSeqId = lastSeqId;
        this.lastSeqId[0] = seqId;
    }

    @Override
    public String getName() {
        return "SLAEventsGetForSeqIdJPAExecutor";
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<SLAEventBean> execute(EntityManager em) throws JPAExecutorException {
        List<SLAEventBean> seBeans;
        try {
            Query q = em.createNamedQuery("GET_SLA_EVENT_NEWER_SEQ_LIMITED");
            q.setParameter("id", seqId);
            q.setMaxResults(limitLen);
            seBeans = q.getResultList();
            for (SLAEventBean j : seBeans) {
                lastSeqId[0] = Math.max(lastSeqId[0], j.getEvent_id());
            }
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
        return seBeans;
    }
}
