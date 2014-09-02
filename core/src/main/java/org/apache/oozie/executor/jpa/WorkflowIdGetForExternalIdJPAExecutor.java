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

import org.apache.oozie.util.ParamChecker;

/**
 * Get the Workflow ID with given external ID which will be assigned for the subworkflows.
 */
public class WorkflowIdGetForExternalIdJPAExecutor implements JPAExecutor<String> {

    private String externalId = null;

    public WorkflowIdGetForExternalIdJPAExecutor(String externalId) {
        ParamChecker.notNull(externalId, "externalId");
        this.externalId = externalId;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "WorkflowIdGetForExternalIdJPAExecutor";
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.EntityManager)
     */
    @Override
    @SuppressWarnings("unchecked")
    public String execute(EntityManager em) throws JPAExecutorException {
        String id = "";
        Query q = em.createNamedQuery("GET_WORKFLOW_ID_FOR_EXTERNAL_ID");
        q.setParameter("externalId", externalId);
        List<String> w = q.getResultList();
        if (w.size() > 0) {
            int index = w.size() - 1;
            id = w.get(index);
        }
        return id;
    }
}
