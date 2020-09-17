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
import org.apache.oozie.WorkflowJobBean;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.List;

public class WorkflowJobsBasicInfoFromWorkflowParentIdJPAExecutor  extends WorkflowJobsBasicInfoFromParentId
        implements JPAExecutor<List<WorkflowJobBean>>
{
    public WorkflowJobsBasicInfoFromWorkflowParentIdJPAExecutor(String parentId, int limit) {
        this(parentId, 0, limit);
    }

    public WorkflowJobsBasicInfoFromWorkflowParentIdJPAExecutor(String parentId, int offset, int limit) {
        super(parentId, offset, limit);
    }

    @Override
    public String getName() {
        return "WorkflowJobsBasicInfoFromWorkflowParentIdJPAExecutor";
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<WorkflowJobBean> execute(EntityManager em) throws JPAExecutorException {
        try {
            Query jobQ = em.createNamedQuery("GET_WORKFLOWS_BASIC_INFO_BY_PARENT_ID");
            jobQ.setParameter("parentId", parentId);
            jobQ.setMaxResults(limit);
            jobQ.setFirstResult(offset);
            return getBeanFromArray(jobQ.getResultList());
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
    }
}
