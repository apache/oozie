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
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.util.ParamChecker;

/**
 * Load workflow actions for a workflow job.
 */
public class WorkflowJobGetActionsJPAExecutor implements JPAExecutor<List<WorkflowActionBean>> {

    private String wfJobId = null;

    public WorkflowJobGetActionsJPAExecutor(String wfJobId) {
        ParamChecker.notNull(wfJobId, "wfJobId");
        this.wfJobId = wfJobId;
    }

    @Override
    public String getName() {
        return "WorkflowJobGetActionsJPAExecutor";
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<WorkflowActionBean> execute(EntityManager em) throws JPAExecutorException {
        List<WorkflowActionBean> actions;
        try {
            Query q = em.createNamedQuery("GET_ACTIONS_FOR_WORKFLOW");
            q.setParameter("wfId", wfJobId);
            actions = q.getResultList();

        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
        return actions;
    }
}
