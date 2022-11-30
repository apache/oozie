/*
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
 * Load the list of WorkflowAction for a WorkflowJob and return the list.
 */
public class WorkflowActionsFailedOutsideOfProvidedActionGetForJobJPAExecutor implements JPAExecutor<List<WorkflowActionBean>> {

    private String wfJobId = null;
    private String wfActionId = null;

    public WorkflowActionsFailedOutsideOfProvidedActionGetForJobJPAExecutor(String wfJobId, String wfActionId) {
        ParamChecker.notEmpty(wfJobId, "wfJobId");
        ParamChecker.notEmpty(wfActionId, "wfActionId");
        this.wfJobId = wfJobId;
        this.wfActionId = wfActionId;
    }

    @Override
    public String getName() {
        return "WorkflowActionsFailedOutsideOfProvidedActionGetForJobJPAExecutor";
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<WorkflowActionBean> execute(EntityManager em) throws JPAExecutorException {
        List<WorkflowActionBean> actions;
        try {
            Query q = em.createNamedQuery("GET_ACTIONS_FAILED_OUTSIDE_OF_PROVIDED_ACTION");
            q.setParameter("wfId", wfJobId);
            q.setParameter("actionId", wfActionId);
            actions = q.getResultList();
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
        return actions;
    }
}
