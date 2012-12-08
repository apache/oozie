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
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.util.ParamChecker;

/**
 * Load the WorkflowJob into a Bean and return it.
 */
public class WorkflowJobGetJPAExecutor implements JPAExecutor<WorkflowJobBean> {

    private String wfJobId = null;

    public WorkflowJobGetJPAExecutor(String wfJobId) {
        ParamChecker.notNull(wfJobId, "wfJobId");
        this.wfJobId = wfJobId;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "WorkflowJobGetJPAExecutor";
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.EntityManager)
     */
    @Override
    @SuppressWarnings("unchecked")
    public WorkflowJobBean execute(EntityManager em) throws JPAExecutorException {
        List<WorkflowJobBean> wjBeans;
        try {
            Query q = em.createNamedQuery("GET_WORKFLOW");
            q.setParameter("id", wfJobId);
            wjBeans = q.getResultList();
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
        WorkflowJobBean bean = null;
        if (wjBeans != null && wjBeans.size() > 0) {
            bean = wjBeans.get(0);
            bean.setStatus(bean.getStatus());
            return bean;
        }
        else {
            throw new JPAExecutorException(ErrorCode.E0604, wfJobId);
        }
    }
}
