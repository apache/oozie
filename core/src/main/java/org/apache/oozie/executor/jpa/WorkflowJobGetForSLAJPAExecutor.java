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

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ParamChecker;

/**
 * Retrieve the workflow job bean for sla service
 */
public class WorkflowJobGetForSLAJPAExecutor implements JPAExecutor<WorkflowJobBean> {

    private String wfJobId;

    public WorkflowJobGetForSLAJPAExecutor(String wfJobId) {
        ParamChecker.notNull(wfJobId, "wfJobId");
        this.wfJobId = wfJobId;
    }

    @Override
    public String getName() {
        return "WorkflowJobGetForSLAJPAExecutor";
    }

    @Override
    public WorkflowJobBean execute(EntityManager em) throws JPAExecutorException {
        try {
            Query q = em.createNamedQuery("GET_WORKFLOW_FOR_SLA");
            q.setParameter("id", wfJobId);
            Object[] obj = (Object[]) q.getSingleResult();
            return getBeanFromArray(obj);
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
    }

    private WorkflowJobBean getBeanFromArray(Object[] arr) {
        WorkflowJobBean wjb = new WorkflowJobBean();
        if (arr[0] != null) {
            wjb.setId((String) arr[0]);
        }
        if (arr[1] != null) {
            wjb.setStatus(WorkflowJob.Status.valueOf((String) arr[1]));
        }
        if (arr[2] != null) {
            wjb.setStartTime(DateUtils.toDate((Timestamp) arr[2]));
        }
        if (arr[3] != null) {
            wjb.setEndTime(DateUtils.toDate((Timestamp) arr[3]));
        }
        return wjb;
    }
}
