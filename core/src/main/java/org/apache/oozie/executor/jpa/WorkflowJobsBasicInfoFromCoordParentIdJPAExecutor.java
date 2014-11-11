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
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.util.DateUtils;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class WorkflowJobsBasicInfoFromCoordParentIdJPAExecutor  implements JPAExecutor<List<WorkflowJobBean>> {
    private String parentId;
    private int limit;
    private int offset;

    public WorkflowJobsBasicInfoFromCoordParentIdJPAExecutor(String parentId, int limit) {
        this(parentId, 0, limit);
    }

    public WorkflowJobsBasicInfoFromCoordParentIdJPAExecutor(String parentId, int offset, int limit) {
        this.parentId = parentId;
        this.offset = offset;
        this.limit = limit;
    }

    @Override
    public String getName() {
        return "WorkflowJobsBasicInfoFromCoordParentIdJPAExecutor";
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<WorkflowJobBean> execute(EntityManager em) throws JPAExecutorException {
        try {
            Query jobQ = em.createNamedQuery("GET_WORKFLOWS_BASIC_INFO_BY_COORD_PARENT_ID");
            jobQ.setParameter("parentId", parentId + "%");  // The '%' is the wildcard
            jobQ.setMaxResults(limit);
            jobQ.setFirstResult(offset);
            return getBeanFromArray(jobQ.getResultList());
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
    }

    private List<WorkflowJobBean> getBeanFromArray(List resultList) {
        List<WorkflowJobBean> wfActionBeanList = new ArrayList<WorkflowJobBean>();
        for (Object element : resultList) {
            WorkflowJobBean wfBean = new WorkflowJobBean();
            Object[] arr = (Object[])element;
            if(arr[0] != null) {
                wfBean.setId((String) arr[0]);
            }
            if(arr[1] != null) {
                wfBean.setStatus(WorkflowJob.Status.valueOf((String) arr[1]));
            }
            if(arr[2] != null) {
                wfBean.setEndTime(DateUtils.toDate((Timestamp) arr[2]));
            }
            wfActionBeanList.add(wfBean);
        }
        return wfActionBeanList;
    }
}
