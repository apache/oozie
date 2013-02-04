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

import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.ErrorCode;

/**
 * Get a list of unpaused bundle Jobs;
 */
public class BundleJobsGetUnpausedJPAExecutor implements JPAExecutor<List<BundleJobBean>> {
    private int limit;

    public BundleJobsGetUnpausedJPAExecutor(int limit) {
        this.limit = limit;
    }

    @Override
    public String getName() {
        return "BundleJobsGetUnpausedJPAExecutor";
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<BundleJobBean> execute(EntityManager em) throws JPAExecutorException {
        List<BundleJobBean> bjBeans;
        List<BundleJobBean> jobList = new ArrayList<BundleJobBean>();
        try {
            Query q = em.createNamedQuery("GET_BUNDLE_JOBS_UNPAUSED");
            if (limit > 0) {
                q.setMaxResults(limit);
            }
            bjBeans = q.getResultList();
            for (BundleJobBean j : bjBeans) {
                jobList.add(j);
            }
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
        return jobList;
    }
}
