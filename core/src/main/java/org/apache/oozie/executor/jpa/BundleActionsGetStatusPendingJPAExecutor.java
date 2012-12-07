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

import org.apache.oozie.BundleActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.Job;
import org.apache.oozie.util.ParamChecker;

/**
 * Load the list of BundleAction return it.
 */
public class BundleActionsGetStatusPendingJPAExecutor implements JPAExecutor<List<BundleActionBean>> {

    private String bundleId = null;

    /**
     * The constructor for class {@link BundleActionsGetStatusPendingJPAExecutor}
     *
     * @param bundleId bundle job id
     */
    public BundleActionsGetStatusPendingJPAExecutor(String bundleId) {
        ParamChecker.notNull(bundleId, "bundleId");
        this.bundleId = bundleId;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "BundleActionsGetStatusPendingJPAExecutor";
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.EntityManager)
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<BundleActionBean> execute(EntityManager em) throws JPAExecutorException {
        List<BundleActionBean> baBeans = new ArrayList<BundleActionBean>();
        try {
            Query q = em.createNamedQuery("GET_BUNDLE_ACTION_STATUS_PENDING_FOR_BUNDLE");
            q.setParameter("bundleId", bundleId);
            List<Object[]> bundleActionList = q.getResultList();
            for (Object[] a : bundleActionList) {
                BundleActionBean bab = createBeanFromBundle(a);
                baBeans.add(bab);
            }
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }

        return baBeans;
    }

    private BundleActionBean createBeanFromBundle(Object[] arr) {
        BundleActionBean bab = new BundleActionBean();
        if (arr[0] != null) {
            bab.setCoordId((String) arr[0]);
        }
        if (arr[1] != null) {
            bab.setStatus(Job.Status.valueOf((String) arr[1]));
        }
        if (arr[2] != null) {
            bab.setPending((Integer) arr[2]);
        }

        return bab;
    }

}
