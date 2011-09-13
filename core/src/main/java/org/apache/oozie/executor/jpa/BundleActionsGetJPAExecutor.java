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

import org.apache.oozie.BundleActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.ParamChecker;

/**
 * Load the list of BundleAction return it.
 */
public class BundleActionsGetJPAExecutor implements JPAExecutor<List<BundleActionBean>> {

    private String bundleId = null;

    /**
     * The constructor for class {@link BundleActionsGetJPAExecutor}
     *
     * @param bundleId bundle job id
     */
    public BundleActionsGetJPAExecutor(String bundleId) {
        ParamChecker.notNull(bundleId, "bundleId");
        this.bundleId = bundleId;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "BundleActionsGetJPAExecutor";
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.EntityManager)
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<BundleActionBean> execute(EntityManager em) throws JPAExecutorException {
        List<BundleActionBean> baBeans;
        try {
            Query q = em.createNamedQuery("GET_BUNDLE_ACTIONS_FOR_BUNDLE");
            q.setParameter("bundleId", bundleId);
            baBeans = q.getResultList();
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e);
        }

        return baBeans;
    }
}
