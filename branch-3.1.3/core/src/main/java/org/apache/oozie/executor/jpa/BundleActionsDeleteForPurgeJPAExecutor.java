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

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.ParamChecker;

/**
 * Delete the list of BundleAction for a BundleJob and return the number of actions been deleted.
 */
public class BundleActionsDeleteForPurgeJPAExecutor implements JPAExecutor<Integer> {

    private String bundleId = null;

    public BundleActionsDeleteForPurgeJPAExecutor(String bundleId) {
        ParamChecker.notNull(bundleId, "bundleId");
        this.bundleId = bundleId;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "BundleActionsDeleteForPurgeJPAExecutor";
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.EntityManager)
     */
    @Override
    public Integer execute(EntityManager em) throws JPAExecutorException {
        int actionsDeleted = 0;
        try {
            Query g = em.createNamedQuery("DELETE_COMPLETED_ACTIONS_FOR_BUNDLE");
            g.setParameter("bundleId", bundleId);
            actionsDeleted = g.executeUpdate();
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e);
        }
        return actionsDeleted;
    }
}
