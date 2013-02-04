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

import java.util.Collection;
import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.FaultInjection;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.util.ParamChecker;

/**
 * Delete job, its list of actions and return the number of
 * actions been deleted.
 */
public class BulkDeleteForPurgeJPAExecutor implements JPAExecutor<Integer> {

    private Collection<JsonBean> deleteList;

    /**
     * Initialize the JPAExecutor using the delete list of JSON beans
     * @param deleteList
     */
    public BulkDeleteForPurgeJPAExecutor(Collection<JsonBean> deleteList) {
        this.deleteList = deleteList;
    }

    public BulkDeleteForPurgeJPAExecutor() {
    }

    /**
     * Sets the delete list for JSON bean
     *
     * @param deleteList
     */
    public void setDeleteList(Collection<JsonBean> deleteList) {
        this.deleteList = deleteList;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "BulkDeleteForPurgeJPAExecutor";
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.
     * EntityManager)
     */
    @Override
    public Integer execute(EntityManager em) throws JPAExecutorException {
        int actionsDeleted = 0;
        try {
            // Only used by test cases to check for rollback of transaction
            FaultInjection.activate("org.apache.oozie.command.SkipCommitFaultInjection");
            if (deleteList != null) {
                for (JsonBean entity : deleteList) {
                    ParamChecker.notNull(entity, "JsonBean");
                    // deleting the job (wf/coord/bundle)
                    em.remove(em.merge(entity));
                    if (entity instanceof WorkflowJobBean) {
                        // deleting the workflow actions for this job
                        Query g = em.createNamedQuery("DELETE_ACTIONS_FOR_WORKFLOW");
                        g.setParameter("wfId", ((WorkflowJobBean) entity).getId());
                        actionsDeleted = g.executeUpdate();
                    }
                    else if (entity instanceof CoordinatorJobBean) {
                        // deleting the coord actions for this job
                        Query g = em.createNamedQuery("DELETE_COMPLETED_ACTIONS_FOR_COORDINATOR");
                        g.setParameter("jobId", ((CoordinatorJobBean) entity).getId());
                        actionsDeleted = g.executeUpdate();
                    }
                    else if (entity instanceof BundleJobBean) {
                        // deleting the bundle actions for this job
                        Query g = em.createNamedQuery("DELETE_COMPLETED_ACTIONS_FOR_BUNDLE");
                        g.setParameter("bundleId", ((BundleJobBean) entity).getId());
                        actionsDeleted = g.executeUpdate();
                    }
                }
            }
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
        return actionsDeleted;
    }
}
