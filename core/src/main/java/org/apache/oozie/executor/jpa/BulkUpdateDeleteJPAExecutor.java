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

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.FaultInjection;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.util.ParamChecker;

/**
 * Class for updating and deleting beans in bulk
 */
public class BulkUpdateDeleteJPAExecutor implements JPAExecutor<Void> {

    private Collection<JsonBean> updateList;
    private Collection<JsonBean> deleteList;
    private boolean forRerun = true;

    /**
     * Initialize the JPAExecutor using the update and delete list of JSON beans
     * @param deleteList
     * @param updateList
     */
    public BulkUpdateDeleteJPAExecutor(Collection<JsonBean> updateList, Collection<JsonBean> deleteList,
            boolean forRerun) {
        this.updateList = updateList;
        this.deleteList = deleteList;
        this.forRerun = forRerun;
    }

    public BulkUpdateDeleteJPAExecutor() {
    }

    /**
     * Sets the update list for JSON bean
     *
     * @param updateList
     */
    public void setUpdateList(Collection<JsonBean> updateList) {
        this.updateList = updateList;
    }

    /**
     * Sets the delete list for JSON bean
     *
     * @param deleteList
     */
    public void setDeleteList(Collection<JsonBean> deleteList) {
        this.deleteList = deleteList;
    }

    /**
     * Sets whether for RerunX command or no. Else it'd be for ChangeX
     *
     * @param forRerun
     */
    public void setForRerun(boolean forRerun) {
        this.forRerun = forRerun;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "BulkUpdateDeleteJPAExecutor";
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.
     * EntityManager)
     */
    @Override
    public Void execute(EntityManager em) throws JPAExecutorException {
        try {
            if (updateList != null) {
                for (JsonBean entity : updateList) {
                    ParamChecker.notNull(entity, "JsonBean");
                    em.merge(entity);
                }
            }
            // Only used by test cases to check for rollback of transaction
            FaultInjection.activate("org.apache.oozie.command.SkipCommitFaultInjection");
            if (deleteList != null) {
                for (JsonBean entity : deleteList) {
                    ParamChecker.notNull(entity, "JsonBean");
                    if (forRerun) {
                        em.remove(em.merge(entity));
                    }
                    else {
                        Query g = em.createNamedQuery("DELETE_UNSCHEDULED_ACTION");
                        String coordActionId = ((CoordinatorActionBean) entity).getId();
                        g.setParameter("id", coordActionId);
                        int actionsDeleted = g.executeUpdate();
                        if (actionsDeleted == 0)
                            throw new JPAExecutorException(ErrorCode.E1022, coordActionId);
                    }
                }
            }
            return null;
        }
        catch (JPAExecutorException je) {
            throw je;
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e);
        }
    }
}
