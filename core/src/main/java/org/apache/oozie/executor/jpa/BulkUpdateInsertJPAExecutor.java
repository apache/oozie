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
import org.apache.oozie.ErrorCode;
import org.apache.oozie.FaultInjection;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.util.ParamChecker;

/**
 * Class for inserting and updating beans in bulk
 * @param <T>
*/
public class BulkUpdateInsertJPAExecutor implements JPAExecutor<Void> {

    private Collection<JsonBean> updateList;
    private Collection<JsonBean> insertList;

    /**
     * Initialize the JPAExecutor using the insert and update list of JSON beans
     * @param updateList
     * @param insertList
     */
    public BulkUpdateInsertJPAExecutor(Collection<JsonBean> updateList, Collection<JsonBean> insertList) {
        this.updateList = updateList;
        this.insertList = insertList;
    }

    public BulkUpdateInsertJPAExecutor() {
    }

    /**
     * Sets the update list for JSON bean
     * @param updateList
     */
    public void setUpdateList(Collection<JsonBean> updateList) {
        this.updateList = updateList;
    }

    /**
     * Sets the insert list for JSON bean
     * @param insertList
     */
    public void setInsertList(Collection<JsonBean> insertList) {
        this.insertList = insertList;
    }


    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "BulkUpdateInsertJPAExecutor";
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.EntityManager)
     */
    @Override
    public Void execute(EntityManager em) throws JPAExecutorException {
        try {
            if (updateList!= null){
                for (JsonBean entity: updateList){
                    ParamChecker.notNull(entity, "JsonBean");
                    em.merge(entity);
                }
            }
            // Only used by test cases to check for rollback of transaction
            FaultInjection.activate("org.apache.oozie.command.SkipCommitFaultInjection");
            if (insertList!= null){
                for (JsonBean entity: insertList){
                    ParamChecker.notNull(entity, "JsonBean");
                    em.persist(entity);
                }
            }
            return null;
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e);
        }
    }


}
