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

import java.util.Date;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.ParamChecker;

/**
 * Updates the action status, pending status, last modified time, actionXml and missingDependencies of CoordinatorAction and persists it.
 * It executes SQL update query and return type is Void.
 */
public class CoordActionUpdateForInputCheckJPAExecutor implements JPAExecutor<Void> {

    private CoordinatorActionBean coordAction = null;

    public CoordActionUpdateForInputCheckJPAExecutor(CoordinatorActionBean coordAction) {
        ParamChecker.notNull(coordAction, "coordAction");
        this.coordAction = coordAction;
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
            Query q = em.createNamedQuery("UPDATE_COORD_ACTION_FOR_INPUTCHECK");
            q.setParameter("id", coordAction.getId());
            q.setParameter("status", coordAction.getStatus().toString());
            q.setParameter("lastModifiedTime", new Date());
            q.setParameter("actionXml", coordAction.getActionXml());
            q.setParameter("missingDependencies", coordAction.getMissingDependencies());
            q.executeUpdate();
            // Since the return type is Void, we have to return null
            return null;
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "CoordActionUpdateForInputCheckJPAExecutor";
    }
}
