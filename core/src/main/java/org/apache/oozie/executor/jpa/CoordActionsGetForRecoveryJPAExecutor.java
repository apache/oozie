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
import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.StringBlob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.util.ParamChecker;

public class CoordActionsGetForRecoveryJPAExecutor implements JPAExecutor<List<CoordinatorActionBean>> {

    private long checkAgeSecs = 0;

    public CoordActionsGetForRecoveryJPAExecutor(final long checkAgeSecs) {
        ParamChecker.notNull(checkAgeSecs, "checkAgeSecs");
        this.checkAgeSecs = checkAgeSecs;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "CoordActionsGetForRecoveryJPAExecutor";
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.EntityManager)
     */
    @SuppressWarnings("unchecked")
    @Override
    public List<CoordinatorActionBean> execute(EntityManager em) throws JPAExecutorException {
        List<CoordinatorActionBean> allActions = new ArrayList<CoordinatorActionBean>();

        try {
            Query q = em.createNamedQuery("GET_COORD_ACTIONS_FOR_RECOVERY_OLDER_THAN");
            Timestamp ts = new Timestamp(System.currentTimeMillis() - this.checkAgeSecs * 1000);
            q.setParameter("lastModifiedTime", ts);
            List<Object[]> objectArrList = q.getResultList();
            for (Object[] arr : objectArrList) {
                CoordinatorActionBean caa = getBeanForCoordinatorActionFromArrayForRecovery(arr);
                allActions.add(caa);
            }

            q = em.createNamedQuery("GET_COORD_ACTIONS_WAITING_SUBMITTED_OLDER_THAN");
            q.setParameter("lastModifiedTime", ts);
            objectArrList = q.getResultList();
            for (Object[] arr : objectArrList) {
                CoordinatorActionBean caa = getBeanForCoordinatorActionFromArrayForWaiting(arr);
                allActions.add(caa);
            }

            return allActions;
        }
        catch (IllegalStateException e) {
            throw new JPAExecutorException(ErrorCode.E0601, e.getMessage(), e);
        }
    }

    private CoordinatorActionBean getBeanForCoordinatorActionFromArrayForRecovery(Object[] arr) {
        CoordinatorActionBean bean = new CoordinatorActionBean();
        if (arr[0] != null) {
            bean.setId((String) arr[0]);
        }
        if (arr[1] != null){
            bean.setJobId((String) arr[1]);
        }
        if (arr[2] != null) {
            bean.setStatus(CoordinatorAction.Status.valueOf((String) arr[2]));
        }
        if (arr[3] != null) {
            bean.setExternalId((String) arr[3]);
        }
        if (arr[4] != null) {
            bean.setPending((Integer) arr[4]);
        }
        return bean;
    }


    private CoordinatorActionBean getBeanForCoordinatorActionFromArrayForWaiting(Object[] arr){
        CoordinatorActionBean bean = new CoordinatorActionBean();
        if (arr[0] != null) {
            bean.setId((String) arr[0]);
        }
        if (arr[1] != null){
            bean.setJobId((String) arr[1]);
        }
        if (arr[2] != null) {
            bean.setStatus(CoordinatorAction.Status.valueOf((String) arr[2]));
        }
        if (arr[3] != null) {
            bean.setExternalId((String) arr[3]);
        }
        if (arr[4] != null) {
            bean.setPushMissingDependenciesBlob((StringBlob) arr[4]);
        }
        return bean;
    }

}
