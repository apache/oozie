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
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.StringBlob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ParamChecker;

/**
 * JPAExecutor to get attributes of CoordinatorActionBean required by CoordActionCheckCommand
 */
public class CoordActionGetForCheckJPAExecutor implements JPAExecutor<CoordinatorActionBean> {

    private String coordActionId = null;

    public CoordActionGetForCheckJPAExecutor(String coordActionId) {
        ParamChecker.notNull(coordActionId, "coordActionId");
        this.coordActionId = coordActionId;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "CoordActionGetForCheckJPAExecutor";
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.EntityManager)
     */
    @Override
    public CoordinatorActionBean execute(EntityManager em) throws JPAExecutorException {
        try {
            Query q = em.createNamedQuery("GET_COORD_ACTION_FOR_CHECK");
            q.setParameter("id", coordActionId);
            Object[] obj = (Object[]) q.getSingleResult();
            CoordinatorActionBean caBean = getBeanForRunningCoordAction(obj);
            return caBean;
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }

    }

    private CoordinatorActionBean getBeanForRunningCoordAction(Object[] arr) {
        CoordinatorActionBean bean = new CoordinatorActionBean();
        if (arr[0] != null) {
            bean.setId((String) arr[0]);
        }
        if (arr[1] != null) {
            bean.setJobId((String) arr[1]);
        }
        if (arr[2] != null) {
            bean.setStatus(CoordinatorAction.Status.valueOf((String) arr[2]));
        }
        if (arr[3] != null) {
            bean.setPending((Integer) arr[3]);
        }
        if (arr[4] != null) {
            bean.setExternalId((String) arr[4]);
        }
        if (arr[5] != null) {
            bean.setLastModifiedTime(DateUtils.toDate((Timestamp) arr[5]));
        }
        if (arr[6] != null){
            bean.setSlaXmlBlob((StringBlob) arr[6]);
        }
        if (arr[7] != null){
            bean.setNominalTime(DateUtils.toDate((Timestamp) arr[7]));
        }
        if (arr[8] != null){
            bean.setCreatedTime(DateUtils.toDate((Timestamp) arr[8]));
        }
        return bean;
    }
}
