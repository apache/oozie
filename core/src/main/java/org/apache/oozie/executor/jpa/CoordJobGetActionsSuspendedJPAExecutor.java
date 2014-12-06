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
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ParamChecker;

/**
 * Load the suspended Coordinator actions of a given Coordinator job
 */
public class CoordJobGetActionsSuspendedJPAExecutor implements JPAExecutor<List<CoordinatorActionBean>> {

    private String coordJobId = null;

    public CoordJobGetActionsSuspendedJPAExecutor(String coordJobId) {
        ParamChecker.notNull(coordJobId, "coordJobId");
        this.coordJobId = coordJobId;
    }

    @Override
    public String getName() {
        return "CoordJobGetActionsSuspendedJPAExecutor";
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<CoordinatorActionBean> execute(EntityManager em) throws JPAExecutorException {
        try {
            List<CoordinatorActionBean> actionBeansList = new ArrayList<CoordinatorActionBean>();
            Query q = em.createNamedQuery("GET_COORD_ACTIONS_SUSPENDED");
            q.setParameter("jobId", coordJobId);
            List<Object[]> objectArrList = q.getResultList();

            for (Object[] arr : objectArrList) {
                CoordinatorActionBean caa = getBeanForCoordinatorActionFromArray(arr);
                actionBeansList.add(caa);
            }
            return actionBeansList;
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
    }

    /*
     * A Coordinator action bean is constructed from an array of objects.
     * ActionId, status, pending and externalId of the Coordinator action are
     * updated. If values of id, status and externalId fetched from db are
     * null, they will be automatically initialized to default values of null as
     * they are Strings. If value of pending fetched from db is null, it will be
     * initialized with default value of 0 as it is a int.
     */
  private CoordinatorActionBean getBeanForCoordinatorActionFromArray(Object[] arr) {
        CoordinatorActionBean bean = new CoordinatorActionBean();
        if (arr[0] != null) {
            bean.setId((String) arr[0]);
        }
        if (arr[1] != null) {
            bean.setStatus(CoordinatorAction.Status.valueOf((String) arr[1]));
        }
        if (arr[2] != null) {
            bean.setPending((Integer) arr[2]);
        }
        if (arr[3] != null) {
            bean.setExternalId((String) arr[3]);
        }
        if (arr[4] != null){
            bean.setNominalTime(DateUtils.toDate((Timestamp) arr[4]));
        }
        if (arr[5] != null){
            bean.setCreatedTime(DateUtils.toDate((Timestamp) arr[5]));
        }
        return bean;
    }

}
