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
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ParamChecker;

/**
 * Load the CoordinatorAction into a Bean and return it.
 */
public class CoordActionGetForInfoJPAExecutor implements JPAExecutor<CoordinatorActionBean> {

    private String coordActionId = null;
    public static final String COORD_GET_ALL_COLS_FOR_ACTION = "oozie.coord.action.get.all.attributes";

    public CoordActionGetForInfoJPAExecutor(String coordActionId) {
        ParamChecker.notNull(coordActionId, "coordActionId");
        this.coordActionId = coordActionId;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "CoordActionGetForInfoJPAExecutor";
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.EntityManager)
     */
    @Override
    @SuppressWarnings("unchecked")
    public CoordinatorActionBean execute(EntityManager em) throws JPAExecutorException {
        // Maintain backward compatibility for action info cmd
        if (!(Services.get().getConf().getBoolean(COORD_GET_ALL_COLS_FOR_ACTION, false))) {
            List<Object[]> actionObjects;
            try {
                Query q = em.createNamedQuery("GET_COORD_ACTION_FOR_INFO");
                q.setParameter("id", coordActionId);
                actionObjects = q.getResultList();
            }
            catch (Exception e) {
                throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
            }

            if (actionObjects != null && actionObjects.size() > 0) {
                CoordinatorActionBean bean = getBeanForRunningCoordAction(actionObjects.get(0));
                return bean;
            }
            else {
                throw new JPAExecutorException(ErrorCode.E0605, coordActionId);
            }
        } else {
            List<CoordinatorActionBean> caBeans;
            try {
                Query q = em.createNamedQuery("GET_COORD_ACTION");
                q.setParameter("id", coordActionId);
                caBeans = q.getResultList();
            }
            catch (Exception e) {
                throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
            }

            if (caBeans != null && caBeans.size() > 0) {
                CoordinatorActionBean bean = getBeanForCoordAction(caBeans.get(0));
                return bean;
            }
            else {
                throw new JPAExecutorException(ErrorCode.E0605, coordActionId);
            }

        }
    }


    private CoordinatorActionBean getBeanForCoordAction(CoordinatorActionBean a){
        if (a != null) {
            CoordinatorActionBean action = new CoordinatorActionBean();
            action.setId(a.getId());
            action.setActionNumber(a.getActionNumber());
            action.setActionXml(a.getActionXml());
            action.setConsoleUrl(a.getConsoleUrl());
            action.setCreatedConf(a.getCreatedConf());
            action.setExternalStatus(a.getExternalStatus());
            action.setMissingDependencies(a.getMissingDependencies());
            action.setRunConf(a.getRunConf());
            action.setTimeOut(a.getTimeOut());
            action.setTrackerUri(a.getTrackerUri());
            action.setType(a.getType());
            action.setCreatedTime(a.getCreatedTime());
            action.setExternalId(a.getExternalId());
            action.setJobId(a.getJobId());
            action.setLastModifiedTime(a.getLastModifiedTime());
            action.setNominalTime(a.getNominalTime());
            action.setSlaXml(a.getSlaXml());
            action.setStatus(a.getStatus());
            action.setPending(a.getPending());
            action.setRerunTime(a.getRerunTime());
            return action;
        }
        return null;
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
            bean.setActionNumber((Integer) arr[2]);
        }
        if (arr[3] != null) {
            bean.setConsoleUrl((String) arr[3]);
        }
        if (arr[4] != null) {
            bean.setErrorCode((String) arr[4]);
        }
        if (arr[5] != null) {
            bean.setErrorMessage((String) arr[5]);
        }
        if (arr[6] != null) {
            bean.setExternalId((String) arr[6]);
        }
        if (arr[7] != null) {
            bean.setExternalStatus((String) arr[7]);
        }
        if (arr[8] != null) {
            bean.setTrackerUri((String) arr[8]);
        }
        if (arr[9] != null) {
            bean.setCreatedTime(DateUtils.toDate((Timestamp) arr[9]));
        }
        if (arr[10] != null) {
            bean.setNominalTime(DateUtils.toDate((Timestamp) arr[10]));
        }
        if (arr[11] != null) {
            bean.setStatus(CoordinatorAction.Status.valueOf((String) arr[11]));
        }
        if (arr[12] != null) {
            bean.setLastModifiedTime(DateUtils.toDate((Timestamp) arr[12]));
        }
        if (arr[13] != null) {
            bean.setMissingDependencies((String) arr[13]);
        }
        return bean;
    }
}
