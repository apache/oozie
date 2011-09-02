/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.executor.jpa;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.ParamChecker;

/**
 * Load the list of CoordinatorAction subset for a CoordJob and return the list.
 */
public class CoordActionsSubsetGetForJobJPAExecutor implements JPAExecutor<List<CoordinatorActionBean>> {

    private String coordJobId = null;
    private int start = 1;
    private int len = 50;

    public CoordActionsSubsetGetForJobJPAExecutor(String coordJobId) {
        ParamChecker.notNull(coordJobId, "coordJobId");
        this.coordJobId = coordJobId;
    }

    public CoordActionsSubsetGetForJobJPAExecutor(String coordJobId, int start, int len) {
        this(coordJobId);
        this.start = start;
        this.len = len;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "CoordActionsSubsetGetForJobJPAExecutor";
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.EntityManager)
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<CoordinatorActionBean> execute(EntityManager em) throws JPAExecutorException {
        List<CoordinatorActionBean> actions;
        List<CoordinatorActionBean> actionList = new ArrayList<CoordinatorActionBean>();
        try {
            Query q = em.createNamedQuery("GET_ACTIONS_FOR_COORD_JOB");
            q.setParameter("jobId", coordJobId);
            q.setFirstResult(start - 1);
            q.setMaxResults(len);
            actions = q.getResultList();
            for (CoordinatorActionBean a : actions) {
                CoordinatorActionBean aa = getBeanForRunningCoordAction(a);
                actionList.add(aa);
            }
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e);
        }
        return actionList;
    }

    private CoordinatorActionBean getBeanForRunningCoordAction(CoordinatorActionBean a) {
        if (a != null) {
            CoordinatorActionBean action = new CoordinatorActionBean();
            action.setId(a.getId());
            action.setActionNumber(a.getActionNumber());
            action.setActionXml(a.getActionXml());
            action.setConsoleUrl(a.getConsoleUrl());
            action.setCreatedConf(a.getCreatedConf());
            // action.setErrorCode(a.getErrorCode());
            // action.setErrorMessage(a.getErrorMessage());
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
            return action;
        }
        return null;
    }

}