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
import java.util.Date;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ParamChecker;

/**
 * Load coordinator action by nominal time.
 */
public class CoordJobGetActionForNominalTimeJPAExecutor implements JPAExecutor<CoordinatorActionBean> {

    private String jobId = null;
    private Date nominalTime = null;

    public CoordJobGetActionForNominalTimeJPAExecutor(String jobId, Date nominalTime) {
        ParamChecker.notNull(jobId, "jobId");
        this.jobId = jobId;
        this.nominalTime = nominalTime;
    }

    @Override
    public String getName() {
        return "CoordJobGetActionForNominalTimeJPAExecutor";
    }

    @Override
    @SuppressWarnings("unchecked")
    public CoordinatorActionBean execute(EntityManager em) throws JPAExecutorException {
        List<CoordinatorActionBean> actions;
        Query q = em.createNamedQuery("GET_ACTION_FOR_NOMINALTIME");
        q.setParameter("jobId", jobId);
        q.setParameter("nominalTime", new Timestamp(nominalTime.getTime()));
        actions = q.getResultList();

        CoordinatorActionBean action = null;
        if (actions.size() > 0) {
            action = actions.get(0);
        }
        else {
            throw new JPAExecutorException(ErrorCode.E0605, DateUtils.convertDateToString(nominalTime));
        }
        return getBeanForRunningCoordAction(action);
    }

    private CoordinatorActionBean getBeanForRunningCoordAction(CoordinatorActionBean a) {
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
            return action;
        }
        return null;
    }
}
