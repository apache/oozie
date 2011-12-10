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

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.util.ParamChecker;

/**
 * Load the WorkflowAction into a Bean and return it.
 */
public class WorkflowActionGetJPAExecutor implements JPAExecutor<WorkflowActionBean> {

    private String wfActionId = null;

    public WorkflowActionGetJPAExecutor(String wfActionId) {
        ParamChecker.notNull(wfActionId, "wfActionId");
        this.wfActionId = wfActionId;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "WorkflowActionGetJPAExecutor";
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.EntityManager)
     */
    @Override
    @SuppressWarnings("unchecked")
    public WorkflowActionBean execute(EntityManager em) throws JPAExecutorException {
        List<WorkflowActionBean> waBeans;
        try {
            Query q = em.createNamedQuery("GET_ACTION");
            q.setParameter("id", wfActionId);
            waBeans = q.getResultList();
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e);
        }
        WorkflowActionBean bean = null;
        if (waBeans != null && waBeans.size() > 0) {
            bean = waBeans.get(0);
            bean = getBeanForRunningAction(bean);
            return bean;
        }
        else {
            throw new JPAExecutorException(ErrorCode.E0605, wfActionId);
        }
    }

    private WorkflowActionBean getBeanForRunningAction(WorkflowActionBean a) {
        if (a != null) {
            WorkflowActionBean action = new WorkflowActionBean();
            action.setId(a.getId());
            action.setConf(a.getConf());
            action.setConsoleUrl(a.getConsoleUrl());
            action.setData(a.getData());
            action.setStats(a.getStats());
            action.setExternalChildIDs(a.getExternalChildIDs());
            action.setErrorInfo(a.getErrorCode(), a.getErrorMessage());
            action.setExternalId(a.getExternalId());
            action.setExternalStatus(a.getExternalStatus());
            action.setName(a.getName());
            action.setCred(a.getCred());
            action.setRetries(a.getRetries());
            action.setTrackerUri(a.getTrackerUri());
            action.setTransition(a.getTransition());
            action.setType(a.getType());
            action.setEndTime(a.getEndTime());
            action.setExecutionPath(a.getExecutionPath());
            action.setLastCheckTime(a.getLastCheckTime());
            action.setLogToken(a.getLogToken());
            if (a.getPending() == true) {
                action.setPending();
            }
            action.setPendingAge(a.getPendingAge());
            action.setSignalValue(a.getSignalValue());
            action.setSlaXml(a.getSlaXml());
            action.setStartTime(a.getStartTime());
            action.setStatus(a.getStatus());
            action.setJobId(a.getWfId());
            action.setUserRetryCount(a.getUserRetryCount());
            action.setUserRetryInterval(a.getUserRetryInterval());
            action.setUserRetryMax(a.getUserRetryMax());
            return action;
        }
        return null;
    }

}
