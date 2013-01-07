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

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;

/**
 * JPA Executor to get running workflow actions
 */
public class WorkflowActionsRunningGetJPAExecutor implements JPAExecutor<List<WorkflowActionBean>> {

    private final long checkAgeSecs;

    public WorkflowActionsRunningGetJPAExecutor(long checkAgeSecs) {
        this.checkAgeSecs = checkAgeSecs;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.EntityManager)
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<WorkflowActionBean> execute(EntityManager em) throws JPAExecutorException {
        List<WorkflowActionBean> actions;
        List<WorkflowActionBean> actionList = new ArrayList<WorkflowActionBean>();
        try {
            Timestamp ts = new Timestamp(System.currentTimeMillis() - checkAgeSecs * 1000);
            Query q = em.createNamedQuery("GET_RUNNING_ACTIONS");
            q.setParameter("lastCheckTime", ts);
            actions = q.getResultList();
            for (WorkflowActionBean a : actions) {
                WorkflowActionBean aa = getBeanForRunningAction(a);
                actionList.add(aa);
            }
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0605, "null", e);
        }
        return actionList;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "WorkflowActionsRunningGetJPAExecutor";
    }

    /**
     * Re-create workflow action bean
     *
     * @param bean
     * @return workflow action bean
     */
    private WorkflowActionBean getBeanForRunningAction(WorkflowActionBean bean){
        if (bean != null) {
            WorkflowActionBean action = new WorkflowActionBean();
            action.setId(bean.getId());
            action.setConf(bean.getConf());
            action.setConsoleUrl(bean.getConsoleUrl());
            action.setData(bean.getData());
            action.setStats(bean.getStats());
            action.setExternalChildIDs(bean.getExternalChildIDs());
            action.setErrorInfo(bean.getErrorCode(), bean.getErrorMessage());
            action.setExternalId(bean.getExternalId());
            action.setExternalStatus(bean.getExternalStatus());
            action.setName(bean.getName());
            action.setCred(bean.getCred());
            action.setRetries(bean.getRetries());
            action.setTrackerUri(bean.getTrackerUri());
            action.setTransition(bean.getTransition());
            action.setType(bean.getType());
            action.setEndTime(bean.getEndTime());
            action.setExecutionPath(bean.getExecutionPath());
            action.setLastCheckTime(bean.getLastCheckTime());
            action.setLogToken(bean.getLogToken());
            if (bean.getPending() == true) {
                action.setPending();
            }
            action.setPendingAge(bean.getPendingAge());
            action.setSignalValue(bean.getSignalValue());
            action.setSlaXml(bean.getSlaXml());
            action.setStartTime(bean.getStartTime());
            action.setStatus(bean.getStatus());
            action.setJobId(bean.getWfId());
            action.setUserRetryCount(bean.getUserRetryCount());
            action.setUserRetryInterval(bean.getUserRetryInterval());
            action.setUserRetryMax(bean.getUserRetryMax());
            return action;
        }
        return null;
    }
}
