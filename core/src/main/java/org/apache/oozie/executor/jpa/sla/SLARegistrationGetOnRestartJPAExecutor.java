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
package org.apache.oozie.executor.jpa.sla;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.executor.jpa.JPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.sla.SLARegistrationBean;

/**
 * Load SLARegistrationBean on restart
 */
public class SLARegistrationGetOnRestartJPAExecutor implements JPAExecutor<SLARegistrationBean> {

    private String id;

    public SLARegistrationGetOnRestartJPAExecutor(String id) {
        this.id = id;
    }

    @Override
    public String getName() {
        return "SLARegistrationGetOnRestartJPAExecutor";
    }

    @Override
    public SLARegistrationBean execute(EntityManager em) throws JPAExecutorException {
        try {
            Query q = em.createNamedQuery("GET_SLA_REG_ON_RESTART");
            q.setParameter("id", id);
            Object[] obj = (Object[]) q.getSingleResult();
            return getBeanFromObj(obj);
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
    }

    private SLARegistrationBean getBeanFromObj(Object[] arr) {
        SLARegistrationBean bean = new SLARegistrationBean();
        if (arr[0] != null) {
            bean.setNotificationMsg((String) arr[0]);
        }
        if (arr[1] != null) {
            bean.setUpstreamApps((String) arr[1]);
        }
        if (arr[2] != null) {
            bean.setSlaConfig((String) arr[2]);
        }
        if (arr[3] != null) {
            bean.setJobData((String) arr[3]);
        }
        return bean;
    }

}
