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

import java.sql.Timestamp;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.AppType;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.executor.jpa.JPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.sla.SLARegistrationBean;
import org.apache.oozie.util.DateUtils;

/**
 * Load the list of SLARegistrationBean and return the list.
 */
public class SLARegistrationGetJPAExecutor implements JPAExecutor<SLARegistrationBean> {

    private String id = null;

    public SLARegistrationGetJPAExecutor(String id) {
        this.id = id;
    }

    @Override
    public String getName() {
        return "SLARegistrationGetJPAExecutor";
    }

    @Override
    public SLARegistrationBean execute(EntityManager em) throws JPAExecutorException {
        try {
            Query q = em.createNamedQuery("GET_SLA_REG_ALL");
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
            bean.setJobId((String) arr[0]);
        }
        if (arr[1] != null) {
            bean.setAppType(AppType.valueOf((String) arr[1]));
        }
        if (arr[2] != null) {
            bean.setAppName((String) arr[2]);
        }
        if (arr[3] != null) {
            bean.setUser((String) arr[3]);
        }
        if (arr[4] != null) {
            bean.setNominalTime(DateUtils.toDate((Timestamp) arr[4]));
        }
        if (arr[5] != null) {
            bean.setExpectedStart(DateUtils.toDate((Timestamp) arr[5]));
        }
        if (arr[6] != null) {
            bean.setExpectedEnd(DateUtils.toDate((Timestamp) arr[6]));
        }
        if (arr[7] != null) {
            bean.setExpectedDuration((Long) arr[7]);
        }
        if (arr[8] != null) {
            bean.setJobData((String) arr[8]);
        }
        if (arr[9] != null) {
            bean.setParentId((String) arr[9]);
        }
        if (arr[10] != null) {
            bean.setNotificationMsg((String) arr[10]);
        }
        if (arr[11] != null) {
            bean.setUpstreamApps((String) arr[11]);
        }
        if (arr[12] != null) {
            bean.setSlaConfig((String) arr[12]);
        }

        return bean;
    }

}
