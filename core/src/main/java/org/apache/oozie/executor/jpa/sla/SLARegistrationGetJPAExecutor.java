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

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.executor.jpa.JPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.sla.SLARegistrationBean;

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

    @SuppressWarnings("unchecked")
    @Override
    public SLARegistrationBean execute(EntityManager em) throws JPAExecutorException {
        List<SLARegistrationBean> regBeans;
        try {
            Query q = em.createNamedQuery("GET_SLA_REG_ALL");
            q.setParameter("id", id);
            regBeans = q.getResultList();
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
        SLARegistrationBean slaRegBean = null;
        if (regBeans != null && regBeans.size() > 0) {
            slaRegBean = regBeans.get(0);
            slaRegBean.setSlaConfig(slaRegBean.getSlaConfig());
        }
        return slaRegBean;
    }

}
