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

import org.apache.oozie.BundleActionBean;
import org.apache.oozie.ErrorCode;

/**
 * Load the list of BundleAction ordered by lastModifiedTime
 */
public class BundleActionsGetByLastModifiedTimeJPAExecutor implements JPAExecutor<List<BundleActionBean>> {
    private Date d = null;

    public BundleActionsGetByLastModifiedTimeJPAExecutor(Date d) {
        this.d = d;
    }

    @Override
    public String getName() {
        return "BundleActionsGetByLastModifiedTimeJPAExecutor";
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<BundleActionBean> execute(EntityManager em) throws JPAExecutorException {
        List<BundleActionBean> baBeans;
        try {
            Query q = em.createNamedQuery("GET_BUNDLE_ACTIONS_BY_LAST_MODIFIED_TIME");
            q.setParameter("lastModifiedTime", new Timestamp(d.getTime()));
            baBeans = q.getResultList();
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }

        return baBeans;
    }
}
