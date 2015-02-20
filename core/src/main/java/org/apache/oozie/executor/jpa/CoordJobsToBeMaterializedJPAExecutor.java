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
import java.util.Date;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.ParamChecker;

/**
 * JPA command to get coordinator jobs which are qualify for Materialization.
 */
public class CoordJobsToBeMaterializedJPAExecutor implements JPAExecutor<List<CoordinatorJobBean>> {

    private Date dateInput;
    private int limit;

    /**
     * @param date
     * @param limit
     */
    public CoordJobsToBeMaterializedJPAExecutor(Date date, int limit) {
        ParamChecker.notNull(date, "Coord Job Materialization Date");
        this.dateInput = date;
        this.limit = limit;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.EntityManager)
     */
    @SuppressWarnings("unchecked")
    @Override
    public List<CoordinatorJobBean> execute(EntityManager em) throws JPAExecutorException {
        List<CoordinatorJobBean> cjBeans;
        try {
            Query q = em.createNamedQuery("GET_COORD_JOBS_OLDER_FOR_MATERIALIZATION");
            q.setParameter("matTime", new Timestamp(this.dateInput.getTime()));
            if (limit > 0) {
                q.setMaxResults(limit);
            }

           cjBeans = q.getResultList();
        }
        catch (IllegalStateException e) {
            throw new JPAExecutorException(ErrorCode.E0601, e.getMessage(), e);
        }
        return cjBeans;
    }

    @Override
    public String getName() {
        return "CoordJobsToBeMaterializedJPAExecutor";
    }

    /**
     * @return the dateInput
     */
    public Date getDateInput() {
        return dateInput;
    }

    /**
     * @param dateInput the dateInput to set
     */
    public void setDateInput(Date dateInput) {
        this.dateInput = dateInput;
    }

    /**
     * @return the limit
     */
    public int getLimit() {
        return limit;
    }

    /**
     * @param limit the limit to set
     */
    public void setLimit(int limit) {
        this.limit = limit;
    }
}
