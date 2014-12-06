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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.executor.jpa.BundleActionQueryExecutor.BundleActionQuery;
import org.apache.oozie.executor.jpa.BundleJobQueryExecutor.BundleJobQuery;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.executor.jpa.SLARegistrationQueryExecutor.SLARegQuery;
import org.apache.oozie.executor.jpa.SLASummaryQueryExecutor.SLASummaryQuery;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor.WorkflowActionQuery;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor.WorkflowJobQuery;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.JPAService.QueryEntry;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.SLARegistrationBean;
import org.apache.oozie.sla.SLASummaryBean;

/**
 * Query Executor that provides API to run multiple update/insert queries in one
 * transaction. This guarantees entire change to be rolled back when one of
 * queries fails.
 */
public class BatchQueryExecutor {

    private static BatchQueryExecutor instance = new BatchQueryExecutor();

    public static class UpdateEntry<E extends Enum<E>> {
        E namedQuery;
        JsonBean bean;

        public UpdateEntry(E namedQuery, JsonBean bean) {
            this.bean = bean;
            this.namedQuery = namedQuery;
        }

        public JsonBean getBean() {
            return this.bean;
        }

        public E getQueryName() {
            return this.namedQuery;
        }
    }

    private BatchQueryExecutor() {
    }

    public static BatchQueryExecutor getInstance() {
        return BatchQueryExecutor.instance;
    }

    @SuppressWarnings("rawtypes")
    public void executeBatchInsertUpdateDelete(Collection<JsonBean> insertList, Collection<UpdateEntry> updateList,
            Collection<JsonBean> deleteList) throws JPAExecutorException {
        List<QueryEntry> queryList = new ArrayList<QueryEntry>();
        JPAService jpaService = Services.get().get(JPAService.class);
        EntityManager em = jpaService.getEntityManager();

        if (updateList != null) {
            for (UpdateEntry entry : updateList) {
                Query query = null;
                JsonBean bean = entry.getBean();
                if (bean instanceof WorkflowJobBean) {
                    query = WorkflowJobQueryExecutor.getInstance().getUpdateQuery(
                            (WorkflowJobQuery) entry.getQueryName(), (WorkflowJobBean) entry.getBean(), em);
                }
                else if (bean instanceof WorkflowActionBean) {
                    query = WorkflowActionQueryExecutor.getInstance().getUpdateQuery(
                            (WorkflowActionQuery) entry.getQueryName(), (WorkflowActionBean) entry.getBean(), em);
                }
                else if (bean instanceof CoordinatorJobBean) {
                    query = CoordJobQueryExecutor.getInstance().getUpdateQuery((CoordJobQuery) entry.getQueryName(),
                            (CoordinatorJobBean) entry.getBean(), em);
                }
                else if (bean instanceof CoordinatorActionBean) {
                    query = CoordActionQueryExecutor.getInstance().getUpdateQuery(
                            (CoordActionQuery) entry.getQueryName(), (CoordinatorActionBean) entry.getBean(), em);
                }
                else if (bean instanceof BundleJobBean) {
                    query = BundleJobQueryExecutor.getInstance().getUpdateQuery((BundleJobQuery) entry.getQueryName(),
                            (BundleJobBean) entry.getBean(), em);
                }
                else if (bean instanceof BundleActionBean) {
                    query = BundleActionQueryExecutor.getInstance().getUpdateQuery(
                            (BundleActionQuery) entry.getQueryName(), (BundleActionBean) entry.getBean(), em);
                }
                else if (bean instanceof SLARegistrationBean) {
                    query = SLARegistrationQueryExecutor.getInstance().getUpdateQuery(
                            (SLARegQuery) entry.getQueryName(), (SLARegistrationBean) entry.getBean(), em);
                }
                else if (bean instanceof SLASummaryBean) {
                    query = SLASummaryQueryExecutor.getInstance().getUpdateQuery(
                            (SLASummaryQuery) entry.getQueryName(), (SLASummaryBean) entry.getBean(), em);
                }
                else {
                    throw new JPAExecutorException(ErrorCode.E0603, "BatchQueryExecutor faield to construct a query");
                }
                queryList.add(new QueryEntry(entry.getQueryName(), query));
            }
        }
        jpaService.executeBatchInsertUpdateDelete(insertList, queryList, deleteList, em);
    }

}
