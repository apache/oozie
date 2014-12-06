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
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.client.Job;
import org.apache.oozie.executor.jpa.BundleActionQueryExecutor.BundleActionQuery;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;

public class TestBundleActionQueryExecutor extends XDataTestCase {
    Services services;
    JPAService jpaService;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        jpaService = Services.get().get(JPAService.class);
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testGetUpdateQuery() throws Exception {
        EntityManager em = jpaService.getEntityManager();
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.RUNNING, false);
        BundleActionBean bundleAction = this.addRecordToBundleActionTable(job.getId(), "action1", 1, Job.Status.PREP);

        // UPDATE_BUNDLE_ACTION_PENDING_MODTIME
        Query query = BundleActionQueryExecutor.getInstance().getUpdateQuery(
                BundleActionQuery.UPDATE_BUNDLE_ACTION_PENDING_MODTIME, bundleAction, em);
        assertEquals(query.getParameterValue("lastModifiedTime"), bundleAction.getLastModifiedTimestamp());
        assertEquals(query.getParameterValue("pending"), bundleAction.getPending());
        assertEquals(query.getParameterValue("bundleActionId"), bundleAction.getBundleActionId());

        // UPDATE_BUNDLE_ACTION_STATUS_PENDING_MODTIME:
        query = BundleActionQueryExecutor.getInstance().getUpdateQuery(
                BundleActionQuery.UPDATE_BUNDLE_ACTION_STATUS_PENDING_MODTIME_COORDID, bundleAction, em);
        assertEquals(query.getParameterValue("status"), bundleAction.getStatus().toString());
        assertEquals(query.getParameterValue("lastModifiedTime"), bundleAction.getLastModifiedTimestamp());
        assertEquals(query.getParameterValue("pending"), bundleAction.getPending());
        assertEquals(query.getParameterValue("coordId"), bundleAction.getCoordId());
        assertEquals(query.getParameterValue("bundleActionId"), bundleAction.getBundleActionId());

        // UPDATE_BUNDLE_ACTION_STATUS_PENDING_MODTIME_COORDID
        query = BundleActionQueryExecutor.getInstance().getUpdateQuery(
                BundleActionQuery.UPDATE_BUNDLE_ACTION_STATUS_PENDING_MODTIME_COORDID, bundleAction, em);
        assertEquals(query.getParameterValue("status"), bundleAction.getStatus().toString());
        assertEquals(query.getParameterValue("lastModifiedTime"), bundleAction.getLastModifiedTimestamp());
        assertEquals(query.getParameterValue("pending"), bundleAction.isPending() ? 1 : 0);
        assertEquals(query.getParameterValue("coordId"), bundleAction.getCoordId());
        assertEquals(query.getParameterValue("bundleActionId"), bundleAction.getBundleActionId());

        em.close();
    }

    public void testGetSelectQuery() throws Exception {

        EntityManager em = jpaService.getEntityManager();
        BundleActionBean bean = addRecordToBundleActionTable("test-bundle-id", "test-coord", 0, Job.Status.RUNNING);

        Query query = null;
        query = BundleActionQueryExecutor.getInstance().getSelectQuery(BundleActionQuery.GET_BUNDLE_ACTION, em,
                bean.getBundleId());
        assertEquals(query.getParameterValue("bundleActionId"), bean.getBundleId());

        query = BundleActionQueryExecutor.getInstance().getSelectQuery(
                BundleActionQuery.GET_BUNDLE_WAITING_ACTIONS_OLDER_THAN, em, (long) 100);
        Date date = DateUtils.toDate((Timestamp) (query.getParameterValue("lastModifiedTime")));
        assertTrue(date.before(Calendar.getInstance().getTime()));

        query = BundleActionQueryExecutor.getInstance().getSelectQuery(
                BundleActionQuery.GET_BUNDLE_UNIGNORED_ACTION_STATUS_PENDING_FOR_BUNDLE, em, bean.getBundleId());
        assertEquals(query.getParameterValue("bundleId"), bean.getBundleId());
    }

    public void testExecuteUpdate() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.RUNNING, false);
        BundleActionBean bean = this.addRecordToBundleActionTable(job.getId(), "action1", 1, Job.Status.PREP);
        bean.setStatus(Job.Status.RUNNING);
        BundleActionQueryExecutor.getInstance().executeUpdate(
                BundleActionQuery.UPDATE_BUNDLE_ACTION_STATUS_PENDING_MODTIME, bean);
        BundleActionBean retBean = BundleActionQueryExecutor.getInstance().get(BundleActionQuery.GET_BUNDLE_ACTION,
                bean.getBundleActionId());
        assertEquals(retBean.getStatus(), Job.Status.RUNNING);
    }

    public void testGet() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.RUNNING, false);
        BundleActionBean bundleAction = this.addRecordToBundleActionTable(job.getId(), "action1", 1, Job.Status.PREP);
        // GET_UNIGNORED_BUNDLE_ACTION_STATUS_PENDING_FOR_BUNDLE
        BundleActionBean retBean = BundleActionQueryExecutor.getInstance().get(
                BundleActionQuery.GET_BUNDLE_UNIGNORED_ACTION_STATUS_PENDING_FOR_BUNDLE, bundleAction.getBundleId());
        assertEquals(bundleAction.getCoordId(), retBean.getCoordId());
        assertEquals(bundleAction.getStatusStr(), retBean.getStatusStr());
        assertEquals(bundleAction.getPending(), retBean.getPending());
        // GET_BUNDLE_ACTION
        retBean = BundleActionQueryExecutor.getInstance().get(BundleActionQuery.GET_BUNDLE_ACTION,
                bundleAction.getBundleActionId());
        assertEquals(bundleAction.getStatus(), retBean.getStatus());
    }

    public void testGetList() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.RUNNING, false);
        this.addRecordToBundleActionTable(job.getId(), "coord1", 0, Job.Status.PREP);
        this.addRecordToBundleActionTable(job.getId(), "coord2", 1, Job.Status.RUNNING);
        this.addRecordToBundleActionTable(job.getId(), "coord3", 1, Job.Status.RUNNING);
        List<BundleActionBean> bActions = BundleActionQueryExecutor.getInstance().getList(
                BundleActionQuery.GET_BUNDLE_WAITING_ACTIONS_OLDER_THAN, (long) (1000 * 60));
        assertEquals(0, bActions.size());
        bActions = BundleActionQueryExecutor.getInstance().getList(
                BundleActionQuery.GET_BUNDLE_WAITING_ACTIONS_OLDER_THAN, (long) (-1000 * 60));
        assertEquals(2, bActions.size());
        // GET_BUNDLE_ACTIONS_STATUS_UNIGNORED_FOR_BUNDLE
        List<BundleActionBean> retList = BundleActionQueryExecutor.getInstance().getList(
                BundleActionQuery.GET_BUNDLE_ACTIONS_STATUS_UNIGNORED_FOR_BUNDLE, job.getId());
        assertEquals(3, retList.size());
        for (BundleActionBean bean : retList) {
            assertTrue(bean.getCoordName().equals("coord1") || bean.getCoordName().equals("coord2")
                    || bean.getCoordName().equals("coord3"));
        }
    }

    public void testInsert() throws Exception {
        BundleActionBean bean = new BundleActionBean();
        bean.setBundleActionId("test-oozie");
        bean.setCoordName("testApp");
        bean.setStatus(Job.Status.RUNNING);
        BundleActionQueryExecutor.getInstance().insert(bean);
        BundleActionBean retBean = BundleActionQueryExecutor.getInstance().get(BundleActionQuery.GET_BUNDLE_ACTION,
                "test-oozie");
        assertEquals(retBean.getCoordName(), "testApp");
        assertEquals(retBean.getStatus(), Job.Status.RUNNING);
    }
}