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

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.client.Job;
import org.apache.oozie.executor.jpa.BundleJobQueryExecutor.BundleJobQuery;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestBundleJobQueryExecutor extends XDataTestCase {
    Services services;
    JPAService jpaService;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        jpaService = Services.get().get(JPAService.class);
        cleanUpDBTables();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testGetUpdateQuery() throws Exception {
        EntityManager em = jpaService.getEntityManager();
        BundleJobBean bean = addRecordToBundleJobTable(Job.Status.PREP, false);
        // UPDATE_BUNDLE_JOB
        Query query = BundleJobQueryExecutor.getInstance().getUpdateQuery(BundleJobQuery.UPDATE_BUNDLE_JOB, bean, em);
        assertEquals(query.getParameterValue("appName"), bean.getAppName());
        assertEquals(query.getParameterValue("appPath"), bean.getAppPath());
        assertEquals(query.getParameterValue("conf"), bean.getConfBlob());
        assertEquals(query.getParameterValue("timeOut"), bean.getTimeout());
        assertEquals(query.getParameterValue("createdTime"), bean.getCreatedTimestamp());
        assertEquals(query.getParameterValue("endTime"), bean.getEndTimestamp());
        assertEquals(query.getParameterValue("jobXml"), bean.getJobXmlBlob());
        assertEquals(query.getParameterValue("lastModifiedTime"), bean.getLastModifiedTimestamp());
        assertEquals(query.getParameterValue("origJobXml"), bean.getOrigJobXmlBlob());
        assertEquals(query.getParameterValue("startTime"), bean.getstartTimestamp());
        assertEquals(query.getParameterValue("status"), bean.getStatus().toString());
        assertEquals(query.getParameterValue("timeUnit"), bean.getTimeUnit());
        assertEquals(query.getParameterValue("pending"), bean.getPending());
        assertEquals(query.getParameterValue("id"), bean.getId());

        // UPDATE_BUNDLE_JOB_STATUS
        query = BundleJobQueryExecutor.getInstance().getUpdateQuery(BundleJobQuery.UPDATE_BUNDLE_JOB_STATUS, bean, em);
        assertEquals(query.getParameterValue("status"), bean.getStatus().toString());
        assertEquals(query.getParameterValue("id"), bean.getId());

        // UPDATE_BUNDLE_JOB_STATUS_PENDING
        query = BundleJobQueryExecutor.getInstance().getUpdateQuery(BundleJobQuery.UPDATE_BUNDLE_JOB_STATUS_PENDING,
                bean, em);
        assertEquals(query.getParameterValue("status"), bean.getStatus().toString());
        assertEquals(query.getParameterValue("pending"), bean.getPending());
        assertEquals(query.getParameterValue("id"), bean.getId());

        // UPDATE_BUNDLE_JOB_STATUS_PENDING_SUSP_MOD_TIME:
        query = BundleJobQueryExecutor.getInstance().getUpdateQuery(
                BundleJobQuery.UPDATE_BUNDLE_JOB_STATUS_PENDING_SUSP_MOD_TIME, bean, em);
        assertEquals(query.getParameterValue("status"), bean.getStatus().toString());
        assertEquals(query.getParameterValue("lastModifiedTime"), bean.getLastModifiedTimestamp());
        assertEquals(query.getParameterValue("pending"), bean.getPending());
        assertEquals(query.getParameterValue("suspendedTime"), bean.getSuspendedTimestamp());
        assertEquals(query.getParameterValue("id"), bean.getId());

        // UPDATE_BUNDLE_JOB_STATUS_PENDING_MODTIME:
        query = BundleJobQueryExecutor.getInstance().getUpdateQuery(
                BundleJobQuery.UPDATE_BUNDLE_JOB_STATUS_PENDING_MODTIME, bean, em);
        assertEquals(query.getParameterValue("status"), bean.getStatus().toString());
        assertEquals(query.getParameterValue("lastModifiedTime"), bean.getLastModifiedTimestamp());
        assertEquals(query.getParameterValue("pending"), bean.getPending());
        assertEquals(query.getParameterValue("id"), bean.getId());

        // UPDATE_BUNDLE_JOB_STATUS_PAUSE_ENDTIME:
        query = BundleJobQueryExecutor.getInstance().getUpdateQuery(
                BundleJobQuery.UPDATE_BUNDLE_JOB_STATUS_PAUSE_ENDTIME, bean, em);
        assertEquals(query.getParameterValue("status"), bean.getStatus().toString());
        assertEquals(query.getParameterValue("pauseTime"), bean.getPauseTimestamp());
        assertEquals(query.getParameterValue("endTime"), bean.getEndTimestamp());
        assertEquals(query.getParameterValue("id"), bean.getId());

        // UPDATE_BUNDLE_JOB_PAUSE_KICKOFF:
        query = BundleJobQueryExecutor.getInstance().getUpdateQuery(BundleJobQuery.UPDATE_BUNDLE_JOB_PAUSE_KICKOFF,
                bean, em);
        assertEquals(query.getParameterValue("pauseTime"), bean.getPauseTimestamp());
        assertEquals(query.getParameterValue("kickoffTime"), bean.getKickoffTimestamp());
        assertEquals(query.getParameterValue("id"), bean.getId());

        em.close();
    }

    public void testExecuteUpdate() throws Exception {
        BundleJobBean bean = addRecordToBundleJobTable(Job.Status.PREP, false);
        bean.setStatus(org.apache.oozie.client.BundleJob.Status.RUNNING);
        BundleJobQueryExecutor.getInstance().executeUpdate(BundleJobQuery.UPDATE_BUNDLE_JOB_STATUS, bean);
        BundleJobBean retBean = BundleJobQueryExecutor.getInstance().get(BundleJobQuery.GET_BUNDLE_JOB, bean.getId());
        assertEquals(retBean.getStatus(), org.apache.oozie.client.BundleJob.Status.RUNNING);
    }

    public void testGet() throws Exception {
        BundleJobBean bean = this.addRecordToBundleJobTable(Job.Status.RUNNING, false);
        // GET_BUNDLE_JOB_ID_STATUS_PENDING_MODTIME
        BundleJobBean retBean = BundleJobQueryExecutor.getInstance().get(
                BundleJobQuery.GET_BUNDLE_JOB_ID_STATUS_PENDING_MODTIME, bean.getId());
        assertEquals(bean.getId(), retBean.getId());
        assertEquals(bean.getStatusStr(), retBean.getStatusStr());
        assertEquals(bean.getPending(), retBean.getPending());
        assertEquals(bean.getLastModifiedTime().getTime(), retBean.getLastModifiedTime().getTime());
        // GET_BUNDLE_JOB_ID_JOBXML_CONF
        retBean = BundleJobQueryExecutor.getInstance().get(BundleJobQuery.GET_BUNDLE_JOB_ID_JOBXML_CONF, bean.getId());
        assertEquals(bean.getId(), retBean.getId());
        assertEquals(bean.getJobXml(), retBean.getJobXml());
        assertEquals(bean.getConf(), retBean.getConf());
        // GET_BUNDLE_JOB_STATUS
        retBean = BundleJobQueryExecutor.getInstance().get(BundleJobQuery.GET_BUNDLE_JOB_STATUS, bean.getId());
        assertEquals(bean.getStatus(), retBean.getStatus());
        assertEquals(bean.getId(), retBean.getId());
    }

    public void testGetList() throws Exception {
        // TODO
    }

    public void testInsert() throws Exception {
        BundleJobBean bean = new BundleJobBean();
        bean.setId("test-oozie");
        bean.setAppName("testApp");
        bean.setUser("oozie");
        BundleJobQueryExecutor.getInstance().insert(bean);
        BundleJobBean retBean = BundleJobQueryExecutor.getInstance().get(BundleJobQuery.GET_BUNDLE_JOB, "test-oozie");
        assertEquals(retBean.getAppName(), "testApp");
        assertEquals(retBean.getUser(), "oozie");
    }
}