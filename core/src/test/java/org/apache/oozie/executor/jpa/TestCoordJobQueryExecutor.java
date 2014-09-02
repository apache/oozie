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

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.SchemaService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestCoordJobQueryExecutor extends XDataTestCase {
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
        CoordinatorJobBean cjBean = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, true, true);

        // UPDATE_COORD_JOB
        Query query = CoordJobQueryExecutor.getInstance().getUpdateQuery(CoordJobQuery.UPDATE_COORD_JOB, cjBean, em);
        assertEquals(query.getParameterValue("appName"), cjBean.getAppName());
        assertEquals(query.getParameterValue("appPath"), cjBean.getAppPath());
        assertEquals(query.getParameterValue("concurrency"), cjBean.getConcurrency());
        assertEquals(query.getParameterValue("conf"), cjBean.getConfBlob());
        assertEquals(query.getParameterValue("externalId"), cjBean.getExternalId());
        assertEquals(query.getParameterValue("frequency"), cjBean.getFrequency());
        assertEquals(query.getParameterValue("lastActionNumber"), cjBean.getLastActionNumber());
        assertEquals(query.getParameterValue("timeOut"), cjBean.getTimeout());
        assertEquals(query.getParameterValue("timeZone"), cjBean.getTimeZone());
        assertEquals(query.getParameterValue("createdTime"), cjBean.getCreatedTimestamp());
        assertEquals(query.getParameterValue("endTime"), cjBean.getEndTimestamp());
        assertEquals(query.getParameterValue("execution"), cjBean.getExecution());
        assertEquals(query.getParameterValue("jobXml"), cjBean.getJobXmlBlob());
        assertEquals(query.getParameterValue("lastAction"), cjBean.getLastActionTimestamp());
        assertEquals(query.getParameterValue("lastModifiedTime"), cjBean.getLastModifiedTimestamp());
        assertEquals(query.getParameterValue("nextMaterializedTime"), cjBean.getNextMaterializedTimestamp());
        assertEquals(query.getParameterValue("origJobXml"), cjBean.getOrigJobXmlBlob());
        assertEquals(query.getParameterValue("slaXml"), cjBean.getSlaXmlBlob());
        assertEquals(query.getParameterValue("startTime"), cjBean.getStartTimestamp());
        assertEquals(query.getParameterValue("status"), cjBean.getStatus().toString());
        assertEquals(query.getParameterValue("timeUnit"), cjBean.getTimeUnit().toString());
        assertEquals(query.getParameterValue("appNamespace"), cjBean.getAppNamespace());
        assertEquals(query.getParameterValue("bundleId"), cjBean.getBundleId());
        assertEquals(query.getParameterValue("id"), cjBean.getId());

        // UPDATE_COORD_JOB_STATUS
        query = CoordJobQueryExecutor.getInstance().getUpdateQuery(CoordJobQuery.UPDATE_COORD_JOB_STATUS, cjBean, em);
        assertEquals(query.getParameterValue("status"), cjBean.getStatus().toString());
        assertEquals(query.getParameterValue("id"), cjBean.getId());

        // UPDATE_COORD_JOB_BUNDLEID
        cjBean.setBundleId("bundleID-test");
        query = CoordJobQueryExecutor.getInstance().getUpdateQuery(CoordJobQuery.UPDATE_COORD_JOB_BUNDLEID, cjBean, em);
        assertEquals(query.getParameterValue("bundleId"), cjBean.getBundleId());
        assertEquals(query.getParameterValue("id"), cjBean.getId());

        // UPDATE_COORD_JOB_STATUS_PENDING
        query = CoordJobQueryExecutor.getInstance().getUpdateQuery(CoordJobQuery.UPDATE_COORD_JOB_STATUS_PENDING,
                cjBean, em);
        assertEquals(query.getParameterValue("status"), cjBean.getStatus().toString());
        assertEquals(query.getParameterValue("pending"), cjBean.isPending() ? 1 : 0);
        assertEquals(query.getParameterValue("id"), cjBean.getId());

        // UPDATE_COORD_JOB_STATUS_MODTIME
        query = CoordJobQueryExecutor.getInstance().getUpdateQuery(CoordJobQuery.UPDATE_COORD_JOB_STATUS_MODTIME,
                cjBean, em);
        assertEquals(query.getParameterValue("status"), cjBean.getStatus().toString());
        assertEquals(query.getParameterValue("lastModifiedTime"), cjBean.getLastModifiedTimestamp());
        assertEquals(query.getParameterValue("id"), cjBean.getId());

        // UPDATE_COORD_JOB_STATUS_PENDING_MODTIME
        query = CoordJobQueryExecutor.getInstance().getUpdateQuery(
                CoordJobQuery.UPDATE_COORD_JOB_STATUS_PENDING_MODTIME, cjBean, em);
        assertEquals(query.getParameterValue("status"), cjBean.getStatus().toString());
        assertEquals(query.getParameterValue("lastModifiedTime"), cjBean.getLastModifiedTimestamp());
        assertEquals(query.getParameterValue("pending"), cjBean.isPending() ? 1 : 0);
        assertEquals(query.getParameterValue("id"), cjBean.getId());

        // UPDATE_COORD_JOB_LAST_MODIFIED_TIME
        query = CoordJobQueryExecutor.getInstance().getUpdateQuery(CoordJobQuery.UPDATE_COORD_JOB_LAST_MODIFIED_TIME,
                cjBean, em);
        assertEquals(query.getParameterValue("lastModifiedTime"), cjBean.getLastModifiedTimestamp());
        assertEquals(query.getParameterValue("id"), cjBean.getId());

        // UPDATE_COORD_JOB_STATUS_PENDING_TIME
        query = CoordJobQueryExecutor.getInstance().getUpdateQuery(CoordJobQuery.UPDATE_COORD_JOB_STATUS_PENDING_TIME,
                cjBean, em);
        assertEquals(query.getParameterValue("status"), cjBean.getStatus().toString());
        assertEquals(query.getParameterValue("lastModifiedTime"), cjBean.getLastModifiedTimestamp());
        assertEquals(query.getParameterValue("doneMaterialization"), cjBean.isDoneMaterialization() ? 1 : 0);
        assertEquals(query.getParameterValue("suspendedTime"), cjBean.getSuspendedTimestamp());
        assertEquals(query.getParameterValue("lastModifiedTime"), cjBean.getLastModifiedTimestamp());
        assertEquals(query.getParameterValue("id"), cjBean.getId());

        // UPDATE_COORD_JOB_MATERIALIZE
        query = CoordJobQueryExecutor.getInstance().getUpdateQuery(CoordJobQuery.UPDATE_COORD_JOB_MATERIALIZE, cjBean,
                em);
        assertEquals(query.getParameterValue("status"), cjBean.getStatus().toString());
        assertEquals(query.getParameterValue("pending"), cjBean.isPending() ? 1 : 0);
        assertEquals(query.getParameterValue("doneMaterialization"), cjBean.isDoneMaterialization() ? 1 : 0);
        assertEquals(query.getParameterValue("lastActionTime"), cjBean.getLastActionTimestamp());
        assertEquals(query.getParameterValue("lastActionNumber"), cjBean.getLastActionNumber());
        assertEquals(query.getParameterValue("nextMatdTime"), cjBean.getNextMaterializedTimestamp());
        assertEquals(query.getParameterValue("id"), cjBean.getId());

        // UPDATE_COORD_JOB_CHANGE
        query = CoordJobQueryExecutor.getInstance().getUpdateQuery(CoordJobQuery.UPDATE_COORD_JOB_CHANGE, cjBean, em);
        assertEquals(query.getParameterValue("endTime"), cjBean.getEndTimestamp());
        assertEquals(query.getParameterValue("status"), cjBean.getStatus().toString());
        assertEquals(query.getParameterValue("pending"), cjBean.isPending() ? 1 : 0);
        assertEquals(query.getParameterValue("doneMaterialization"), cjBean.isDoneMaterialization() ? 1 : 0);
        assertEquals(query.getParameterValue("concurrency"), cjBean.getConcurrency());
        assertEquals(query.getParameterValue("pauseTime"), cjBean.getPauseTimestamp());
        assertEquals(query.getParameterValue("lastActionNumber"), cjBean.getLastActionNumber());
        assertEquals(query.getParameterValue("lastActionTime"), cjBean.getLastActionTimestamp());
        assertEquals(query.getParameterValue("nextMatdTime"), cjBean.getNextMaterializedTimestamp());
        assertEquals(query.getParameterValue("lastModifiedTime"), cjBean.getLastModifiedTimestamp());
        assertEquals(query.getParameterValue("id"), cjBean.getId());
        em.close();
    }

    public void testExecuteUpdate() throws Exception {

        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB_STATUS, job);
        CoordinatorJobBean job1 = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB, job.getId());
        assertEquals(job1.getStatus(), CoordinatorJob.Status.RUNNING);

        job1.setStatus(CoordinatorJob.Status.SUCCEEDED);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB_STATUS_PENDING, job1);
        CoordinatorJobBean job2 = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB, job1.getId());
        assertEquals(job2.getStatus(), CoordinatorJob.Status.SUCCEEDED);

        CoordinatorJobBean job3 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        Date initialLMT = job3.getLastModifiedTime();
        job3.setLastModifiedTime(new Date()); // similar to what's done by e.g. the change command
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB_CHANGE, job3);
        job3 = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB, job3.getId());
        Date afterChangeLMT = job3.getLastModifiedTime();
        assertNotNull(job3.getLastModifiedTimestamp());
        assertTrue(afterChangeLMT.after(initialLMT));

    }

    public void testGet() throws Exception {
        CoordinatorJobBean bean = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, true);
        bean.setAppNamespace(SchemaService.COORDINATOR_NAMESPACE_URI_1);
        bean.setBundleId("dummy-bundleid");
        bean.setOrigJobXml("dummy-origjobxml");
        bean.setSlaXml("<sla></sla>");
        bean.setExecution("LIFO");  // FIFO is the default
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, bean);
        CoordinatorJobBean retBean;
        // GET_COORD_JOB_USER_APPNAME
        retBean = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB_USER_APPNAME, bean.getId());
        assertEquals(bean.getUser(), retBean.getUser());
        assertEquals(bean.getAppName(), retBean.getAppName());
        // GET_COORD_JOB_STATUS_PARENTID
        retBean = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB_STATUS_PARENTID, bean.getId());
        assertEquals(bean.getBundleId(), retBean.getBundleId());
        assertEquals(bean.getStatus(), retBean.getStatus());
        assertEquals(bean.getId(), retBean.getId());
        // GET_COORD_JOB_INPUTCHECK
        retBean = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB_INPUT_CHECK, bean.getId());
        assertEquals(bean.getUser(), retBean.getUser());
        assertEquals(bean.getAppName(), retBean.getAppName());
        assertEquals(bean.getStatusStr(), retBean.getStatusStr());
        assertEquals(bean.getAppNamespace(), retBean.getAppNamespace());
        assertEquals(bean.getExecution(), retBean.getExecution());
        assertEquals(bean.getFrequency(), retBean.getFrequency());
        assertEquals(bean.getTimeUnit(), retBean.getTimeUnit());
        assertEquals(bean.getTimeZone(), retBean.getTimeZone());
        assertEquals(bean.getEndTime(), retBean.getEndTime());
        assertNull(retBean.getConf());
        assertNull(retBean.getJobXmlBlob());
        assertNull(retBean.getOrigJobXmlBlob());
        assertNull(retBean.getSlaXmlBlob());
        // GET_COORD_JOB_ACTION_READY
        retBean = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB_ACTION_READY, bean.getId());
        assertEquals(bean.getId(), retBean.getId());
        assertEquals(bean.getUser(), retBean.getUser());
        assertEquals(bean.getGroup(), retBean.getGroup());
        assertEquals(bean.getAppName(), retBean.getAppName());
        assertEquals(bean.getStatusStr(), retBean.getStatusStr());
        assertEquals(bean.getExecution(), retBean.getExecution());
        assertEquals(bean.getConcurrency(), retBean.getConcurrency());
        assertNull(retBean.getConf());
        assertNull(retBean.getJobXmlBlob());
        assertNull(retBean.getOrigJobXmlBlob());
        assertNull(retBean.getSlaXmlBlob());
        // GET_COORD_JOB_ACTION_KILL
        retBean = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB_ACTION_KILL, bean.getId());
        assertEquals(bean.getId(), retBean.getId());
        assertEquals(bean.getUser(), retBean.getUser());
        assertEquals(bean.getGroup(), retBean.getGroup());
        assertEquals(bean.getAppName(), retBean.getAppName());
        assertEquals(bean.getStatusStr(), retBean.getStatusStr());
        assertNull(retBean.getConf());
        assertNull(retBean.getJobXmlBlob());
        assertNull(retBean.getOrigJobXmlBlob());
        assertNull(retBean.getSlaXmlBlob());
        // GET_COORD_JOB_MATERIALIZE
        retBean = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB_MATERIALIZE, bean.getId());
        assertEquals(bean.getId(), retBean.getId());
        assertEquals(bean.getUser(), retBean.getUser());
        assertEquals(bean.getGroup(), retBean.getGroup());
        assertEquals(bean.getAppName(), retBean.getAppName());
        assertEquals(bean.getStatusStr(), retBean.getStatusStr());
        assertEquals(bean.getFrequency(), retBean.getFrequency());
        assertEquals(bean.getMatThrottling(), retBean.getMatThrottling());
        assertEquals(bean.getTimeout(), retBean.getTimeout());
        assertEquals(bean.getTimeZone(), retBean.getTimeZone());
        assertEquals(bean.getStartTime(), retBean.getStartTime());
        assertEquals(bean.getEndTime(), retBean.getEndTime());
        assertEquals(bean.getPauseTime(), retBean.getPauseTime());
        assertEquals(bean.getNextMaterializedTime(), retBean.getNextMaterializedTime());
        assertEquals(bean.getLastActionTime(), retBean.getLastActionTime());
        assertEquals(bean.getLastActionNumber(), retBean.getLastActionNumber());
        assertEquals(bean.isDoneMaterialization(), retBean.isDoneMaterialization());
        assertEquals(bean.getBundleId(), retBean.getBundleId());
        assertEquals(bean.getConf(), retBean.getConf());
        assertEquals(bean.getJobXml(), retBean.getJobXml());
        assertEquals(bean.getExecution(), retBean.getExecution());
        assertNull(retBean.getOrigJobXmlBlob());
        assertNull(retBean.getSlaXmlBlob());
        // GET_COORD_JOB_SUSPEND_KILL
        retBean = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB_SUSPEND_KILL, bean.getId());
        assertEquals(bean.getId(), retBean.getId());
        assertEquals(bean.getUser(), retBean.getUser());
        assertEquals(bean.getGroup(), retBean.getGroup());
        assertEquals(bean.getAppName(), retBean.getAppName());
        assertEquals(bean.getStatusStr(), retBean.getStatusStr());
        assertEquals(bean.getBundleId(), retBean.getBundleId());
        assertEquals(bean.getAppNamespace(), retBean.getAppNamespace());
        assertEquals(bean.isDoneMaterialization(), retBean.isDoneMaterialization());
        assertNull(retBean.getConf());
        assertNull(retBean.getJobXmlBlob());
        assertNull(retBean.getOrigJobXmlBlob());
        assertNull(retBean.getSlaXmlBlob());
    }

    public void testGetList() throws Exception {
        CoordinatorJobBean bean1 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, true, true);
        CoordinatorJobBean bean2 = addRecordToCoordJobTable(CoordinatorJob.Status.DONEWITHERROR, true, true);

        // time to check last modified time against
        Date queryTime = new Date();
        bean1.setLastModifiedTime(new Date(queryTime.getTime() + 1000));
        bean2.setLastModifiedTime(new Date(queryTime.getTime() + 2000));
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB_LAST_MODIFIED_TIME, bean1);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB_LAST_MODIFIED_TIME, bean2);

        // GET_COORD_JOBS_CHANGED
        List<CoordinatorJobBean> retBeans = CoordJobQueryExecutor.getInstance().getList(
                CoordJobQuery.GET_COORD_JOBS_CHANGED, new Timestamp(queryTime.getTime()));
        assertEquals(2, retBeans.size());
        assertEquals(bean1.getId(), retBeans.get(0).getId());
        assertEquals(bean1.getStatus(), retBeans.get(0).getStatus());

        assertEquals(bean2.getId(), retBeans.get(1).getId());
        assertEquals(bean2.getStatus(), retBeans.get(1).getStatus());
    }

    public void testInsert() throws Exception {
        CoordinatorJobBean bean = new CoordinatorJobBean();
        bean.setId("test-oozie");
        bean.setAppName("testApp");
        bean.setUser("oozie");
        CoordJobQueryExecutor.getInstance().insert(bean);
        CoordinatorJobBean retBean = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB, "test-oozie");
        assertEquals(retBean.getAppName(), "testApp");
        assertEquals(retBean.getUser(), "oozie");
    }
}
