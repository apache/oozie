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

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.service.JPAService;
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
        cleanUpDBTables();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testGetQuery() throws Exception {
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

    }

    public void testGet() throws Exception {
        // TODO
    }

    public void testGetList() throws Exception {
        // TODO
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
