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
import org.apache.oozie.client.event.SLAEvent.SLAStatus;
import org.apache.oozie.executor.jpa.SLARegistrationQueryExecutor.SLARegQuery;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.SLARegistrationBean;
import org.apache.oozie.test.XDataTestCase;

public class TestSLARegistrationQueryExecutor extends XDataTestCase {
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
        SLARegistrationBean bean = addRecordToSLARegistrationTable("test-application", SLAStatus.MET);

        // UPDATE_SLA_REG_ALL
        Query query = SLARegistrationQueryExecutor.getInstance().getUpdateQuery(SLARegQuery.UPDATE_SLA_REG_ALL, bean,
                em);

        assertEquals(query.getParameterValue("jobId"), bean.getId());
        assertEquals(query.getParameterValue("nominalTime"), bean.getNominalTimestamp());
        assertEquals(query.getParameterValue("expectedStartTime"), bean.getExpectedStartTimestamp());
        assertEquals(query.getParameterValue("expectedEndTime"), bean.getExpectedEndTimestamp());
        assertEquals(query.getParameterValue("expectedDuration"), bean.getExpectedDuration());
        assertEquals(query.getParameterValue("slaConfig"), bean.getSlaConfig());
        assertEquals(query.getParameterValue("notificationMsg"), bean.getNotificationMsg());
        assertEquals(query.getParameterValue("upstreamApps"), bean.getUpstreamApps());
        assertEquals(query.getParameterValue("appType"), bean.getAppType().toString());
        assertEquals(query.getParameterValue("appName"), bean.getAppName());
        assertEquals(query.getParameterValue("user"), bean.getUser());
        assertEquals(query.getParameterValue("parentId"), bean.getParentId());
        assertEquals(query.getParameterValue("jobData"), bean.getJobData());

        em.close();
    }

    public void testExecuteUpdate() throws Exception {
        // TODO
    }

    public void testGet() throws Exception {
        // TODO
    }

    public void testGetList() throws Exception {
        // TODO
    }

    public void testInsert() throws Exception {
        // TODO
    }
}
