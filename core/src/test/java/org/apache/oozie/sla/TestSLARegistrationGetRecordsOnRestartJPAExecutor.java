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
package org.apache.oozie.sla;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.executor.jpa.sla.SLACalculationInsertUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.sla.SLARegistrationGetOnRestartJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.SLARegistrationBean;
import org.apache.oozie.test.XDataTestCase;

public class TestSLARegistrationGetRecordsOnRestartJPAExecutor extends XDataTestCase {
    Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        cleanUpDBTables();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testSLARegistrationGetRecordsOnRestart() throws Exception {
        Date current = new Date();
        final String jobId = "0000000-" + current.getTime() + "-TestSLARegGetRestartJPAExecutor-W";
        SLARegistrationBean reg = new SLARegistrationBean();
        reg.setId(jobId);
        reg.setNotificationMsg("dummyMessage");
        reg.setUpstreamApps("upApps");
        reg.setAlertEvents("miss");
        reg.setAlertContact("abc@y.com");
        reg.setJobData("jobData");
        JPAService jpaService = Services.get().get(JPAService.class);
        List<JsonBean> insert = new ArrayList<JsonBean>();
        insert.add(reg);
        SLACalculationInsertUpdateJPAExecutor slaInsertCmd = new SLACalculationInsertUpdateJPAExecutor(insert, null);
        jpaService.execute(slaInsertCmd);
        assertNotNull(jpaService);
        SLARegistrationGetOnRestartJPAExecutor readCmd = new SLARegistrationGetOnRestartJPAExecutor(jobId);
        SLARegistrationBean bean = jpaService.execute(readCmd);
        assertEquals("dummyMessage", bean.getNotificationMsg());
        assertEquals ("upApps", bean.getUpstreamApps());
        assertEquals ("miss", bean.getAlertEvents());
        assertEquals ("abc@y.com", bean.getAlertContact());
        assertEquals ("jobData", bean.getJobData());
    }

}
