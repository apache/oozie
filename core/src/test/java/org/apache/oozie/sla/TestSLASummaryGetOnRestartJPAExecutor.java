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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.sla.SLASummaryGetRecordsOnRestartJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestSLASummaryGetOnRestartJPAExecutor extends XDataTestCase {
    Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testSLARegistrationGet() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        SLASummaryBean sla1 = new SLASummaryBean();
        sla1.setId("jobId");
        sla1.setAppName("appName");
        sla1.setUser("user");
        sla1.setParentId("parent");
        sla1.setEventProcessed(7);
        // set to 5 days back from now
        sla1.setLastModifiedTime(new Date(System.currentTimeMillis() - 5*24*60*60*1000));

        SLASummaryBean sla2 = new SLASummaryBean();
        sla2.setId("jobId2");
        sla2.setEventProcessed(6);
        // set to long time back
        sla2.setLastModifiedTime(sdf.parse("2009-06-03"));

        List<JsonBean> insert = new ArrayList<JsonBean>();
        insert.add(sla1);
        insert.add(sla2);
        BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(insert, null, null);
        // get all records modified in last 7 days
        SLASummaryGetRecordsOnRestartJPAExecutor slaGetOnRestart = new SLASummaryGetRecordsOnRestartJPAExecutor(7);
        List<SLASummaryBean> beans = jpaService.execute(slaGetOnRestart);
        assertEquals(1, beans.size());
        assertEquals("jobId", beans.get(0).getId());
        assertEquals("appName", beans.get(0).getAppName());
        assertEquals("user", beans.get(0).getUser());
        assertEquals("parent", beans.get(0).getParentId());
        assertEquals(7, beans.get(0).getEventProcessed());
    }

}
