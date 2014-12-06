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

import org.apache.oozie.AppType;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.sla.SLARegistrationGetJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.SLARegistrationBean;
import org.apache.oozie.test.XDataTestCase;

public class TestSLARegistrationGetJPAExecutor extends XDataTestCase {
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
        Date current = new Date();
        final String jobId = "0000000-" + current.getTime() + "-TestSLARegGetJPAExecutor-W";
        _addRecordToSLARegistrationTable(jobId, AppType.WORKFLOW_JOB, current, new Date(), "END_MISS",
                "alert@example.com");
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        SLARegistrationGetJPAExecutor readCmd = new SLARegistrationGetJPAExecutor(jobId);
        SLARegistrationBean bean = jpaService.execute(readCmd);
        assertEquals(jobId, bean.getId());
        assertEquals(AppType.WORKFLOW_JOB, bean.getAppType());
        assertEquals(current, bean.getExpectedStart());
        assertEquals(2, bean.getSlaConfigMap().size());
        assertEquals("END_MISS", bean.getAlertEvents());
        assertEquals("alert@example.com", bean.getAlertContact());
    }

    private void _addRecordToSLARegistrationTable(String jobId, AppType appType, Date start, Date end,
            String alertEvent, String alertContact) throws Exception {
        SLARegistrationBean reg = new SLARegistrationBean();
        reg.setId(jobId);
        reg.setAppType(appType);
        reg.setExpectedStart(start);
        reg.setExpectedEnd(end);
        reg.setAlertEvents(alertEvent);
        reg.setAlertContact(alertContact);
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            List<JsonBean> insert = new ArrayList<JsonBean>();
            insert.add(reg);
            BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(insert, null, null);
        }
        catch (JPAExecutorException je) {
            fail("Unable to insert the test sla registration record to table");
            throw je;
        }
    }

}
