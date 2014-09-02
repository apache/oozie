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
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.oozie.AppType;
import org.apache.oozie.FaultInjection;
import org.apache.oozie.client.event.SLAEvent.EventStatus;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.command.SkipCommitFaultInjection;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.BatchQueryExecutor.UpdateEntry;
import org.apache.oozie.executor.jpa.SLASummaryQueryExecutor;
import org.apache.oozie.executor.jpa.SLASummaryQueryExecutor.SLASummaryQuery;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Testcase to check db operations on SLA_SUMMARY table
 */
public class TestSLACalculationJPAExecutor extends XDataTestCase {
    Services services;
    Calendar cal;

    @Override
    @Before
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        cal = Calendar.getInstance();
    }

    @Override
    @After
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    /**
     * Test simple insert
     *
     * @throws Exception
     */
    @Test
    public void testInsert() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        String wfId = "workflow-1";

        cal.setTime(new Date());
        cal.add(Calendar.DAY_OF_MONTH, -2);
        Date expStart = cal.getTime();
        cal.add(Calendar.DAY_OF_MONTH, -1);
        Date expEnd = cal.getTime();
        Date actStart = new Date();
        cal.add(Calendar.DAY_OF_MONTH, 2);
        Date actEnd = cal.getTime();
        SLASummaryBean bean2 = _createSLASummaryBean(wfId, "RUNNING", EventStatus.START_MISS, expStart, expEnd, 1000,
                actStart, actEnd, 2000, (byte) 1, actEnd);

        List<JsonBean> insertList = new ArrayList<JsonBean>();
        insertList.add(bean2);
        BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(insertList, null, null);
        SLASummaryBean sBean = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, wfId);
        assertEquals(wfId, sBean.getId());
        assertEquals("RUNNING", sBean.getJobStatus());
        assertEquals(EventStatus.START_MISS, sBean.getEventStatus());
        assertEquals(expStart, sBean.getExpectedStart());
        assertEquals(expEnd, sBean.getExpectedEnd());
        assertEquals(1000, sBean.getExpectedDuration());
        assertEquals(actStart, sBean.getActualStart());
        assertEquals(actEnd, sBean.getActualEnd());
        assertEquals(2000, sBean.getActualDuration());
        assertEquals(actEnd, sBean.getLastModifiedTime());

    }

    /**
     * Test insert + update
     *
     * @throws Exception
     */
    @Test
    public void testInsertUpdate() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        String wfId = "workflow-1";
        // initial insert
        cal.setTime(new Date());
        cal.add(Calendar.DAY_OF_MONTH, -2);
        Date expStart = cal.getTime();
        cal.add(Calendar.DAY_OF_MONTH, -1);
        Date expEnd = cal.getTime();
        Date actStart = new Date();
        SLASummaryBean bean2 = _createSLASummaryBean(wfId, "RUNNING", EventStatus.START_MISS, expStart, expEnd, 1000,
                actStart, null, 2000, (byte) 0, actStart);
        List<JsonBean> insertList = new ArrayList<JsonBean>();
        insertList.add(bean2);
        BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(insertList, null, null);

        // update existing record
        Date newDate = new Date();
        bean2 = _createSLASummaryBean(wfId, "RUNNING", EventStatus.DURATION_MISS, expStart, expEnd, 1000, actStart,
                newDate, 2000, (byte) 1, newDate);
        bean2.setAppType(AppType.WORKFLOW_ACTION);
        List<UpdateEntry> updateList = new ArrayList<UpdateEntry>();
        SLASummaryQueryExecutor.getInstance().executeUpdate(SLASummaryQuery.UPDATE_SLA_SUMMARY_ALL, bean2);

        SLASummaryBean sBean = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, wfId);
        // check updated + original fields
        assertEquals(wfId, sBean.getId());
        assertEquals(EventStatus.DURATION_MISS, sBean.getEventStatus());
        assertEquals(expStart, sBean.getExpectedStart());
        assertEquals(expEnd, sBean.getExpectedEnd());
        assertEquals(1000, sBean.getExpectedDuration());
        assertEquals(actStart, sBean.getActualStart());
        assertEquals(newDate, sBean.getActualEnd());
        assertEquals(2000, sBean.getActualDuration());
        assertEquals(newDate, sBean.getLastModifiedTime());

    }

    /**
     * Test inserts and updates rollback
     *
     * @throws Exception
     */
    @Test
    public void testRollback() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        String wfId1 = "workflow-1";
        String wfId2 = "workflow-2";
        // initial insert
        SLASummaryBean bean1 = _createSLASummaryBean(wfId1, "RUNNING", EventStatus.START_MISS, new Date(), new Date(),
                1000, null, null, 2000, 0, null);
        List<JsonBean> list = new ArrayList<JsonBean>();
        list.add(bean1);
        BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(list, null, null);

        // update existing record and insert another
        Date newDate = new Date();
        bean1 = new SLASummaryBean();
        bean1.setId(wfId1);
        bean1.setActualEnd(newDate);
        List<UpdateEntry> updateList = new ArrayList<UpdateEntry>();
        updateList.add(new UpdateEntry<SLASummaryQuery>(SLASummaryQuery.UPDATE_SLA_SUMMARY_ALL,bean1));

        SLASummaryBean bean2 = _createSLASummaryBean(wfId2, "RUNNING", EventStatus.END_MISS, new Date(), new Date(),
                1000, null, null, 2000, 0, null);
        List<JsonBean> insertList = new ArrayList<JsonBean>();
        insertList.add(bean2);

        // set fault injection to true, so transaction is roll backed
        setSystemProperty(FaultInjection.FAULT_INJECTION, "true");
        setSystemProperty(SkipCommitFaultInjection.ACTION_FAILOVER_FAULT_INJECTION, "true");
        try {
            BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(insertList, updateList, null);
            fail("Expected exception due to commit failure but didn't get any");
        }
        catch (Exception e) {
        }
        FaultInjection.deactivate("org.apache.oozie.command.SkipCommitFaultInjection");

        // Check whether transactions are rolled back or not
        SLASummaryBean sBean = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, wfId1);

        // isSlaProcessed should NOT be changed to 1
        // actualEnd should be null as before
        assertNull(sBean.getActualEnd());

        sBean = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, wfId2);
        assertNull(sBean); //new bean should not have been inserted due to rollback

    }

    private SLASummaryBean _createSLASummaryBean(String jobId, String status, EventStatus slaType, Date eStart,
            Date eEnd, long eDur, Date aStart, Date aEnd, long aDur, int slaProc, Date lastMod) {
        SLASummaryBean bean = new SLASummaryBean();
        bean.setId(jobId);
        bean.setJobStatus(status);
        bean.setEventStatus(slaType);
        bean.setExpectedStart(eStart);
        bean.setExpectedEnd(eEnd);
        bean.setExpectedDuration(eDur);
        bean.setActualStart(aStart);
        bean.setActualEnd(aEnd);
        bean.setActualDuration(aDur);
        bean.setLastModifiedTime(lastMod);
        return bean;
    }

}
