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

import org.apache.oozie.ErrorCode;
import org.apache.oozie.FaultInjection;
import org.apache.oozie.client.event.SLAEvent.EventStatus;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.command.SkipCommitFaultInjection;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.sla.SLACalculationInsertUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.sla.SLACalculatorGetJPAExecutor;
import org.apache.oozie.executor.jpa.sla.SLASummaryGetJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Testcase to check db operations on tables SLA_CALCULATOR and SLA_SUMMARY
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
        cleanUpDBTables();
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
        SLACalculatorBean bean1 = _createSLACalcBean(wfId, false, false);

        cal.setTime(new Date());
        cal.add(Calendar.DAY_OF_MONTH, -2);
        Date expStart = cal.getTime();
        cal.add(Calendar.DAY_OF_MONTH, -1);
        Date expEnd = cal.getTime();
        Date actStart = new Date();
        cal.add(Calendar.DAY_OF_MONTH, 2);
        Date actEnd = cal.getTime();
        SLASummaryBean bean2 = _createSLASummaryBean(wfId, "RUNNING", EventStatus.START_MISS, expStart, expEnd, 1000,
                actStart, actEnd, 2000);

        List<JsonBean> list = new ArrayList<JsonBean>();
        list.add(bean1);
        list.add(bean2);
        SLACalculationInsertUpdateJPAExecutor writeCmd = new SLACalculationInsertUpdateJPAExecutor();
        writeCmd.setInsertList(list);
        jpaService.execute(writeCmd);

        SLACalculatorGetJPAExecutor readCmd1 = new SLACalculatorGetJPAExecutor(wfId);
        SLACalculatorBean scBean = jpaService.execute(readCmd1);
        assertNotNull(scBean);
        assertEquals(wfId, scBean.getJobId());
        assertFalse(scBean.isStartProcessed());
        assertFalse(scBean.isEndProcessed());

        SLASummaryGetJPAExecutor readCmd2 = new SLASummaryGetJPAExecutor(wfId);
        SLASummaryBean ssBean = jpaService.execute(readCmd2);
        assertEquals(wfId, ssBean.getJobId());
        assertEquals("RUNNING", ssBean.getJobStatus());
        assertEquals(EventStatus.START_MISS, ssBean.getEventStatus());
        assertEquals(expStart, ssBean.getExpectedStart());
        assertEquals(expEnd, ssBean.getExpectedEnd());
        assertEquals(1000, ssBean.getExpectedDuration());
        assertEquals(actStart, ssBean.getActualStart());
        assertEquals(actEnd, ssBean.getActualEnd());
        assertEquals(2000, ssBean.getActualDuration());

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
        SLACalculatorBean bean1 = _createSLACalcBean(wfId, false, false);

        cal.setTime(new Date());
        cal.add(Calendar.DAY_OF_MONTH, -2);
        Date expStart = cal.getTime();
        cal.add(Calendar.DAY_OF_MONTH, -1);
        Date expEnd = cal.getTime();
        Date actStart = new Date();
        SLASummaryBean bean2 = _createSLASummaryBean(wfId, "RUNNING", EventStatus.START_MISS, expStart, expEnd, 1000,
                actStart, null, 2000);
        List<JsonBean> list = new ArrayList<JsonBean>();
        list.add(bean1);
        list.add(bean2);
        SLACalculationInsertUpdateJPAExecutor writeCmd = new SLACalculationInsertUpdateJPAExecutor();
        writeCmd.setInsertList(list);
        jpaService.execute(writeCmd);

        // update existing record
        Date newDate = new Date();
        bean1 = _createSLACalcBean(wfId, true, true);
        bean2 = _createSLASummaryBean(wfId, "RUNNING", EventStatus.DURATION_MISS, expStart, expEnd, 1000, actStart,
                newDate, 2000);
        list = new ArrayList<JsonBean>();
        list.add(bean1);
        list.add(bean2);
        writeCmd.setUpdateList(list);
        writeCmd.setInsertList(null);
        jpaService.execute(writeCmd);

        SLACalculatorGetJPAExecutor readCmd1 = new SLACalculatorGetJPAExecutor(wfId);
        SLACalculatorBean scBean = jpaService.execute(readCmd1);
        assertNotNull(scBean);
        assertEquals(wfId, scBean.getJobId());
        assertTrue(scBean.isStartProcessed());
        assertTrue(scBean.isEndProcessed());

        SLASummaryGetJPAExecutor readCmd2 = new SLASummaryGetJPAExecutor(wfId);
        SLASummaryBean sdBean = jpaService.execute(readCmd2);
        // check updated + original fields
        assertEquals(wfId, sdBean.getJobId());
        assertEquals(EventStatus.DURATION_MISS, sdBean.getEventStatus());
        assertEquals(expStart, sdBean.getExpectedStart());
        assertEquals(expEnd, sdBean.getExpectedEnd());
        assertEquals(1000, sdBean.getExpectedDuration());
        assertEquals(actStart, sdBean.getActualStart());
        assertEquals(newDate, sdBean.getActualEnd());
        assertEquals(2000, sdBean.getActualDuration());

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
        SLACalculatorBean bean1 = _createSLACalcBean(wfId1, false, false);
        SLASummaryBean bean2 = _createSLASummaryBean(wfId1, "RUNNING", EventStatus.START_MISS, new Date(), new Date(),
                1000, null, null, 2000);
        List<JsonBean> list = new ArrayList<JsonBean>();
        list.add(bean1);
        list.add(bean2);
        SLACalculationInsertUpdateJPAExecutor writeCmd = new SLACalculationInsertUpdateJPAExecutor(list, null);
        jpaService.execute(writeCmd);

        // update existing record and insert another
        Date newDate = new Date();
        bean1 = _createSLACalcBean(wfId1, true, true);
        bean2 = new SLASummaryBean();
        bean2.setJobId(wfId1);
        bean2.setActualEnd(newDate);
        List<JsonBean> updateList = new ArrayList<JsonBean>();
        updateList.add(bean1);
        updateList.add(bean2);

        SLACalculatorBean bean3 = _createSLACalcBean(wfId2, false, false);
        SLASummaryBean bean4 = _createSLASummaryBean(wfId2, "RUNNING", EventStatus.END_MISS, new Date(), new Date(),
                1000, null, null, 2000);
        List<JsonBean> insertList = new ArrayList<JsonBean>();
        insertList.add(bean3);
        insertList.add(bean4);
        writeCmd = new SLACalculationInsertUpdateJPAExecutor(insertList, updateList);

        // set fault injection to true, so transaction is roll backed
        setSystemProperty(FaultInjection.FAULT_INJECTION, "true");
        setSystemProperty(SkipCommitFaultInjection.ACTION_FAILOVER_FAULT_INJECTION, "true");
        try {
            jpaService.execute(writeCmd);
            fail("Expected exception due to commit failure but didn't get any");
        }
        catch (Exception e) {
        }
        FaultInjection.deactivate("org.apache.oozie.command.SkipCommitFaultInjection");

        // Check whether transactions are rolled back or not
        SLACalculatorGetJPAExecutor readCmd1 = new SLACalculatorGetJPAExecutor(wfId1);
        SLACalculatorBean scBean = jpaService.execute(readCmd1);
        // isStartProcessed should NOT be toggled to true
        assertFalse(scBean.isStartProcessed());
        SLASummaryGetJPAExecutor readCmd2 = new SLASummaryGetJPAExecutor(wfId1);
        SLASummaryBean sdBean = jpaService.execute(readCmd2);
        // actualEnd should be null as before
        assertNull(sdBean.getActualEnd());

        readCmd1 = new SLACalculatorGetJPAExecutor(wfId2);
        try {
            scBean = jpaService.execute(readCmd1);
            fail("Expected exception but didnt get any");
        }
        catch (JPAExecutorException jpaee) {
            assertEquals(ErrorCode.E0603, jpaee.getErrorCode());
        }
    }

    private SLACalculatorBean _createSLACalcBean(String jobId, boolean startProc, boolean endProc) {
        SLACalculatorBean bean = new SLACalculatorBean();
        bean.setJobId(jobId);
        bean.setStartProcessed(startProc);
        bean.setEndProcessed(endProc);
        return bean;
    }

    private SLASummaryBean _createSLASummaryBean(String jobId, String status, EventStatus slaType, Date eStart,
            Date eEnd, long eDur, Date aStart, Date aEnd, long aDur) {
        SLASummaryBean bean = new SLASummaryBean();
        bean.setJobId(jobId);
        bean.setJobStatus(status);
        bean.setEventStatus(slaType);
        bean.setExpectedStart(eStart);
        bean.setExpectedEnd(eEnd);
        bean.setExpectedDuration(eDur);
        bean.setActualStart(aStart);
        bean.setActualEnd(aEnd);
        bean.setActualDuration(aDur);
        return bean;
    }
}
