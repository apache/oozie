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

import java.util.Date;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.client.event.SLAEvent.SLAStatus;
import org.apache.oozie.executor.jpa.SLARegistrationQueryExecutor.SLARegQuery;
import org.apache.oozie.executor.jpa.SLASummaryQueryExecutor.SLASummaryQuery;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.SLASummaryBean;
import org.apache.oozie.test.XDataTestCase;

public class TestSLASummaryQueryExecutor extends XDataTestCase {
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

    public void testGetQuery() throws Exception {
        EntityManager em = jpaService.getEntityManager();
        SLASummaryBean bean = addRecordToSLASummaryTable("test-sla-summary", SLAStatus.IN_PROCESS);
        // GET_SLA_SUMMARY
        Query query = SLASummaryQueryExecutor.getInstance().getSelectQuery(SLASummaryQuery.GET_SLA_SUMMARY, em,
                bean.getId());
        assertEquals(query.getParameterValue("id"), bean.getId());
        // GET_SLA_SUMMARY_EVENTPROCESSED
        query = SLASummaryQueryExecutor.getInstance().getSelectQuery(SLASummaryQuery.GET_SLA_SUMMARY_EVENTPROCESSED,
                em, bean.getId());
        assertEquals(query.getParameterValue("id"), bean.getId());
    }

    public void testUpdateQuery() throws Exception {
        EntityManager em = jpaService.getEntityManager();
        SLASummaryBean bean = addRecordToSLASummaryTable("test-sla-summary", SLAStatus.IN_PROCESS);

        // UPDATE_SLA_SUMMARY_FOR_STATUS_ACTUAL_TIMES
        Query query = SLASummaryQueryExecutor.getInstance().getUpdateQuery(
                SLASummaryQuery.UPDATE_SLA_SUMMARY_FOR_STATUS_ACTUAL_TIMES, bean, em);

        assertEquals(query.getParameterValue("jobId"), bean.getId());
        assertEquals(query.getParameterValue("slaStatus"), bean.getSLAStatus().toString());
        assertEquals(query.getParameterValue("lastModifiedTS"), bean.getLastModifiedTimestamp());
        assertEquals(query.getParameterValue("eventStatus"), bean.getEventStatus().toString());
        assertEquals(query.getParameterValue("jobStatus"), bean.getJobStatus());
        assertEquals(query.getParameterValue("eventProcessed"), bean.getEventProcessed());
        assertEquals(query.getParameterValue("actualStartTS"), bean.getActualStartTimestamp());
        assertEquals(query.getParameterValue("actualEndTS"), bean.getActualEndTimestamp());
        assertEquals(query.getParameterValue("actualDuration"), bean.getActualDuration());

        // UPDATE_SLA_SUMMARY_REGISTRATION
        query = SLASummaryQueryExecutor.getInstance().getUpdateQuery(SLASummaryQuery.UPDATE_SLA_SUMMARY_ALL, bean, em);

        assertEquals(query.getParameterValue("appName"), bean.getAppName());
        assertEquals(query.getParameterValue("appType"), bean.getAppType().toString());
        assertEquals(query.getParameterValue("nominalTime"), bean.getNominalTimestamp());
        assertEquals(query.getParameterValue("expectedStartTime"), bean.getExpectedStartTimestamp());
        assertEquals(query.getParameterValue("expectedEndTime"), bean.getExpectedEndTimestamp());
        assertEquals(query.getParameterValue("expectedDuration"), bean.getExpectedDuration());
        assertEquals(query.getParameterValue("jobStatus"), bean.getJobStatus());
        assertEquals(query.getParameterValue("slaStatus"), bean.getSLAStatus().toString());
        assertEquals(query.getParameterValue("eventStatus"), bean.getEventStatus().toString());
        assertEquals(query.getParameterValue("lastModTime"), bean.getLastModifiedTimestamp());
        assertEquals(query.getParameterValue("user"), bean.getUser());
        assertEquals(query.getParameterValue("parentId"), bean.getParentId());
        assertEquals(query.getParameterValue("eventProcessed"), bean.getEventProcessed());
        assertEquals(query.getParameterValue("actualDuration"), bean.getActualDuration());
        assertEquals(query.getParameterValue("actualEndTS"), bean.getActualEndTimestamp());
        assertEquals(query.getParameterValue("actualStartTS"), bean.getActualStartTimestamp());
        assertEquals(query.getParameterValue("jobId"), bean.getId());

        em.close();
    }

    public void testExecuteUpdate() throws Exception {
        SLASummaryBean bean = addRecordToSLASummaryTable("test-sla-summary", SLAStatus.IN_PROCESS);
        SLASummaryBean retBean = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY,
                bean.getId());
        Date createdTime = retBean.getCreatedTime();
        assertNotNull(createdTime);
        Date startTime = new Date(System.currentTimeMillis() - 1000 * 3600 * 2);
        Date endTime = new Date(System.currentTimeMillis() - 1000 * 3600 * 1);
        bean.setActualStart(startTime);
        bean.setActualEnd(endTime);
        bean.setSLAStatus(SLAStatus.MET);
        bean.setCreatedTime(startTime); // Should not be updated
        SLASummaryQueryExecutor.getInstance().executeUpdate(SLASummaryQuery.UPDATE_SLA_SUMMARY_ALL, bean);
        retBean = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY,
                bean.getId());
        assertEquals(bean.getActualStartTimestamp(), retBean.getActualStartTimestamp());
        assertEquals(bean.getActualEndTimestamp(), retBean.getActualEndTimestamp());
        assertEquals(SLAStatus.MET, retBean.getSLAStatus());
        assertEquals(createdTime, retBean.getCreatedTime()); // Created time should not be updated

        //test UPDATE_SLA_SUMMARY_FOR_ACTUAL_TIMES
        bean = addRecordToSLASummaryTable("test-sla-summary", SLAStatus.IN_PROCESS);
        bean.setActualStart(startTime);
        bean.setActualStart(endTime);
        bean.setActualDuration(endTime.getTime() - startTime.getTime());
        bean.setLastModifiedTime(new Date());
        bean.setEventProcessed(8);
        SLASummaryQueryExecutor.getInstance().executeUpdate(SLASummaryQuery.UPDATE_SLA_SUMMARY_FOR_ACTUAL_TIMES, bean);
        retBean = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, bean.getId());
        assertEquals(bean.getActualStartTimestamp(), retBean.getActualStartTimestamp());
        assertEquals(bean.getActualEndTimestamp(), retBean.getActualEndTimestamp());
        assertEquals(bean.getActualDuration(), retBean.getActualDuration());
        assertEquals(bean.getLastModifiedTimestamp(), retBean.getLastModifiedTimestamp());
        assertEquals(bean.getEventProcessed(), retBean.getEventProcessed());
    }

    public void testGet() throws Exception {
        SLASummaryBean bean = addRecordToSLASummaryTable("test-sla-summary", SLAStatus.IN_PROCESS);
        //GET_SLA_REG_ON_RESTART
        SLASummaryBean sBean = SLASummaryQueryExecutor.getInstance().get(
                SLASummaryQuery.GET_SLA_SUMMARY_EVENTPROCESSED, bean.getId());
        assertEquals(bean.getEventProcessed(), sBean.getEventProcessed());
    }

    public void testGetValue() throws Exception {
        SLASummaryBean bean = addRecordToSLASummaryTable("test-sla-summary", SLAStatus.IN_PROCESS);
        //GET_SLA_REG_ON_RESTART
        Object ret  = ((SLASummaryQueryExecutor) SLASummaryQueryExecutor.getInstance()).getSingleValue(
                SLASummaryQuery.GET_SLA_SUMMARY_EVENTPROCESSED, bean.getId());
        assertEquals(bean.getEventProcessed(), ((Byte)ret).byteValue());
    }
    public void testGetList() throws Exception {
        // TODO
    }

    public void testInsert() throws Exception {
        // TODO
    }
}
