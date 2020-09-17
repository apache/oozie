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

package org.apache.oozie.servlet;

import com.google.common.collect.ListMultimap;
import org.apache.oozie.AppType;
import org.apache.oozie.client.event.SLAEvent.EventStatus;
import org.apache.oozie.client.event.SLAEvent.SLAStatus;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.SLASummaryQueryExecutor;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.SLASummaryBean;
import org.apache.oozie.test.XDataTestCase;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Calendar;
import java.util.Date;

public abstract class V2SLAServletTestCase extends XDataTestCase {

    private V2SLAServlet v2SLAServlet;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        Services services = new Services();
        services.init();
        v2SLAServlet = new V2SLAServlet();
    }

    protected void insertEntriesIntoSLASummaryTable(int numEntries, String jobIDPrefix, String jobIDSuffix,
                                                    String parentId, Date startNominalTime, String appName, AppType appType,
                                                    Date currentTime, EventStatus eventStatus,
                                                    SLAStatus slaStatus) throws JPAExecutorException {
        Calendar nominalTime = Calendar.getInstance();
        nominalTime.setTime(startNominalTime);
        for (int i = 1; i <= numEntries; i++) {
            Calendar actualStart = (Calendar) nominalTime.clone();
            actualStart.add(Calendar.MINUTE, i);
            Calendar expectedEnd = (Calendar) nominalTime.clone();
            expectedEnd.add(Calendar.MINUTE, 60);
            Calendar actualEnd = (Calendar) expectedEnd.clone();
            actualEnd.add(Calendar.MINUTE, i);
            insertEntriesIntoSLASummaryTable(jobIDPrefix + i + jobIDSuffix, parentId, appName, appType, eventStatus,
                    slaStatus, nominalTime.getTime(), actualStart.getTime(), ((long) 10), ((long) 15),
                    expectedEnd.getTime(), actualEnd.getTime(), nominalTime.getTime());
            nominalTime.add(Calendar.HOUR, 1);
        }
    }

    protected void insertEntriesIntoSLASummaryTable(String jobID, String parentId, String appName, AppType appType,
                                                    EventStatus eventStatus, SLAStatus slaStatus, Date expectedStartTime,
                                                    Date actualStartTime, long expectedDuration, long actualDuration,
                                                    Date expectedEndTime, Date actualEndTime, Date nominalTime)
            throws JPAExecutorException {
        SLASummaryBean bean = new SLASummaryBean();
        bean.setId(jobID);
        bean.setParentId(parentId);
        bean.setAppName(appName);
        bean.setAppType(appType);
        bean.setJobStatus("RUNNING");
        bean.setEventStatus(eventStatus);
        bean.setSLAStatus(slaStatus);
        bean.setNominalTime(nominalTime);
        bean.setExpectedStart(expectedStartTime);
        bean.setActualStart(actualStartTime);
        bean.setExpectedDuration(expectedDuration);
        bean.setActualDuration(actualDuration);
        bean.setExpectedEnd(expectedEndTime);
        bean.setActualEnd(actualEndTime);
        bean.setUser("testuser");
        bean.setLastModifiedTime(Calendar.getInstance().getTime());
        SLASummaryQueryExecutor.getInstance().insert(bean);
    }

    protected JSONArray getSLAJSONResponse(ListMultimap<String, String> filterParams) throws Exception {
        return getSLAJSONResponse(null, filterParams);
    }

    protected JSONArray getSLAJSONResponse(String timeZoneId, ListMultimap<String, String> filterParams) throws Exception {
        return getSLAJSONResponse(timeZoneId, filterParams, null, false);
    }


    protected JSONArray getSLAJSONResponse(String timeZoneId, ListMultimap<String, String> filterParams, String sortbyColumn,
                                           boolean isDescendingOrder) throws Exception {
        JSONObject json = v2SLAServlet.getSLASummaryListByFilterParams(timeZoneId, 1000, filterParams, sortbyColumn,
                isDescendingOrder);
        JSONArray array = (JSONArray)json.get(JsonTags.SLA_SUMMARY_LIST);
        return array;
    }

    protected void assertSLAJSONResponse(JSONArray array, int startRange, int endRange, String jobIDPrefix,
                                         String jobIDSuffix, String parentId, Date startNominalTime, String appName,
                                         AppType appType, Date currentTime) {
        assertSLAJSONResponse(array, 0, array.size(), startRange, endRange, jobIDPrefix, jobIDSuffix, parentId, startNominalTime,
                appName, appType, currentTime);
    }

        protected void assertSLAJSONResponse(JSONArray array, int arrayStartIndex, int arrayEndIndex, int startRange, int endRange,
                                             String jobIDPrefix,
                                       String jobIDSuffix, String parentId, Date startNominalTime, String appName, AppType appType,
                                       Date currentTime) {
        Calendar nominalTime = Calendar.getInstance();
        nominalTime.setTime(startNominalTime);
        nominalTime.add(Calendar.HOUR, (startRange - 1));
        int index = arrayStartIndex;
        assertEquals("JSON array size", endRange - (startRange - 1), arrayEndIndex - arrayStartIndex);
        for (int i = startRange; i <= endRange; i++) {
            Calendar actualStart = (Calendar) nominalTime.clone();
            actualStart.add(Calendar.MINUTE, i);
            Calendar expectedEnd = (Calendar) nominalTime.clone();
            expectedEnd.add(Calendar.MINUTE, 60);
            Calendar actualEnd = (Calendar) expectedEnd.clone();
            actualEnd.add(Calendar.MINUTE, i);
            JSONObject json = (JSONObject) array.get(index++);
            assertEquals("id JSON tag", jobIDPrefix + i + jobIDSuffix, json.get(JsonTags.SLA_SUMMARY_ID));
            assertEquals("parentId JSON tag", parentId, json.get(JsonTags.SLA_SUMMARY_PARENT_ID));
            assertEquals("appName JSON tag", appName, json.get(JsonTags.SLA_SUMMARY_APP_NAME));
            assertEquals("appType JSON tag", appType.name(), json.get(JsonTags.SLA_SUMMARY_APP_TYPE));
            assertEquals("jobStatus JSON tag", "RUNNING", json.get(JsonTags.SLA_SUMMARY_JOB_STATUS));
            assertEquals("slaStatus JSON tag", SLAStatus.IN_PROCESS.name(), json.get(JsonTags.SLA_SUMMARY_SLA_STATUS));
            assertEquals("nominalTime JSON tag", nominalTime.getTimeInMillis(), json.get(JsonTags.SLA_SUMMARY_NOMINAL_TIME));
            assertEquals("expectedStart JSON tag", nominalTime.getTimeInMillis(), json.get(JsonTags.SLA_SUMMARY_EXPECTED_START));
            assertEquals("actualStart JSON tag", actualStart.getTimeInMillis(), json.get(JsonTags.SLA_SUMMARY_ACTUAL_START));
            assertEquals("expectedEnd JSON tag", expectedEnd.getTimeInMillis(), json.get(JsonTags.SLA_SUMMARY_EXPECTED_END));
            assertEquals("actualEnd JSON tag", actualEnd.getTimeInMillis(), json.get(JsonTags.SLA_SUMMARY_ACTUAL_END));
            assertEquals("expectedDuration JSON tag", 10L, json.get(JsonTags.SLA_SUMMARY_EXPECTED_DURATION));
            assertEquals("actualDuration JSON tag", 15L, json.get(JsonTags.SLA_SUMMARY_ACTUAL_DURATION));
            nominalTime.add(Calendar.HOUR, 1);
        }
    }

}
