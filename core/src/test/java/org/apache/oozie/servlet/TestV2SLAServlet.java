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

import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.servlet.http.HttpServletResponse;

import org.apache.oozie.AppType;
import org.apache.oozie.client.event.SLAEvent.EventStatus;
import org.apache.oozie.client.event.SLAEvent.SLAStatus;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.SLASummaryBean;
import org.apache.oozie.util.DateUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class TestV2SLAServlet extends DagServletTestCase {

    private static final boolean IS_SECURITY_ENABLED = false;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    public void testSLA() throws Exception {
        runTest("/v2/sla", V2SLAServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {

                final Date currentTime = new Date(System.currentTimeMillis());
                final Date nominalTime1 = DateUtils.parseDateUTC("2012-06-01T10:00Z");
                final Date nominalTime2 = DateUtils.parseDateUTC("2012-06-02T10:20Z");
                final Date nominalTime3 = DateUtils.parseDateUTC("2012-06-03T14:00Z");
                insertEntriesIntoSLASummaryTable(2, "1-", "-W", "1-C", nominalTime1, "testapp-1", AppType.WORKFLOW_JOB,
                        currentTime);
                insertEntriesIntoSLASummaryTable(3, "2-", "-W", null, nominalTime2, "testapp-2", AppType.WORKFLOW_JOB,
                        currentTime);
                insertEntriesIntoSLASummaryTable(6, "3-", "-W", "2-C", nominalTime3, "testapp-3", AppType.WORKFLOW_JOB,
                        currentTime);

                Map<String, String> queryParams = new HashMap<String, String>();
                JSONArray array = null;

                URL url = createURL("", queryParams);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());

                queryParams.put(RestConstants.JOBS_FILTER_PARAM, "app_name=testapp-1");
                array = getSLAJSONResponse(queryParams);
                // Matches first two - 1-1-W and 1-2-W
                assertSLAJSONResponse(array, 1, 2, "1-", "-W", "1-C", nominalTime1, "testapp-1", AppType.WORKFLOW_JOB,
                        currentTime);

                queryParams.put(RestConstants.JOBS_FILTER_PARAM, "app_name=testapp-2;id=2-2-W");
                array = getSLAJSONResponse(queryParams);
                // Matches second element - 2-2-W
                assertSLAJSONResponse(array, 2, 2, "2-", "-W", null, nominalTime2, "testapp-2", AppType.WORKFLOW_JOB,
                        currentTime);

                queryParams.put(RestConstants.JOBS_FILTER_PARAM, "app_name=testapp-3;nominal_start=2012-06-03T16:00Z");
                array = getSLAJSONResponse(queryParams);
                // Matches 3-6 elements - 3-3-W 3-4-W 3-5-W 3-6-W
                assertSLAJSONResponse(array, 3, 6, "3-", "-W", "2-C", nominalTime3, "testapp-3", AppType.WORKFLOW_JOB,
                        currentTime);

                queryParams.put(RestConstants.JOBS_FILTER_PARAM,
                        "parent_id=2-C;nominal_start=2012-06-03T016:00Z;nominal_end=2012-06-03T17:00Z");
                array = getSLAJSONResponse(queryParams);
                // Matches 3rd and 4th element - 3-3-W 3-4-W
                assertSLAJSONResponse(array, 3, 4, "3-", "-W", "2-C", nominalTime3, "testapp-3", AppType.WORKFLOW_JOB,
                        currentTime);

                return null;
            }
        });
    }

    private JSONArray getSLAJSONResponse(Map<String, String> queryParams) throws Exception {
        URL url = createURL("", queryParams);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
        assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
        JSONObject json = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
        JSONArray array = (JSONArray) json.get(JsonTags.SLA_SUMMARY_LIST);
        return array;
    }

    private void assertSLAJSONResponse(JSONArray array, int startRange, int endRange, String jobIDPrefix,
            String jobIDSuffix, String parentId, Date startNominalTime, String appName, AppType appType,
            Date currentTime) throws Exception {
        Calendar nominalTime = Calendar.getInstance();
        nominalTime.setTime(startNominalTime);
        nominalTime.add(Calendar.HOUR, (startRange - 1));
        int index = 0;
        assertEquals(endRange - (startRange - 1), array.size());
        for (int i = startRange; i <= endRange; i++) {
            Calendar actualStart = (Calendar) nominalTime.clone();
            actualStart.add(Calendar.MINUTE, i);
            Calendar expectedEnd = (Calendar) nominalTime.clone();
            expectedEnd.add(Calendar.MINUTE, 60);
            Calendar actualEnd = (Calendar) expectedEnd.clone();
            actualEnd.add(Calendar.MINUTE, i);
            JSONObject json = (JSONObject) array.get(index++);
            assertEquals(jobIDPrefix + i + jobIDSuffix, json.get(JsonTags.SLA_SUMMARY_ID));
            assertEquals(parentId, json.get(JsonTags.SLA_SUMMARY_PARENT_ID));
            assertEquals(appName, json.get(JsonTags.SLA_SUMMARY_APP_NAME));
            assertEquals(appType.name(), json.get(JsonTags.SLA_SUMMARY_APP_TYPE));
            assertEquals("RUNNING", json.get(JsonTags.SLA_SUMMARY_JOB_STATUS));
            assertEquals(SLAStatus.IN_PROCESS.name(), json.get(JsonTags.SLA_SUMMARY_SLA_STATUS));
            assertEquals(nominalTime.getTimeInMillis(), json.get(JsonTags.SLA_SUMMARY_NOMINAL_TIME));
            assertEquals(nominalTime.getTimeInMillis(), json.get(JsonTags.SLA_SUMMARY_EXPECTED_START));
            assertEquals(actualStart.getTimeInMillis(), json.get(JsonTags.SLA_SUMMARY_ACTUAL_START));
            assertEquals(expectedEnd.getTimeInMillis(), json.get(JsonTags.SLA_SUMMARY_EXPECTED_END));
            assertEquals(actualEnd.getTimeInMillis(), json.get(JsonTags.SLA_SUMMARY_ACTUAL_END));
            assertEquals(10L, json.get(JsonTags.SLA_SUMMARY_EXPECTED_DURATION));
            assertEquals(15L, json.get(JsonTags.SLA_SUMMARY_ACTUAL_DURATION));
            assertEquals(currentTime.getTime(), json.get(JsonTags.SLA_SUMMARY_LAST_MODIFIED));
            nominalTime.add(Calendar.HOUR, 1);
        }
    }

    private void insertEntriesIntoSLASummaryTable(int numEntries, String jobIDPrefix, String jobIDSuffix,
            String parentId, Date startNominalTime, String appName, AppType appType, Date currentTime)
            throws JPAExecutorException {
        List<JsonBean> list = new ArrayList<JsonBean>();
        Calendar nominalTime = Calendar.getInstance();
        nominalTime.setTime(startNominalTime);
        for (int i = 1; i <= numEntries; i++) {
            Calendar actualStart = (Calendar) nominalTime.clone();
            actualStart.add(Calendar.MINUTE, i);
            Calendar expectedEnd = (Calendar) nominalTime.clone();
            expectedEnd.add(Calendar.MINUTE, 60);
            Calendar actualEnd = (Calendar) expectedEnd.clone();
            actualEnd.add(Calendar.MINUTE, i);
            SLASummaryBean bean = new SLASummaryBean();
            bean.setId(jobIDPrefix + i + jobIDSuffix);
            bean.setParentId(parentId);
            bean.setAppName(appName);
            bean.setAppType(appType);
            bean.setJobStatus("RUNNING");
            bean.setEventStatus(EventStatus.END_MISS);
            bean.setSLAStatus(SLAStatus.IN_PROCESS);
            bean.setNominalTime(nominalTime.getTime());
            bean.setExpectedStart(nominalTime.getTime());
            bean.setActualStart(actualStart.getTime());
            bean.setExpectedEnd(expectedEnd.getTime());
            bean.setActualEnd(actualEnd.getTime());
            bean.setExpectedDuration(10);
            bean.setActualDuration(15);
            bean.setUser("testuser");
            bean.setLastModifiedTime(currentTime);
            list.add(bean);
            nominalTime.add(Calendar.HOUR, 1);
        }

        BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(list, null, null);
    }
}
