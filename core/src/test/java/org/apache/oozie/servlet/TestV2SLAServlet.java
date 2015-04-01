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
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.event.SLAEvent.EventStatus;
import org.apache.oozie.client.event.SLAEvent.SLAStatus;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.SLASummaryQueryExecutor;
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
                        currentTime, EventStatus.END_MISS, SLAStatus.IN_PROCESS);
                insertEntriesIntoSLASummaryTable(3, "2-", "-W", null, nominalTime2, "testapp-2", AppType.WORKFLOW_JOB,
                        currentTime, EventStatus.END_MISS, SLAStatus.IN_PROCESS);
                insertEntriesIntoSLASummaryTable(6, "3-", "-W", "2-C", nominalTime3, "testapp-3", AppType.WORKFLOW_JOB,
                        currentTime, EventStatus.END_MISS, SLAStatus.IN_PROCESS);

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

    public void testBundleSLA() throws Exception {
        runTest("/v2/sla", V2SLAServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {

                //insert Bundle Job/Action, Coord Job/Action
                List<JsonBean> beans = new ArrayList<JsonBean> ();
                String bundleId = "0000000-000000000000000-"+ Services.get().getSystemId() + "-B";
                BundleJobBean bjBean = createBundleJob(bundleId,Job.Status.RUNNING, false);
                String bundleName = bjBean.getAppName();
                beans.add(bjBean);
                CoordinatorJobBean cjBean1 = createCoordJob(CoordinatorJob.Status.SUCCEEDED, false, true);
                beans.add(cjBean1);
                CoordinatorJobBean cjBean2 = createCoordJob(CoordinatorJob.Status.SUCCEEDED, false, true);
                beans.add(cjBean2);

                BundleActionBean baBean1 = createBundleAction(bundleId, cjBean1.getId(), "bundle-action-1", 0,
                        Job.Status.RUNNING);
                beans.add(baBean1);
                BundleActionBean baBean2 = createBundleAction(bundleId, cjBean2.getId(), "bundle-action-2", 0,
                        Job.Status.RUNNING);
                beans.add(baBean2);

                BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(beans, null, null);

                Calendar cal = Calendar.getInstance();
                cal.add(Calendar.MINUTE, -12);  //current -12
                Date actualStartForMet = cal.getTime();
                cal.add(Calendar.MINUTE, 2);   //current -10
                Date expectedStart = cal.getTime();
                cal.add(Calendar.MINUTE, 1);   //current -9
                Date actualStartForMiss = cal.getTime();
                cal.add(Calendar.MINUTE, 3);    //current -6
                Date actualEndForMet = cal.getTime();
                cal.add(Calendar.MINUTE, 1);    //current -5
                Date expectedEnd = cal.getTime();
                cal.add(Calendar.MINUTE, 2);    //current -3
                Date actualEndForMiss = cal.getTime();
                cal.add(Calendar.MINUTE, 8);   //current + 5
                Date futureExpectedEnd = cal.getTime();

                // START_MET, DURATION_MET, END_MET
                insertEntriesIntoSLASummaryTable(cjBean1.getId() + "@1", cjBean1.getId(), "testapp-1",
                        AppType.COORDINATOR_ACTION, EventStatus.END_MET, SLAStatus.MET, expectedStart,
                        actualStartForMet, 7, 6, expectedEnd, actualEndForMet, actualStartForMet);

                // START_MISS, DURATION_MISS, END_MISS
                insertEntriesIntoSLASummaryTable(cjBean1.getId() + "@2", cjBean1.getId(), "testapp-1",
                        AppType.COORDINATOR_ACTION, EventStatus.END_MISS, SLAStatus.MISS, expectedStart,
                        actualStartForMiss, 5, 6, expectedEnd, actualEndForMiss, actualStartForMet);

                // // START_MISS, DURATION_MISS (still running, Not Ended, but
                // expected Duration/End already passed by now)
                insertEntriesIntoSLASummaryTable(cjBean2.getId() + "@1", cjBean2.getId(), "testapp-2",
                        AppType.COORDINATOR_ACTION, EventStatus.DURATION_MISS, SLAStatus.IN_PROCESS, expectedStart,
                        actualStartForMiss, 8, 9, futureExpectedEnd, null, actualStartForMet);

                // START_MISS only, (Not Started YET, and Expected Duration/End
                // Time not yet passed)
                insertEntriesIntoSLASummaryTable(cjBean2.getId() + "@2", cjBean2.getId(), "testapp-2",
                        AppType.COORDINATOR_ACTION, null, SLAStatus.NOT_STARTED, expectedStart, null, 10, -1,
                        futureExpectedEnd, null, expectedStart);

                Map<String, String> queryParams = new HashMap<String, String>();
                JSONArray array = null;

                URL url = createURL("", queryParams);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());

                //test filter bundle ID
                queryParams.put(RestConstants.TIME_ZONE_PARAM, "GMT");
                queryParams.put(RestConstants.JOBS_FILTER_PARAM, String.format("bundle=%s",bundleId));
                array = getSLAJSONResponse(queryParams);
                assertEquals(4, array.size());
                for(int i=0; i < array.size(); i++) {
                    JSONObject json = (JSONObject) array.get(i);
                    String id = (String)json.get(JsonTags.SLA_SUMMARY_ID);
                    if(id.equals(cjBean1.getId() + "@1")) {
                        assertEquals(-2L, json.get(JsonTags.SLA_SUMMARY_START_DELAY));
                        assertEquals(-1L, json.get(JsonTags.SLA_SUMMARY_DURATION_DELAY));
                        assertEquals(-1L, json.get(JsonTags.SLA_SUMMARY_END_DELAY));
                    }
                }

                //test filter bundle Name
                queryParams.clear();
                queryParams.put(RestConstants.TIME_ZONE_PARAM, "GMT");
                queryParams.put(RestConstants.JOBS_FILTER_PARAM, String.format("bundle=%s",bundleName));
                array = getSLAJSONResponse(queryParams);
                assertEquals(4, array.size());

                //test filter bundle ID + EventStatus
                queryParams.clear();
                queryParams.put(RestConstants.TIME_ZONE_PARAM, "GMT");
                queryParams.put(RestConstants.JOBS_FILTER_PARAM, String.format("bundle=%s;event_status=END_MISS",bundleId));
                array = getSLAJSONResponse(queryParams);
                assertEquals(1, array.size());

                JSONObject json = (JSONObject) array.get(0);
                String parentId = (String) json.get(JsonTags.SLA_SUMMARY_PARENT_ID);
                assertTrue(parentId.equals(cjBean1.getId()) || parentId.equals(cjBean2.getId()));
                String id = (String) json.get(JsonTags.SLA_SUMMARY_ID);
                assertTrue(id.equals(cjBean1.getId() + "@2"));
                String es = (String) json.get(JsonTags.SLA_SUMMARY_EVENT_STATUS);
                assertTrue(es.contains(EventStatus.END_MISS.toString()));

                // test filter bundle ID + EventStatus + SlaStus
                queryParams.clear();
                queryParams.put(RestConstants.JOBS_FILTER_PARAM, String.format("bundle=%s;sla_status=MISS", bundleId));
                array = getSLAJSONResponse(queryParams);
                assertEquals(1, array.size());

                json = (JSONObject) array.get(0);
                id = (String) json.get(JsonTags.SLA_SUMMARY_ID);
                assertTrue(id.equals(cjBean1.getId() + "@2"));
                parentId = (String) json.get(JsonTags.SLA_SUMMARY_PARENT_ID);
                assertTrue(parentId.equals(cjBean1.getId()));
                assertEquals(1L, json.get(JsonTags.SLA_SUMMARY_START_DELAY));
                assertEquals(1L, json.get(JsonTags.SLA_SUMMARY_DURATION_DELAY));
                assertEquals(2L, json.get(JsonTags.SLA_SUMMARY_END_DELAY));

                //test filter bundleName + Multiple EventStatus
                queryParams.clear();
                queryParams.put(RestConstants.JOBS_FILTER_PARAM,
                        String.format("bundle=%s;event_status=START_MISS,END_MISS", bundleName));
                array = getSLAJSONResponse(queryParams);
                assertEquals(3, array.size());

                for(int i=0; i < array.size(); i++) {
                    json = (JSONObject) array.get(i);
                    id = (String)json.get(JsonTags.SLA_SUMMARY_ID);
                    assertTrue(id.equals(cjBean1.getId()+"@2") || id.equals(cjBean2.getId()+"@1") || id.equals(cjBean2.getId()+"@2"));
                    parentId = (String) json.get(JsonTags.SLA_SUMMARY_PARENT_ID);
                    assertTrue(parentId.equals(cjBean1.getId()) || parentId.equals(cjBean2.getId()));
                }

                //test filter bundleName + Multiple EventStatus + Multiple SlaStus
                queryParams.clear();
                queryParams.put(RestConstants.JOBS_FILTER_PARAM,
                        String.format("bundle=%s;event_status=DURATION_MISS;sla_status=IN_PROCESS", bundleName));
                array = getSLAJSONResponse(queryParams);
                assertEquals(1, array.size());
                json = (JSONObject) array.get(0);
                assertEquals(cjBean2.getId() + "@1", (String) json.get(JsonTags.SLA_SUMMARY_ID));
                assertEquals(cjBean2.getId(), (String) json.get(JsonTags.SLA_SUMMARY_PARENT_ID));
                String eventStatus = (String)json.get(JsonTags.SLA_SUMMARY_EVENT_STATUS);
                assertTrue(eventStatus.contains("DURATION_MISS"));
                assertTrue(eventStatus.contains("START_MISS"));
                assertFalse(eventStatus.contains("END_MISS") || eventStatus.contains("END_MET"));
                // actualDuration is null on DB while job is running, populates it in API call
                assertEquals(9L, json.get(JsonTags.SLA_SUMMARY_ACTUAL_DURATION));
                assertEquals(1L, json.get(JsonTags.SLA_SUMMARY_DURATION_DELAY));
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
            nominalTime.add(Calendar.HOUR, 1);
        }
    }

    private void insertEntriesIntoSLASummaryTable(int numEntries, String jobIDPrefix, String jobIDSuffix,
            String parentId, Date startNominalTime, String appName, AppType appType, Date currentTime,
            EventStatus eventStatus, SLAStatus slaStatus) throws JPAExecutorException {
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

    private void insertEntriesIntoSLASummaryTable(String jobID, String parentId, String appName, AppType appType,
            EventStatus eventStatus, SLAStatus slaStatus, Date expectedStartTime, Date actualStartTime,
            long expectedDuration, long actualDuration, Date expectedEndTime, Date actualEndTime, Date nominalTime)
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
}
