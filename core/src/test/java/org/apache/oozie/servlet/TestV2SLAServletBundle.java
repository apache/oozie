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

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.oozie.AppType;
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.event.SLAEvent;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.service.Services;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class TestV2SLAServletBundle extends V2SLAServletTestCase {

    private String sampleBundleId;
    private String sampleBundleName;
    private CoordinatorJobBean cjBean1;
    private CoordinatorJobBean cjBean2;
    private Date actualStartForMet;
    private Date expectedStart;
    private Date actualStartForMiss;
    private Date actualEndForMet;
    private Date expectedEnd;
    private Date actualEndForMiss;
    private Date futureExpectedEnd;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MINUTE, -12);  //current -12
        actualStartForMet = cal.getTime();
        cal.add(Calendar.MINUTE, 2);   //current -10
        expectedStart = cal.getTime();
        cal.add(Calendar.MINUTE, 1);   //current -9
        actualStartForMiss = cal.getTime();
        cal.add(Calendar.MINUTE, 3);    //current -6
        actualEndForMet = cal.getTime();
        cal.add(Calendar.MINUTE, 1);    //current -5
        expectedEnd = cal.getTime();
        cal.add(Calendar.MINUTE, 2);    //current -3
        actualEndForMiss = cal.getTime();
        cal.add(Calendar.MINUTE, 8);   //current + 5
        futureExpectedEnd = cal.getTime();
        setUpBundle();
    }


    private void setUpBundle() throws Exception {
        //insert Bundle Job/Action, Coord Job/Action
        List<JsonBean> beans = new ArrayList<>();
        sampleBundleId = "0000000-000000000000000-" + Services.get().getSystemId() + "-B";
        BundleJobBean bjBean = createBundleJob(sampleBundleId, Job.Status.RUNNING, false);
        sampleBundleName = bjBean.getAppName();
        beans.add(bjBean);
        cjBean1 = createCoordJob(CoordinatorJob.Status.SUCCEEDED, false, true);
        beans.add(cjBean1);
        cjBean2 = createCoordJob(CoordinatorJob.Status.SUCCEEDED, false, true);
        beans.add(cjBean2);

        BundleActionBean baBean1 = createBundleAction(sampleBundleId, cjBean1.getId(), "bundle-action-1", 0,
                Job.Status.RUNNING);
        beans.add(baBean1);
        BundleActionBean baBean2 = createBundleAction(sampleBundleId, cjBean2.getId(), "bundle-action-2", 0,
                Job.Status.RUNNING);
        beans.add(baBean2);

        BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(beans, null, null);

        // START_MET, DURATION_MET, END_MET
        insertEntriesIntoSLASummaryTable(cjBean1.getId() + "@1", cjBean1.getId(), "testapp-1",
                AppType.COORDINATOR_ACTION, SLAEvent.EventStatus.END_MET, SLAEvent.SLAStatus.MET, expectedStart,
                actualStartForMet, 7, 6, expectedEnd, actualEndForMet, actualStartForMet);

        // START_MISS, DURATION_MISS, END_MISS
        insertEntriesIntoSLASummaryTable(cjBean1.getId() + "@2", cjBean1.getId(), "testapp-1",
                AppType.COORDINATOR_ACTION, SLAEvent.EventStatus.END_MISS, SLAEvent.SLAStatus.MISS, expectedStart,
                actualStartForMiss, 5, 6, expectedEnd, actualEndForMiss, actualStartForMet);

        // // START_MISS, DURATION_MISS (still running, Not Ended, but
        // expected Duration/End already passed by now)
        insertEntriesIntoSLASummaryTable(cjBean2.getId() + "@1", cjBean2.getId(), "testapp-2",
                AppType.COORDINATOR_ACTION, SLAEvent.EventStatus.DURATION_MISS, SLAEvent.SLAStatus.IN_PROCESS, expectedStart,
                actualStartForMiss, 8, 9, futureExpectedEnd, null, actualStartForMet);

        // START_MISS only, (Not Started YET, and Expected Duration/End
        // Time not yet passed)
        insertEntriesIntoSLASummaryTable(cjBean2.getId() + "@2", cjBean2.getId(), "testapp-2",
                AppType.COORDINATOR_ACTION, null, SLAEvent.SLAStatus.NOT_STARTED, expectedStart, null, 10, -1,
                futureExpectedEnd, null, expectedStart);
    }

    public void testNonExistentBundleId() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("bundle", "xxxx");
        JSONArray array = getSLAJSONResponse("GMT", filterParams);
        assertEquals( "JSON array size", 0, array.size());
    }

    public void testBundleId() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("bundle", sampleBundleId);
        JSONArray array = getSLAJSONResponse("GMT", filterParams);
        assertEquals(4, array.size());
        for (int i = 0; i < array.size(); i++) {
            JSONObject json = (JSONObject) array.get(i);
            String id = (String) json.get(JsonTags.SLA_SUMMARY_ID);
            if (id.equals(cjBean1.getId() + "@1")) {
                assertEquals("startDelay JSON tag", -2L, json.get(JsonTags.SLA_SUMMARY_START_DELAY));
                assertEquals("durationDelay JSON tag", 0L, json.get(JsonTags.SLA_SUMMARY_DURATION_DELAY));
                assertEquals("endDelay JSON tag",-1L, json.get(JsonTags.SLA_SUMMARY_END_DELAY));
            }
        }
    }

    public void testIdBundleId() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("id", cjBean2.getId() + "@1");
        filterParams.put("bundle", sampleBundleId);
        JSONArray array = getSLAJSONResponse("GMT", filterParams);
        assertEquals("sla filter result size for id + sampleBundleId", 1, array.size());
        for (int i = 0; i < array.size(); i++) {
            JSONObject json = (JSONObject) array.get(i);
            String id = (String) json.get(JsonTags.SLA_SUMMARY_ID);
            if (id.equals(cjBean1.getId() + "@1")) {
                assertEquals("id + sampleBundleId filter summary start delay",
                        -2L, json.get(JsonTags.SLA_SUMMARY_START_DELAY));
                assertEquals("id + sampleBundleId filter summary duration delay", 0L,
                        json.get(JsonTags.SLA_SUMMARY_DURATION_DELAY));
                assertEquals("id + sampleBundleId filter summary end delay",
                        -1L, json.get(JsonTags.SLA_SUMMARY_END_DELAY));
            }
        }
    }

    public void testNonMatchingParentIdBundleId() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("parent_id", "xxx");
        filterParams.put("bundle", sampleBundleId);
        JSONArray array = getSLAJSONResponse("GMT", filterParams);
        assertEquals("sla filter result size for parent_id + sampleBundleId", 0, array.size());
    }

    public void testBundleName() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("bundle", sampleBundleName);
        JSONArray array = getSLAJSONResponse("GMT", filterParams);
        assertEquals("JSON array size", 4, array.size());
        for (int i = 0; i < array.size(); i++) {
            JSONObject json = (JSONObject) array.get(i);
            String id = (String) json.get(JsonTags.SLA_SUMMARY_ID);
            if (id.equals(cjBean1.getId() + "@1")) {
                assertEquals("startDelay JSON tag", -2L, json.get(JsonTags.SLA_SUMMARY_START_DELAY));
                assertEquals("durationDelay JSON tag", 0L, json.get(JsonTags.SLA_SUMMARY_DURATION_DELAY));
                assertEquals("endDelay JSON tag", -1L, json.get(JsonTags.SLA_SUMMARY_END_DELAY));
            }
        }
    }

    public void testBundleSLAEventEventStatus() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("bundle", sampleBundleId);
        filterParams.put("event_status", "END_MISS");
        JSONArray array = getSLAJSONResponse("GMT", filterParams);
        assertEquals("JSON array size", 1, array.size());
        JSONObject json = (JSONObject) array.get(0);
        String parentId = (String) json.get(JsonTags.SLA_SUMMARY_PARENT_ID);
        assertTrue("Invalid parentId JSON tag", parentId.equals(cjBean1.getId()) || parentId.equals(cjBean2.getId()));
        String id = (String) json.get(JsonTags.SLA_SUMMARY_ID);
        assertEquals("id JSON tag", cjBean1.getId() + "@2", id);
        String es = (String) json.get(JsonTags.SLA_SUMMARY_EVENT_STATUS);
        assertTrue("eventStatus JSON tag should contain END_MISS", es.contains("END_MISS"));
    }

    public void testBundleSLAEventEventStatusStartMet() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("bundle", sampleBundleId);
        filterParams.put("event_status", "START_MET");
        JSONArray array = getSLAJSONResponse("GMT", filterParams);
        assertEquals("JSON array size", 1, array.size());
        JSONObject json = (JSONObject) array.get(0);
        String parentId = (String) json.get(JsonTags.SLA_SUMMARY_PARENT_ID);
        assertTrue("Invalid parentId JSON tag", parentId.equals(cjBean1.getId()) || parentId.equals(cjBean2.getId()));
        String id = (String) json.get(JsonTags.SLA_SUMMARY_ID);
        assertEquals("id JSON tag", cjBean1.getId() + "@1", id);
        String es = (String) json.get(JsonTags.SLA_SUMMARY_EVENT_STATUS);
        assertTrue("eventStatus JSON tag should contain START_MET", es.contains("START_MET"));
    }

    public void testBundleSLAEventSlaStatus() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("bundle", sampleBundleId);
        filterParams.put("sla_status", "MISS");
        JSONArray array = getSLAJSONResponse("GMT", filterParams);
        assertEquals("JSON array size", 1, array.size());
        JSONObject json = (JSONObject) array.get(0);
        String id = (String) json.get(JsonTags.SLA_SUMMARY_ID);
        assertEquals("id JSON tag", cjBean1.getId() + "@2", id);
        String parentId = (String) json.get(JsonTags.SLA_SUMMARY_PARENT_ID);
        assertEquals("parentId JSON tag", cjBean1.getId(), parentId);
        assertEquals("startDelay JSON tag", 1L, json.get(JsonTags.SLA_SUMMARY_START_DELAY));
        assertEquals("durationDelay JSON tag", 0L, json.get(JsonTags.SLA_SUMMARY_DURATION_DELAY));
        assertEquals("endDelay JSON tag", 2L, json.get(JsonTags.SLA_SUMMARY_END_DELAY));
    }

    public void testBundleSLAEventMultipleEventStatus() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("bundle", sampleBundleName);
        filterParams.put("event_status", "START_MISS");
        filterParams.put("event_status", "END_MISS");
        JSONArray array = getSLAJSONResponse(filterParams);
        assertEquals("JSON array size", 3, array.size());
        for (int i = 0; i < array.size(); i++) {
            JSONObject json = (JSONObject) array.get(i);
            String id = (String) json.get(JsonTags.SLA_SUMMARY_ID);
            assertTrue("invalid id JSON tag", id.equals(cjBean1.getId() + "@2") || id.equals(cjBean2.getId() + "@1")
                    || id.equals(cjBean2.getId() + "@2"));
            String parentId = (String) json.get(JsonTags.SLA_SUMMARY_PARENT_ID);
            assertTrue("invalid parentId JSON tag", parentId.equals(cjBean1.getId()) || parentId.equals(cjBean2.getId()));
        }
    }

    public void testBundleSLAEventEventStatusSlaStatus() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("bundle", sampleBundleName);
        filterParams.put("event_status", "DURATION_MISS");
        filterParams.put("sla_status", "IN_PROCESS");
        JSONArray array = getSLAJSONResponse(filterParams);
        assertEquals("JSON array size", 1, array.size());
        JSONObject json = (JSONObject) array.get(0);
        assertEquals("id JSON tag", cjBean2.getId() + "@1", (String) json.get(JsonTags.SLA_SUMMARY_ID));
        assertEquals("parentId JSON tag", cjBean2.getId(), (String) json.get(JsonTags.SLA_SUMMARY_PARENT_ID));
        String eventStatus = (String) json.get(JsonTags.SLA_SUMMARY_EVENT_STATUS);
        assertTrue("eventStatus JSON tag should contain DURATION_MISS", eventStatus.contains("DURATION_MISS"));
        assertTrue("eventStatus JSON tag should contain START_MISS", eventStatus.contains("START_MISS"));
        assertFalse("eventStatus JSON tag should contain END_MISS or END_MET",
                eventStatus.contains("END_MISS") || eventStatus.contains("END_MET"));
        // actualDuration is null on DB while job is running, populates it in API call
        assertEquals("actualDuration JSON tag", 9L, json.get(JsonTags.SLA_SUMMARY_ACTUAL_DURATION));
        assertEquals("durationDelay JSON tag", 0L, json.get(JsonTags.SLA_SUMMARY_DURATION_DELAY));
    }

}