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
import org.apache.oozie.client.event.SLAEvent.EventStatus;
import org.apache.oozie.client.event.SLAEvent.SLAStatus;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.util.DateUtils;
import org.json.simple.JSONArray;

import java.util.Collections;
import java.util.Date;

public class TestV2SLAServletSLAJSONResponse extends V2SLAServletTestCase {

    private Date currentTime;
    private Date nominalTime1;
    private Date nominalTime2;
    private Date nominalTime3;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        currentTime = new Date(System.currentTimeMillis());
        nominalTime1 = DateUtils.parseDateUTC("2012-06-01T10:00Z");
        nominalTime2 = DateUtils.parseDateUTC("2012-06-02T10:20Z");
        nominalTime3 = DateUtils.parseDateUTC("2012-06-03T14:00Z");
        setUpSLASummaryTableForTestSLA();
    }

    private void setUpSLASummaryTableForTestSLA() throws JPAExecutorException {
        insertEntriesIntoSLASummaryTable(2, "1-", "-W", "1-C", nominalTime1, "test_app-1", AppType.WORKFLOW_JOB,
                currentTime, EventStatus.END_MISS, SLAStatus.IN_PROCESS);
        insertEntriesIntoSLASummaryTable(3, "2-", "-W", null, nominalTime2, "test_app-2", AppType.WORKFLOW_JOB,
                currentTime, EventStatus.END_MISS, SLAStatus.IN_PROCESS);
        insertEntriesIntoSLASummaryTable(6, "3-", "-W", "2-C", nominalTime3, "test_app-3", AppType.WORKFLOW_JOB,
                currentTime, EventStatus.END_MISS, SLAStatus.IN_PROCESS);
    }

    public void testSLAAppName() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("app_name", "test_app-1");
        JSONArray array = getSLAJSONResponse(filterParams);
        // Matches first two - 1-1-W and 1-2-W
        assertSLAJSONResponse(array, 1, 2, "1-", "-W", "1-C", nominalTime1, "test_app-1", AppType.WORKFLOW_JOB,
                currentTime);
    }

    public void testSLAAppNameId() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("app_name", "test_app-2");
        filterParams.put("id", "2-2-W");
        JSONArray array = getSLAJSONResponse(filterParams);
        // Matches second element - 2-2-W
        assertSLAJSONResponse(array, 2, 2, "2-", "-W", null, nominalTime2, "test_app-2", AppType.WORKFLOW_JOB,
                currentTime);
    }

    public void testSLAAppNameNominalStart() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("app_name", "test_app-3");
        filterParams.put("nominal_after", "2012-06-03T16:00Z");
        JSONArray array = getSLAJSONResponse(filterParams);
        // Matches 3-6 elements - 3-3-W 3-4-W 3-5-W 3-6-W
        assertSLAJSONResponse(array, 3, 6, "3-", "-W", "2-C", nominalTime3, "test_app-3", AppType.WORKFLOW_JOB,
                currentTime);
    }

    public void testSLAAppNameNominalEnd() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("parent_id", "2-C");
        filterParams.put("nominal_after", "2012-06-03T16:00Z");
        filterParams.put("nominal_before", "2012-06-03T17:00Z");
        JSONArray array = getSLAJSONResponse(filterParams);
        // Matches 3rd and 4th element - 3-3-W 3-4-W
        assertSLAJSONResponse(array, 3, 4, "3-", "-W", "2-C", nominalTime3, "test_app-3", AppType.WORKFLOW_JOB,
                currentTime);
    }

    public void testSLAAppNameCreatedStart() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("app_name", "test_app-3");
        filterParams.put("created_after", "2012-06-03T16:00Z");
        JSONArray array = getSLAJSONResponse(filterParams);
        // No matches
        assertEquals("sla filter result size for app_name + created_after", 0, array.size());
    }

    public void testSLAAppNameCreatedEnd() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("parent_id", "2-C");
        filterParams.put("created_after", "2012-06-03T16:00Z");
        filterParams.put("created_before", "2012-06-03T17:00Z");
        JSONArray array = getSLAJSONResponse(filterParams);
        // No matches
        assertEquals("sla filter result size for parent_id + created_after + created_before", 0, array.size());
    }

    public void testSLAAppNameExpectedStart() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("app_name", "test_app-3");
        filterParams.put("expectedstart_after", "2012-06-03T16:00Z");
        JSONArray array = getSLAJSONResponse(filterParams);
        // Matches 3-6 elements - 3-3-W 3-4-W 3-5-W 3-6-W
        assertSLAJSONResponse(array, 3, 6, "3-", "-W", "2-C", nominalTime3, "test_app-3", AppType.WORKFLOW_JOB,
                currentTime);
    }

    public void testSLAParentIdExpectedStartInterval() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("parent_id", "2-C");
        filterParams.put("expectedstart_after", "2012-06-03T16:00Z");
        filterParams.put("expectedstart_before", "2012-06-03T17:00Z");
        JSONArray array = getSLAJSONResponse(filterParams);
        // Matches 3rd and 4th element - 3-3-W 3-4-W
        assertSLAJSONResponse(array, 3, 4, "3-", "-W", "2-C", nominalTime3, "test_app-3", AppType.WORKFLOW_JOB,
                currentTime);
    }

    public void testSLAAppNameExpectedEndStart() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("app_name", "test_app-3");
        filterParams.put("expectedend_after", "2012-06-03T17:00Z");
        JSONArray array = getSLAJSONResponse(filterParams);
        // Matches 3-6 elements - 3-3-W 3-4-W 3-5-W 3-6-W
        assertSLAJSONResponse(array, 3, 6, "3-", "-W", "2-C", nominalTime3, "test_app-3", AppType.WORKFLOW_JOB,
                currentTime);
    }

    public void testSLASLAParentIdExpectedEndInterval() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("parent_id", "2-C");
        filterParams.put("expectedend_after", "2012-06-03T15:00Z");
        filterParams.put("expectedend_before", "2012-06-03T16:00Z");
        JSONArray array = getSLAJSONResponse(filterParams);
        // Matches 1st and 2nd element - 3-3-W 3-4-W
        assertSLAJSONResponse(array, 1, 2, "3-", "-W", "2-C", nominalTime3, "test_app-3", AppType.WORKFLOW_JOB,
                currentTime);
    }

    public void testSLAAppNameActualStartStart() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("app_name", "test_app-3");
        filterParams.put("actualstart_after", "2012-06-03T16:00Z");
        JSONArray array = getSLAJSONResponse(filterParams);
        // Matches 3-6 elements - 3-3-W 3-4-W 3-5-W 3-6-W
        assertSLAJSONResponse(array, 3, 6, "3-", "-W", "2-C", nominalTime3, "test_app-3", AppType.WORKFLOW_JOB,
                currentTime);
    }

    public void testSLASLAParentIdActualStartInterval() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("parent_id", "2-C");
        filterParams.put("actualstart_after", "2012-06-03T16:00Z");
        filterParams.put("actualstart_before", "2012-06-03T18:00Z");
        JSONArray array = getSLAJSONResponse(filterParams);
        // Matches 3rd and 4th element - 3-3-W 3-4-W
        assertSLAJSONResponse(array, 3, 4, "3-", "-W", "2-C", nominalTime3, "test_app-3", AppType.WORKFLOW_JOB,
                currentTime);
    }

    public void testSLAAppNameActualEndStart() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("app_name", "test_app-3");
        filterParams.put("actualend_after", "2012-06-03T16:30Z");

        JSONArray array = getSLAJSONResponse(filterParams);
        // Matches 3-6 elements - 3-3-W 3-4-W 3-5-W 3-6-W
        assertSLAJSONResponse(array, 3, 6, "3-", "-W", "2-C", nominalTime3, "test_app-3", AppType.WORKFLOW_JOB,
                currentTime);
    }

    public void testSLASLAParentIdActualEndInterval() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("parent_id", "2-C");
        filterParams.put("actualend_after", "2012-06-03T16:30Z");
        filterParams.put("actualend_before", "2012-06-03T18:30Z");
        JSONArray array = getSLAJSONResponse(filterParams);
        // Matches 3rd and 4th element - 3-3-W 3-4-W
        assertSLAJSONResponse(array, 3, 4, "3-", "-W", "2-C", nominalTime3, "test_app-3", AppType.WORKFLOW_JOB,
                currentTime);
    }

    public void testSLAAppNameAppType() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("app_name", "test_app-1");
        filterParams.put("app_type", AppType.WORKFLOW_JOB.toString());
        JSONArray array = getSLAJSONResponse(filterParams);
        // Matches first two - 1-1-W and 1-2-W
        assertSLAJSONResponse(array, 1, 2, "1-", "-W", "1-C", nominalTime1, "test_app-1", AppType.WORKFLOW_JOB,
                currentTime);
    }

    public void testSLAAppNameUserName() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("app_name", "test_app-1");
        filterParams.put("user_name", "testuser");
        JSONArray array = getSLAJSONResponse(filterParams);
        // Matches first two - 1-1-W and 1-2-W
        assertSLAJSONResponse(array, 1, 2, "1-", "-W", "1-C", nominalTime1, "test_app-1", AppType.WORKFLOW_JOB,
                currentTime);
    }

    public void testSLAAppNameJobStatus() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("app_name", "test_app-3");
        filterParams.put("job_status", "RUNNING");
        JSONArray array = getSLAJSONResponse(filterParams);
        // Matches 1-6 elements - 3-1-W 3-2-W 3-3-W 3-4-W 3-5-W 3-6-W
        assertSLAJSONResponse(array, 1, 6, "3-", "-W", "2-C", nominalTime3, "test_app-3", AppType.WORKFLOW_JOB,
                currentTime);
    }

    public void testSLAAppNameJobStatusDescendingDefaultField() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("app_name", "test_app-3");
        filterParams.put("job_status", "RUNNING");
        JSONArray array = getSLAJSONResponse(null, filterParams, null, true);
        // Matches 1-6 elements i reverse order- 3-6-W 3-5-W 3-4-W 3-3-W 3-2-W 3-1-W
        Collections.reverse(array);
        assertSLAJSONResponse(array, 1, 6, "3-", "-W", "2-C", nominalTime3, "test_app-3", AppType.WORKFLOW_JOB,
                currentTime);
    }

    public void testSLAAppNameJobStatusAscendingJobId() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("app_name", "test_app-3");
        filterParams.put("job_status", "RUNNING");
        JSONArray array = getSLAJSONResponse(null, filterParams, "jobId", false);
        // Matches 1-6 elements - 3-1-W 3-2-W 3-3-W 3-4-W 3-5-W 3-6-W
        assertSLAJSONResponse(array, 1, 6, "3-", "-W", "2-C", nominalTime3, "test_app-3", AppType.WORKFLOW_JOB,
                currentTime);
    }

    public void testSLAAppNameJobStatusDescendingJobId() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("app_name", "test_app-3");
        filterParams.put("job_status", "RUNNING");
        JSONArray array = getSLAJSONResponse(null, filterParams, "jobId", true);
        // Matches 1-6 elements i reverse order- 3-6-W 3-5-W 3-4-W 3-3-W 3-2-W 3-1-W
        Collections.reverse(array);
        assertSLAJSONResponse(array, 1, 6, "3-", "-W", "2-C", nominalTime3, "test_app-3", AppType.WORKFLOW_JOB,
                currentTime);
    }

    public void testSLAAscendingAppName() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("job_status", "RUNNING");
        JSONArray array = getSLAJSONResponse(null, filterParams, "appName", false);
        // Matches all elements
        //   3-1-W 3-2-W 3-3-W 3-4-W 3-5-W 3-6-W
        //   2-1-W 2-2-W 2-3-W
        //   1-1-W and 1-2-W
        assertSLAJSONResponse(array, 0, 2, 1, 2, "1-", "-W", "1-C", nominalTime1, "test_app-1", AppType.WORKFLOW_JOB,
                currentTime);
        assertSLAJSONResponse(array, 2, 5,  1, 3, "2-", "-W", null, nominalTime2, "test_app-2", AppType.WORKFLOW_JOB,
                currentTime);
        assertSLAJSONResponse(array, 5, 11, 1, 6, "3-", "-W", "2-C", nominalTime3, "test_app-3", AppType.WORKFLOW_JOB,
                currentTime);
    }

    public void testSLALikeAppNamePercentSign() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("app_name", "test_app%");
        filterParams.put("job_status", "RUNNING");
        JSONArray array = getSLAJSONResponse(null, filterParams, "appName", false);
        // Matches all elements
        //   3-1-W 3-2-W 3-3-W 3-4-W 3-5-W 3-6-W
        //   2-1-W 2-2-W 2-3-W
        //   1-1-W and 1-2-W
        assertSLAJSONResponse(array, 0, 2, 1, 2, "1-", "-W", "1-C", nominalTime1, "test_app-1", AppType.WORKFLOW_JOB,
                currentTime);
        assertSLAJSONResponse(array, 2, 5,  1, 3, "2-", "-W", null, nominalTime2, "test_app-2", AppType.WORKFLOW_JOB,
                currentTime);
        assertSLAJSONResponse(array, 5, 11, 1, 6, "3-", "-W", "2-C", nominalTime3, "test_app-3", AppType.WORKFLOW_JOB,
                currentTime);
    }

    public void testSLALikeAppNameLikeUnderscore1() throws Exception {
        ListMultimap<String, String> filterParams = LinkedListMultimap.create();
        filterParams.put("app_name", "_est_app-1");
        JSONArray array = getSLAJSONResponse(filterParams);
        assertEquals( "JSON array size", 2, array.size());
        assertSLAJSONResponse(array, 0, 2, 1, 2, "1-", "-W", "1-C", nominalTime1, "test_app-1", AppType.WORKFLOW_JOB,
                currentTime);
    }

}
