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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.CoordinatorJobInfo;
import org.apache.oozie.XException;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.store.StoreStatusFilter;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.ErrorCode;

public class TestCoordJobInfoGetJPAExecutor extends XDataTestCase {
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

    public void testCoordJobGet() throws Exception {
        CoordinatorJobBean coordinatorJob1 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        addRecordToCoordJobTable(CoordinatorJob.Status.KILLED, false, false);
        _testGetJobInfoForStatus();
        _testGetJobInfoForGroup();
        addRecordToCoordJobTable(CoordinatorJob.Status.KILLED, false, false);
        _testGetJobInfoForAppName();
        _testGetJobInfoForUser();
        _testGetJobInfoForUserAndStatus();
        _testGetJobInfoForFrequency();
        _testGetJobInfoForId(coordinatorJob1.getId());
        _testGetJobInfoForFrequencyAndUnit();
    }

    public void testGetJobInfoForStartCreatedTime() throws Exception {
        CoordinatorJobBean coordinatorJob1 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        CoordinatorJobBean coordinatorJob2 = addRecordToCoordJobTable(CoordinatorJob.Status.KILLED, false, false);
        Date createTime1 = DateUtils.parseDateUTC("2012-01-01T10:00Z");
        Date createTime2 = DateUtils.parseDateUTC("2012-01-05T10:00Z");
        coordinatorJob1.setCreatedTime(createTime1);
        coordinatorJob2.setCreatedTime(createTime2);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQueryExecutor.CoordJobQuery.UPDATE_COORD_JOB, coordinatorJob1);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQueryExecutor.CoordJobQuery.UPDATE_COORD_JOB, coordinatorJob2);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        CoordJobInfoGetJPAExecutor coordInfoGetCmd = new CoordJobInfoGetJPAExecutor(filter, 1, 20);
        CoordinatorJobInfo ret = jpaService.execute(coordInfoGetCmd);
        assertNotNull(ret);
        assertEquals(2, ret.getCoordJobs().size());
        filter.clear();

        filter.put(OozieClient.FILTER_CREATED_TIME_START, Arrays.asList("2012-01-02T10:00Z"));
        coordInfoGetCmd = new CoordJobInfoGetJPAExecutor(filter, 1, 20);
        ret = jpaService.execute(coordInfoGetCmd);
        assertNotNull(ret);
        assertEquals(1, ret.getCoordJobs().size());
        CoordinatorJobBean jobBean = ret.getCoordJobs().get(0);
        assertEquals(coordinatorJob2.getStatus(), jobBean.getStatus());
        assertEquals(coordinatorJob2.getCreatedTime(), jobBean.getCreatedTime());
    }

    public void testGetJobInfoForEndCreatedTime() throws Exception {
        CoordinatorJobBean coordinatorJob1 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        CoordinatorJobBean coordinatorJob2 = addRecordToCoordJobTable(CoordinatorJob.Status.KILLED, false, false);
        CoordinatorJobBean coordinatorJob3 = addRecordToCoordJobTable(CoordinatorJob.Status.FAILED, false, false);
        Date createTime1 = DateUtils.parseDateUTC("2012-01-03T10:00Z");
        Date createTime2 = DateUtils.parseDateUTC("2012-01-05T10:00Z");
        Date createTime3 = DateUtils.parseDateUTC("2012-01-010T10:00Z");
        coordinatorJob1.setCreatedTime(createTime1);
        coordinatorJob2.setCreatedTime(createTime2);
        coordinatorJob3.setCreatedTime(createTime3);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQueryExecutor.CoordJobQuery.UPDATE_COORD_JOB, coordinatorJob1);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQueryExecutor.CoordJobQuery.UPDATE_COORD_JOB, coordinatorJob2);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQueryExecutor.CoordJobQuery.UPDATE_COORD_JOB, coordinatorJob3);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        CoordJobInfoGetJPAExecutor coordInfoGetCmd = new CoordJobInfoGetJPAExecutor(filter, 1, 20);
        CoordinatorJobInfo ret = jpaService.execute(coordInfoGetCmd);
        assertNotNull(ret);
        assertEquals(3, ret.getCoordJobs().size());
        filter.clear();

        filter.put(OozieClient.FILTER_CREATED_TIME_START, Arrays.asList("2012-01-02T10:00Z"));
        filter.put(OozieClient.FILTER_CREATED_TIME_END, Arrays.asList("2012-01-07T10:00Z"));
        coordInfoGetCmd = new CoordJobInfoGetJPAExecutor(filter, 1, 20);
        ret = jpaService.execute(coordInfoGetCmd);
        assertNotNull(ret);
        assertEquals(2, ret.getCoordJobs().size());
        // default, expected order of results is by createTime DESC
        CoordinatorJobBean jobBean = ret.getCoordJobs().get(0);
        assertEquals(coordinatorJob2.getStatus(), jobBean.getStatus());
        assertEquals(coordinatorJob2.getCreatedTime(), jobBean.getCreatedTime());
        CoordinatorJobBean jobBean1 = ret.getCoordJobs().get(1);
        assertEquals(coordinatorJob1.getStatus(), jobBean1.getStatus());
        assertEquals(coordinatorJob1.getCreatedTime(), jobBean1.getCreatedTime());
    }

    public void testGetJobInfoForWrongTimeFormat() throws Exception {
        addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        CoordJobInfoGetJPAExecutor coordInfoGetCmd = new CoordJobInfoGetJPAExecutor(filter, 1, 20);
        CoordinatorJobInfo ret = jpaService.execute(coordInfoGetCmd);
        assertNotNull(ret);
        assertEquals(2, ret.getCoordJobs().size());
        filter.clear();

        filter.put(OozieClient.FILTER_CREATED_TIME_START, Arrays.asList("2012-01-02T10:00"));
        coordInfoGetCmd = new CoordJobInfoGetJPAExecutor(filter, 1, 20);
        try {
            jpaService.execute(coordInfoGetCmd);
            fail("This should not happen. Check the createdTime passed.");
        } catch (JPAExecutorException e) {
            assertEquals(e.getErrorCode(), ErrorCode.E0302);
            assertTrue(e.getMessage().contains(StoreStatusFilter.TIME_FORMAT));
        }
    }

    private void _testGetJobInfoForStatus() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        List<String> list = new ArrayList<String>();
        list.add("RUNNING");
        list.add("KILLED");
        filter.put(OozieClient.FILTER_STATUS, list);

        CoordJobInfoGetJPAExecutor coordInfoGetCmd = new CoordJobInfoGetJPAExecutor(filter, 1, 20);
        CoordinatorJobInfo ret = jpaService.execute(coordInfoGetCmd);
        assertNotNull(ret);
        assertEquals(ret.getCoordJobs().size(), 2);
    }

    private void _testGetJobInfoForGroup() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        List<String> list = new ArrayList<String>();
        list.add(getTestGroup());
        filter.put(OozieClient.FILTER_GROUP, list);

        CoordJobInfoGetJPAExecutor coordInfoGetCmd = new CoordJobInfoGetJPAExecutor(filter, 1, 20);
        CoordinatorJobInfo ret = jpaService.execute(coordInfoGetCmd);
        assertNotNull(ret);
        assertEquals(ret.getCoordJobs().size(), 2);
    }

    private void _testGetJobInfoForAppName() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        List<String> list = new ArrayList<String>();
        list.add("COORD-TEST");
        filter.put(OozieClient.FILTER_NAME, list);

        CoordJobInfoGetJPAExecutor coordInfoGetCmd = new CoordJobInfoGetJPAExecutor(filter, 1, 20);
        CoordinatorJobInfo ret = jpaService.execute(coordInfoGetCmd);
        assertNotNull(ret);
        assertEquals(ret.getCoordJobs().size(), 3);
    }

    private void _testGetJobInfoForUser() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        List<String> list = new ArrayList<String>();
        list.add(getTestUser());
        filter.put(OozieClient.FILTER_USER, list);

        CoordJobInfoGetJPAExecutor coordInfoGetCmd = new CoordJobInfoGetJPAExecutor(filter, 1, 20);
        CoordinatorJobInfo ret = jpaService.execute(coordInfoGetCmd);
        assertNotNull(ret);
        assertEquals(ret.getCoordJobs().size(), 3);
    }

    private void _testGetJobInfoForUserAndStatus() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        List<String> list1 = new ArrayList<String>();
        list1.add(getTestUser());
        filter.put(OozieClient.FILTER_USER, list1);
        List<String> list2 = new ArrayList<String>();
        list2.add("KILLED");
        filter.put(OozieClient.FILTER_STATUS, list2);

        CoordJobInfoGetJPAExecutor coordInfoGetCmd = new CoordJobInfoGetJPAExecutor(filter, 1, 20);
        CoordinatorJobInfo ret = jpaService.execute(coordInfoGetCmd);
        assertNotNull(ret);
        assertEquals(ret.getCoordJobs().size(), 2);
    }

    public void testGetJobInfoForText() throws Exception {
        CoordinatorJobBean coordinatorJob1 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        CoordinatorJobBean coordinatorJob2 = addRecordToCoordJobTable(CoordinatorJob.Status.KILLED, false, false);

        coordinatorJob1.setAppName("oozie-coord1");
        coordinatorJob2.setAppName("oozie-coord2");

        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQueryExecutor.CoordJobQuery.UPDATE_COORD_JOB, coordinatorJob1);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQueryExecutor.CoordJobQuery.UPDATE_COORD_JOB, coordinatorJob2);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        CoordJobInfoGetJPAExecutor coordInfoGetCmd = new CoordJobInfoGetJPAExecutor(filter, 1, 20);
        CoordinatorJobInfo ret = jpaService.execute(coordInfoGetCmd);
        assertNotNull(ret);
        assertEquals(2, ret.getCoordJobs().size());
        filter.clear();

        ArrayList<String> textList = new ArrayList<String>();
        textList.add("tes");
        filter.put(OozieClient.FILTER_TEXT, textList);
        coordInfoGetCmd = new CoordJobInfoGetJPAExecutor(filter, 1, 20);
        ret = jpaService.execute(coordInfoGetCmd);
        assertNotNull(ret);
        assertEquals(ret.getCoordJobs().size(), 2);

        textList.clear();
        textList.add("oozie-coord1");
        filter.put(OozieClient.FILTER_TEXT, textList);
        coordInfoGetCmd = new CoordJobInfoGetJPAExecutor(filter, 1, 20);
        ret = jpaService.execute(coordInfoGetCmd);
        assertNotNull(ret);
        assertEquals(ret.getCoordJobs().size(), 1);

        textList.clear();
        textList.add("random");
        filter.put(OozieClient.FILTER_TEXT, textList);
        coordInfoGetCmd = new CoordJobInfoGetJPAExecutor(filter, 1, 20);
        ret = jpaService.execute(coordInfoGetCmd);
        assertNotNull(ret);
        assertEquals(ret.getCoordJobs().size(), 0);

        textList.clear();
        textList.add("oozie-coord");
        filter.put(OozieClient.FILTER_TEXT, textList);
        coordInfoGetCmd = new CoordJobInfoGetJPAExecutor(filter, 1, 20);
        ret = jpaService.execute(coordInfoGetCmd);
        assertNotNull(ret);
        assertEquals(ret.getCoordJobs().size(), 2);

        textList.add("tes");
        filter.put(OozieClient.FILTER_TEXT, textList);
        coordInfoGetCmd = new CoordJobInfoGetJPAExecutor(filter, 1, 20);
        try{
            jpaService.execute(coordInfoGetCmd);
            fail("CoordJobInfoGetJPAExecutor should have thrown E0302 exception.");
        } catch (XException e) {
            assertEquals(ErrorCode.E0302, e.getErrorCode());
            assertEquals(e.getMessage(), "E0302: Invalid parameter [cannot specify multiple strings to search]");
        }

        // Update coord appName and validate text filter
        coordinatorJob1.setAppName("updated-app-name1");
        coordinatorJob2.setAppName("updated-app-name2");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQueryExecutor.CoordJobQuery.UPDATE_COORD_JOB, coordinatorJob1);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQueryExecutor.CoordJobQuery.UPDATE_COORD_JOB, coordinatorJob2);

        textList.clear();
        textList.add("oozie-coord");
        filter.put(OozieClient.FILTER_TEXT, textList);
        coordInfoGetCmd = new CoordJobInfoGetJPAExecutor(filter, 1, 20);
        ret = jpaService.execute(coordInfoGetCmd);
        assertNotNull(ret);
        assertEquals(ret.getCoordJobs().size(), 0);

        textList.clear();
        textList.add("updated-app");
        filter.put(OozieClient.FILTER_TEXT, textList);
        coordInfoGetCmd = new CoordJobInfoGetJPAExecutor(filter, 1, 20);
        ret = jpaService.execute(coordInfoGetCmd);
        assertNotNull(ret);
        assertEquals(ret.getCoordJobs().size(), 2);

        textList.clear();
        textList.add("updated-app-name1");
        coordInfoGetCmd = new CoordJobInfoGetJPAExecutor(filter, 1, 20);
        ret = jpaService.execute(coordInfoGetCmd);
        assertNotNull(ret);
        assertEquals(ret.getCoordJobs().size(), 1);
    }

    public void testGetJobInfoForTextAndStatus() throws Exception {
        CoordinatorJobBean coordinatorJob = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        coordinatorJob.setAppName("coord-job-1");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQueryExecutor.CoordJobQuery.UPDATE_COORD_JOB, coordinatorJob);

        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        List<String> textFilterList = new ArrayList<String>();
        textFilterList.add("coord-job-1");
        List<String> textStatusList = new ArrayList<String>();
        textStatusList.add(CoordinatorJob.Status.RUNNING.toString());
        filter.put(OozieClient.FILTER_TEXT, textFilterList);
        filter.put(OozieClient.FILTER_STATUS, textStatusList);

        JPAService jpaService = Services.get().get(JPAService.class);
        CoordJobInfoGetJPAExecutor coordInfoGetCmd = new CoordJobInfoGetJPAExecutor(filter, 1, 20);
        CoordinatorJobInfo coordJobsInfo = jpaService.execute(coordInfoGetCmd);
        assertEquals(1, coordJobsInfo.getCoordJobs().size());
    }

    private void _testGetJobInfoForFrequency() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        List<String> frequencyList = new ArrayList<String>();
        frequencyList.add("1");
        filter.put(OozieClient.FILTER_FREQUENCY, frequencyList);

        CoordJobInfoGetJPAExecutor coordInfoGetCmd = new CoordJobInfoGetJPAExecutor(filter, 1, 20);
        CoordinatorJobInfo ret = jpaService.execute(coordInfoGetCmd);
        assertNotNull(ret);
        assertEquals(ret.getCoordJobs().size(), 3);
    }

    private void _testGetJobInfoForId(String jobId) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        List<String> jobIdList = new ArrayList<String>();
        jobIdList.add(jobId);
        filter.put(OozieClient.FILTER_ID, jobIdList);

        CoordJobInfoGetJPAExecutor coordInfoGetCmd = new CoordJobInfoGetJPAExecutor(filter, 1, 20);
        CoordinatorJobInfo ret = jpaService.execute(coordInfoGetCmd);
        assertNotNull(ret);
        assertEquals(ret.getCoordJobs().size(), 1);
    }

    /**
     * Test to verify various combinations of frequency and time unit filters for jobs
     *
     * @throws Exception
     */
    private void _testGetJobInfoForFrequencyAndUnit() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        // Test specifying frequency value as 1 minute
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        List<String> unitList = new ArrayList<String>();
        List<String> frequencyList = new ArrayList<String>();
        unitList.add("MINUTE");
        filter.put(OozieClient.FILTER_UNIT, unitList);
        frequencyList = new ArrayList<String>();
        frequencyList.add("1");
        filter.put(OozieClient.FILTER_FREQUENCY, frequencyList);
        CoordJobInfoGetJPAExecutor coordInfoGetCmd = new CoordJobInfoGetJPAExecutor(filter, 1, 20);
        CoordinatorJobInfo ret = jpaService.execute(coordInfoGetCmd);
        assertNotNull(ret);
        assertEquals(ret.getCoordJobs().size(), 0);
        frequencyList.remove(0);
        unitList.remove(0);

        // Test specifying frequency value as 3 days
        unitList.add("DAY");
        filter.put(OozieClient.FILTER_UNIT, unitList);
        frequencyList.add("3");
        filter.put(OozieClient.FILTER_FREQUENCY, frequencyList);
        coordInfoGetCmd = new CoordJobInfoGetJPAExecutor(filter, 1, 20);
        ret = jpaService.execute(coordInfoGetCmd);
        assertNotNull(ret);
        assertEquals(ret.getCoordJobs().size(), 0);
        frequencyList.remove(0);
        unitList.remove(0);

        // Test specifying frequency value as 1 day
        unitList.add("DAY");
        filter.put(OozieClient.FILTER_UNIT, unitList);
        frequencyList.add("1");
        filter.put(OozieClient.FILTER_FREQUENCY, frequencyList);
        coordInfoGetCmd = new CoordJobInfoGetJPAExecutor(filter, 1, 20);
        ret = jpaService.execute(coordInfoGetCmd);
        assertNotNull(ret);
        assertEquals(ret.getCoordJobs().size(), 3);
    }

    public void testCoordGetJobsSortBy() throws Exception {
        CoordinatorJobBean coordinatorJob1 = addRecordToCoordJobTable(CoordinatorJob.Status.FAILED, false, false);
        CoordinatorJobBean coordinatorJob2 = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);

        coordinatorJob1.setLastModifiedTime(DateUtils.parseDateUTC("2012-01-04T10:00Z"));
        coordinatorJob1.setCreatedTime(DateUtils.parseDateUTC("2012-01-03T10:00Z"));
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQueryExecutor.CoordJobQuery.UPDATE_COORD_JOB, coordinatorJob1);

        coordinatorJob2.setLastModifiedTime(DateUtils.parseDateUTC("2012-01-05T10:00Z"));
        coordinatorJob2.setCreatedTime(DateUtils.parseDateUTC("2012-01-02T10:00Z"));
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQueryExecutor.CoordJobQuery.UPDATE_COORD_JOB, coordinatorJob2);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        List<String> list = new ArrayList<String>();
        list.add("lastmodifiedtime");
        filter.put(OozieClient.FILTER_SORT_BY, list);
        CoordJobInfoGetJPAExecutor coordInfoGetCmd = new CoordJobInfoGetJPAExecutor(filter, 1, 20);
        CoordinatorJobInfo ret = jpaService.execute(coordInfoGetCmd);
        assertNotNull(ret);
        assertEquals(2, ret.getCoordJobs().size());
        compareCoordJobs(coordinatorJob2, ret.getCoordJobs().get(0));
        //test default behavior
        filter.clear();
        list.clear();
        coordInfoGetCmd = new CoordJobInfoGetJPAExecutor(filter, 1, 20);
        ret = jpaService.execute(coordInfoGetCmd);
        assertNotNull(ret);
        assertEquals(2, ret.getCoordJobs().size());
        compareCoordJobs(coordinatorJob1, ret.getCoordJobs().get(0));
    }

    public void testGetJobInfoForBundleId() throws Exception {
        String bundleId = "0000000-171003192756908-oozie-test-B";
        String coordId = "0000000-171003192756999-oozie-test-C";
        addRecordToCoordJobTableWithBundle(bundleId, coordId, CoordinatorJob.Status.SUCCEEDED, true, true, 1);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        List<String> jobIdList = new ArrayList<String>();
        jobIdList.add(coordId);
        filter.put(OozieClient.FILTER_ID, jobIdList);

        CoordJobInfoGetJPAExecutor coordInfoGetCmd = new CoordJobInfoGetJPAExecutor(filter, 1, 20);
        CoordinatorJobInfo ret = jpaService.execute(coordInfoGetCmd);
        assertNotNull("CoordinatorJobInfo should not be null", ret);
        assertEquals("The number of coord jobs should be 1", 1, ret.getCoordJobs().size());
        assertEquals("Failed to verify bundle id of coord job", bundleId, ret.getCoordJobs().get(0).getBundleId());
    }

    private void compareCoordJobs(CoordinatorJobBean coordBean, CoordinatorJobBean retCoordBean) {
        assertEquals(coordBean.getId(), retCoordBean.getId());
        assertEquals(coordBean.getStatusStr(), retCoordBean.getStatusStr());
        assertEquals(coordBean.getCreatedTime(), retCoordBean.getCreatedTime());
    }
}
