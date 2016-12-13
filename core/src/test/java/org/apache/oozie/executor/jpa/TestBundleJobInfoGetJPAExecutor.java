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

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.BundleJobInfo;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;

public class TestBundleJobInfoGetJPAExecutor extends XDataTestCase {
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

    public void testBundleJobInfoGet() throws Exception {
        BundleJobBean bundleJob1 = addRecordToBundleJobTable(Job.Status.PREP, false);
        addRecordToBundleJobTable(Job.Status.RUNNING, false);
        _testGetJobInfoForStatus();
        _testGetJobInfoForGroup();
        addRecordToBundleJobTable(Job.Status.KILLED, false);
        addRecordToBundleJobTable(Job.Status.SUCCEEDED, false);
        _testGetJobInfoForAppName();
        _testGetJobInfoForUser();
        _testGetJobInfoForUserAndStatus();
        _testGetJobInfoForId(bundleJob1.getId());
    }

    public void testGetJobInfoForStartCreatedTime() throws Exception {
        BundleJobBean bundleJob1 = addRecordToBundleJobTable(BundleJob.Status.PREP, false);
        BundleJobBean bundleJob2 = addRecordToBundleJobTable(BundleJob.Status.KILLED, false);
        Date createTime1 = DateUtils.parseDateUTC("2012-01-01T10:00Z");
        Date createTime2 = DateUtils.parseDateUTC("2012-01-05T10:00Z");
        bundleJob1.setCreatedTime(createTime1);
        bundleJob2.setCreatedTime(createTime2);
        BundleJobQueryExecutor.getInstance().executeUpdate(BundleJobQueryExecutor.BundleJobQuery.UPDATE_BUNDLE_JOB, bundleJob1);
        BundleJobQueryExecutor.getInstance().executeUpdate(BundleJobQueryExecutor.BundleJobQuery.UPDATE_BUNDLE_JOB, bundleJob2);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        BundleJobInfoGetJPAExecutor BundleInfoGetCmd = new BundleJobInfoGetJPAExecutor(filter, 1, 20);
        BundleJobInfo ret = jpaService.execute(BundleInfoGetCmd);
        assertNotNull(ret);
        assertEquals(2, ret.getBundleJobs().size());
        filter.clear();

        filter.put(OozieClient.FILTER_CREATED_TIME_START, Arrays.asList("2012-01-02T10:00Z"));
        BundleInfoGetCmd = new BundleJobInfoGetJPAExecutor(filter, 1, 20);
        ret = jpaService.execute(BundleInfoGetCmd);
        assertNotNull(ret);
        assertEquals(1, ret.getBundleJobs().size());
        BundleJobBean jobBean = ret.getBundleJobs().get(0);
        assertEquals(bundleJob2.getStatus(), jobBean.getStatus());
        assertEquals(bundleJob2.getCreatedTime(), jobBean.getCreatedTime());
    }

    public void testGetJobInfoForEndCreatedTime() throws Exception {
        BundleJobBean bundleJob1 = addRecordToBundleJobTable(BundleJob.Status.RUNNING, false);
        BundleJobBean bundleJob2 = addRecordToBundleJobTable(BundleJob.Status.KILLED, false);
        BundleJobBean bundleJob3 = addRecordToBundleJobTable(BundleJob.Status.FAILED, false);
        Date createTime1 = DateUtils.parseDateUTC("2012-01-03T10:00Z");
        Date createTime2 = DateUtils.parseDateUTC("2012-01-05T10:00Z");
        Date createTime3 = DateUtils.parseDateUTC("2012-01-010T10:00Z");
        bundleJob1.setCreatedTime(createTime1);
        bundleJob2.setCreatedTime(createTime2);
        bundleJob3.setCreatedTime(createTime3);
        BundleJobQueryExecutor.getInstance().executeUpdate(BundleJobQueryExecutor.BundleJobQuery.UPDATE_BUNDLE_JOB, bundleJob1);
        BundleJobQueryExecutor.getInstance().executeUpdate(BundleJobQueryExecutor.BundleJobQuery.UPDATE_BUNDLE_JOB, bundleJob2);
        BundleJobQueryExecutor.getInstance().executeUpdate(BundleJobQueryExecutor.BundleJobQuery.UPDATE_BUNDLE_JOB, bundleJob3);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        BundleJobInfoGetJPAExecutor bundleInfoGetCmd = new BundleJobInfoGetJPAExecutor(filter, 1, 20);
        BundleJobInfo ret = jpaService.execute(bundleInfoGetCmd);
        assertNotNull(ret);
        assertEquals(3, ret.getBundleJobs().size());
        filter.clear();

        filter.put(OozieClient.FILTER_CREATED_TIME_START, Arrays.asList("2012-01-02T10:00Z"));
        filter.put(OozieClient.FILTER_CREATED_TIME_END, Arrays.asList("2012-01-07T10:00Z"));
        bundleInfoGetCmd = new BundleJobInfoGetJPAExecutor(filter, 1, 20);
        ret = jpaService.execute(bundleInfoGetCmd);
        assertNotNull(ret);
        assertEquals(2, ret.getBundleJobs().size());
        // default, expected order of results is by createTime DESC
        BundleJobBean jobBean = ret.getBundleJobs().get(0);
        assertEquals(bundleJob2.getStatus(), jobBean.getStatus());
        assertEquals(bundleJob2.getCreatedTime(), jobBean.getCreatedTime());
        BundleJobBean jobBean1 = ret.getBundleJobs().get(1);
        assertEquals(bundleJob1.getStatus(), jobBean1.getStatus());
        assertEquals(bundleJob1.getCreatedTime(), jobBean1.getCreatedTime());
    }

    private void _testGetJobInfoForStatus() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        List<String> list = new ArrayList<String>();
        list.add("RUNNING");
        list.add("PREP");
        filter.put(OozieClient.FILTER_STATUS, list);

        BundleJobInfoGetJPAExecutor bundleInfoGetCmd = new BundleJobInfoGetJPAExecutor(filter, 1, 20);
        BundleJobInfo ret = jpaService.execute(bundleInfoGetCmd);
        assertNotNull(ret);
        assertEquals(2, ret.getBundleJobs().size());
    }

    public void testGetJobInfoForText() throws Exception {
        BundleJobBean bundleJob1 = addRecordToBundleJobTable(BundleJob.Status.RUNNING, false);
        BundleJobBean bundleJob2 = addRecordToBundleJobTable(BundleJob.Status.KILLED, false);

        bundleJob1.setAppName("oozie-bundle1");
        bundleJob2.setAppName("oozie-bundle2");

        BundleJobQueryExecutor.getInstance().executeUpdate(BundleJobQueryExecutor.BundleJobQuery.UPDATE_BUNDLE_JOB, bundleJob1);
        BundleJobQueryExecutor.getInstance().executeUpdate(BundleJobQueryExecutor.BundleJobQuery.UPDATE_BUNDLE_JOB, bundleJob2);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        BundleJobInfoGetJPAExecutor bundleInfoGetCmd = new BundleJobInfoGetJPAExecutor(filter, 1, 20);
        BundleJobInfo ret = jpaService.execute(bundleInfoGetCmd);
        assertNotNull(ret);
        assertEquals(2, ret.getBundleJobs().size());
        filter.clear();

        ArrayList<String> textList = new ArrayList<String>();
        textList.add("tes");
        filter.put(OozieClient.FILTER_TEXT, textList);
        bundleInfoGetCmd = new BundleJobInfoGetJPAExecutor(filter, 1, 20);
        ret = jpaService.execute(bundleInfoGetCmd);
        assertNotNull(ret);
        assertEquals(ret.getBundleJobs().size(), 2);

        textList.clear();
        textList.add("oozie-bundle1");
        filter.put(OozieClient.FILTER_TEXT, textList);
        bundleInfoGetCmd = new BundleJobInfoGetJPAExecutor(filter, 1, 20);
        ret = jpaService.execute(bundleInfoGetCmd);
        assertNotNull(ret);
        assertEquals(ret.getBundleJobs().size(), 1);

        textList.clear();
        textList.add("random");
        filter.put(OozieClient.FILTER_TEXT, textList);
        bundleInfoGetCmd = new BundleJobInfoGetJPAExecutor(filter, 1, 20);
        ret = jpaService.execute(bundleInfoGetCmd);
        assertNotNull(ret);
        assertEquals(ret.getBundleJobs().size(), 0);

        textList.clear();
        textList.add("oozie-bundle");
        filter.put(OozieClient.FILTER_TEXT, textList);
        bundleInfoGetCmd = new BundleJobInfoGetJPAExecutor(filter, 1, 20);
        ret = jpaService.execute(bundleInfoGetCmd);
        assertNotNull(ret);
        assertEquals(ret.getBundleJobs().size(), 2);

        textList.add("tes");
        filter.put(OozieClient.FILTER_TEXT, textList);
        bundleInfoGetCmd = new BundleJobInfoGetJPAExecutor(filter, 1, 20);
        try{
            jpaService.execute(bundleInfoGetCmd);
            fail("BundleJobInfoGetJPAExecutor should have thrown E0302 exception.");
        } catch (XException e) {
            assertEquals(ErrorCode.E0302, e.getErrorCode());
            assertEquals(e.getMessage(), "E0302: Invalid parameter [cannot specify multiple strings to search]");
        }

        // Update bundle appName, user and validate text filter
        bundleJob1.setAppName("updated-app-name1");
        bundleJob2.setAppName("updated-app-name2");
        BundleJobQueryExecutor.getInstance().executeUpdate(BundleJobQueryExecutor.BundleJobQuery.UPDATE_BUNDLE_JOB, bundleJob1);
        BundleJobQueryExecutor.getInstance().executeUpdate(BundleJobQueryExecutor.BundleJobQuery.UPDATE_BUNDLE_JOB, bundleJob2);

        textList.clear();
        textList.add("oozie-bundle");
        filter.put(OozieClient.FILTER_TEXT, textList);
        bundleInfoGetCmd = new BundleJobInfoGetJPAExecutor(filter, 1, 20);
        ret = jpaService.execute(bundleInfoGetCmd);
        assertNotNull(ret);
        assertEquals(ret.getBundleJobs().size(), 0);

        textList.clear();
        textList.add("updated-app");
        filter.put(OozieClient.FILTER_TEXT, textList);
        bundleInfoGetCmd = new BundleJobInfoGetJPAExecutor(filter, 1, 20);
        ret = jpaService.execute(bundleInfoGetCmd);
        assertNotNull(ret);
        assertEquals(ret.getBundleJobs().size(), 2);

        textList.clear();
        textList.add("updated-app-name1");
        bundleInfoGetCmd = new BundleJobInfoGetJPAExecutor(filter, 1, 20);
        ret = jpaService.execute(bundleInfoGetCmd);
        assertNotNull(ret);
        assertEquals(ret.getBundleJobs().size(), 1);
    }

    public void testGetJobInfoForTextAndStatus() throws Exception {
        BundleJobBean bundleJob = addRecordToBundleJobTable(BundleJob.Status.RUNNING, false);
        bundleJob.setAppName("bundle-job-1");
        BundleJobQueryExecutor.getInstance().executeUpdate(BundleJobQueryExecutor.BundleJobQuery.UPDATE_BUNDLE_JOB, bundleJob);

        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        List<String> textFilterList = new ArrayList<String>();
        textFilterList.add("bundle-job-1");
        List<String> textStatusList = new ArrayList<String>();
        textStatusList.add(BundleJob.Status.RUNNING.toString());
        filter.put(OozieClient.FILTER_TEXT, textFilterList);
        filter.put(OozieClient.FILTER_STATUS, textStatusList);

        JPAService jpaService = Services.get().get(JPAService.class);
        BundleJobInfoGetJPAExecutor bundleInfoGetCmd = new BundleJobInfoGetJPAExecutor(filter, 1, 20);
        BundleJobInfo bundleJobsInfo = jpaService.execute(bundleInfoGetCmd);
        assertEquals(1, bundleJobsInfo.getBundleJobs().size());
    }

    private void _testGetJobInfoForGroup() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        List<String> list = new ArrayList<String>();
        list.add(getTestGroup());
        filter.put(OozieClient.FILTER_GROUP, list);

        BundleJobInfoGetJPAExecutor bundleInfoGetCmd = new BundleJobInfoGetJPAExecutor(filter, 1, 20);
        BundleJobInfo ret = jpaService.execute(bundleInfoGetCmd);
        assertNotNull(ret);
        assertEquals(2, ret.getBundleJobs().size());
    }

    private void _testGetJobInfoForAppName() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        List<String> list = new ArrayList<String>();
        list.add("BUNDLE-TEST");
        filter.put(OozieClient.FILTER_NAME, list);

        BundleJobInfoGetJPAExecutor bundleInfoGetCmd = new BundleJobInfoGetJPAExecutor(filter, 1, 20);
        BundleJobInfo ret = jpaService.execute(bundleInfoGetCmd);
        assertNotNull(ret);
        assertEquals(4, ret.getBundleJobs().size());
    }

    private void _testGetJobInfoForUser() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        List<String> list = new ArrayList<String>();
        list.add(getTestUser());
        filter.put(OozieClient.FILTER_USER, list);

        BundleJobInfoGetJPAExecutor bundleInfoGetCmd = new BundleJobInfoGetJPAExecutor(filter, 1, 20);
        BundleJobInfo ret = jpaService.execute(bundleInfoGetCmd);
        assertNotNull(ret);
        assertEquals(4, ret.getBundleJobs().size());
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

        BundleJobInfoGetJPAExecutor bundleInfoGetCmd = new BundleJobInfoGetJPAExecutor(filter, 1, 20);
        BundleJobInfo ret = jpaService.execute(bundleInfoGetCmd);
        assertNotNull(ret);
        assertEquals(1, ret.getBundleJobs().size());
    }

    private void _testGetJobInfoForId(String jobId) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        List<String> jobIdList = new ArrayList<String>();
        jobIdList.add(jobId);
        filter.put(OozieClient.FILTER_ID, jobIdList);

        BundleJobInfoGetJPAExecutor bundleInfoGetCmd = new BundleJobInfoGetJPAExecutor(filter, 1, 20);
        BundleJobInfo ret = jpaService.execute(bundleInfoGetCmd);
        assertNotNull(ret);
        assertEquals(ret.getBundleJobs().size(), 1);
    }

    public void testBundleJobsSortBy() throws Exception {
        BundleJobBean bundleJob1 = addRecordToBundleJobTable(Job.Status.PREP, false);
        BundleJobBean bundleJob2 = addRecordToBundleJobTable(Job.Status.RUNNING, false);

        bundleJob1.setLastModifiedTime(DateUtils.parseDateUTC("2012-01-04T10:00Z"));
        bundleJob1.setCreatedTime(DateUtils.parseDateUTC("2012-01-03T10:00Z"));
        BundleJobQueryExecutor.getInstance().executeUpdate(BundleJobQueryExecutor.BundleJobQuery.UPDATE_BUNDLE_JOB, bundleJob1);

        bundleJob2.setLastModifiedTime(DateUtils.parseDateUTC("2012-01-05T10:00Z"));
        bundleJob2.setCreatedTime(DateUtils.parseDateUTC("2012-01-02T10:00Z"));
        BundleJobQueryExecutor.getInstance().executeUpdate(BundleJobQueryExecutor.BundleJobQuery.UPDATE_BUNDLE_JOB, bundleJob2);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        List<String> list = new ArrayList<String>();
        list.add("lastmodifiedTime");
        filter.put(OozieClient.FILTER_SORT_BY, list);

        BundleJobInfoGetJPAExecutor bundleInfoGetCmd = new BundleJobInfoGetJPAExecutor(filter, 1, 20);
        BundleJobInfo ret = jpaService.execute(bundleInfoGetCmd);
        assertNotNull(ret);
        assertEquals(2, ret.getBundleJobs().size());
        compareBundleJobs(bundleJob2, ret.getBundleJobs().get(0));
        //test the default behavior
        filter.clear();
        list.clear();
        bundleInfoGetCmd = new BundleJobInfoGetJPAExecutor(filter, 1, 20);
        ret = jpaService.execute(bundleInfoGetCmd);
        assertNotNull(ret);
        assertEquals(2, ret.getBundleJobs().size());
        compareBundleJobs(bundleJob1, ret.getBundleJobs().get(0));
    }

    private void compareBundleJobs(BundleJobBean bundleJobBean, BundleJobBean retBundleJobBean) {
        assertEquals(bundleJobBean.getId(), retBundleJobBean.getId());
        assertEquals(bundleJobBean.getCreatedTime(), retBundleJobBean.getCreatedTime());
        assertEquals(bundleJobBean.getStatusStr(), retBundleJobBean.getStatusStr());
    }
}
