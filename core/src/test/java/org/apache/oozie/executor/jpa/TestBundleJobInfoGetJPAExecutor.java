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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.BundleJobInfo;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

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

}
