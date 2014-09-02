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

package org.apache.oozie.command.bundle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.oozie.BundleJobInfo;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestBundleJobsXCommand extends XDataTestCase {
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

    public void testBundleJobsGet() throws Exception {
        addRecordToBundleJobTable(Job.Status.PREP, false);
        addRecordToBundleJobTable(Job.Status.PREP, false);
        addRecordToBundleJobTable(Job.Status.RUNNING, false);
        addRecordToBundleJobTable(Job.Status.RUNNING, false);
        _testGetJobsForStatus();
        _testGetJobsForGroup();
        addRecordToBundleJobTable(Job.Status.KILLED, false);
        addRecordToBundleJobTable(Job.Status.SUCCEEDED, false);
        _testGetJobsForAppName();
        _testGetJobInfoForUser();
        _testGetJobsForUserAndStatus();
    }

    private void _testGetJobsForStatus() throws Exception {
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        List<String> list = new ArrayList<String>();
        list.add("RUNNING");
        list.add("PREP");
        filter.put(OozieClient.FILTER_STATUS, list);

        BundleJobsXCommand bundlesGetCmd = new BundleJobsXCommand(filter, 1, 20);
        BundleJobInfo ret = bundlesGetCmd.call();
        assertNotNull(ret);
        assertEquals(4, ret.getBundleJobs().size());
    }

    private void _testGetJobsForGroup() throws Exception {
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        List<String> list = new ArrayList<String>();
        list.add(getTestGroup());
        filter.put(OozieClient.FILTER_GROUP, list);

        BundleJobsXCommand bundlesGetCmd = new BundleJobsXCommand(filter, 1, 20);
        BundleJobInfo ret = bundlesGetCmd.call();
        assertNotNull(ret);
        assertEquals(4, ret.getBundleJobs().size());
    }

    private void _testGetJobsForAppName() throws Exception {
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        List<String> list = new ArrayList<String>();
        list.add("BUNDLE-TEST");
        filter.put(OozieClient.FILTER_NAME, list);

        BundleJobsXCommand bundlesGetCmd = new BundleJobsXCommand(filter, 1, 20);
        BundleJobInfo ret = bundlesGetCmd.call();
        assertNotNull(ret);
        assertEquals(6, ret.getBundleJobs().size());
    }

    private void _testGetJobInfoForUser() throws Exception {
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        List<String> list = new ArrayList<String>();
        list.add(getTestUser());
        filter.put(OozieClient.FILTER_USER, list);

        BundleJobsXCommand bundlesGetCmd = new BundleJobsXCommand(filter, 1, 20);
        BundleJobInfo ret = bundlesGetCmd.call();
        assertNotNull(ret);
        assertEquals(6, ret.getBundleJobs().size());
    }

    private void _testGetJobsForUserAndStatus() throws Exception {
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        List<String> list1 = new ArrayList<String>();
        list1.add(getTestUser());
        filter.put(OozieClient.FILTER_USER, list1);
        List<String> list2 = new ArrayList<String>();
        list2.add("KILLED");
        filter.put(OozieClient.FILTER_STATUS, list2);

        BundleJobsXCommand bundlesGetCmd = new BundleJobsXCommand(filter, 1, 20);
        BundleJobInfo ret = bundlesGetCmd.call();
        assertNotNull(ret);
        assertEquals(1, ret.getBundleJobs().size());
    }

}
