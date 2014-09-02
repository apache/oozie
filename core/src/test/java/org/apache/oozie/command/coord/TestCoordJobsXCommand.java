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

package org.apache.oozie.command.coord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.oozie.CoordinatorJobInfo;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestCoordJobsXCommand extends XDataTestCase {
    Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        LocalOozie.start();
    }

    @Override
    protected void tearDown() throws Exception {
        LocalOozie.stop();
        services.destroy();
        super.tearDown();
    }

    public void testCoordJobsGet() throws Exception {
        addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        addRecordToCoordJobTable(CoordinatorJob.Status.KILLED, false, false);
        _testGetJobsForStatus();
        _testGetJobsForGroup();
        addRecordToCoordJobTable(CoordinatorJob.Status.KILLED, false, false);
        _testGetJobsForAppName();
        _testGetJobInfoForUser();
        _testGetJobsForUserAndStatus();
    }

    private void _testGetJobsForStatus() throws Exception {
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        List<String> list = new ArrayList<String>();
        list.add("RUNNING");
        list.add("KILLED");
        filter.put(OozieClient.FILTER_STATUS, list);

        CoordJobsXCommand coordsGetCmd = new CoordJobsXCommand(filter, 1, 20);
        CoordinatorJobInfo ret = coordsGetCmd.call();
        assertNotNull(ret);
        assertEquals(ret.getCoordJobs().size(), 2);
    }

    private void _testGetJobsForGroup() throws Exception {
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        List<String> list = new ArrayList<String>();
        list.add(getTestGroup());
        filter.put(OozieClient.FILTER_GROUP, list);

        CoordJobsXCommand coordsGetCmd = new CoordJobsXCommand(filter, 1, 20);
        CoordinatorJobInfo ret = coordsGetCmd.call();
        assertNotNull(ret);
        assertEquals(ret.getCoordJobs().size(), 2);
    }

    private void _testGetJobsForAppName() throws Exception {
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        List<String> list = new ArrayList<String>();
        list.add("COORD-TEST");
        filter.put(OozieClient.FILTER_NAME, list);

        CoordJobsXCommand coordsGetCmd = new CoordJobsXCommand(filter, 1, 20);
        CoordinatorJobInfo ret = coordsGetCmd.call();
        assertNotNull(ret);
        assertEquals(ret.getCoordJobs().size(), 3);
    }

    private void _testGetJobInfoForUser() throws Exception {
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        List<String> list = new ArrayList<String>();
        list.add(getTestUser());
        filter.put(OozieClient.FILTER_USER, list);

        CoordJobsXCommand coordsGetCmd = new CoordJobsXCommand(filter, 1, 20);
        CoordinatorJobInfo ret = coordsGetCmd.call();
        assertNotNull(ret);
        assertEquals(ret.getCoordJobs().size(), 3);
    }

    private void _testGetJobsForUserAndStatus() throws Exception {
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        List<String> list1 = new ArrayList<String>();
        list1.add(getTestUser());
        filter.put(OozieClient.FILTER_USER, list1);
        List<String> list2 = new ArrayList<String>();
        list2.add("KILLED");
        filter.put(OozieClient.FILTER_STATUS, list2);

        CoordJobsXCommand coordsGetCmd = new CoordJobsXCommand(filter, 1, 20);
        CoordinatorJobInfo ret = coordsGetCmd.call();
        assertNotNull(ret);
        assertEquals(ret.getCoordJobs().size(), 2);
    }

}
