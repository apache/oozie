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
import java.util.List;

import org.apache.oozie.BulkResponseInfo;
import org.apache.oozie.BundleEngine;
import org.apache.oozie.BundleEngineException;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.rest.BulkResponseImpl;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;

public class TestBulkMonitorJPAExecutor extends XDataTestCase {
    Services services;
    JPAService jpaService;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        cleanUpDBTables();
        jpaService = Services.get().get(JPAService.class);
        addRecordsForBulkMonitor();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testSingleRecord() throws Exception {

        String request = "bundle=" + bundleName + ";actionstatus=FAILED;"
                + "startcreatedtime=2012-07-21T00:00Z;endcreatedtime=2012-07-22T02:00Z";

        List<BulkResponseImpl> brList = _execQuery(request);
        assertEquals(1, brList.size()); // only 1 action satisfies the
                                        // conditions
        BulkResponseImpl br = brList.get(0);
        assertEquals(bundleName, br.getBundle().getAppName());
        assertEquals("Coord1", br.getCoordinator().getAppName());
        assertEquals(CoordinatorAction.Status.FAILED, br.getAction().getStatus());
        assertEquals(DateUtils.parseDateUTC(CREATE_TIME).toString(), br.getAction().getCreatedTime().toString());
    }

    public void testMultipleRecords() throws Exception {

        String request = "bundle=" + bundleName + ";actionstatus=FAILED,KILLED;"
                + "startcreatedtime=2012-07-21T00:00Z;endcreatedtime=2012-07-22T02:00Z";

        List<BulkResponseImpl> brList = _execQuery(request);
        assertEquals(3, brList.size()); // 3 actions satisfy the conditions
        List<String> possibleStatus = new ArrayList<String>(Arrays.asList("KILLED", "FAILED"));
        List<String> resultStatus = new ArrayList<String>();
        resultStatus.add(brList.get(0).getAction().getStatus().toString());
        resultStatus.add(brList.get(1).getAction().getStatus().toString());
        assertEquals(possibleStatus, resultStatus);
    }

    public void testJavaNoRecords() throws Exception {

        String request = "bundle=BUNDLE-ABC;actionstatus=FAILED";

        BulkJPAExecutor bulkjpa = new BulkJPAExecutor(BundleEngine.parseBulkFilter(request), 1, 10);
        try {
            jpaService.execute(bulkjpa);
            fail(); // exception expected due to no records found for this
                    // bundle
        }
        catch (JPAExecutorException jex) {
            assertTrue(jex.getMessage().contains("No bundle entries found"));
        }
    }

    public void testMultipleCoordinators() throws Exception {
        // there are 3 coordinators but giving range as only two of them
        String request = "bundle=" + bundleName + ";coordinators=Coord1,Coord2;actionstatus=KILLED";
        List<BulkResponseImpl> brList = _execQuery(request);
        assertEquals(2, brList.size()); // 2 actions satisfy the conditions
        assertEquals(brList.get(0).getAction().getId(), "Coord1@2");
        assertEquals(brList.get(1).getAction().getId(), "Coord2@1");
    }

    public void testDefaultStatus() throws Exception {
        // adding coordinator action #4 to Coord#3
        addRecordToCoordActionTable("Coord3", 1, CoordinatorAction.Status.FAILED, "coord-action-get.xml", 0);

        String request = "bundle=" + bundleName + ";";
        List<BulkResponseImpl> brList = _execQuery(request);
        assertEquals(4, brList.size()); // 4 actions satisfy the conditions
        List<String> possibleStatus = new ArrayList<String>(Arrays.asList("FAILED", "KILLED"));
        List<String> resultStatus = new ArrayList<String>();
        resultStatus.add(brList.get(0).getAction().getStatus().toString());
        resultStatus.add(brList.get(1).getAction().getStatus().toString());
        assertEquals(possibleStatus, resultStatus);
    }

    public void testMultipleBundleIdsForName() throws Exception {
        // Adding another bundle having same name
        BundleJobBean bundle = new BundleJobBean();
        bundle.setId("00002-12345-B");
        bundle.setAppName(bundleName);
        bundle.setStatus(BundleJob.Status.RUNNING);
        bundle.setStartTime(new Date());
        BundleJobInsertJPAExecutor bundleInsert = new BundleJobInsertJPAExecutor(bundle);

        jpaService.execute(bundleInsert);
        String request = "bundle=" + bundleName;
        BulkJPAExecutor bulkjpa = new BulkJPAExecutor(BundleEngine.parseBulkFilter(request), 1, 10);
        try {
            jpaService.execute(bulkjpa);
            fail(); // exception expected due to >1 records found for same
                    // bundle name
        }
        catch (JPAExecutorException jex) {
            assertTrue(jex.getMessage().contains("Non-unique bundles present for same bundle name"));
        }
    }

    private List<BulkResponseImpl> _execQuery(String request) throws JPAExecutorException, BundleEngineException {
        BulkJPAExecutor bulkjpa = new BulkJPAExecutor(BundleEngine.parseBulkFilter(request), 1, 10);
        BulkResponseInfo response = jpaService.execute(bulkjpa);
        assertNotNull(response);
        return response.getResponses();
    }

}