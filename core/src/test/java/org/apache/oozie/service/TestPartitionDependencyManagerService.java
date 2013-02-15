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
package org.apache.oozie.service;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.net.URISyntaxException;
import java.util.Collection;

import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.HCatURI;
import org.apache.oozie.util.XLog;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class to test the addition, removal and available operations
 * on the partition dependencies cache structure
 */
public class TestPartitionDependencyManagerService extends XDataTestCase {

    private static XLog LOG = XLog.getLog(TestPartitionDependencyManagerService.class);
    protected Services services;
    @Before
    protected void setUp() throws Exception {
        super.setUp();
        services = super.setupServicesForHCatalog();
        services.init();
    }

    @After
    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    @Test
    public void testPartitionDependency() throws Exception {
        // Test all APIs related to dependency caching
        String actionId1 = "1234465451";
        String actionId2 = "1234465452";
        String actionId3 = "1234465453";
        String actionId4 = "1234465454";

        String server = "hcat.server.com:5080";
        String db = "mydb";
        String table = "mytbl";
        // add partition as missing
        HCatURI dep1 = new HCatURI("hcat://hcat.server.com:5080/mydb/mytbl/dt=20120101;country=us");
        HCatURI dep2 = new HCatURI("hcat://hcat.server.com:5080/mydb/mytbl/country=us;dt=20120101");
        HCatURI dep3 = new HCatURI("hcat://hcat.server.com:5080/mydb/mytbl/dt=20120102;country=us");
        HCatURI dep4 = new HCatURI("hcat://hcat.server.com:5080/mydb/mytbl/dt=20120102;country=us;state=CA");
        PartitionDependencyManagerService pdms = Services.get().get(PartitionDependencyManagerService.class);
        pdms.addMissingDependency(dep1, actionId1);
        pdms.addMissingDependency(dep2, actionId1);
        pdms.addMissingDependency(dep2, actionId2);
        pdms.addMissingDependency(dep2, actionId3);
        pdms.addMissingDependency(dep3, actionId3);
        pdms.addMissingDependency(dep4, actionId4);
        assertTrue(pdms.getWaitingActions(dep1).contains(actionId1));
        assertTrue(pdms.getWaitingActions(dep2).contains(actionId1));
        assertTrue(pdms.getWaitingActions(dep2).contains(actionId2));
        assertTrue(pdms.getWaitingActions(dep2).contains(actionId2));
        assertTrue(pdms.getWaitingActions(dep3).contains(actionId3));
        assertTrue(pdms.getWaitingActions(dep4).contains(actionId4));

        pdms.removeMissingDependency(dep2, actionId1);
        assertTrue(pdms.getWaitingActions(dep1).contains(actionId1));
        assertEquals(2, pdms.getWaitingActions(dep2).size());
        assertTrue(!pdms.getWaitingActions(dep2).contains(actionId1));
        assertNull(pdms.getAvailableDependencyURIs(actionId1));

        pdms.partitionAvailable(server, db, table, getPartitionMap("dt=20120102;country=us;state=NY"));
        assertNull(pdms.getWaitingActions(dep3));
        assertTrue(pdms.getAvailableDependencyURIs(actionId3).contains(dep3.getURI().toString()));

        pdms.partitionAvailable(server, db, table, getPartitionMap("dt=20120102;country=us;state=CA"));
        assertNull(pdms.getWaitingActions(dep4));
        assertTrue(pdms.getAvailableDependencyURIs(actionId4).contains(dep4.getURI().toString()));

        pdms.partitionAvailable(server, db, table, getPartitionMap("dt=20120101;country=us"));
        assertNull(pdms.getWaitingActions(dep1));
        assertNull(pdms.getWaitingActions(dep2));
        assertTrue(pdms.getAvailableDependencyURIs(actionId2).contains(dep2.getURI().toString()));
        assertTrue(pdms.getAvailableDependencyURIs(actionId3).contains(dep2.getURI().toString()));
        assertTrue(pdms.getAvailableDependencyURIs(actionId3).contains(dep3.getURI().toString()));

        assertTrue(pdms.removeAvailableDependencyURIs(actionId3, pdms.getAvailableDependencyURIs(actionId3)));
        assertNull(pdms.getAvailableDependencyURIs(actionId3));
    }

    public void testMemoryUsageAndSpeed() throws Exception {
        PartitionDependencyManagerService pdms = Services.get().get(PartitionDependencyManagerService.class);
        int numItems = 60000;
        System.gc();
        MemoryMXBean mb = ManagementFactory.getMemoryMXBean();
        long usedMemBeforeInsert = mb.getHeapMemoryUsage().getUsed();
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < numItems; i++) {
            HCatURI dep = new HCatURI("hcat://hcat.server.com:5080/mydb/mytbl/id=" + i);
            pdms.addMissingDependency(dep, "" + i);
        }
        long usedMemAfterInsert = mb.getHeapMemoryUsage().getUsed();
        long endTime = System.currentTimeMillis();
        LOG.info("Time taken to insert " + numItems + " items is " + (endTime - startTime));
        assertTrue((endTime - startTime) < 4000); // 2 to 4 seconds to insert 60K

        LOG.info("Memory before and after insert: " + usedMemBeforeInsert + ","  + usedMemAfterInsert);
        for (int i = 0; i < numItems; i++) {
            verifyWaitingAction(pdms, "" + i);
        }
        LOG.info("Time taken to retrieve " + numItems + " items is " + (System.currentTimeMillis() - endTime));
        assertTrue((System.currentTimeMillis() - endTime) < 2000); // 1 to 2 seconds to retrieve 60K

        long usedMemAfterRetrieval = mb.getHeapMemoryUsage().getUsed();
        System.gc();
        long usedMemAfterGC = mb.getHeapMemoryUsage().getUsed();

        LOG.info("Memory before insert = " + usedMemBeforeInsert);
        LOG.info("Memory after insert = " + usedMemAfterInsert);
        LOG.info("Memory after retrieval = " + usedMemAfterRetrieval);
        LOG.info("Memory after GC = " + usedMemAfterGC);

        assertTrue((usedMemAfterInsert - usedMemBeforeInsert) < 45000000); //35-45MB for 60K entries
    }

    protected void verifyWaitingAction(PartitionDependencyManagerService pdms, String actionID) throws URISyntaxException {
        HCatURI dep = new HCatURI("hcat://hcat.server.com:5080/mydb/mytbl/id=" + actionID);
        Collection<String> waitingActions = pdms.getWaitingActions(dep);
        assertNotNull(dep.toURIString() + " is missing in cache", waitingActions);
        assertTrue(dep.toURIString() + " is missing in cache", waitingActions.contains(actionID));
    }

}
