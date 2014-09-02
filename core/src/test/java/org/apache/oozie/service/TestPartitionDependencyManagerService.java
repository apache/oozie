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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.client.CoordinatorAction.Status;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.dependency.hcat.HCatMessageHandler;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.jms.JMSConnectionInfo;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.HCatURI;
import org.apache.oozie.util.XLog;
import org.junit.Test;

/**
 * Test class to test the addition, removal and available operations
 * on the partition dependencies cache structure
 */
public class TestPartitionDependencyManagerService extends XDataTestCase {

    private static XLog LOG = XLog.getLog(TestPartitionDependencyManagerService.class);
    protected Services services;

    protected void setUp() throws Exception {
        super.setUp();
        services = super.setupServicesForHCatalog();
        // disable regular cache purge
        services.getConf().setInt(PartitionDependencyManagerService.CACHE_PURGE_INTERVAL, 1000000);
        services.init();
    }

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

        String server1 = "hcat-server1.domain.com:5080";
        String server2 = "hcat-server2.domain.com:5080";
        String db = "mydb";
        String table1 = "mytbl1";
        String table2 = "mytbl2";
        // add partition as missing
        HCatURI dep1 = new HCatURI("hcat://hcat-server1.domain.com:5080/mydb/mytbl1/dt=20120101;country=us");
        HCatURI dep2 = new HCatURI("hcat://hcat-server1.domain.com:5080/mydb/mytbl1/country=us;dt=20120101");
        HCatURI dep3 = new HCatURI("hcat://hcat-server2.domain.com:5080/mydb/mytbl2/dt=20120102;country=us");
        HCatURI dep4 = new HCatURI("hcat://hcat-server2.domain.com:5080/mydb/mytbl2/dt=20120102;country=us;state=CA");

        PartitionDependencyManagerService pdms = Services.get().get(PartitionDependencyManagerService.class);
        addMissingDependencyAndRegister(dep1, actionId1, pdms);
        addMissingDependencyAndRegister(dep2, actionId1, pdms);
        addMissingDependencyAndRegister(dep2, actionId2, pdms);
        addMissingDependencyAndRegister(dep2, actionId3, pdms);
        addMissingDependencyAndRegister(dep3, actionId3, pdms);
        addMissingDependencyAndRegister(dep4, actionId4, pdms);
        // Add duplicates. RecoveryService will add duplicates
        addMissingDependencyAndRegister(dep4, actionId4, pdms);
        addMissingDependencyAndRegister(dep4, actionId4, pdms);

        HCatAccessorService hcatService = Services.get().get(HCatAccessorService.class);
        JMSAccessorService jmsService = Services.get().get(JMSAccessorService.class);
        JMSConnectionInfo connInfo = hcatService.getJMSConnectionInfo(dep1.getURI());
        assertTrue(hcatService.isRegisteredForNotification(dep1)); //server1,db,table1
        assertTrue(hcatService.isRegisteredForNotification(dep3)); //server2,db,table2
        assertTrue(jmsService.isListeningToTopic(connInfo, dep1.getDb() + "." + dep1.getTable()));
        assertTrue(jmsService.isListeningToTopic(connInfo, dep3.getDb() + "." + dep3.getTable()));

        assertTrue(pdms.getWaitingActions(dep1).contains(actionId1));
        assertTrue(pdms.getWaitingActions(dep2).contains(actionId1));
        assertTrue(pdms.getWaitingActions(dep2).contains(actionId2));
        assertTrue(pdms.getWaitingActions(dep2).contains(actionId2));
        assertTrue(pdms.getWaitingActions(dep3).contains(actionId3));
        assertTrue(pdms.getWaitingActions(dep4).contains(actionId4));
        // Should not contain duplicates
        assertEquals(1, pdms.getWaitingActions(dep4).size());

        pdms.removeMissingDependency(dep2, actionId1);
        assertTrue(pdms.getWaitingActions(dep1).contains(actionId1));
        assertEquals(2, pdms.getWaitingActions(dep2).size());
        assertTrue(!pdms.getWaitingActions(dep2).contains(actionId1));
        assertNull(pdms.getAvailableDependencyURIs(actionId1));

        pdms.partitionAvailable(server2, db, table2, getPartitionMap("dt=20120102;country=us;state=NY"));
        assertNull(pdms.getWaitingActions(dep3));
        assertTrue(pdms.getAvailableDependencyURIs(actionId3).contains(dep3.getURI().toString()));

        pdms.partitionAvailable(server2, db, table2, getPartitionMap("dt=20120102;country=us;state=CA"));
        assertNull(pdms.getWaitingActions(dep4));
        assertTrue(pdms.getAvailableDependencyURIs(actionId4).contains(dep4.getURI().toString()));

        pdms.partitionAvailable(server1, db, table1, getPartitionMap("dt=20120101;country=us"));
        assertNull(pdms.getWaitingActions(dep1));
        assertNull(pdms.getWaitingActions(dep2));
        assertTrue(pdms.getAvailableDependencyURIs(actionId2).contains(dep2.getURI().toString()));
        assertTrue(pdms.getAvailableDependencyURIs(actionId3).contains(dep2.getURI().toString()));
        assertTrue(pdms.getAvailableDependencyURIs(actionId3).contains(dep3.getURI().toString()));

        assertTrue(pdms.removeAvailableDependencyURIs(actionId3, pdms.getAvailableDependencyURIs(actionId3)));
        assertNull(pdms.getAvailableDependencyURIs(actionId3));

        assertFalse(hcatService.isRegisteredForNotification(dep1)); //server1,db,table1
        assertFalse(hcatService.isRegisteredForNotification(dep3)); //server2,db,table2
        assertFalse(jmsService.isListeningToTopic(connInfo, dep1.getDb() + "." + dep1.getTable()));
        assertFalse(jmsService.isListeningToTopic(connInfo, dep3.getDb() + "." + dep3.getTable()));
    }

    protected void addMissingDependencyAndRegister(HCatURI hcatURI, String actionId, PartitionDependencyManagerService pdms) {
        pdms.addMissingDependency(hcatURI, actionId);
        HCatAccessorService hcatService = Services.get().get(HCatAccessorService.class);
        if (!hcatService.isRegisteredForNotification(hcatURI)) {
            hcatService.registerForNotification(hcatURI, hcatURI.getDb() + "." + hcatURI.getTable(),
                    new HCatMessageHandler(hcatURI.getServer()));
        }
    }

    @Test
    public void testMemoryUsageAndSpeed() throws Exception {
        // 2 to 4 seconds to insert 60K and 1 to 2 seconds to retrieve 60K
        // 35-45MB for 60K entries
        assertSpeedAndMemory(60000, 4000, 2000, 45000000, 40000000);
    }

    protected void assertSpeedAndMemory(int numItems, int insertTimeinMillis, int retrievalTimeinMillis,
            long memIncreaseAfterInsert, long memIncreaseAfterInsertAndGC) throws Exception {
        PartitionDependencyManagerService pdms = Services.get().get(PartitionDependencyManagerService.class);
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
        assertTrue((endTime - startTime) < insertTimeinMillis);

        LOG.info("Memory before and after insert: " + usedMemBeforeInsert + ","  + usedMemAfterInsert);
        verifyWaitingAction(pdms, numItems);
        LOG.info("Time taken to retrieve " + numItems + " items is " + (System.currentTimeMillis() - endTime));
        assertTrue((System.currentTimeMillis() - endTime) < retrievalTimeinMillis);

        long usedMemAfterRetrieval = mb.getHeapMemoryUsage().getUsed();
        System.gc();
        long usedMemAfterGC = mb.getHeapMemoryUsage().getUsed();

        LOG.info("Memory before insert = " + usedMemBeforeInsert);
        LOG.info("Memory after insert = " + usedMemAfterInsert);
        LOG.info("Memory after retrieval = " + usedMemAfterRetrieval);
        LOG.info("Memory after GC = " + usedMemAfterGC);

        // Commenting out as memory assertion is not reliable when running the full suite of tests.
        //assertTrue((usedMemAfterInsert - usedMemBeforeInsert) < memIncreaseAfterInsert);
        //assertTrue((usedMemAfterGC - usedMemBeforeInsert) < memIncreaseAfterInsertAndGC);
    }

    protected void verifyWaitingAction(PartitionDependencyManagerService pdms, int numItems) throws URISyntaxException {
        for (int i = 0; i < numItems; i++) {
            String actionID = "" + i;
            HCatURI dep = new HCatURI("hcat://hcat.server.com:5080/mydb/mytbl/id=" + actionID);
            Collection<String> waitingActions = pdms.getWaitingActions(dep);
            assertNotNull(dep.toURIString() + " is missing in cache", waitingActions);
            assertTrue(dep.toURIString() + " is missing in cache", waitingActions.contains(actionID));
        }
    }
}
