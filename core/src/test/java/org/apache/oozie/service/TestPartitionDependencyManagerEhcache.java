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

import java.util.Collection;

import org.apache.oozie.dependency.hcat.EhcacheHCatDependencyCache;
import org.apache.oozie.util.HCatURI;
import org.apache.oozie.util.XLog;
import org.junit.Test;

public class TestPartitionDependencyManagerEhcache extends TestPartitionDependencyManagerService {

    private static XLog LOG = XLog.getLog(TestPartitionDependencyManagerEhcache.class);

    private void setupServices(String cacheName) throws ServiceException {
        Services.get().destroy();
        services = super.setupServicesForHCatalog();
        services.getConf().set(PartitionDependencyManagerService.CACHE_MANAGER_IMPL,
                EhcacheHCatDependencyCache.class.getName());
        if (cacheName != null) {
            services.getConf().set(EhcacheHCatDependencyCache.CONF_CACHE_NAME, cacheName);
        }
        services.init();
    }

    @Override
    @Test
    public void testPartitionDependency() throws Exception {
        setupServices(null);
        super.testPartitionDependency();
    }

    @Override
    @Test
    public void testMemoryUsageAndSpeed() throws Exception {
        // use all small case. Configured insrc/test/resources/ehcache.xml
        setupServices("testnospilltodisk");
        assertSpeedAndMemory(60000, 4500, 2000, 45000000, 40000000);
    }

    @Test
    public void testMemoryUsageAndSpeedOverflowToDisk() throws Exception {
        setupServices("testspilltodisk"); // maxElementsInMemory="20000". 2/3 on disk
        // Insert and retrieve are between 15-30 seconds
        // When run individually memIncreaseAfterInsert is < 45MB. But running with
        // all tests it goes to 60MB.
        assertSpeedAndMemory(60000, 30000, 11000, 60000000, 25000000);
    }

    @Test
    public void testEvictionOnTimeToIdle() throws Exception {
        setupServices("testevictionontimetoidle");
        PartitionDependencyManagerService pdms = Services.get().get(PartitionDependencyManagerService.class);
        int numItems = 50;
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < numItems; i++) {
            HCatURI dep = new HCatURI("hcat://hcat.server.com:5080/mydb/mytbl/id=" + i);
            pdms.addMissingDependency(dep, "" + i);
        }
        verifyWaitingAction(pdms, numItems);
        LOG.info("Time taken to insert and retrive " + numItems + " items is "
                + (System.currentTimeMillis() - startTime));
        // timeToIdleSeconds is 1
        Thread.sleep(1100);
        for (int i = 0; i < numItems; i++) {
            assertNull(pdms.getWaitingActions(new HCatURI("hcat://hcat.server.com:5080/mydb/mytbl/id=" + "" + i)));
        }
    }

    @Test
    public void testEvictionOnTimeToLive() throws Exception {
        setupServices("testevictionontimetolive");
        PartitionDependencyManagerService pdms = Services.get().get(PartitionDependencyManagerService.class);
        int numItems = 50;
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < numItems; i++) {
            HCatURI dep = new HCatURI("hcat://hcat.server.com:5080/mydb/mytbl/id=" + i);
            pdms.addMissingDependency(dep, "" + i);
        }
        verifyWaitingAction(pdms, numItems);
        LOG.info("Time taken to insert and retrive " + numItems + " items is "
                + (System.currentTimeMillis() - startTime));
        // timeToLiveSeconds is 1
        Thread.sleep(1100);
        for (int i = 0; i < numItems; i++) {
            assertNull(pdms.getWaitingActions(new HCatURI("hcat://hcat.server.com:5080/mydb/mytbl/id=" + "" + i)));
        }
    }

    @Test
    public void testMaxElementsInMemory() throws Exception {
        setupServices("testmaxelementsinmemory"); // maxElementsInMemory="500" overflowToDisk="false"
        PartitionDependencyManagerService pdms = Services.get().get(PartitionDependencyManagerService.class);
        int numItems = 1000;
        for (int i = 0; i < numItems; i++) {
            HCatURI dep = new HCatURI("hcat://hcat.server.com:5080/mydb/mytbl/id=" + i);
            pdms.addMissingDependency(dep, "" + i);
        }
        // First 500 should have been evicted. But it is LRU and the last 350 removed is between 250 and 750.
        for (int i = 0; i < 150; i++) {
            assertNull(pdms.getWaitingActions(new HCatURI("hcat://hcat.server.com:5080/mydb/mytbl/id=" + "" + i)));
        }
        int evicted = 0;
        for (int i = 150; i < 750; i++) {
            if (pdms.getWaitingActions(new HCatURI("hcat://hcat.server.com:5080/mydb/mytbl/id=" + "" + i)) == null) {
                evicted++;
            }
        }
        assertEquals(350, evicted);
        for (int i = 750; i < 1000; i++) {
            String actionID = "" + i;
            HCatURI dep = new HCatURI("hcat://hcat.server.com:5080/mydb/mytbl/id=" + actionID);
            Collection<String> waitingActions = pdms.getWaitingActions(dep);
            assertNotNull(dep.toURIString() + " is missing in cache", waitingActions);
            assertTrue(dep.toURIString() + " is missing in cache", waitingActions.contains(actionID));
        }
    }
}
