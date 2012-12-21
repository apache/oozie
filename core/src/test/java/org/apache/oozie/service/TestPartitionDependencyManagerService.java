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

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.service.PartitionDependencyManagerService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.HCatURI;
import org.apache.oozie.util.PartitionWrapper;
import org.apache.oozie.util.PartitionsGroup;
import org.apache.oozie.util.WaitingActions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class to test the addition, removal and available operations
 * on the partition dependencies cache structure
 */
public class TestPartitionDependencyManagerService extends XDataTestCase {

    private Services services;
    @Before
    protected void setUp() throws Exception {
        super.setUp();
        setSystemProperty(PartitionDependencyManagerService.MAP_MAX_WEIGHTED_CAPACITY, "100");
        services = super.setupServicesForHCatalog();
        services.init();
    }

    @After
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    /**
     * Test basic service startup and required structures
     * @throws MetadataServiceException
     */
    @Test
    public void testBasicService() throws MetadataServiceException {
        Services services = Services.get();
        PartitionDependencyManagerService pdms = services.get(PartitionDependencyManagerService.class);
        assertNotNull(pdms);
        assertNotNull(pdms.getHCatMap());
        assertNotNull(pdms.getAvailableMap());
    }

    /**
     * Test addition of missing partition into cache
     *
     * @throws MetadataServiceException
     * @throws URISyntaxException
     */
    @Test
    public void testAddMissingPartition() throws MetadataServiceException, URISyntaxException {
        PartitionDependencyManagerService pdms = services.get(PartitionDependencyManagerService.class);
        String newHCatDependency = "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12&region=us";
        JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        jmsService.getOrCreateConnection("hcat://hcat.server.com:5080");
        String actionId = "myAction";
        pdms.addMissingPartition(newHCatDependency, actionId);

        HCatURI hcatUri = new HCatURI(newHCatDependency);
        Map<String, PartitionsGroup> tablePartitionsMap = pdms.getHCatMap().get(hcatUri.getServer() + "#" +
                                                                            hcatUri.getDb()); // clicks
        assertNotNull(tablePartitionsMap);
        assertTrue(tablePartitionsMap.containsKey("clicks"));
        PartitionsGroup missingPartitions = tablePartitionsMap.get(hcatUri.getTable());
        assertNotNull(missingPartitions);

        assertEquals(missingPartitions.getPartitionsMap().keySet().iterator().next(),
                new PartitionWrapper(hcatUri)); // datastamp=12;region=us
        WaitingActions actions = missingPartitions.getPartitionsMap().get(new PartitionWrapper(hcatUri));
        assertNotNull(actions);
        assertTrue(actions.getActions().contains(actionId));
    }

    /**
     * Test removal of partition from cache
     *
     * @throws MetadataServiceException
     * @throws URISyntaxException
     */
    @Test
    public void testRemovePartition() throws Exception {
        Services services = Services.get();
        PartitionDependencyManagerService pdms = services.get(PartitionDependencyManagerService.class);
        String newHCatDependency = "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12&region=us";
        JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        jmsService.getOrCreateConnection("hcat://hcat.server.com:5080");
        String actionId = "myAction";
        pdms.addMissingPartition(newHCatDependency, actionId);

        HCatURI hcatUri = new HCatURI(newHCatDependency);
        Map<String, PartitionsGroup> tablePartitionsMap = pdms.getHCatMap().get(hcatUri.getServer() + "#" +
                                                                            hcatUri.getDb()); // clicks
        assertNotNull(tablePartitionsMap);
        assertTrue(tablePartitionsMap.containsKey("clicks"));
        PartitionsGroup missingPartitions = tablePartitionsMap.get(hcatUri.getTable());
        assertNotNull(missingPartitions);

        // remove with cascading - OFF
        pdms.removePartition(newHCatDependency, false);
        assertFalse(missingPartitions.getPartitionsMap().containsKey(hcatUri.getPartitionMap()));

        pdms.addMissingPartition(newHCatDependency, actionId);
        assertNotNull(missingPartitions);

        // remove with cascading - ON
        pdms.removePartition(newHCatDependency);
        assertFalse(pdms.getHCatMap().containsKey(hcatUri.getTable()));
    }

    /**
     * Test partition available function on cache
     *
     * @throws MetadataServiceException
     * @throws URISyntaxException
     */
    @Test
    public void testAvailablePartition() throws MetadataServiceException, URISyntaxException {
        Services services = Services.get();
        PartitionDependencyManagerService pdms = services.get(PartitionDependencyManagerService.class);
        String newHCatDependency = "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12&region=us";
        JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        jmsService.getOrCreateConnection("hcat://hcat.server.com:5080");
        String actionId = "myAction";
        pdms.addMissingPartition(newHCatDependency, actionId);

        HCatURI hcatUri = new HCatURI(newHCatDependency);
        Map<String, PartitionsGroup> tablePartitionsMap = pdms.getHCatMap().get(hcatUri.getServer() + "#" +
                                                                            hcatUri.getDb()); // clicks
        assertNotNull(tablePartitionsMap);
        assertTrue(tablePartitionsMap.containsKey("clicks"));
        PartitionsGroup missingPartitions = tablePartitionsMap.get(hcatUri.getTable());
        assertNotNull(missingPartitions);

        pdms.partitionAvailable(newHCatDependency);
        Map<String, List<PartitionWrapper>> availMap = pdms.getAvailableMap();
        assertNotNull(availMap);
        assertTrue(availMap.containsKey(actionId)); //found in 'available' cache
        assertFalse(pdms.getHCatMap().containsKey(hcatUri.getTable())); //removed from 'missing' cache
                                                                        //cascade - ON
        assertEquals(availMap.get(actionId).get(0), new PartitionWrapper(hcatUri));
    }

    /**
     * Test removal of action ID from missing partition
     *
     * @throws MetadataServiceException
     * @throws URISyntaxException
     */
    @Test
    public void testRemoveActionFromMissingPartition() throws MetadataServiceException, URISyntaxException {
        Services services = Services.get();
        PartitionDependencyManagerService pdms = services.get(PartitionDependencyManagerService.class);
        String newHCatDependency1 = "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12";
        String newHCatDependency2 = "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12&region=us";
        JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        jmsService.getOrCreateConnection("hcat://hcat.server.com:5080");
        String actionId1 = "1";
        String actionId2 = "2";
        pdms.addMissingPartition(newHCatDependency1, actionId1);
        pdms.addMissingPartition(newHCatDependency2, actionId2);
        // remove newHCatDependency2
        pdms.removeActionFromMissingPartitions(newHCatDependency2, actionId2);

        HCatURI hcatUri = new HCatURI(newHCatDependency1);
        String prefix = PartitionWrapper.makePrefix(hcatUri.getServer(), hcatUri.getDb());
        Map<String, PartitionsGroup> tablePartitionsMap = pdms.getHCatMap().get(prefix);
        PartitionsGroup missingPartitions = tablePartitionsMap.get(hcatUri.getTable());
        assertNotNull(missingPartitions);

        WaitingActions actions = missingPartitions.getPartitionsMap().get(new PartitionWrapper(hcatUri));
        assertNotNull(actions);
        assertTrue(actions.getActions().contains(actionId1));
        assertFalse(actions.getActions().contains(actionId2));
    }



    /**
     * Test removal of partitions from Available map
     */
    @Test
    public void testRemovePartitionsFromAvailMap() {
        try {
            Services services = Services.get();
            PartitionDependencyManagerService pdms = services.get(PartitionDependencyManagerService.class);
            String newHCatDependency1 = "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12";
            String newHCatDependency2 = "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12&region=us";
            String newHCatDependency3 = "hcat://hcat.server.com:5080/mydb/clicks/datastamp=13&region=us";
            String actionId1 = "1";

            HCatURI uri = new HCatURI(newHCatDependency1);
            PartitionWrapper partition1 = new PartitionWrapper(uri.getServer(), uri.getDb(), uri.getTable(),
                    uri.getPartitionMap());
            uri = new HCatURI(newHCatDependency2);
            PartitionWrapper partition2 = new PartitionWrapper(uri.getServer(), uri.getDb(), uri.getTable(),
                    uri.getPartitionMap());
            uri = new HCatURI(newHCatDependency3);
            PartitionWrapper partition3 = new PartitionWrapper(uri.getServer(), uri.getDb(), uri.getTable(),
                    uri.getPartitionMap());

            List<PartitionWrapper> partitionList = new ArrayList<PartitionWrapper>();
            partitionList.add(partition1);
            partitionList.add(partition2);
            partitionList.add(partition3);

            Map<String, List<PartitionWrapper>> availMap = pdms.getAvailableMap();
            // Add 3 partitions to availMap for a given action
            availMap.put(actionId1, partitionList);

            List<PartitionWrapper> availPartitionList = new ArrayList<PartitionWrapper>();
            availPartitionList.add(partition1);
            availPartitionList.add(partition2);
            // add two partitions to be removed
            assertTrue(pdms.removeAvailablePartitions(availPartitionList, actionId1));
            // check if the size of avail map reduces to 1
            assertEquals(1, pdms.getAvailableMap().get(actionId1).size());

        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Unexpected exception " + e);
        }
    }


}
