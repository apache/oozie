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
package org.apache.oozie.test;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.EnsurePath;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.InstanceSerializer;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.FixedJsonInstanceSerializer;

/**
 * Provides a version of XTestCase that also runs a ZooKeeper server and provides some utilities for interacting and simulating ZK
 * related things.  By default, the ZooKeeper ID of this Oozie server will be "1234" (instead of the hostname).
 * <p>
 * Unlike the code, which uses ZKUtils for interacting with ZK, this class doesn't to make sure that (a) we test ZKUtils and (b) so
 * that the test class doesn't advertise on the ZK service discovery; the test class has access to a CuratorFramework (client) and
 * the ServiceDiscovery, but its "passive".
 * To simulate another Oozie server, the DummyZKOozie object can be used; you can specify a ZooKeeper ID and Oozie URL for it in
 * the constructor.  Unlike this test class, it will advertise on the ZK service discovery, so it will appear as another Oozie
 * Server to anything using ZKUtils (though it does not use ZKUtils itself so it can have different information).
 */
public abstract class ZKXTestCase extends XTestCase {
    private TestingServer zkServer;
    private CuratorFramework client = null;
    private ServiceDiscovery<Map> sDiscovery = null;
    /**
     * The ZooKeeper ID for "this" Oozie server
     */
    protected static final String ZK_ID = "1234";

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        new Services().init();
        // Start the ZooKeeper server and set Oozie ZK properties
        zkServer = new TestingServer();
        Services.get().getConf().set("oozie.zookeeper.connection.string", zkServer.getConnectString());
        Services.get().getConf().set("oozie.zookeeper.oozie.id", ZK_ID);
        createClient();
        createServiceDiscovery();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        Services.get().destroy();
        sDiscovery.close();
        sDiscovery = null;
        client.close();
        client = null;
        zkServer.stop();
        // If we don't delete the temp dir used by ZK, it will quickly start eating up GBs of space as every test method creates a
        // new dir about 70MB
        FileUtils.deleteDirectory(zkServer.getTempDirectory());
    }

    /**
     * Returns the connection string for ZooKeeper.
     *
     * @return the conection string for ZooKeeeper
     */
    protected String getConnectString() {
        return zkServer.getConnectString();
    }

    /**
     * Get the CuratorFramework (client).
     *
     * @return the CuratorFramework (client)
     */
    protected CuratorFramework getClient() {
        return client;
    }

    /**
     * Get the ServiceDiscovery object
     *
     * @return the ServiceDiscovery object
     */
    protected ServiceDiscovery<Map> getServiceDiscovery() {
        return sDiscovery;
    }

    private void createClient() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        String zkConnectionString = Services.get().getConf().get("oozie.zookeeper.connection.string", zkServer.getConnectString());
        String zkNamespace = Services.get().getConf().get("oozie.zookeeper.namespace", "oozie");
        client = CuratorFrameworkFactory.builder()
                                            .namespace(zkNamespace)
                                            .connectString(zkConnectionString)
                                            .retryPolicy(retryPolicy)
                                            .build();
        client.start();
    }

    private void createServiceDiscovery() throws Exception {
        InstanceSerializer<Map> instanceSerializer = new FixedJsonInstanceSerializer<Map>(Map.class);
        sDiscovery = ServiceDiscoveryBuilder.builder(Map.class)
                                                .basePath("/services")
                                                .client(client)
                                                .serializer(instanceSerializer)
                                                .build();
        sDiscovery.start();
        // Important, we're not advertising
    }

    /**
     * Provides a class that can pretend to be another Oozie Server as far as ZooKeeper and anything using ZKUtils is concerned.
     * You can specify the ID and URL of the Oozie Server.  It will "start" when the constructor is called and can be "stopped"
     * by calling {@link DummyZKOozie#teardown() }.  Make sure to tear down any DummyZKOozies that you create.
     */
    protected class DummyZKOozie {
        private CuratorFramework client = null;
        private String zkId;
        private ServiceDiscovery<Map> sDiscovery;
        private String metadataUrl;

        /**
         * Creates a DummyZKOozie.
         *
         * @param zkId The ID of this new Oozie "server"
         * @param metadataUrl The URL to advertise for this "server"
         * @throws Exception
         */
        public DummyZKOozie(String zkId, String metadataUrl) throws Exception {
            this.zkId = zkId;
            this.metadataUrl = metadataUrl;
            createClient();
            advertiseService();
        }

        private void createClient() throws Exception {
            // Connect to the ZooKeeper server
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
            String zkConnectionString = Services.get().getConf().get("oozie.zookeeper.connection.string", "localhost:2181");
            String zkNamespace = Services.get().getConf().get("oozie.zookeeper.namespace", "Oozie");
            client = CuratorFrameworkFactory.builder()
                                                .namespace(zkNamespace)
                                                .connectString(zkConnectionString)
                                                .retryPolicy(retryPolicy)
                                                .build();
            client.start();
        }

        private void advertiseService() throws Exception {
            // Advertise on the service discovery
            new EnsurePath("/services").ensure(client.getZookeeperClient());
            InstanceSerializer<Map> instanceSerializer = new FixedJsonInstanceSerializer<Map>(Map.class);
            sDiscovery = ServiceDiscoveryBuilder.builder(Map.class)
                                                    .basePath("/services")
                                                    .client(client)
                                                    .serializer(instanceSerializer)
                                                    .build();
            sDiscovery.start();
            sDiscovery.registerService(getMetadataInstance());
            sleep(1000);    // Sleep to allow ZKUtils ServiceCache to update
        }

        private void unadvertiseService() throws Exception {
            // Unadvertise on the service discovery
            sDiscovery.unregisterService(getMetadataInstance());
            sDiscovery.close();
            sleep(1000);    // Sleep to allow ZKUtils ServiceCache to update
        }

        public void teardown() {
            try {
                unadvertiseService();
            }
            catch (Exception ex) {
                log.warn("Exception occurred while unadvertising: " + ex.getMessage(), ex);
            }
            client.close();
        }

        private ServiceInstance<Map> getMetadataInstance() throws Exception {
            // Creates the metadata that this server is providing to ZooKeeper and other Oozie Servers
            Map<String, String> map = new HashMap<String, String>();
            map.put("OOZIE_ID", zkId);
            map.put("OOZIE_URL", metadataUrl);

            return ServiceInstance.<Map>builder()
                .name("servers")
                .id(zkId)
                .payload(map)
                .build();
        }
    }
}

