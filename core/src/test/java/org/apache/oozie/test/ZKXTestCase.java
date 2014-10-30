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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.EnsurePath;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.InstanceSerializer;
import org.apache.oozie.event.listener.ZKConnectionListener;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.FixedJsonInstanceSerializer;
import org.apache.oozie.util.ZKUtils;
import org.apache.hadoop.conf.Configuration;

/**
 * Provides a version of XTestCase that also runs a ZooKeeper server and provides some utilities for interacting and simulating ZK
 * related things.  By default, the Instance ID of this Oozie server will be "1234" (instead of the hostname).
 * <p>
 * Unlike the code, which uses ZKUtils for interacting with ZK, this class doesn't to make sure that (a) we test ZKUtils and (b) so
 * that the test class doesn't advertise on the ZK service discovery; the test class has access to a CuratorFramework (client) and
 * the ServiceDiscovery, but its "passive".
 * To simulate another Oozie server, the DummyZKOozie object can be used; you can specify a ZooKeeper ID and Oozie URL for it in
 * the constructor.  Unlike this test class, it will advertise on the ZK service discovery, so it will appear as another Oozie
 * Server to anything using ZKUtils (though it does not use ZKUtils itself so it can have different information).
 * To simulate another ZK-aware class, DummyUser can be used, which will use ZKUtils for interacting with ZK, including advertising
 * on the service discovery; it also provides access to its ZKUtils instance.
 * <p>
 * To use security, see {@link ZKXTestCaseWithSecurity}.
 */
public abstract class ZKXTestCase extends XDataTestCase {
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
        setUpZK();
    }

    protected void setUp(Configuration conf) throws Exception {
        super.setUp();
        Services services = new Services();
        if(conf != null && conf.size()>0){
            for (Iterator<Entry<String, String>> itr = (Iterator<Entry<String, String>>) conf.iterator(); itr.hasNext();) {
                Entry<String, String> entry = itr.next();
                services.getConf().set(entry.getKey(), entry.getValue());
            }
        }
        services.init();
        setUpZK();
    }

    private void setUpZK() throws Exception {
        zkServer = setupZKServer();
        Services.get().getConf().set("oozie.zookeeper.connection.string", zkServer.getConnectString());
        Services.get().getConf().set("oozie.instance.id", ZK_ID);
        Services.get().getConf().setBoolean(ZKConnectionListener.CONF_SHUTDOWN_ON_TIMEOUT, false);
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
        zkServer.close();   // also deletes the temp dir so we don't start eating up GBs of space
    }

    /**
     * Creates and sets up the embedded ZooKeeper server.  Test subclasses should have no reason to override this method.
     *
     * @return the embedded ZooKeeper server
     * @throws Exception
     */
    protected TestingServer setupZKServer() throws Exception {
        return new TestingServer();
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
        RetryPolicy retryPolicy = ZKUtils.getRetryPolicy();
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
     * by calling {@link DummyZKOozie#teardown()}.  It can also optionally join the ZKJobsConcurrencyService leader election.
     * Make sure to tear down any DummyZKOozies that you create.
     */
    protected class DummyZKOozie {
        private CuratorFramework client = null;
        private String zkId;
        private ServiceDiscovery<Map> sDiscovery;
        private String metadataUrl;
        private LeaderLatch leaderLatch = null;

        /**
         * Creates a DummyZKOozie.  Will not join the ZKJobsConcurrencyService leader election.
         *
         * @param zkId The ID of this new Oozie "server"
         * @param metadataUrl The URL to advertise for this "server"
         * @throws Exception
         */
        public DummyZKOozie(String zkId, String metadataUrl) throws Exception {
            this(zkId, metadataUrl, false);
        }

        /**
         * Creates a DummyZKOozie.
         *
         * @param zkId The ID of this new Oozie "server"
         * @param metadataUrl The URL to advertise for this "server"
         * @param joinConcurrencyLeaderElection true if should join ZKJobsConcurrencyService leader election; false if not
         * @throws Exception
         */
        public DummyZKOozie(String zkId, String metadataUrl, boolean joinConcurrencyLeaderElection) throws Exception {
            this.zkId = zkId;
            this.metadataUrl = metadataUrl;
            createClient();
            advertiseService();
            if (joinConcurrencyLeaderElection) {
                joinConcurrencyLeaderElection();
            }
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

        private void joinConcurrencyLeaderElection() throws Exception {
            leaderLatch = new LeaderLatch(client, "/services/concurrencyleader", zkId);
            leaderLatch.start();
        }

        public boolean isLeader() {
            if (leaderLatch != null) {
                return leaderLatch.hasLeadership();
            }
            throw new RuntimeException("Must join concurrency leader election");
        }

        public void teardown() {
            if (leaderLatch != null) {
                try {
                    leaderLatch.close();
                } catch (IOException ioe) {
                    log.warn("Exception occured while leaving leader latch", ioe);
                }
            }
            try {
                unadvertiseService();
            } catch (Exception ex) {
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

    /**
     * Provides a class that can can register/unregister with the ZKUtils.  It also provides access to the ZKUtils object.  This is
     * useful for testing features of the of ZKUtils class.  It will register when {@link DummyUser#register()} is called.  Make
     * sure to call {@link DummyUser#unregister()} when done using it.
     */
    protected class DummyUser {

        public DummyUser() {
        }
        private ZKUtils zk = null;

        /**
         * Registers with ZKUtils.
         *
         * @throws Exception
         */
        public void register() throws Exception {
            zk = ZKUtils.register(this);
            sleep(1000);    // Sleep to allow ZKUtils ServiceCache to update
        }

        /**
         * Unregisters with ZKUtils.
         */
        public void unregister() {
            if (zk != null) {
                zk.unregister(this);
                sleep(1000);    // Sleep to allow ZKUtils ServiceCache to update
            }
            zk = null;
        }

        /**
         * Accessor for the ZKUtils object used by this class.
         *
         * @return The ZKUtils object
         */
        public ZKUtils getZKUtils() {
            return zk;
        }
    }
}

