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
package org.apache.oozie.util;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.EnsurePath;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.InstanceSerializer;
import org.apache.oozie.service.Services;


/**
 * This class provides a singleton for interacting with ZooKeeper that other classes can use.  It handles connecting to ZooKeeper,
 * service discovery, and publishing metadata about this server.
 * <p>
 * Users of this class should call {@link ZKUtils#register(java.lang.Object)} to obtain the singleton.  This will ensure that we're
 * properly connected and ready to go with ZooKeeper.  When the user is done (i.e. on shutdown), it should call
 * {@link ZKUtils#unregister(java.lang.Object)} to let this class know; once there are no more users, this class will automatically
 * remove itself from ZooKeeper.
 * <p>
 * Each Oozie Server provides metadata that can be shared with the other Oozie Servers.  To keep things simple and to make it easy
 * to add additional metadata in the future, we share a Map.  They keys are defined in {@link ZKMetadataKeys}.
 * <p>
 * For the service discovery, the structure in ZooKeeper is /oozie.zookeeper.namespace/ZK_BASE_PATH/ (default is /oozie/services/).
 * There is currently only one service, named "servers" under which each Oozie server creates a ZNode named oozie.zookeeper.oozie.id
 * (default is the hostname) that contains the metadata payload.  For example, with the default settings, an Oozie server named
 * "foo" would create a ZNode at /oozie/services/servers/foo where the foo ZNode contains the metadata.
 */
public class ZKUtils {
    /**
     * oozie-site property for specifying the ZooKeeper connection string.  Comma-separated values of host:port pairs of the
     * ZooKeeper servers.
     */
    public static final String ZK_CONNECTION_STRING = "oozie.zookeeper.connection.string";
    /**
     * oozie-site property for specifying the ZooKeeper namespace to use (e.g. "oozie").  All of the Oozie servers that are planning
     * on talking to each other should have the same value for this.
     */
    public static final String ZK_NAMESPACE = "oozie.zookeeper.namespace";
    /**
     * oozie-site property for specifying the ID for this Oozie Server.  Each Oozie server should have a unique ID.
     */
    public static final String ZK_ID = "oozie.zookeeper.oozie.id";
    private static final String ZK_OOZIE_SERVICE = "servers";
    private static final String ZK_BASE_PATH = "/services";

    private static Set<Object> users = new HashSet<Object>();
    private CuratorFramework client = null;
    private String zkId;
    private long zkRegTime;
    private ServiceDiscovery<Map> sDiscovery;
    private ServiceCache<Map> sCache;
    private XLog log;

    private static ZKUtils zk = null;

    /**
     * Private Constructor for the singleton; it connects to ZooKeeper and advertises this Oozie Server.
     *
     * @throws Exception
     */
    private ZKUtils() throws Exception {
        log = XLog.getLog(getClass());
        zkId = Services.get().getConf().get(ZK_ID, System.getProperty("oozie.http.hostname"));
        createClient();
        advertiseService();
    }

    /**
     * Classes that want to use ZooKeeper should call this method to get the ZKUtils singleton.
     *
     * @param user The calling class
     * @return the ZKUtils singleton
     * @throws Exception
     */
    public static synchronized ZKUtils register(Object user) throws Exception {
        if (zk == null) {
            zk = new ZKUtils();
        }
        // Remember the calling class so we can disconnect when everybody is done
        users.add(user);
        return zk;
    }

    /**
     * Classes should call this when they are done (i.e. shutdown).
     *
     * @param user The calling class
     */
    public synchronized void unregister(Object user) {
        // If there are no more classes using ZooKeeper, we should teardown everything.
        users.remove(user);
        if (users.isEmpty() && zk != null) {
            zk.teardown();
            zk = null;
        }
    }

    private void createClient() throws Exception {
        // Connect to the ZooKeeper server
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        String zkConnectionString = Services.get().getConf().get(ZK_CONNECTION_STRING, "localhost:2181");
        String zkNamespace = Services.get().getConf().get(ZK_NAMESPACE, "oozie");
        client = CuratorFrameworkFactory.builder()
                                            .namespace(zkNamespace)
                                            .connectString(zkConnectionString)
                                            .retryPolicy(retryPolicy)
                                            .build();
        client.start();
    }

    private void advertiseService() throws Exception {
        // Advertise on the service discovery
        new EnsurePath(ZK_BASE_PATH).ensure(client.getZookeeperClient());
        InstanceSerializer<Map> instanceSerializer = new FixedJsonInstanceSerializer<Map>(Map.class);
        sDiscovery = ServiceDiscoveryBuilder.builder(Map.class)
                                                .basePath(ZK_BASE_PATH)
                                                .client(client)
                                                .serializer(instanceSerializer)
                                                .build();
        sDiscovery.start();
        sDiscovery.registerService(getMetadataInstance());

        // Create the service discovery cache
        sCache = sDiscovery.serviceCacheBuilder().name(ZK_OOZIE_SERVICE).build();
        sCache.start();

        zkRegTime = sDiscovery.queryForInstance(ZK_OOZIE_SERVICE, zkId).getRegistrationTimeUTC();
    }

    private void unadvertiseService() throws Exception {
        // Stop the service discovery cache
        sCache.close();

        // Unadvertise on the service discovery
        sDiscovery.unregisterService(getMetadataInstance());
        sDiscovery.close();
    }

    private void teardown() {
        try {
            zk.unadvertiseService();
        }
        catch (Exception ex) {
            log.warn("Exception occurred while unadvertising: " + ex.getMessage(), ex);
        }
        client.close();
        client = null;
    }

    private ServiceInstance<Map> getMetadataInstance() throws Exception {
        // Creates the metadata that this server is providing to ZooKeeper and other Oozie Servers
        String url = ConfigUtils.getOozieEffectiveUrl();
        Map<String, String> map = new HashMap<String, String>();
        map.put(ZKMetadataKeys.OOZIE_ID, zkId);
        map.put(ZKMetadataKeys.OOZIE_URL, url);

        return ServiceInstance.<Map>builder()
            .name(ZK_OOZIE_SERVICE)
            .id(zkId)
            .payload(map)
            .build();
    }

    /**
     * Returns a list of the metadata provided by all of the Oozie Servers.  Note that the metadata is cached so it may be a second
     * or two stale.
     *
     * @return a List of the metadata provided by all of the Oozie Servers.
     * @throws Exception
     */
    public List<ServiceInstance<Map>> getAllMetaData() {
        List<ServiceInstance<Map>> instances = null;
        if (sCache != null) {
            instances = sCache.getInstances();
        }
        return instances;
    }

    /**
     * Returns the ID of this Oozie Server as seen by ZooKeeper and other Oozie Servers
     *
     * @return the ID of this Oozie Server
     */
    public String getZKId() {
        return zkId;
    }

    /**
     * Returns the {@link CuratorFramework} used for managing the ZooKeeper connection; it can be used by calling classes to perform
     * more direct operations on ZooKeeper.  Most of the time, this shouldn't be needed.
     * <p>
     * Be careful not to close the connection.
     *
     * @return the CuratorFramework object
     */
    public CuratorFramework getClient() {
        return client;
    }

    /**
     * Returns the index of this Oozie Server in ZooKeeper's list of Oozie Servers (ordered by registration time)
     *
     * @param oozies The collection of metadata provided by all of the Oozie Servers (from calling {@link ZKUtils#getAllMetaData())
     * @return the index of this Oozie Server in ZooKeeper's list of Oozie Servers (ordered by registration time)
     */
    public int getZKIdIndex(List<ServiceInstance<Map>> oozies) {
        int index = 0;
        // We don't actually have to sort all of the IDs, we can simply find out how many are before our zkId
        for (ServiceInstance<Map> oozie : oozies) {
            long otherRegTime = oozie.getRegistrationTimeUTC();
            if (otherRegTime < zkRegTime) {
                index++;
            }
        }
        return index;
    }

    /**
     * Useful for tests to get the registered classes
     *
     * @return the set of registered classes
     */
    @VisibleForTesting
    public static Set<Object> getUsers() {
        return users;
    }

    /**
     * Keys used in the metadata provided by each Oozie Server to ZooKeeper and other Oozie Servers
     */
    public abstract class ZKMetadataKeys {
        /**
         * The ID of the Oozie Server
         */
        public static final String OOZIE_ID = "OOZIE_ID";
        /**
         * The URL of the Oozie Server
         */
        public static final String OOZIE_URL = "OOZIE_URL";
    }
}
