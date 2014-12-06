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

import java.util.List;
import java.util.Map;
import static junit.framework.Assert.assertEquals;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.oozie.test.ZKXTestCase;


public class TestZKUtils extends ZKXTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testRegisterAdvertiseUnadvertiseUnregister() throws Exception {
        CuratorFramework client = getClient();
        ServiceDiscovery<Map> sDiscovery = getServiceDiscovery();

        assertNull(client.checkExists().forPath("/services"));
        assertEquals(0, sDiscovery.queryForInstances("servers").size());
        assertNull(sDiscovery.queryForInstance("servers", ZK_ID));
        DummyUser du = new DummyUser();
        DummyUser du2 = new DummyUser();
        try {
            du.register();
            assertNotNull(client.checkExists().forPath("/services"));
            assertEquals(1, sDiscovery.queryForInstances("servers").size());
            assertNotNull(sDiscovery.queryForInstance("servers", ZK_ID));
            du2.register();
            assertNotNull(client.checkExists().forPath("/services"));
            assertEquals(1, sDiscovery.queryForInstances("servers").size());
            assertNotNull(sDiscovery.queryForInstance("servers", ZK_ID));

            du.unregister();
            assertNotNull(client.checkExists().forPath("/services"));
            assertEquals(1, sDiscovery.queryForInstances("servers").size());
            assertNotNull(sDiscovery.queryForInstance("servers", ZK_ID));
            du2.unregister();
            assertNotNull(client.checkExists().forPath("/services"));
            assertEquals(0, sDiscovery.queryForInstances("servers").size());
            assertNull(sDiscovery.queryForInstance("servers", ZK_ID));
        }
        finally {
            du.unregister();
            du2.unregister();
        }
    }

    public void testMetaData() throws Exception {
        ServiceDiscovery<Map> sDiscovery = getServiceDiscovery();

        assertNull(sDiscovery.queryForInstance("servers", ZK_ID));
        DummyUser du = new DummyUser();
        try {
            du.register();
            assertNotNull(sDiscovery.queryForInstance("servers", ZK_ID));
            List<ServiceInstance<Map>> allMetaData = du.getZKUtils().getAllMetaData();
            assertEquals(1, allMetaData.size());
            ServiceInstance<Map> meta = allMetaData.get(0);
            assertEquals(ZK_ID, meta.getId());
            assertEquals("servers", meta.getName());
            Map<String, String> data = meta.getPayload();
            assertEquals(3, data.size());
            assertEquals(ZK_ID, data.get("OOZIE_ID"));
            String url = ConfigUtils.getOozieURL(false);
            assertEquals(url, data.get("OOZIE_URL"));
            assertEquals("java.util.HashMap", data.get("@class"));
        }
        finally {
            du.unregister();
        }
    }

    public void testGetZKId() throws Exception {
        DummyUser du = new DummyUser();
        try {
            du.register();
            assertEquals(ZK_ID, du.getZKUtils().getZKId());
        }
        finally {
            du.unregister();
        }
    }

    public void testGetZKIdIndex() throws Exception {
        DummyUser du = new DummyUser();
        DummyZKOozie dummyOozie = null;
        DummyZKOozie dummyOozie2 = null;
        try {
            dummyOozie = new DummyZKOozie("a", "http://blah");
            du.register();
            assertEquals(1, du.getZKUtils().getZKIdIndex(du.getZKUtils().getAllMetaData()));
            dummyOozie2 = new DummyZKOozie("b", "http://blah");
            assertEquals(1, du.getZKUtils().getZKIdIndex(du.getZKUtils().getAllMetaData()));
            dummyOozie.teardown();
            assertEquals(0, du.getZKUtils().getZKIdIndex(du.getZKUtils().getAllMetaData()));
        }
        finally {
            du.unregister();
            if (dummyOozie != null) {
                dummyOozie.teardown();
            }
            if (dummyOozie2 != null) {
                dummyOozie2.teardown();
            }
        }
    }
}
