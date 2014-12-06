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
import static junit.framework.Assert.assertEquals;

import org.apache.oozie.lock.LockToken;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.ZKLocksService;
import org.apache.oozie.test.ZKXTestCaseWithSecurity;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public class TestZKUtilsWithSecurity extends ZKXTestCaseWithSecurity {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testCheckAndSetACLs() throws Exception {
        // We want to verify the ACLs on locks and the service discovery; ZKUtils does the service discovery and starting
        // ZKLocksService will use ZKUtils which will start advertising on the service discovery.  We can also acquire a lock so
        // it will create a lock znode.
        ZKLocksService zkls = new ZKLocksService();
        try {
            zkls.init(Services.get());
            LockToken lock = zkls.getWriteLock("foo", 3);
            lock.release();
            List<ACL> acls = getClient().getZookeeperClient().getZooKeeper().getACL("/oozie", new Stat());
            assertEquals(ZooDefs.Perms.ALL, acls.get(0).getPerms());
            assertEquals("world", acls.get(0).getId().getScheme());
            assertEquals("anyone", acls.get(0).getId().getId());
            acls = getClient().getACL().forPath("/locks");
            assertEquals(ZooDefs.Perms.ALL, acls.get(0).getPerms());
            assertEquals("world", acls.get(0).getId().getScheme());
            assertEquals("anyone", acls.get(0).getId().getId());
            acls = getClient().getACL().forPath("/locks/foo");
            assertEquals(ZooDefs.Perms.ALL, acls.get(0).getPerms());
            assertEquals("world", acls.get(0).getId().getScheme());
            assertEquals("anyone", acls.get(0).getId().getId());
            acls = getClient().getACL().forPath("/services");
            assertEquals(ZooDefs.Perms.ALL, acls.get(0).getPerms());
            assertEquals("world", acls.get(0).getId().getScheme());
            assertEquals("anyone", acls.get(0).getId().getId());
            acls = getClient().getACL().forPath("/services/servers");
            assertEquals(ZooDefs.Perms.ALL, acls.get(0).getPerms());
            assertEquals("world", acls.get(0).getId().getScheme());
            assertEquals("anyone", acls.get(0).getId().getId());
            acls = getClient().getACL().forPath("/services/servers/" + ZK_ID);
            assertEquals(ZooDefs.Perms.ALL, acls.get(0).getPerms());
            assertEquals("world", acls.get(0).getId().getScheme());
            assertEquals("anyone", acls.get(0).getId().getId());
        }
        finally {
            // unregistering all users of ZKUtils (i.e. ZKLocksService) will cause it to disconnect so when we set
            // "oozie.zookeeper.secure" to true, it will again connect but using SASL/Kerberos
            zkls.destroy();
        }

        // Verify that the expected paths created above still exist with the "world" ACLs
        List<ACL> acls = getClient().getZookeeperClient().getZooKeeper().getACL("/oozie", new Stat());
        assertEquals(ZooDefs.Perms.ALL, acls.get(0).getPerms());
        assertEquals("world", acls.get(0).getId().getScheme());
        assertEquals("anyone", acls.get(0).getId().getId());
        acls = getClient().getACL().forPath("/locks");
        assertEquals(ZooDefs.Perms.ALL, acls.get(0).getPerms());
        assertEquals("world", acls.get(0).getId().getScheme());
        assertEquals("anyone", acls.get(0).getId().getId());
        acls = getClient().getACL().forPath("/locks/foo");
        assertEquals(ZooDefs.Perms.ALL, acls.get(0).getPerms());
        assertEquals("world", acls.get(0).getId().getScheme());
        assertEquals("anyone", acls.get(0).getId().getId());
        acls = getClient().getACL().forPath("/services");
        assertEquals(ZooDefs.Perms.ALL, acls.get(0).getPerms());
        assertEquals("world", acls.get(0).getId().getScheme());
        assertEquals("anyone", acls.get(0).getId().getId());
        acls = getClient().getACL().forPath("/services/servers");
        assertEquals(ZooDefs.Perms.ALL, acls.get(0).getPerms());
        assertEquals("world", acls.get(0).getId().getScheme());
        assertEquals("anyone", acls.get(0).getId().getId());

        zkls = new ZKLocksService();
        try {
            Services.get().getConf().set("oozie.zookeeper.secure", "true");
            // Now that security is enabled, it will trigger the checkAndSetACLs() code to go through and set all of the previously
            // created znodes to have "sasl" ACLs
            zkls.init(Services.get());
            acls = getClient().getZookeeperClient().getZooKeeper().getACL("/oozie", new Stat());
            assertEquals(ZooDefs.Perms.ALL, acls.get(0).getPerms());
            assertEquals("sasl", acls.get(0).getId().getScheme());
            assertEquals(PRIMARY_PRINCIPAL, acls.get(0).getId().getId());
            acls = getClient().getACL().forPath("/locks");
            assertEquals(ZooDefs.Perms.ALL, acls.get(0).getPerms());
            assertEquals("sasl", acls.get(0).getId().getScheme());
            assertEquals(PRIMARY_PRINCIPAL, acls.get(0).getId().getId());
            acls = getClient().getACL().forPath("/locks/foo");
            assertEquals(ZooDefs.Perms.ALL, acls.get(0).getPerms());
            assertEquals("sasl", acls.get(0).getId().getScheme());
            assertEquals(PRIMARY_PRINCIPAL, acls.get(0).getId().getId());
            acls = getClient().getACL().forPath("/services");
            assertEquals(ZooDefs.Perms.ALL, acls.get(0).getPerms());
            assertEquals("sasl", acls.get(0).getId().getScheme());
            assertEquals(PRIMARY_PRINCIPAL, acls.get(0).getId().getId());
            acls = getClient().getACL().forPath("/services/servers");
            assertEquals(ZooDefs.Perms.ALL, acls.get(0).getPerms());
            assertEquals("sasl", acls.get(0).getId().getScheme());
            assertEquals(PRIMARY_PRINCIPAL, acls.get(0).getId().getId());
            acls = getClient().getACL().forPath("/services/servers/" + ZK_ID);
            assertEquals(ZooDefs.Perms.ALL, acls.get(0).getPerms());
            assertEquals("sasl", acls.get(0).getId().getScheme());
            assertEquals(PRIMARY_PRINCIPAL, acls.get(0).getId().getId());
        }
        finally {
            zkls.destroy();
            Services.get().getConf().set("oozie.zookeeper.secure", "false");
        }
    }

    public void testNewUsingACLs() throws Exception {
        // We want to verify the ACLs on new locks and the service discovery; ZKUtils does the service discovery and starting
        // ZKLocksService will use ZKUtils which will start advertising on the service discovery.  We can also acquire a lock so
        // it will create a lock znode.
        ZKLocksService zkls = new ZKLocksService();
        try {
            Services.get().getConf().set("oozie.zookeeper.secure", "true");
            // Verify that the znodes don't already exist
            assertNull(getClient().getZookeeperClient().getZooKeeper().exists("/oozie", null));
            assertNull(getClient().checkExists().forPath("/locks"));
            assertNull(getClient().checkExists().forPath("/services"));
            // Check that new znodes will use the ACLs
            zkls.init(Services.get());
            LockToken lock = zkls.getWriteLock("foo", 3);
            lock.release();
            List<ACL> acls = getClient().getZookeeperClient().getZooKeeper().getACL("/oozie", new Stat());
            assertEquals(ZooDefs.Perms.ALL, acls.get(0).getPerms());
            assertEquals("sasl", acls.get(0).getId().getScheme());
            assertEquals(PRIMARY_PRINCIPAL, acls.get(0).getId().getId());
            acls = getClient().getACL().forPath("/locks");
            assertEquals(ZooDefs.Perms.ALL, acls.get(0).getPerms());
            assertEquals("sasl", acls.get(0).getId().getScheme());
            assertEquals(PRIMARY_PRINCIPAL, acls.get(0).getId().getId());
            acls = getClient().getACL().forPath("/locks/foo");
            assertEquals(ZooDefs.Perms.ALL, acls.get(0).getPerms());
            assertEquals("sasl", acls.get(0).getId().getScheme());
            assertEquals(PRIMARY_PRINCIPAL, acls.get(0).getId().getId());
            acls = getClient().getACL().forPath("/services");
            assertEquals(ZooDefs.Perms.ALL, acls.get(0).getPerms());
            assertEquals("sasl", acls.get(0).getId().getScheme());
            assertEquals(PRIMARY_PRINCIPAL, acls.get(0).getId().getId());
            acls = getClient().getACL().forPath("/services/servers");
            assertEquals(ZooDefs.Perms.ALL, acls.get(0).getPerms());
            assertEquals("sasl", acls.get(0).getId().getScheme());
            assertEquals(PRIMARY_PRINCIPAL, acls.get(0).getId().getId());
            acls = getClient().getACL().forPath("/services/servers/" + ZK_ID);
            assertEquals(ZooDefs.Perms.ALL, acls.get(0).getPerms());
            assertEquals("sasl", acls.get(0).getId().getScheme());
            assertEquals(PRIMARY_PRINCIPAL, acls.get(0).getId().getId());
        }
        finally {
            zkls.destroy();
            Services.get().getConf().set("oozie.zookeeper.secure", "false");
        }
    }
}
