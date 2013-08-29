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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.oozie.test.ZKXTestCase;
import org.apache.oozie.util.ConfigUtils;
import org.apache.oozie.util.ZKUtils;

public class TestZKJobsConcurrencyService extends ZKXTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testRegisterUnregister() throws Exception {
        assertEquals(0, ZKUtils.getUsers().size());
        ZKJobsConcurrencyService zkjcs = new ZKJobsConcurrencyService();
        try {
            zkjcs.init(Services.get());
            assertEquals(1, ZKUtils.getUsers().size());
            assertEquals(zkjcs, ZKUtils.getUsers().iterator().next());
            zkjcs.destroy();
            assertEquals(0, ZKUtils.getUsers().size());
        }
        finally {
           zkjcs.destroy();
        }
    }

    public void testIsFirstServer() throws Exception {
        ZKJobsConcurrencyService zkjcs = new ZKJobsConcurrencyService();
        // We'll use some DummyZKXOozies here to pretend to be other Oozie servers that will influence isFirstServer()
        // once they are running in that it will only return true for the first Oozie "server"
        DummyZKOozie dummyOozie = null;
        DummyZKOozie dummyOozie2 = null;
        try {
            dummyOozie = new DummyZKOozie("a", "http://blah");
            zkjcs.init(Services.get());
            assertFalse(zkjcs.isFirstServer());
            dummyOozie2 = new DummyZKOozie("b", "http://blah");
            assertFalse(zkjcs.isFirstServer());
            dummyOozie.teardown();
            assertTrue(zkjcs.isFirstServer());
        }
        finally {
            zkjcs.destroy();
            if (dummyOozie != null) {
                dummyOozie.teardown();
            }
            if (dummyOozie2 != null) {
                dummyOozie2.teardown();
            }
        }
    }

    public void testIsJobIdForThisServer() throws Exception {
        ZKJobsConcurrencyService zkjcs = new ZKJobsConcurrencyService();
        // We'll use some DummyZKXOozies here to pretend to be other Oozie servers that will influence isJobIdForThisServer()
        // once they are running in that the indecies of the job ids will cause each job id to belong to different Oozie "servers"
        DummyZKOozie dummyOozie = null;
        DummyZKOozie dummyOozie2 = null;
        try {
            dummyOozie = new DummyZKOozie("a", "http://blah");
            zkjcs.init(Services.get());
            assertFalse(zkjcs.isJobIdForThisServer("0000000-130521183438837-oozie-rkan-W"));
            assertTrue(zkjcs.isJobIdForThisServer("0000001-130521183438837-oozie-rkan-W"));
            assertFalse(zkjcs.isJobIdForThisServer("0000002-130521183438837-oozie-rkan-W"));
            assertTrue(zkjcs.isJobIdForThisServer("0000003-130521183438837-oozie-rkan-W"));
            assertFalse(zkjcs.isJobIdForThisServer("0000004-130521183438837-oozie-rkan-W"));
            assertTrue(zkjcs.isJobIdForThisServer("0000005-130521183438837-oozie-rkan-W"));
            assertFalse(zkjcs.isJobIdForThisServer("0000006-130521183438837-oozie-rkan-W"));
            assertTrue(zkjcs.isJobIdForThisServer("blah"));
            dummyOozie2 = new DummyZKOozie("b", "http://blah");
            assertFalse(zkjcs.isJobIdForThisServer("0000000-130521183438837-oozie-rkan-W"));
            assertTrue(zkjcs.isJobIdForThisServer("0000001-130521183438837-oozie-rkan-W"));
            assertFalse(zkjcs.isJobIdForThisServer("0000002-130521183438837-oozie-rkan-W"));
            assertFalse(zkjcs.isJobIdForThisServer("0000003-130521183438837-oozie-rkan-W"));
            assertTrue(zkjcs.isJobIdForThisServer("0000004-130521183438837-oozie-rkan-W"));
            assertFalse(zkjcs.isJobIdForThisServer("0000005-130521183438837-oozie-rkan-W"));
            assertFalse(zkjcs.isJobIdForThisServer("0000006-130521183438837-oozie-rkan-W"));
            assertTrue(zkjcs.isJobIdForThisServer("blah"));
            dummyOozie.teardown();
            assertTrue(zkjcs.isJobIdForThisServer("0000000-130521183438837-oozie-rkan-W"));
            assertFalse(zkjcs.isJobIdForThisServer("0000001-130521183438837-oozie-rkan-W"));
            assertTrue(zkjcs.isJobIdForThisServer("0000002-130521183438837-oozie-rkan-W"));
            assertFalse(zkjcs.isJobIdForThisServer("0000003-130521183438837-oozie-rkan-W"));
            assertTrue(zkjcs.isJobIdForThisServer("0000004-130521183438837-oozie-rkan-W"));
            assertFalse(zkjcs.isJobIdForThisServer("0000005-130521183438837-oozie-rkan-W"));
            assertTrue(zkjcs.isJobIdForThisServer("0000006-130521183438837-oozie-rkan-W"));
            assertTrue(zkjcs.isJobIdForThisServer("blah"));
            dummyOozie2.teardown();
            assertTrue(zkjcs.isJobIdForThisServer("0000000-130521183438837-oozie-rkan-W"));
            assertTrue(zkjcs.isJobIdForThisServer("0000001-130521183438837-oozie-rkan-W"));
            assertTrue(zkjcs.isJobIdForThisServer("0000002-130521183438837-oozie-rkan-W"));
            assertTrue(zkjcs.isJobIdForThisServer("0000003-130521183438837-oozie-rkan-W"));
            assertTrue(zkjcs.isJobIdForThisServer("0000004-130521183438837-oozie-rkan-W"));
            assertTrue(zkjcs.isJobIdForThisServer("0000005-130521183438837-oozie-rkan-W"));
            assertTrue(zkjcs.isJobIdForThisServer("0000006-130521183438837-oozie-rkan-W"));
            assertTrue(zkjcs.isJobIdForThisServer("blah"));
        }
        finally {
            zkjcs.destroy();
            if (dummyOozie != null) {
                dummyOozie.teardown();
            }
            if (dummyOozie2 != null) {
                dummyOozie2.teardown();
            }
        }
    }

    public void testGetJobIdsForThisServer() throws Exception {
        ZKJobsConcurrencyService zkjcs = new ZKJobsConcurrencyService();
        // We'll use some DummyZKXOozies here to pretend to be other Oozie servers that will influence getJobIdsForThisServer()
        // once they are running in that the indecies of the job ids will cause each job id to belong to different Oozie "servers"
        DummyZKOozie dummyOozie = null;
        DummyZKOozie dummyOozie2 = null;
        try {
            List<String> ids = new ArrayList<String>();
            ids.add("0000000-130521183438837-oozie-rkan-W");
            ids.add("0000001-130521183438837-oozie-rkan-W");
            ids.add("0000002-130521183438837-oozie-rkan-W");
            ids.add("0000003-130521183438837-oozie-rkan-W");
            ids.add("0000004-130521183438837-oozie-rkan-W");
            ids.add("0000005-130521183438837-oozie-rkan-W");
            ids.add("0000006-130521183438837-oozie-rkan-W");
            ids.add("blah");
            dummyOozie = new DummyZKOozie("a", "http://blah");
            zkjcs.init(Services.get());
            List<String> ids2 = zkjcs.getJobIdsForThisServer(ids);
            List<String> ids3 = new ArrayList<String>();
            ids3.add("0000001-130521183438837-oozie-rkan-W");
            ids3.add("0000003-130521183438837-oozie-rkan-W");
            ids3.add("0000005-130521183438837-oozie-rkan-W");
            ids3.add("blah");
            assertEquals(4, ids2.size());
            assertTrue(ids2.containsAll(ids3));
            dummyOozie2 = new DummyZKOozie("b", "http://blah");
            ids2 = zkjcs.getJobIdsForThisServer(ids);
            ids3 = new ArrayList<String>();
            ids3.add("0000001-130521183438837-oozie-rkan-W");
            ids3.add("0000004-130521183438837-oozie-rkan-W");
            ids3.add("blah");
            assertEquals(3, ids2.size());
            assertTrue(ids2.containsAll(ids3));
            dummyOozie.teardown();
            ids2 = zkjcs.getJobIdsForThisServer(ids);
            ids3 = new ArrayList<String>();
            ids3.add("0000000-130521183438837-oozie-rkan-W");
            ids3.add("0000002-130521183438837-oozie-rkan-W");
            ids3.add("0000004-130521183438837-oozie-rkan-W");
            ids3.add("0000006-130521183438837-oozie-rkan-W");
            ids3.add("blah");
            assertEquals(5, ids2.size());
            assertTrue(ids2.containsAll(ids3));
            dummyOozie2.teardown();
            ids2 = zkjcs.getJobIdsForThisServer(ids);
            assertEquals(8, ids2.size());
            assertTrue(ids2.containsAll(ids));
        }
        finally {
            zkjcs.destroy();
            if (dummyOozie != null) {
                dummyOozie.teardown();
            }
            if (dummyOozie2 != null) {
                dummyOozie2.teardown();
            }
        }
    }

    public void testGetServerUrls() throws Exception {
        ZKJobsConcurrencyService zkjcs = new ZKJobsConcurrencyService();
        // We'll use some DummyZKXOozies here to pretend to be other Oozie servers that will influence getServerUrls()
        // once they are running in that the there will be other Oozie "server" urls to return
        DummyZKOozie dummyOozie = null;
        DummyZKOozie dummyOozie2 = null;
        try {
            zkjcs.init(Services.get());
            Map<String, String> map = zkjcs.getServerUrls();
            assertEquals(1, map.size());
            assertEquals(ZK_ID, map.keySet().iterator().next());
            assertEquals(ConfigUtils.getOozieURL(false), map.get(ZK_ID));
            dummyOozie = new DummyZKOozie("0000", "http://blah1");
            map = zkjcs.getServerUrls();
            assertEquals(2, map.size());
            assertEquals(ZK_ID, map.keySet().iterator().next());
            assertEquals(ConfigUtils.getOozieURL(false), map.get(ZK_ID));
            assertEquals(ZK_ID, map.keySet().iterator().next());
            assertEquals("http://blah1", map.get("0000"));
            dummyOozie2 = new DummyZKOozie("z", "http://blah2");
            map = zkjcs.getServerUrls();
            assertEquals(3, map.size());
            assertEquals(ZK_ID, map.keySet().iterator().next());
            assertEquals(ConfigUtils.getOozieURL(false), map.get(ZK_ID));
            assertEquals(ZK_ID, map.keySet().iterator().next());
            assertEquals("http://blah1", map.get("0000"));
            assertEquals(ZK_ID, map.keySet().iterator().next());
            assertEquals("http://blah2", map.get("z"));
            dummyOozie.teardown();
            map = zkjcs.getServerUrls();
            assertEquals(2, map.size());
            assertEquals(ZK_ID, map.keySet().iterator().next());
            assertEquals(ConfigUtils.getOozieURL(false), map.get(ZK_ID));
            assertEquals(ZK_ID, map.keySet().iterator().next());
            assertEquals("http://blah2", map.get("z"));
            dummyOozie2.teardown();
            map = zkjcs.getServerUrls();
            assertEquals(1, map.size());
            assertEquals(ZK_ID, map.keySet().iterator().next());
            assertEquals(ConfigUtils.getOozieURL(false), map.get(ZK_ID));
        }
        finally {
            zkjcs.destroy();
            if (dummyOozie != null) {
                dummyOozie.teardown();
            }
            if (dummyOozie2 != null) {
                dummyOozie2.teardown();
            }
        }
    }
}
