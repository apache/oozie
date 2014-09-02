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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static junit.framework.Assert.assertEquals;

import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.test.ZKXTestCase;
import org.apache.oozie.util.ConfigUtils;
import org.apache.oozie.util.Instrumentation;
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

    public void testIsLeader() throws Exception {
        ZKJobsConcurrencyService zkjcs = new ZKJobsConcurrencyService();
        // We'll use some DummyZKXOozies here to pretend to be other Oozie servers.  It chooses randomly so we can't check that a
        // specific server gets chosen.
        DummyZKOozie dummyOozie = null;
        DummyZKOozie dummyOozie2 = null;
        try {
            zkjcs.init(Services.get());
            dummyOozie = new DummyZKOozie("a", "http://blah", true);
            dummyOozie2 = new DummyZKOozie("b", "http://blah", true);
            sleep(3 * 1000);
            if (zkjcs.isLeader()) {
                assertFalse(dummyOozie.isLeader());
                assertFalse(dummyOozie2.isLeader());
                zkjcs.destroy();
                sleep(3 * 1000);
                if (dummyOozie.isLeader()) {
                    assertFalse(dummyOozie2.isLeader());
                } else if (dummyOozie2.isLeader()) {
                    assertFalse(dummyOozie.isLeader());
                } else {
                    fail("No leader");
                }
            } else if (dummyOozie.isLeader()) {
                assertFalse(zkjcs.isLeader());
                assertFalse(dummyOozie2.isLeader());
                dummyOozie.teardown();
                sleep(3 * 1000);
                if (zkjcs.isLeader()) {
                    assertFalse(dummyOozie2.isLeader());
                } else if (dummyOozie2.isLeader()) {
                    assertFalse(zkjcs.isLeader());
                } else {
                    fail("No leader");
                }
            } else if (dummyOozie2.isLeader()) {
                assertFalse(zkjcs.isLeader());
                assertFalse(dummyOozie.isLeader());
                dummyOozie2.teardown();
                sleep(3 * 1000);
                if (zkjcs.isLeader()) {
                    assertFalse(dummyOozie.isLeader());
                } else if (dummyOozie.isLeader()) {
                    assertFalse(zkjcs.isLeader());
                } else {
                    fail("No leader");
                }
            } else {
                fail("No leader");
            }
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

    public void testisAllServerRequest() throws Exception {
        ZKJobsConcurrencyService zkjcs = new ZKJobsConcurrencyService();
        try {
            zkjcs.init(Services.get());
            assertTrue(zkjcs.isAllServerRequest(null));
            Map<String, String[]> param = new HashMap<String, String[]>();
            assertTrue(zkjcs.isAllServerRequest(param));
            param.put(RestConstants.ALL_SERVER_REQUEST, new String[] { "test" });
            assertTrue(zkjcs.isAllServerRequest(param));
            param.put(RestConstants.ALL_SERVER_REQUEST, new String[] { "true" });
            assertTrue(zkjcs.isAllServerRequest(param));
            param.put(RestConstants.ALL_SERVER_REQUEST, new String[] { "false" });
            assertFalse(zkjcs.isAllServerRequest(param));
        }
        finally {
            zkjcs.destroy();
        }
    }

    public void testInstrumentation() throws Exception {
        ZKJobsConcurrencyService zkjcs = new ZKJobsConcurrencyService();
        // We'll use some DummyZKXOozies here to pretend to be other Oozie servers that will influence the instrumentation
        // once they are running in that the there will be other Oozie "servers"
        DummyZKOozie dummyOozie = null;
        DummyZKOozie dummyOozie2 = null;
        Instrumentation instr = new Instrumentation();
        try {
            zkjcs.init(Services.get());
            zkjcs.instrument(instr);
            String servers = ZK_ID + "=" + ConfigUtils.getOozieURL(false);
            assertEquals(servers, instr.getVariables().get("oozie").get("servers").getValue());
            dummyOozie = new DummyZKOozie("0000", "http://blah1");
            servers = ZK_ID + "=" + ConfigUtils.getOozieURL(false) + ",0000=http://blah1";
            assertEquals(servers, instr.getVariables().get("oozie").get("servers").getValue());
            dummyOozie2 = new DummyZKOozie("z", "http://blah2");
            servers = ZK_ID + "=" + ConfigUtils.getOozieURL(false) + ",0000=http://blah1" + ",z=http://blah2";
            assertEquals(servers, instr.getVariables().get("oozie").get("servers").getValue());
            dummyOozie.teardown();
            servers = ZK_ID + "=" + ConfigUtils.getOozieURL(false) + ",z=http://blah2";
            assertEquals(servers, instr.getVariables().get("oozie").get("servers").getValue());
            dummyOozie2.teardown();
            servers = ZK_ID + "=" + ConfigUtils.getOozieURL(false);
            assertEquals(servers, instr.getVariables().get("oozie").get("servers").getValue());
        } finally {
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
