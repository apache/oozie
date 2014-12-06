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

import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.ConfigUtils;
import org.apache.oozie.util.Instrumentation;

public class TestJobsConcurrencyService extends XTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        new Services().init();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testIsLeader() throws Exception {
        JobsConcurrencyService jcs = new JobsConcurrencyService();
        try {
            jcs.init(Services.get());
            assertTrue(jcs.isLeader());
        }
        finally {
            jcs.destroy();
        }
    }

    public void testIsJobIdForThisServer() throws Exception {
        JobsConcurrencyService jcs = new JobsConcurrencyService();
        try {
            jcs.init(Services.get());
            assertTrue(jcs.isJobIdForThisServer("blah"));
        }
        finally {
            jcs.destroy();
        }
    }

    public void testGetJobIdsForThisServer() throws Exception {
        JobsConcurrencyService jcs = new JobsConcurrencyService();
        try {
            jcs.init(Services.get());
            List<String> ids = new ArrayList<String>();
            ids.add("blah");
            ids.add("0000002-130521183438837-oozie-rkan-W");
            ids.add("0000001-130521155209569-oozie-rkan-W");
            List<String> ids2 = jcs.getJobIdsForThisServer(ids);
            assertEquals(ids.size(), ids2.size());
            assertTrue(ids2.containsAll(ids));
        }
        finally {
            jcs.destroy();
        }
    }

    public void testGetServerUrls() throws Exception {
        JobsConcurrencyService jcs = new JobsConcurrencyService();
        try {
            jcs.init(Services.get());
            Map<String, String> map = jcs.getServerUrls();
            assertEquals(1, map.size());
            assertEquals(Services.get().getConf().get("oozie.instance.id"), map.keySet().iterator().next());
            assertEquals(ConfigUtils.getOozieURL(false), map.get(Services.get().getConf().get("oozie.instance.id")));
        }
        finally {
            jcs.destroy();
        }
    }

    public void testsAllServerRequest() throws Exception {
        JobsConcurrencyService jcs = new JobsConcurrencyService();
        try {
            jcs.init(Services.get());
            assertFalse(jcs.isAllServerRequest(null));
            Map<String, String[]> param = new HashMap<String, String[]>();
            assertFalse(jcs.isAllServerRequest(param));
            param.put(RestConstants.ALL_SERVER_REQUEST, new String[] { "test" });
            assertFalse(jcs.isAllServerRequest(param));
            param.put(RestConstants.ALL_SERVER_REQUEST, new String[] { "true" });
            assertFalse(jcs.isAllServerRequest(param));
            param.put(RestConstants.ALL_SERVER_REQUEST, new String[] { "false" });
            assertFalse(jcs.isAllServerRequest(param));
        }
        finally {
            jcs.destroy();
        }
    }

    public void testInstrumentation() throws Exception {
        JobsConcurrencyService jcs = new JobsConcurrencyService();
        Instrumentation instr = new Instrumentation();
        try {
            jcs.init(Services.get());
            jcs.instrument(instr);
            String servers = ConfigurationService.get("oozie.instance.id") + "=" + ConfigUtils.getOozieEffectiveUrl();
            assertEquals(servers, instr.getVariables().get("oozie").get("servers").getValue());
        } finally {
            jcs.destroy();
        }
    }
}
