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

package org.apache.oozie;

import org.apache.oozie.CoordinatorEngine.FILTER_COMPARATORS;
import org.apache.oozie.client.CoordinatorAction.Status;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.Pair;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class TestCoordinatorEngineSimple extends XTestCase {

    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testParseJobFilter() throws CoordinatorEngineException {
        final CoordinatorEngine ce = new CoordinatorEngine();

        //valid status filter
        Map<Pair<String, FILTER_COMPARATORS>, List<Object>> map = ce.parseJobFilter("staTus=succeeded; status=waiTing");
        assertNotNull(map);
        assertEquals(1, map.size());
        Pair<String, FILTER_COMPARATORS> key = map.keySet().iterator().next();
        assertNotNull(key);
        assertEquals(OozieClient.FILTER_STATUS, key.getFist());
        assertEquals(FILTER_COMPARATORS.EQUALS, key.getSecond());
        List<Object> list = map.get(key);
        assertNotNull(list);
        assertEquals(2, list.size());
        assertEquals(Status.SUCCEEDED.name(), (String) list.get(0));
        assertEquals(Status.WAITING.name(), (String)list.get(1));

        //valid nominal time filter
        map = ce.parseJobFilter("nominaltime>=2013-05-01T10:00Z");
        assertNotNull(map);
        assertEquals(1, map.size());
        key = map.keySet().iterator().next();
        assertNotNull(key);
        assertEquals(OozieClient.FILTER_NOMINAL_TIME, key.getFist());
        assertEquals(FILTER_COMPARATORS.GREATER_EQUAL, key.getSecond());
        list = map.get(key);
        assertNotNull(list);
        assertEquals(1, list.size());
        assertEquals("2013-05-01T10:00Z", DateUtils.formatDateOozieTZ(new Date(((Timestamp) list.get(0)).getTime())));

        //invalid format
        try {
            ce.parseJobFilter("winniethepooh");
            fail("CoordinatorEngineException expected.");
        }
        catch (CoordinatorEngineException bee) {
            assertEquals(ErrorCode.E0421, bee.getErrorCode());
        }

        //invalid key
        try {
            ce.parseJobFilter("stat=some");
            fail("CoordinatorEngineException expected.");
        }
        catch (CoordinatorEngineException bee) {
            assertEquals(ErrorCode.E0421, bee.getErrorCode());
        }

        //invalid status value
        try {
            ce.parseJobFilter("status=some");
            fail("CoordinatorEngineException expected.");
        }
        catch (CoordinatorEngineException bee) {
            assertEquals(ErrorCode.E0421, bee.getErrorCode());
        }

        //invalid comparator for status
        try {
            ce.parseJobFilter("status>=some");
            fail("CoordinatorEngineException expected.");
        }
        catch (CoordinatorEngineException bee) {
            assertEquals(ErrorCode.E0421, bee.getErrorCode());
        }

        //invalid nominal time value
        try {
            ce.parseJobFilter("nominaltime=2013-13-01T00:00Z");
            fail("CoordinatorEngineException expected.");
        }
        catch (CoordinatorEngineException bee) {
            assertEquals(ErrorCode.E0421, bee.getErrorCode());
        }

        //invalid comparator
        try {
            ce.parseJobFilter("nominaltime*2013-13-01T00:00Z");
            fail("CoordinatorEngineException expected.");
        }
        catch (CoordinatorEngineException bee) {
            assertEquals(ErrorCode.E0421, bee.getErrorCode());
        }
    }

    public void testParseFilterNegative() throws CoordinatorEngineException {
        final CoordinatorEngine ce = new CoordinatorEngine();

        // null argument:
        Map<String, List<String>> map = ce.parseJobsFilter(null);
        assertNotNull(map);
        assertEquals(0, map.size());

        // empty String:
        map = ce.parseJobsFilter("");
        assertNotNull(map);
        assertEquals(0, map.size());

        // no eq sign in token:
        try {
            ce.parseJobsFilter("winniethepooh");
            fail("CoordinatorEngineException expected.");
        }
        catch (CoordinatorEngineException bee) {
            assertEquals(ErrorCode.E0420, bee.getErrorCode());
        }
        // incorrect k=v:
        try {
            ce.parseJobsFilter("kk=vv=zz");
            fail("CoordinatorEngineException expected.");
        }
        catch (CoordinatorEngineException cee) {
            assertEquals(ErrorCode.E0420, cee.getErrorCode());
        }
        // unknown key in key=value pair:
        try {
            ce.parseJobsFilter("foo=moo");
            fail("CoordinatorEngineException expected.");
        }
        catch (CoordinatorEngineException bee) {
            assertEquals(ErrorCode.E0420, bee.getErrorCode());
        }
        // incorrect "status" key value:
        try {
            ce.parseJobsFilter("status=foo");
            fail("CoordinatorEngineException expected.");
        }
        catch (CoordinatorEngineException bee) {
            assertEquals(ErrorCode.E0420, bee.getErrorCode());
        }
        // unparseable "frequency" value:
        try {
            ce.parseJobsFilter("FreQuency=foo");
            fail("CoordinatorEngineException expected.");
        }
        catch (CoordinatorEngineException bee) {
            assertEquals(ErrorCode.E0420, bee.getErrorCode());
        }
        // unparseable "unit" value:
        try {
            ce.parseJobsFilter("UniT=foo");
            fail("CoordinatorEngineException expected.");
        }
        catch (CoordinatorEngineException bee) {
            assertEquals(ErrorCode.E0420, bee.getErrorCode());
        }
        // "unit" specified, but "frequency" is not:
        try {
            ce.parseJobsFilter("unit=minutes");
            fail("CoordinatorEngineException expected.");
        }
        catch (CoordinatorEngineException bee) {
            assertEquals(ErrorCode.E0420, bee.getErrorCode());
        }
    }

    public void testParseFilterPositive() throws CoordinatorEngineException {
        final CoordinatorEngine ce = new CoordinatorEngine();

        Map<String, List<String>> map = ce.parseJobsFilter("frequency=5;unit=hours;user=foo;status=FAILED");
        assertEquals(4, map.size());
        assertEquals("300", map.get("frequency").get(0));
        assertEquals("MINUTE", map.get("unit").get(0));
        assertEquals("foo", map.get("user").get(0));
        assertEquals("FAILED", map.get("status").get(0));
    }
}
