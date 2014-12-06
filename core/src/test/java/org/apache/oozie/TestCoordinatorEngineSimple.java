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

import java.util.List;
import java.util.Map;

import org.apache.oozie.service.Services;
import org.apache.oozie.test.XTestCase;
import org.junit.Test;

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

    @Test
    public void testParseFilterNegative() throws CoordinatorEngineException {
        final CoordinatorEngine ce = new CoordinatorEngine();

        // null argument:
        Map<String, List<String>> map = ce.parseFilter(null);
        assertNotNull(map);
        assertEquals(0, map.size());

        // empty String:
        map = ce.parseFilter("");
        assertNotNull(map);
        assertEquals(0, map.size());

        // no eq sign in token:
        try {
            ce.parseFilter("winniethepooh");
            fail("CoordinatorEngineException expected.");
        }
        catch (CoordinatorEngineException bee) {
            assertEquals(ErrorCode.E0420, bee.getErrorCode());
        }
        // incorrect k=v:
        try {
            map = ce.parseFilter("kk=vv=zz");
            fail("CoordinatorEngineException expected.");
        }
        catch (CoordinatorEngineException cee) {
            assertEquals(ErrorCode.E0420, cee.getErrorCode());
        }
        // unknown key in key=value pair:
        try {
            ce.parseFilter("foo=moo");
            fail("CoordinatorEngineException expected.");
        }
        catch (CoordinatorEngineException bee) {
            assertEquals(ErrorCode.E0420, bee.getErrorCode());
        }
        // incorrect "status" key value:
        try {
            ce.parseFilter("status=foo");
            fail("CoordinatorEngineException expected.");
        }
        catch (CoordinatorEngineException bee) {
            assertEquals(ErrorCode.E0420, bee.getErrorCode());
        }
        // unparseable "frequency" value:
        try {
            ce.parseFilter("FreQuency=foo");
            fail("CoordinatorEngineException expected.");
        }
        catch (CoordinatorEngineException bee) {
            assertEquals(ErrorCode.E0420, bee.getErrorCode());
        }
        // unparseable "unit" value:
        try {
            ce.parseFilter("UniT=foo");
            fail("CoordinatorEngineException expected.");
        }
        catch (CoordinatorEngineException bee) {
            assertEquals(ErrorCode.E0420, bee.getErrorCode());
        }
        // "unit" specified, but "frequency" is not:
        try {
            ce.parseFilter("unit=minutes");
            fail("CoordinatorEngineException expected.");
        }
        catch (CoordinatorEngineException bee) {
            assertEquals(ErrorCode.E0420, bee.getErrorCode());
        }
    }

    @Test
    public void testParseFilterPositive() throws CoordinatorEngineException {
        final CoordinatorEngine ce = new CoordinatorEngine();

        Map<String, List<String>> map = ce.parseFilter("frequency=5;unit=hours;user=foo;status=FAILED");
        assertEquals(4, map.size());
        assertEquals("300", map.get("frequency").get(0));
        assertEquals("MINUTE", map.get("unit").get(0));
        assertEquals("foo", map.get("user").get(0));
        assertEquals("FAILED", map.get("status").get(0));
    }
}
