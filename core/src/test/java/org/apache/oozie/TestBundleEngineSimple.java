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

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.rest.BulkResponseImpl;

/**
 * Test non-argument constructor and methods of {@link BundleEngine} that either throw exceptions or return null.
 * {@link BundleEngineException} covered as well.
 */
public class TestBundleEngineSimple extends TestCase {

    public void testGetCoordJob1() {
        BundleEngine be = new BundleEngine();
        try {
            CoordinatorJob cj = be.getCoordJob("foo");
            fail("Expected BundleEngineException was not thrown.");
        }
        catch (BundleEngineException bee) {
            assertEquals(ErrorCode.E0301, bee.getErrorCode());
        }
    }

    public void testGetCoordJob4() {
        BundleEngine be = new BundleEngine();
        try {
            CoordinatorJob cj = be.getCoordJob("foo", "filter", 0, 1, false);
            fail("Expected BundleEngineException was not thrown.");
        }
        catch (BundleEngineException bee) {
            assertEquals(ErrorCode.E0301, bee.getErrorCode());
        }
    }

    public void testGetJob1() {
        BundleEngine be = new BundleEngine();
        try {
            WorkflowJob wj = be.getJob("foo");
            fail("Expected BundleEngineException was not thrown.");
        }
        catch (BundleEngineException bee) {
            assertEquals(ErrorCode.E0301, bee.getErrorCode());
        }
    }

    public void testGetJob3() {
        BundleEngine be = new BundleEngine();
        try {
            WorkflowJob wj = be.getJob("foo", 0, 1);
            fail("Expected BundleEngineException was not thrown.");
        }
        catch (BundleEngineException bee) {
            assertEquals(ErrorCode.E0301, bee.getErrorCode());
        }
    }

    @SuppressWarnings("deprecation")
    public void testReRun2() {
        BundleEngine be = new BundleEngine();
        try {
            Configuration c = new Configuration();
            be.reRun("foo", c);
            fail("Expected BundleEngineException was not thrown.");
        }
        catch (BundleEngineException bee) {
            assertEquals(ErrorCode.E0301, bee.getErrorCode());
        }
    }

    public void testGetJobForExternalId() throws BundleEngineException {
        BundleEngine be = new BundleEngine();
        String job = be.getJobIdForExternalId("externalFoo");
        assertNull(job);
    }

    /**
     * Test negative cases of the filter parsing by
     * {@link BundleEngine#parseFilter(String)}.
     */
    public void testParseFilterNegative() {
        BundleEngine be = new BundleEngine();
        // no eq sign in token:
        try {
            be.parseFilter("winniethepooh");
            fail("BundleEngineException expected.");
        }
        catch (BundleEngineException bee) {
            assertEquals(ErrorCode.E0420, bee.getErrorCode());
        }
        // incorrect key=value pair syntax:
        try {
            be.parseFilter("xx=yy=zz");
            fail("BundleEngineException expected.");
        }
        catch (BundleEngineException bee) {
            assertEquals(ErrorCode.E0420, bee.getErrorCode());
        }
        // unknown key in key=value pair:
        try {
            be.parseFilter("foo=moo");
            fail("BundleEngineException expected.");
        }
        catch (BundleEngineException bee) {
            assertEquals(ErrorCode.E0420, bee.getErrorCode());
        }
        // incorrect "status" key value:
        try {
            be.parseFilter("status=foo");
            fail("BundleEngineException expected.");
        }
        catch (BundleEngineException bee) {
            assertEquals(ErrorCode.E0420, bee.getErrorCode());
        }
    }

    /**
     * Test negative cases of method
     * {@link BundleEngine#parseBulkFilter(String)}
     */
    public void testParseBulkFilterNegative() {
        // incorrect key=value pair syntax:
        try {
            BundleEngine.parseBulkFilter("xx=yy=zz");
            fail("BundleEngineException expected.");
        }
        catch (BundleEngineException bee) {
            assertEquals(bee.toString(), ErrorCode.E0420, bee.getErrorCode());
        }
        // no eq sign in token:
        try {
            BundleEngine.parseBulkFilter("winniethepooh");
            fail("BundleEngineException expected.");
        }
        catch (BundleEngineException bee) {
            assertEquals(bee.toString(), ErrorCode.E0420, bee.getErrorCode());
        }
        // one of the values is a whitespace:
        try {
            BundleEngine.parseBulkFilter(BulkResponseImpl.BULK_FILTER_BUNDLE + "=aaa, ,bbb");
            fail("BundleEngineException expected.");
        }
        catch (BundleEngineException bee) {
            assertEquals(bee.toString(), ErrorCode.E0420, bee.getErrorCode());
        }
        // unparseable time value:
        try {
            BundleEngine.parseBulkFilter(BulkResponseImpl.BULK_FILTER_START_CREATED_EPOCH + "=blah-blah");
            fail("BundleEngineException expected.");
        }
        catch (BundleEngineException bee) {
            assertEquals(bee.toString(), ErrorCode.E0420, bee.getErrorCode());
        }
        // incorrect status:
        try {
            BundleEngine.parseBulkFilter(BulkResponseImpl.BULK_FILTER_STATUS + "=foo");
            fail("BundleEngineException expected.");
        }
        catch (BundleEngineException bee) {
            assertEquals(bee.toString(), ErrorCode.E0420, bee.getErrorCode());
        }
        // filter does not contain "BulkResponseImpl.BULK_FILTER_BUNDLE_NAME" field:
        try {
            BundleEngine.parseBulkFilter(BulkResponseImpl.BULK_FILTER_LEVEL + "=foo");
            fail("BundleEngineException expected.");
        }
        catch (BundleEngineException bee) {
            assertEquals(bee.toString(), ErrorCode.E0305, bee.getErrorCode());
        }
    }

}
