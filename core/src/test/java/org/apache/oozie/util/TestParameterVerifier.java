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

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.test.XTestCase;

public class TestParameterVerifier extends XTestCase {

    public void testVerifyParametersNull() throws Exception {
        try {
            ParameterVerifier.verifyParameters(null, XmlUtils.parseXml("<root xmlns=\"uri:oozie:workflow:0.4\"></root>"));
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("conf cannot be null", ex.getMessage());
        }
        
        Configuration conf = new Configuration(false);
        conf.set("A", "a");
        ParameterVerifier.verifyParameters(conf, null);
        assertEquals(1, conf.size());
        assertEquals("a", conf.get("A"));
        
        try {
            ParameterVerifier.verifyParameters(null, null);
        } catch (IllegalArgumentException ex) {
            assertEquals("conf cannot be null", ex.getMessage());
        }
    }
    
    public void testVerifyParametersEmpty() throws Exception {
        Configuration conf = new Configuration(false);
        conf.set("A", "a");
        
        ParameterVerifier.verifyParameters(conf, XmlUtils.parseXml("<root xmlns=\"uri:oozie:workflow:0.4\"></root>"));
        assertEquals(1, conf.size());
        assertEquals("a", conf.get("A"));
        
        ParameterVerifier.verifyParameters(conf, XmlUtils.parseXml("<root xmlns=\"uri:oozie:workflow:0.4\">"
                + "<parameters></parameters></root>"));
        assertEquals(1, conf.size());
        assertEquals("a", conf.get("A"));
    }
    
    public void testVerifyParametersMissing() throws Exception {
        Configuration conf = new Configuration(false);
        
        String str = "<root xmlns=\"uri:oozie:workflow:0.4\"><parameters>"
                + "<property><name>hello</name></property>"
                + "</parameters></root>";
        try {
            ParameterVerifier.verifyParameters(conf, XmlUtils.parseXml(str));
            fail();
        } catch(ParameterVerifierException ex) {
            assertEquals(ErrorCode.E0738, ex.getErrorCode());
            assertTrue(ex.getMessage().endsWith("hello"));
            assertTrue(ex.getMessage().contains("1"));
            assertEquals(0, conf.size());
        }
        
        conf = new Configuration(false);
        
        str = "<root xmlns=\"uri:oozie:workflow:0.4\"><parameters>"
                + "<property><name>hello</name><value>world</value></property>"
                + "</parameters></root>";
        ParameterVerifier.verifyParameters(conf, XmlUtils.parseXml(str));
        assertEquals(1, conf.size());
        assertEquals("world", conf.get("hello"));
        
        conf = new Configuration(false);
        
        str = "<root xmlns=\"uri:oozie:workflow:0.4\"><parameters>"
                + "<property><name>hello</name></property>"
                + "<property><name>foo</name><value>bar</value></property>"
                + "<property><name>meh</name></property>"
                + "</parameters></root>";
        try {
            ParameterVerifier.verifyParameters(conf, XmlUtils.parseXml(str));
            fail();
        } catch(ParameterVerifierException ex) {
            assertEquals(ErrorCode.E0738, ex.getErrorCode());
            assertTrue(ex.getMessage().endsWith("hello, meh"));
            assertFalse(ex.getMessage().contains("foo"));
            assertTrue(ex.getMessage().contains("2"));
            assertEquals(1, conf.size());
            assertEquals("bar", conf.get("foo"));
        }
    }
    
    public void testVerifyParametersDefined() throws Exception {
        Configuration conf = new Configuration(false);
        conf.set("hello", "planet");
        
        String str = "<root xmlns=\"uri:oozie:workflow:0.4\"><parameters>"
                + "<property><name>hello</name></property>"
                + "</parameters></root>";
        ParameterVerifier.verifyParameters(conf, XmlUtils.parseXml(str));
        assertEquals(1, conf.size());
        assertEquals("planet", conf.get("hello"));
        
        str = "<root xmlns=\"uri:oozie:workflow:0.4\"><parameters>"
                + "<property><name>hello</name><value>world</value></property>"
                + "</parameters></root>";
        ParameterVerifier.verifyParameters(conf, XmlUtils.parseXml(str));
        assertEquals(1, conf.size());
        assertEquals("planet", conf.get("hello"));
    }
    
    public void testVerifyParametersEmptyName() throws Exception {
        Configuration conf = new Configuration(false);
        
        String str = "<root xmlns=\"uri:oozie:workflow:0.4\"><parameters>"
                + "<property><name></name></property>"
                + "</parameters></root>";
        try {
            ParameterVerifier.verifyParameters(conf, XmlUtils.parseXml(str));
            fail();
        } catch(ParameterVerifierException ex) {
            assertEquals(ErrorCode.E0739, ex.getErrorCode());
        }
        
        str = "<root xmlns=\"uri:oozie:workflow:0.4\"><parameters>"
                + "<property><name>hello</name></property>"
                + "<property><name></name></property>"
                + "</parameters></root>";
        try {
            ParameterVerifier.verifyParameters(conf, XmlUtils.parseXml(str));
            fail();
        } catch(ParameterVerifierException ex) {
            assertEquals(ErrorCode.E0739, ex.getErrorCode());
        }
    }
    
    public void testSupportsParameters() throws Exception {
        assertFalse(ParameterVerifier.supportsParameters("uri:oozie:workflow:0.3"));
        assertTrue(ParameterVerifier.supportsParameters("uri:oozie:workflow:0.4"));
        assertTrue(ParameterVerifier.supportsParameters("uri:oozie:workflow:0.5"));
        
        assertFalse(ParameterVerifier.supportsParameters("uri:oozie:coordinator:0.3"));
        assertTrue(ParameterVerifier.supportsParameters("uri:oozie:coordinator:0.4"));
        assertTrue(ParameterVerifier.supportsParameters("uri:oozie:coordinator:0.5"));
        
        assertFalse(ParameterVerifier.supportsParameters("uri:oozie:bundle:0.1"));
        assertTrue(ParameterVerifier.supportsParameters("uri:oozie:bundle:0.2"));
        assertTrue(ParameterVerifier.supportsParameters("uri:oozie:bundle:0.3"));
        
        assertFalse(ParameterVerifier.supportsParameters("uri:oozie:foo:0.4"));
        assertFalse(ParameterVerifier.supportsParameters("foo"));
    }
}
