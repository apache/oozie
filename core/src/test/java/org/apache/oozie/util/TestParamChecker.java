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

import org.apache.oozie.test.XTestCase;

import java.util.ArrayList;
import java.util.Arrays;

public class TestParamChecker extends XTestCase {

    public void testNotNull() {
        ParamChecker.notNull("value", "name");
        try {
            ParamChecker.notNull(null, "name");
            fail();
        }
        catch (IllegalArgumentException ex) {
            // nop
        }
    }

    public void testNotNullElements() {
        ParamChecker.notEmptyElements(new ArrayList<String>(), "name");
        ParamChecker.notEmptyElements(Arrays.asList("a"), "name");
        try {
            ParamChecker.notEmptyElements(null, "name");
            fail();
        }
        catch (IllegalArgumentException ex) {
            // nop
        }
        try {
            ParamChecker.notEmptyElements(Arrays.asList("a", null), "name");
            fail();
        }
        catch (IllegalArgumentException ex) {
            // nop
        }
    }

    public void testNotEmpty() {
        ParamChecker.notEmpty("value", "name");
        try {
            ParamChecker.notEmpty(null, "name");
            fail();
        }
        catch (IllegalArgumentException ex) {
            // nop
        }
        try {
            ParamChecker.notEmpty("", "name");
            fail();
        }
        catch (IllegalArgumentException ex) {
            // nop
        }
    }

    public void testNotEmptyElements() {
        ParamChecker.notEmptyElements(new ArrayList<String>(), "name");
        ParamChecker.notEmptyElements(Arrays.asList("a"), "name");
        try {
            ParamChecker.notEmptyElements(null, "name");
            fail();
        }
        catch (IllegalArgumentException ex) {
            // nop
        }
        try {
            ParamChecker.notEmptyElements(Arrays.asList("a", null), "name");
            fail();
        }
        catch (IllegalArgumentException ex) {
            // nop
        }
    }

    public void testValidToken() {
        ParamChecker.validateActionName("azAZ09_-");
        try {
            ParamChecker.validateActionName(null);
            fail();
        }
        catch (IllegalArgumentException ex) {
            // nop
        }
        try {
            ParamChecker.validateActionName("");
            fail();
        }
        catch (IllegalArgumentException ex) {
            // nop
        }
        try {
            ParamChecker.validateActionName("@");
            fail();
        }
        catch (IllegalArgumentException ex) {
            // nop
        }
    }

    public void testValidIdentifier() {
        assertTrue(ParamChecker.isValidIdentifier("a"));
        assertTrue(ParamChecker.isValidIdentifier("a1"));
        assertTrue(ParamChecker.isValidIdentifier("a_"));
        assertTrue(ParamChecker.isValidIdentifier("_"));
        assertFalse(ParamChecker.isValidIdentifier("!"));
        assertFalse(ParamChecker.isValidIdentifier("1"));
    }

    public void testCheckGTZero() {
        assertEquals(120, ParamChecker.checkGTZero(120, "test"));
        try {
            ParamChecker.checkGTZero(0, "test");
            fail();
        }
        catch (Exception ex) {
        }
        try {
            ParamChecker.checkGTZero(-1, "test");
            fail();
        }
        catch (Exception ex) {
        }
    }

    public void testCheckGEZero() {
        assertEquals(120, ParamChecker.checkGEZero(120, "test"));
        assertEquals(0, ParamChecker.checkGEZero(0, "test"));
        try {
            ParamChecker.checkGEZero(-1, "test");
            fail();
        }
        catch (Exception ex) {
        }
    }

    public void testCheckInteger() {
        assertEquals(120, ParamChecker.checkInteger("120", "test"));
        assertEquals(-12, ParamChecker.checkInteger("-12", "test"));
        try {
            ParamChecker.checkInteger("ABCD", "test");
            fail();
        }
        catch (Exception ex) {
        }
        try {
            ParamChecker.checkInteger("1.5", "test");
            fail();
        }
        catch (Exception ex) {
        }
    }

    public void testCheckUTC() {
        ParamChecker.checkUTC("2009-02-01T01:00Z", "test");
        try {
            ParamChecker.checkUTC("2009-02-01T01:00", "test");
            fail();
        }
        catch (Exception ex) {
        }
        try {
            ParamChecker.checkUTC("2009-02-01U01:00Z", "test");
            fail();
        }
        catch (Exception ex) {
        }
    }

    public void testCheckTimeZone() {
        ParamChecker.checkTimeZone("UTC", "test");
        try {
            ParamChecker.checkTimeZone("UTZ", "test");
            fail();
        }
        catch (Exception ex) {
        }
        ParamChecker.checkTimeZone("America/Los_Angeles", "test");
        try {
            ParamChecker.checkTimeZone("America/Los_Angles", "test");
            fail();
        }
        catch (Exception ex) {
        }
    }

    public void testIsMember() {
        String[] members = {"LIFO", "FIFO", "ONLYLAST"};
        ParamChecker.isMember("FIFO", members, "test");
        try {
            ParamChecker.isMember("FIF", members, "test");
            fail();
        }
        catch (Exception ex) {
        }

    }

}
