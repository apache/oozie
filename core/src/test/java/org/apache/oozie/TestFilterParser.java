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

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.servlet.ServletException;

import static org.junit.Assert.assertEquals;

public class TestFilterParser {
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testNullFilter() throws ServletException {
        Multimap<String, String> filterMap = FilterParser.parseFilter(null);
        assertEquals("Filter map size should be zero for null filter", 0, filterMap.size());
    }

    @Test
    public void testEmptyString() throws ServletException {
        Multimap<String, String> filterMap = FilterParser.parseFilter("");
        assertEquals("Filter map size should be zero for empty filter", 0, filterMap.size());
    }

    @Test
    public void testMissingEquals() throws ServletException {
        expectedException.expect(ServletException.class);
        FilterParser.parseFilter("keyvalue");
    }

    @Test
    public void testMissingKey() throws ServletException {
        expectedException.expect(ServletException.class);
        FilterParser.parseFilter("=value");
    }

    @Test
    public void testTooManyEquals() throws ServletException {
        expectedException.expect(ServletException.class);
        FilterParser.parseFilter("key=value1=value2");
    }

    @Test
    public void testMissingValue() throws ServletException {
        expectedException.expect(ServletException.class);
        FilterParser.parseFilter("key1=");
    }

    @Test
    public void testSingleParameter() throws ServletException {
        ListMultimap<String, String> filterMap = FilterParser.parseFilter("key1=value1");
        ListMultimap<String, String> expectedMap = LinkedListMultimap.create();
        expectedMap.put("key1", "value1");
        assertEquals("Different filter map", expectedMap, filterMap);
    }

    @Test
    public void testTwoParameters() throws ServletException {
        Multimap<String, String> filterMap = FilterParser.parseFilter("key1=value1;key2=value2");
        ListMultimap<String, String> expectedMap = LinkedListMultimap.create();
        expectedMap.put("key1", "value1");
        expectedMap.put("key2", "value2");
        assertEquals("Different filter map", expectedMap, filterMap);
    }

    @Test
    public void testRepeatedKeys() throws ServletException {
        Multimap<String, String> filterMap = FilterParser.parseFilter("key1=value1;key1=value2");
        ListMultimap<String, String> expectedMap = LinkedListMultimap.create();
        expectedMap.put("key1", "value1");
        expectedMap.put("key1", "value2");
        assertEquals("Different filter map", expectedMap, filterMap);
    }

}