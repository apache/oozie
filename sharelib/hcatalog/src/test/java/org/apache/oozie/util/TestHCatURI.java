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

import static org.junit.Assert.*;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;
import org.apache.oozie.util.HCatURI;

public class TestHCatURI {

    @Test
    public void testHCatURIParseValidURI() {
        String input = "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12;region=us";
        HCatURI uri = null;
        try {
            uri = new HCatURI(input);
        }
        catch (Exception ex) {
            System.err.print(ex.getMessage());
        }
        assertEquals(uri.getServerEndPoint(), "hcat://hcat.server.com:5080");
        assertEquals(uri.getDb(), "mydb");
        assertEquals(uri.getTable(), "clicks");
        assertEquals(uri.getPartitionValue("datastamp"), "12");
        assertEquals(uri.getPartitionValue("region"), "us");

    }

    @Test(expected = URISyntaxException.class)
    public void testHCatURIParseInvalidURI() throws Exception {
        String input = "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12;region=us/invalid";
        new HCatURI(input);
    }

    @Test(expected = URISyntaxException.class)
    public void testHCatURIParseInvalidPartition() throws Exception {
        String input = "hcat://hcat.server.com:5080/mydb/clicks/datastamp";
        new HCatURI(input);
    }

    @Test(expected = URISyntaxException.class)
    public void testHCatURIParseServerMissing() throws Exception {
        String input = "hcat:///mydb/clicks/datastamp=12;region=us";
        new HCatURI(input);
    }

    @Test(expected = URISyntaxException.class)
    public void testHCatURIParseDBMissing() throws Exception {
        String input = "hcat://hcat.server.com:5080//clicks/datastamp=12;region=us";
        new HCatURI(input);
    }

    @Test(expected = URISyntaxException.class)
    public void testHCatURIParseTableMissing() throws Exception {
        String input = "hcat://hcat.server.com:5080/mydb//datastamp=12;region=us";
        new HCatURI(input);
    }

    @Test
    public void testGetHCatUri() {
        Map<String, String> partitions = new LinkedHashMap<String, String>();
        partitions.put("datastamp", "12");
        partitions.put("region", "us");
        String hcatUri = HCatURI.getHCatURI("hcat", "hcat.server.com:5080", "mydb", "clicks", partitions);

        assertEquals("hcat://hcat.server.com:5080/mydb/clicks/datastamp=12;region=us", hcatUri);
    }

    @Test
    public void testEqualsPositive() {
        HCatURI uri1 = null;
        HCatURI uri2 = null;
        try {
            uri1 = new HCatURI("hcat://hcat.server.com:5080/mydb/clicks/datastamp=12;region=us;timestamp=1201");
            uri2 = new HCatURI("hcat://hcat.server.com:5080/mydb/clicks/datastamp=12;region=us;timestamp=1201");
        }
        catch (URISyntaxException e) {
            fail(e.getMessage());
        }

        assertEquals(uri1, uri2);
    }

    @Test
    public void testEqualsNegative() {
        HCatURI uri1 = null;
        HCatURI uri2 = null;
        HCatURI uri3 = null;
        HCatURI uri4 = null;
        HCatURI uri5 = null;
        try {
            uri1 = new HCatURI("hcat://hcat.server.com:5080/mydb/clicks/datastamp=12;region=us;timestamp=1201");
            uri2 = new HCatURI("hcat://hcat.server.com:5080/mydb2/clicks/region=us;timestamp=1201;datastamp=12");
            uri3 = new HCatURI("hcat://hcat.server.com:5080/mydb/clicks2/region=us;timestamp=1201;datastamp=12");
            uri4 = new HCatURI("hcat://hcat.server.com:5080/mydb/clicks/region=uk;timestamp=1201;datastamp=12");
            uri5 = new HCatURI("hcat://hcat.server.com:5080/mydb/clicks/region=us;timestamp=1201");
        }
        catch (URISyntaxException e) {
            fail(e.getMessage());
        }
        assertFalse(uri1.equals(uri2));
        assertFalse(uri2.equals(uri1));
        assertFalse(uri1.equals(uri3));
        assertFalse(uri3.equals(uri1));
        assertFalse(uri1.equals(uri4));
        assertFalse(uri4.equals(uri1));
        assertFalse(uri1.equals(uri5));
        assertFalse(uri5.equals(uri1));
    }

    @Test
    public void testToFilter() {
        String hcatURI = "hcat://hcat.server.com:5080/mydb/clicks/datastamp=20120230;region=us";
        String filter = "";
        try {
            filter = new HCatURI(hcatURI).toPartitionFilter("java");
        }
        catch (URISyntaxException e) {
            fail(e.getMessage());
        }
        assertTrue(filter.equals("(datastamp='20120230' AND region='us')")
                || filter.equals("(region='us' AND datastamp='20120230')"));
    }
}
