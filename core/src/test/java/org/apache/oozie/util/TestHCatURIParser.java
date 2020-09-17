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

import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XTestCase;

import java.net.URI;
import java.util.regex.Pattern;

public class TestHCatURIParser extends XTestCase {

    private Services services;
    private Pattern HCAT_URI_PATTERN;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        String HCAT_URI_REGEX_CONFIG = ConfigurationService.get("oozie.hcat.uri.regex.pattern");
        HCAT_URI_PATTERN = Pattern.compile(HCAT_URI_REGEX_CONFIG);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        services.destroy();
    }

    public void testWhenMultipleHCatURIsAreSplitPartsAreExtractedCorrectly1() {
        String uri = "hcat://hostname1:1000,hcat://hostname2:2000/mydb/clicks/datastamp=12;region=us,scheme://hostname3:3000," +
                "scheme://hostname4:4000,scheme://hostname5:5000/db/table/p1=12;p2=us,scheme://hostname4:4000/d/t/p=1";
        String[] uris = HCatURIParser.splitHCatUris(uri, HCAT_URI_PATTERN);
        assertEquals(3, uris.length);
        assertEquals("hcat://hostname1:1000,hcat://hostname2:2000/mydb/clicks/datastamp=12;region=us", uris[0]);
        assertEquals("scheme://hostname3:3000,scheme://hostname4:4000,scheme://hostname5:5000/db/table/p1=12;p2=us", uris[1]);
        assertEquals("scheme://hostname4:4000/d/t/p=1", uris[2]);
    }

    public void testWhenMultipleHCatURIsAreSplitPartsAreExtractedCorrectly2() {
        String uri = "thrift://host.name1:1000/mydb/clicks/datastamp=12;region=u_s";
        String[] uris = HCatURIParser.splitHCatUris(uri, HCAT_URI_PATTERN);
        assertEquals(1, uris.length);
        assertEquals("thrift://host.name1:1000/mydb/clicks/datastamp=12;region=u_s", uris[0]);
    }

    public void testWhenMultipleHCatURIsAreSplitPartsAreExtractedCorrectly3() {
        String uri = "hcat://10.10.10.10:9083/default/invites/ds=2010-01-01;region=usa";
        String[] uris = HCatURIParser.splitHCatUris(uri, HCAT_URI_PATTERN);
        assertEquals(1, uris.length);
        assertEquals("hcat://10.10.10.10:9083/default/invites/ds=2010-01-01;region=usa", uris[0]);
    }

    public void testParsingMultipleHCatServerURI() throws Exception {
        String uriStr = "hcat://hcat.server.com:5080,hcat://hcat.server1.com:5080/mydb/clicks/datastamp=12;region=us";
        String[] uris = HCatURIParser.splitHCatUris(uriStr, HCAT_URI_PATTERN);
        URI uri = HCatURIParser.parseURI(new URI(uris[0]));
        assertEquals("hcat", uri.getScheme());
        assertEquals("hcat.server.com:5080,hcat.server1.com:5080", uri.getAuthority());
    }

    public void testParsingSingleServerURI() throws Exception {
        String uriStr = "hdfs://namenode.example.com:8020/path/to/directory/file";
        URI uri = HCatURIParser.parseURI(new URI(uriStr));
        assertEquals("hdfs", uri.getScheme());
        assertEquals("namenode.example.com:8020", uri.getAuthority());
        assertEquals("/path/to/directory/file", uri.getPath());
    }
}