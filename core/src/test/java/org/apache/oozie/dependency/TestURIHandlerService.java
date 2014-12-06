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

package org.apache.oozie.dependency;

import java.net.URI;

import org.apache.oozie.service.Services;
import org.apache.oozie.service.URIHandlerService;
import org.apache.oozie.test.XTestCase;
import org.junit.Test;

public class TestURIHandlerService extends XTestCase {

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
    public void testGetAuthorityWithScheme() throws Exception {
        URIHandlerService uriService = new URIHandlerService();
        URI uri = uriService.getAuthorityWithScheme("hdfs://nn1:8020/dataset/${YEAR}/${MONTH}");
        assertEquals("hdfs://nn1:8020", uri.toString());
        uri = uriService.getAuthorityWithScheme("hdfs://nn1:8020");
        assertEquals("hdfs://nn1:8020", uri.toString());
        uri = uriService.getAuthorityWithScheme("hdfs://nn1:8020/");
        assertEquals("hdfs://nn1:8020", uri.toString());
        uri = uriService.getAuthorityWithScheme("hdfs://///tmp/file");
        assertEquals("hdfs:///", uri.toString());
        uri = uriService.getAuthorityWithScheme("hdfs:///tmp/file");
        assertEquals("hdfs:///", uri.toString());
        uri = uriService.getAuthorityWithScheme("/tmp/file");
        assertEquals("/", uri.toString());
    }

    @Test
    public void testGetURIHandler() throws Exception {
        URIHandlerService uriService = services.get(URIHandlerService.class);
        URI uri = uriService.getAuthorityWithScheme("/tmp/file");
        URIHandler uriHandler = uriService.getURIHandler(uri);
        assertTrue(uriHandler instanceof FSURIHandler);
    }

}
