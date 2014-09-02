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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.URIHandlerService;
import org.apache.oozie.test.XFsTestCase;
import org.junit.Test;

public class TestFSURIHandler extends XFsTestCase {

    private Services services = null;
    private URIHandlerService uriService;
    private JobConf conf;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        uriService = services.get(URIHandlerService.class);
        conf = createJobConf();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    @Test
    public void testExists() throws Exception {
        Path path1 = new Path(getFsTestCaseDir() + "/2012/12/02/");
        Path path2 = new Path(getFsTestCaseDir() + "/2012/12/12/");
        getFileSystem().mkdirs(path1);
        URIHandler handler = uriService.getURIHandler(path1.toUri());
        assertTrue(handler.exists(path1.toUri(), conf, getTestUser()));
        assertFalse(handler.exists(path2.toUri(), conf, getTestUser()));
        // Try without the scheme.
        handler = uriService.getURIHandler(path1.toUri(), false);
        assertTrue(handler.exists(new URI(path1.toUri().getPath()), conf, getTestUser()));

    }

}
