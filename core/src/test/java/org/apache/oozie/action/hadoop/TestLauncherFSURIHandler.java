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

package org.apache.oozie.action.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.URIHandlerService;
import org.apache.oozie.test.XFsTestCase;
import org.junit.Test;

public class TestLauncherFSURIHandler extends XFsTestCase {

    private Services services = null;
    private JobConf conf;
    private LauncherURIHandlerFactory uriHandlerFactory;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        URIHandlerService uriService = services.get(URIHandlerService.class);
        uriHandlerFactory = new LauncherURIHandlerFactory(uriService.getLauncherConfig());
        conf = createJobConf();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    @Test
    public void testCreate() throws Exception {
        Path path = new Path(getFsTestCaseDir() + "/2012/12/02/");
        LauncherURIHandler handler = uriHandlerFactory.getURIHandler(path.toUri());
        assertTrue(handler.create(path.toUri(), conf));
        assertTrue(getFileSystem().exists(path));
    }

    @Test
    public void testDelete() throws Exception {
        Path path = new Path(getFsTestCaseDir() + "/2012/12/02/");
        LauncherURIHandler handler = uriHandlerFactory.getURIHandler(path.toUri());
        getFileSystem().mkdirs(path);
        assertTrue(handler.delete(path.toUri(), conf));
        assertFalse(getFileSystem().exists(path));
    }

}
