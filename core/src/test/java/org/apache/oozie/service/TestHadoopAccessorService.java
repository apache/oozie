/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.service;

import org.apache.oozie.test.XTestCase;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;

public class TestHadoopAccessorService extends XTestCase {

    protected void setUp() throws Exception {
        super.setUp();
        setSystemProperty(Services.CONF_SERVICE_EXT_CLASSES, HadoopAccessorService.class.getName());
        Services services = new Services();
        services.init();
    }

    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    public void testService() throws Exception {
        Services services = Services.get();
        HadoopAccessorService has = services.get(HadoopAccessorService.class);
        assertNotNull(has);
    }

    public void testAccessor() throws Exception {
        Services services = Services.get();
        HadoopAccessorService has = services.get(HadoopAccessorService.class);
        JobConf conf = new JobConf();
        conf.set("mapred.job.tracker", getJobTrackerUri());
        conf.set("fs.default.name", getNameNodeUri());
        URI uri = new URI(getNameNodeUri());

        String user = getTestUser() + "-invalid";
        String group = getTestGroup();

        try {
            has.createJobClient(null, group, conf);
            fail();
        }
        catch (IllegalArgumentException ex) {
        }

        try {
            has.createJobClient(user, null, conf);
            fail();
        }
        catch (IllegalArgumentException ex) {
        }

        user = getTestUser();
        JobClient jc = has.createJobClient(user, group, conf);
        assertNotNull(jc);

        FileSystem fs = has.createFileSystem(user, group, conf);
        assertNotNull(fs);

        fs = has.createFileSystem(user, group, uri, conf);
        assertNotNull(fs);
    }

}
