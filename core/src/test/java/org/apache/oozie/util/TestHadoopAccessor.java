/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.util;

import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.HadoopAccessor;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;

public class TestHadoopAccessor extends XTestCase {

    public void testAccessor() throws Exception {
        HadoopAccessor ha = new HadoopAccessor();
        JobConf conf = new JobConf();
        conf.set("mapred.job.tracker", getJobTrackerUri());
        conf.set("fs.default.name", getNameNodeUri());
        URI uri = new URI(getNameNodeUri());



        try {
            ha.createJobClient(conf);
            fail();
        }
        catch (IllegalStateException ex) {
        }

        String user = System.getProperty("user.name") + "-test";
        String group = "users";

        ha.setUGI(user, group);

        JobClient jc = ha.createJobClient(conf);
        assertNotNull(jc);
        assertEquals(user, jc.getConf().get("user.name"));
        assertEquals(user + "," + group, jc.getConf().get("hadoop.job.ugi"));

        FileSystem fs = ha.createFileSystem(conf);
        assertNotNull(fs);
        assertEquals(user, fs.getConf().get("user.name"));
        assertEquals(user + "," + group, fs.getConf().get("hadoop.job.ugi"));

        fs = ha.createFileSystem(uri, conf);
        assertNotNull(fs);
        assertEquals(user, fs.getConf().get("user.name"));
        assertEquals(user + "," + group, fs.getConf().get("hadoop.job.ugi"));
    }

}