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
package org.apache.oozie.example;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.IOUtils;

import java.io.IOException;
import java.io.File;
import java.io.FileOutputStream;

public class TestLocalOozieExample extends TestCase {
    private String oozieLocalLog;
    private String testDir;
    private FileSystem fileSystem;
    private Path fsTestDir;

    protected void setUp() throws Exception {
        super.setUp();
        File dir = new File(System.getProperty("oozie.test.dir", "/tmp"));
        dir = new File(dir, "oozietests");
        dir = new File(dir, getClass().getName());
        dir = new File(dir, getName());
        dir.mkdirs();
        testDir = dir.getAbsolutePath();
        Configuration conf = new Configuration();
        conf.set(WorkflowAppService.HADOOP_USER, System.getProperty("user.name"));
        conf.set(WorkflowAppService.HADOOP_UGI, System.getProperty("user.name") + ",other");
        String nameNode = System.getProperty("oozie.test.name.node", "hdfs://localhost:9000");
        fileSystem = FileSystem.get(new Path(nameNode).toUri(), conf);

        Path path = new Path(fileSystem.getWorkingDirectory(), "oozietests/" + getClass().getName() + "/" + getName());
        fsTestDir = fileSystem.makeQualified(path);
        System.out.println(XLog.format("Setting FS testcase work dir[{0}]", fsTestDir));
        fileSystem.delete(fsTestDir, true);
        if (!fileSystem.mkdirs(path)) {
            throw new IOException(XLog.format("Could not create FS testcase dir [{0}]", fsTestDir));
        }
        oozieLocalLog = System.getProperty("oozielocal.log");
        System.setProperty("oozielocal.log", "/tmp/oozielocal.log");
    }

    protected void tearDown() throws Exception {
        fileSystem = null;
        fsTestDir = null;
        if (oozieLocalLog != null) {
            System.setProperty("oozielocal.log", oozieLocalLog);
        }
        else {
            System.getProperties().remove("oozielocal.log");
        }
        super.tearDown();
    }

    public void testLocalOozieExampleEnd() throws IOException {
        Path app = new Path(fsTestDir, "app");
        File props = new File(testDir, "job.properties");
        IOUtils.copyStream(IOUtils.getResourceAsStream("localoozieexample-wf.xml", -1),
                           fileSystem.create(new Path(app, "workflow.xml")));
        IOUtils.copyStream(IOUtils.getResourceAsStream("localoozieexample-end.properties", -1),
                           new FileOutputStream(props));
        assertEquals(0, LocalOozieExample.execute(app.toString(), props.toString()));
    }

    public void testLocalOozieExampleKill() throws IOException {
        Path app = new Path(fsTestDir, "app");
        File props = new File(testDir, "job.properties");
        IOUtils.copyStream(IOUtils.getResourceAsStream("localoozieexample-wf.xml", -1),
                           fileSystem.create(new Path(app, "workflow.xml")));
        IOUtils.copyStream(IOUtils.getResourceAsStream("localoozieexample-kill.properties", -1),
                           new FileOutputStream(props));
        assertEquals(-1, LocalOozieExample.execute(app.toString(), props.toString()));
    }

}
