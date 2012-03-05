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
package org.apache.oozie.example;

import junit.framework.TestCase;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class TestLocalOozieExample extends XFsTestCase {
    private String oozieLocalLog;
    private String testDir;
    private FileSystem fileSystem;
    private Path fsTestDir;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        File dir = new File(System.getProperty("oozie.test.dir", "/tmp"));
        dir = new File(dir, "oozietests");
        dir = new File(dir, getClass().getName());
        dir = new File(dir, getName());
        dir.mkdirs();
        testDir = dir.getAbsolutePath();

        fileSystem = getFileSystem();

        Path path = new Path(fileSystem.getWorkingDirectory(), "oozietests/" + getClass().getName() + "/" + getName());
        fsTestDir = fileSystem.makeQualified(path);
        System.out.println(XLog.format("Setting FS testcase work dir[{0}]", fsTestDir));
        fileSystem.delete(fsTestDir, true);
        if (!fileSystem.mkdirs(path)) {
            throw new IOException(XLog.format("Could not create FS testcase dir [{0}]", fsTestDir));
        }
        oozieLocalLog = System.getProperty("oozielocal.log");
        System.setProperty("oozielocal.log", "/tmp/oozielocal.log");

        String testCaseDir = getTestCaseDirInternal(this);
        File file = new File(testCaseDir);
        delete(file);
        if (!file.mkdir()) {
            throw new RuntimeException(XLog.format("could not create path [{0}]", file.getAbsolutePath()));
        }
        file = new File(file, "conf");
        if (!file.mkdir()) {
            throw new RuntimeException(XLog.format("could not create path [{0}]", file.getAbsolutePath()));
        }
        //setting up Oozie HOME and an empty conf directory
        System.setProperty(Services.OOZIE_HOME_DIR, testCaseDir);
    }

    @Override
    protected void delete(File file) throws IOException {
        ParamChecker.notNull(file, "file");
        if (file.getAbsolutePath().length() < 5) {
            throw new RuntimeException(XLog.format("path [{0}] is too short, not deleting", file.getAbsolutePath()));
        }
        if (file.exists()) {
            if (file.isDirectory()) {
                File[] children = file.listFiles();
                if (children != null) {
                    for (File child : children) {
                        delete(child);
                    }
                }
            }
            if (!file.delete()) {
                throw new RuntimeException(XLog.format("could not delete path [{0}]", file.getAbsolutePath()));
            }
        }
    }

    private String getTestCaseDirInternal(TestCase testCase) {
        ParamChecker.notNull(testCase, "testCase");
        File dir = new File(System.getProperty("oozie.test.dir", "/tmp"));
        dir = new File(dir, "oozietests");
        dir = new File(dir, testCase.getClass().getName());
        dir = new File(dir, testCase.getName());
        return dir.getAbsolutePath();
    }

    @Override
    protected void tearDown() throws Exception {
        fileSystem = null;
        fsTestDir = null;
        if (oozieLocalLog != null) {
            System.setProperty("oozielocal.log", oozieLocalLog);
        }
        else {
            System.getProperties().remove("oozielocal.log");
        }
        System.getProperties().remove(Services.OOZIE_HOME_DIR);
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
