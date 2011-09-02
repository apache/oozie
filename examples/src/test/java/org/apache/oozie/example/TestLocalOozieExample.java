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
package org.apache.oozie.example;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.action.hadoop.DoAs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.Callable;

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
        final Configuration conf = new Configuration();
        conf.set(WorkflowAppService.HADOOP_USER, "test");
        conf.set(WorkflowAppService.HADOOP_UGI, "test,users");
        String jtKerberosPrincipal = System.getProperty("oozie.test.kerberos.jobtracker.principal",
                                                        "mapred/localhost") + "@" +
                System.getProperty("oozie.test.kerberos.realm", "LOCALHOST");
        String nnKerberosPrincipal = System.getProperty("oozie.test.kerberos.namenode.principal",
                                                        "hdfs/localhost") + "@" +
                System.getProperty("oozie.test.kerberos.realm", "LOCALHOST");
        conf.set(WorkflowAppService.HADOOP_JT_KERBEROS_NAME, jtKerberosPrincipal);
        conf.set(WorkflowAppService.HADOOP_NN_KERBEROS_NAME, nnKerberosPrincipal);

// TODO restore this when getting rid of DoAs trick

//        if (System.getProperty("oozie.test.kerberos", "off").equals("on")) {
//            Configuration c = new Configuration();
//            c.set("hadoop.security.authentication", "kerberos");
//            UserGroupInformation.setConfiguration(c);
//            String principal = System.getProperty("oozie.test.kerberos.oozie.principal",
//                                                  System.getProperty("user.name") + "/localhost") + "@"
//                    + System.getProperty("oozie.test.kerberos.realm", "LOCALHOST");
//            String defaultFile = new File(System.getProperty("user.home"), "oozie.keytab").getAbsolutePath();
//            String keytabFile = System.getProperty("oozie.test.kerberos.keytab.file", defaultFile);
//            UserGroupInformation.loginUserFromKeytab(principal, keytabFile);
////            System.setProperty("oozie.service.HadoopAccessorService.kerberos.enabled", "kerberos");
//        }

        Class klass;
        try {
            klass = Class.forName("org.apache.oozie.action.hadoop.KerberosDoAs");
        }
        catch (ClassNotFoundException ex) {
            klass = DoAs.class;
        }
        DoAs doAs = (DoAs) klass.newInstance();
        final FileSystem[] fs = new FileSystem[1];
        doAs.setCallable(new Callable<Void>() {
            public Void call() throws Exception {
                Configuration defaultConf = new Configuration();
                XConfiguration.copy(conf, defaultConf);
                fs[0] = FileSystem.get(defaultConf);
                return null;
            }
        });
        doAs.setUser("test");
        doAs.call();
        fileSystem = fs[0];

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
        System.setProperty(Services.OOZIE_HOME_ENV, testCaseDir);
    }

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

    protected void tearDown() throws Exception {
        fileSystem = null;
        fsTestDir = null;
        if (oozieLocalLog != null) {
            System.setProperty("oozielocal.log", oozieLocalLog);
        }
        else {
            System.getProperties().remove("oozielocal.log");
        }
        System.getProperties().remove(Services.OOZIE_HOME_ENV);
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
