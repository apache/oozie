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

package org.apache.oozie.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;

import java.io.IOException;
import java.net.URI;

/**
 * Base JUnit <code>TestCase</code> subclass used by all Oozie testcases that need Hadoop FS access. <p/> As part of its
 * setup, this testcase class creates a unique test working directory per test method in the FS. <p/> The URI of the FS
 * namenode must be specified via the {@link XTestCase#OOZIE_TEST_NAME_NODE} system property. The default value is
 * 'hdfs://localhost:9000'.
 *
 * The test working directory is created in the specified FS URI, under the current user name home directory, under the
 * subdirectory name specified wit the system property {@link XTestCase#OOZIE_TEST_DIR}. The default value is '/tmp'.
 * <p/> The path of the test working directory is: '$FS_URI/user/$USER/$OOZIE_TEST_DIR/oozietest/$TEST_CASE_CLASS/$TEST_CASE_METHOD/'
 * <p/> For example: 'hdfs://localhost:9000/user/tucu/tmp/oozietest/org.apache.oozie.service.TestELService/testEL/'
 */
public abstract class XFsTestCase extends XTestCase {
    private static HadoopAccessorService has;
    private FileSystem fileSystem;
    private FileSystem fileSystem2;
    private Path fsTestDir;
    private Path fsTestDir2;

    /**
     * Set up the testcase.
     *
     * @throws Exception thrown if the test case could no be set up.
     */
    protected void setUp() throws Exception {
        super.setUp();
        Configuration conf = new XConfiguration();
        conf.setBoolean("oozie.service.HadoopAccessorService.kerberos.enabled",
                        System.getProperty("oozie.test.hadoop.security", "simple").equals("kerberos"));
        conf.set("oozie.service.HadoopAccessorService.keytab.file", getKeytabFile());
        conf.set("oozie.service.HadoopAccessorService.kerberos.principal", getOoziePrincipal());
        conf.set("local.realm", getRealm());


        conf.set("oozie.service.HadoopAccessorService.hadoop.configurations", "*=hadoop-conf");
        conf.set("oozie.service.HadoopAccessorService.action.configurations", "*=action-conf");

        has = new HadoopAccessorService();
        has.init(conf);
        JobConf jobConf = has.createJobConf(getNameNodeUri());
        XConfiguration.copy(conf, jobConf);
        fileSystem = has.createFileSystem(getTestUser(), new URI(getNameNodeUri()), jobConf);
        fsTestDir = initFileSystem(fileSystem);
        if (System.getProperty("oozie.test.hadoop.minicluster2", "false").equals("true")) {
            fileSystem2 = has.createFileSystem(getTestUser(), new URI(getNameNode2Uri()), jobConf);
            fsTestDir2 = initFileSystem(fileSystem2);
        }
    }

    private Path initFileSystem(FileSystem fs) throws Exception {
        Path path = new Path(fs.getWorkingDirectory(), java.util.UUID.randomUUID().toString());
        Path testDirInFs = fs.makeQualified(path);
        System.out.println(XLog.format("Setting FS testcase work dir[{0}]", testDirInFs));
        if (fs.exists(testDirInFs)) {
            setAllPermissions(fs, testDirInFs);
        }
        fs.delete(testDirInFs, true);
        if (!fs.mkdirs(path)) {
            throw new IOException(XLog.format("Could not create FS testcase dir [{0}]", testDirInFs));
        }
        fs.setOwner(testDirInFs, getTestUser(), getTestGroup());
        fs.setPermission(testDirInFs, FsPermission.valueOf("-rwxrwx--x"));
        return testDirInFs;
    }

    private void setAllPermissions(FileSystem fileSystem, Path path) throws IOException {
        FsPermission fsPermission = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);
        try {
            fileSystem.setPermission(path, fsPermission);
        }
        catch (IOException ex) {
            //NOP
        }
        FileStatus fileStatus = fileSystem.getFileStatus(path);
        if (fileStatus.isDir()) {
            for (FileStatus status : fileSystem.listStatus(path)) {
                setAllPermissions(fileSystem, status.getPath());
            }
        }
    }

    /**
     * Tear down the testcase.
     */
    protected void tearDown() throws Exception {
        fileSystem = null;
        fsTestDir = null;
        super.tearDown();
    }

    /**
     * Return the file system used by the test case.
     *
     * @return the file system used by the test case.
     */
    protected FileSystem getFileSystem() {
        return fileSystem;
    }

    /**
     * Return the file system of the second cluster.
     *
     * @return the second file system used by the test case.
     */
    protected FileSystem getFileSystem2() {
        return fileSystem2;
    }

    /**
     * Return the FS test working directory. The directory name is the full class name of the test plus the test method
     * name.
     *
     * @return the test working directory path, it is always an full and absolute path.
     */
    protected Path getFsTestCaseDir() {
        return fsTestDir;
    }

    /**
     * Return the FS test working directory of the second cluster. The directory name is
     * the full class name of the test plus the test method name.
     *
     * @return the second FS test working directory path, it is always an full and absolute path.
     */
    protected Path getFs2TestCaseDir() {
        return fsTestDir2;
    }

    /**
     * Return a JobClient to the test JobTracker.
     *
     * @return a JobClient to the test JobTracker.
     * @throws HadoopAccessorException thrown if the JobClient could not be obtained.
     */
    protected JobClient createJobClient() throws HadoopAccessorException {
        JobConf conf = has.createJobConf(getJobTrackerUri());
        conf.set("mapred.job.tracker", getJobTrackerUri());
        conf.set("fs.default.name", getNameNodeUri());

        return has.createJobClient(getTestUser(), conf);
    }

}
