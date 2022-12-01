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

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.DagELFunctions;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.LiteWorkflowStoreService;
import org.apache.oozie.workflow.lite.EndNodeDef;
import org.apache.oozie.workflow.lite.LiteWorkflowApp;
import org.apache.oozie.workflow.lite.LiteWorkflowInstance;
import org.apache.oozie.workflow.lite.StartNodeDef;
import org.apache.oozie.service.ELService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.XConfiguration;

public class TestFsELFunctions extends XFsTestCase {

    public static final String PASSWORD_FILE_KEY = "hadoop.security.credstore.java-keystore-provider.password-file";
    public static final String CREDENTIAL_PATH_KEY = "hadoop.security.credential.provider.path";
    public static final String HDFS_FILE = "hdfs://path/to/file";
    public static final String JCEKS_FILE = "jceks://path/to/file";
    public static final String PASSWORD_FILE = "password.file";
    public static final String KEY_WE_DONOT_WANT = "key.shall.not.used";
    public static final String HDFS_ELF_FS_CONF_PREFIX = FsELFunctions.FS_EL_FUNCTIONS_CONF + "hdfs";
    private Configuration jobConf;
    private Configuration protoConf;
    private Configuration conf;
    private LiteWorkflowInstance job;
    private WorkflowJobBean wf;
    private FileSystem fs;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        new Services().init();

        String file1 = new Path(getFsTestCaseDir(), "file1").toString();
        String file2 = new Path(getFsTestCaseDir(), "file2").toString();
        String dir = new Path(getFsTestCaseDir(), "dir").toString();
        protoConf = new Configuration();
        protoConf.set(OozieClient.USER_NAME, getTestUser());
        protoConf.set("hadoop.job.ugi", getTestUser() + "," + "group");

        fs = getFileSystem();
        fs.mkdirs(new Path(dir));
        fs.create(new Path(file1)).close();
        OutputStream os = fs.create(new Path(dir, "a"));
        byte[] arr = new byte[1];
        os.write(arr);
        os.close();
        os = fs.create(new Path(dir, "b"));
        arr = new byte[2];
        os.write(arr);
        os.close();

        URI filebURI = new Path(dir, "b").toUri();
        String filebExtraSlashInPath = new URI(filebURI.getScheme(), filebURI.getAuthority(),
                "/" + filebURI.getPath(), null, null).toString();
        URI dirURI = new Path(dir).toUri();
        String dirExtraSlashInPath = new URI(dirURI.getScheme(), dirURI.getAuthority(),
                "/" + dirURI.getPath(), null, null).toString();

        conf = new XConfiguration();
        conf.set(OozieClient.APP_PATH, "appPath");
        conf.set(OozieClient.USER_NAME, getTestUser());

        conf.set("test.dir", getTestCaseDir());
        conf.set("file1", file1);
        conf.set("file2", file2);
        conf.set("file3", "${file2}");
        conf.set("file4", getFsTestCaseDir()+"/file{1,2}");
        conf.set("file5", getFsTestCaseDir()+"/file*");
        conf.set("file6", getFsTestCaseDir()+"/file_*");
        conf.set("dir", dir);
        conf.set("dirExtraSlashInPath", dirExtraSlashInPath);
        conf.set("filebExtraSlashInPath", filebExtraSlashInPath);

        LiteWorkflowApp def =
                new LiteWorkflowApp("name", "<workflow-app/>",
                        new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "end")).
                        addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));
        job = new LiteWorkflowInstance(def, conf, "wfId");

        wf = new WorkflowJobBean();
        wf.setId(job.getId());
        wf.setAppName("name");
        wf.setAppPath("appPath");
        wf.setUser(getTestUser());
        wf.setGroup("group");
        wf.setWorkflowInstance(job);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        protoConf.writeXml(baos);
        wf.setProtoActionConf(baos.toString(StandardCharsets.UTF_8.name()));
    }

    @Override
    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    public void testFunctions() throws Exception {
        WorkflowActionBean action = new WorkflowActionBean();
        action.setId("actionId");
        action.setName("actionName");

        ELEvaluator eval = Services.get().get(ELService.class).createEvaluator("workflow");
        DagELFunctions.configureEvaluator(eval, wf, action);

        assertEquals(true, (boolean) eval.evaluate("${fs:exists(wf:conf('file1'))}", Boolean.class));
        assertEquals(false, (boolean) eval.evaluate("${fs:exists(wf:conf('file2'))}", Boolean.class));
        assertEquals(true, (boolean) eval.evaluate("${fs:exists(wf:conf('file4'))}", Boolean.class));
        assertEquals(true, (boolean) eval.evaluate("${fs:exists(wf:conf('file5'))}", Boolean.class));
        assertEquals(false, (boolean) eval.evaluate("${fs:exists(wf:conf('file6'))}", Boolean.class));
        assertEquals(true, (boolean) eval.evaluate("${fs:exists(wf:conf('dir'))}", Boolean.class));
        assertEquals(false, (boolean) eval.evaluate("${fs:isDir(wf:conf('file1'))}", Boolean.class));
        assertEquals(0, (int) eval.evaluate("${fs:fileSize(wf:conf('file1'))}", Integer.class));
        assertEquals(-1, (int) eval.evaluate("${fs:fileSize(wf:conf('file2'))}", Integer.class));
        assertEquals(3, (int) eval.evaluate("${fs:dirSize(wf:conf('dir'))}", Integer.class));
        assertEquals(-1, (int) eval.evaluate("${fs:blockSize(wf:conf('file2'))}", Integer.class));
        assertTrue(eval.evaluate("${fs:blockSize(wf:conf('file1'))}", Integer.class) > 0);
        assertEquals("Size of fileb with extra slash in path should be 2",
                2, (int) eval.evaluate("${fs:fileSize(wf:conf('filebExtraSlashInPath'))}", Integer.class));
        assertEquals("Size of dir with extra slash in path should be 3",
                3, (int) eval.evaluate("${fs:dirSize(wf:conf('dirExtraSlashInPath'))}", Integer.class));
    }

    public void testCustomFileSystemPropertiesCanBeSet() throws Exception {
        jobConf = new Configuration();
        jobConf.set(HDFS_ELF_FS_CONF_PREFIX + "." + CREDENTIAL_PATH_KEY, JCEKS_FILE);
        jobConf.set(HDFS_ELF_FS_CONF_PREFIX + "." + PASSWORD_FILE_KEY, PASSWORD_FILE);
        jobConf.set("hadoop.irrelevant.configuration", "value");
        jobConf.set("user.name", "Malice");
        jobConf.set(HDFS_ELF_FS_CONF_PREFIX + "user.name", "Malice");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        jobConf.writeXml(baos);
        wf.setConf(baos.toString(StandardCharsets.UTF_8.name()));

        Configuration resultFsConf = new Configuration(false);
        FsELFunctions.extractExtraFsConfiguration(wf, resultFsConf, fs.getUri());
        assertEquals(JCEKS_FILE, resultFsConf.get(CREDENTIAL_PATH_KEY));
        assertEquals(PASSWORD_FILE, resultFsConf.get(PASSWORD_FILE_KEY));
        assertNull("Irrelevant property shall not be set.", resultFsConf.get("hadoop.irrelevant.configuration"));
        assertNull("Disallowed property shall not be set.", resultFsConf.get("user.name"));

    }

    public void testOozieSiteConfigRead() throws Exception {
        Configuration cnf = new Configuration(false);
        URI uri = new URI(HDFS_FILE);
        ConfigurationService.set(HDFS_ELF_FS_CONF_PREFIX,
                CREDENTIAL_PATH_KEY + "=" + JCEKS_FILE + "," + PASSWORD_FILE_KEY + "=" + PASSWORD_FILE);
        ConfigurationService.set(FsELFunctions.FS_EL_FUNCTIONS_CONF + "hdfsx", KEY_WE_DONOT_WANT + "=value");

        jobConf = new Configuration();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        jobConf.writeXml(baos);
        wf.setConf(baos.toString(StandardCharsets.UTF_8.name()));

        FsELFunctions.extractExtraFsConfiguration(wf, cnf, uri);

        assertEquals(JCEKS_FILE, cnf.get(CREDENTIAL_PATH_KEY));
        assertEquals(PASSWORD_FILE, cnf.get(PASSWORD_FILE_KEY));
        assertNull(cnf.get(KEY_WE_DONOT_WANT));
    }

    public void testIfWorkflowConfOverwritesSiteConf() throws Exception {
        Configuration cnf = new Configuration(false);
        URI uri = new URI(HDFS_FILE);
        String KEY_TO_OVERRIDE = CREDENTIAL_PATH_KEY;
        ConfigurationService.set(HDFS_ELF_FS_CONF_PREFIX, KEY_TO_OVERRIDE + "=" + JCEKS_FILE);

        jobConf = new Configuration();
        jobConf.set(HDFS_ELF_FS_CONF_PREFIX  + "." + KEY_TO_OVERRIDE, "Desired value");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        jobConf.writeXml(baos);
        wf.setConf(baos.toString(StandardCharsets.UTF_8.name()));

        FsELFunctions.extractExtraFsConfiguration(wf, cnf, uri);

        assertEquals("Desired value", cnf.get(CREDENTIAL_PATH_KEY));
    }
}
