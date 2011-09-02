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
package org.apache.oozie.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.DagEngine;
import org.apache.oozie.ForTestingActionExecutor;
import org.apache.oozie.ErrorCode;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

/**
 * Tests the authorization service.
 */
public class TestAuthorizationService extends XFsTestCase {

    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        setSystemProperty(WorkflowSchemaService.CONF_EXT_SCHEMAS, "wf-ext-schema.xsd");

        Reader adminListReader = IOUtils.getResourceAsReader("adminusers.txt", -1);
        Writer adminListWriter = new FileWriter(getTestCaseDir() + "/adminusers.txt");
        IOUtils.copyCharStream(adminListReader, adminListWriter);

        Reader logPropReader = IOUtils.getResourceAsReader("oozie-log4j.properties", -1);
        Writer logPropWriter = new FileWriter(getTestCaseDir() + "/oozie-log4j.properties");
        IOUtils.copyCharStream(logPropReader, logPropWriter);

        services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_CLASSES,
                 conf.get(Services.CONF_SERVICE_CLASSES) + "," + AuthorizationService.class.getName());
        services.init();
        setSystemProperty(ConfigurationService.CONFIG_PATH, getTestCaseDir());
        services.getConf().setBoolean(AuthorizationService.CONF_SECURITY_ENABLED, true);
        services.get(AuthorizationService.class).init(services);
        services.get(ActionService.class).register(ForTestingActionExecutor.class);
    }

    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    /**
     * Tests the Authorization Service API.
     */
    public void testAuthorizationService() throws Exception {
        Reader reader = IOUtils.getResourceAsReader("wf-ext-schema-valid.xml", -1);
        Writer writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
        IOUtils.copyCharStream(reader, writer);

        final DagEngine engine = new DagEngine("u", "a");
        Configuration jobConf = new XConfiguration();
        jobConf.set(OozieClient.APP_PATH, getTestCaseDir());
        jobConf.set(OozieClient.USER_NAME, "u");
        jobConf.set(OozieClient.GROUP_NAME, "g");
        jobConf.set(OozieClient.LOG_TOKEN, "t");

        jobConf.set("external-status", "ok");
        jobConf.set("signal-value", "based_on_action_status");

        final String jobId = engine.submitJob(jobConf, true);

        Configuration conf = new Configuration();
        conf.set("hadoop.job.ugi", System.getProperty("user.name") + "," + "others");
        FileSystem fileSystem = getFileSystem();
        Path path = new Path(fileSystem.getWorkingDirectory(), getTestCaseDir().substring(1));
        Path fsTestDir = fileSystem.makeQualified(path);
        System.out.println(XLog.format("Setting FS testcase work dir[{0}]", fsTestDir));
        fileSystem.delete(fsTestDir, true);
        if (!fileSystem.mkdirs(path)) {
            throw new IOException(XLog.format("Could not create FS testcase dir [{0}]", fsTestDir));
        }

        String appPath = fsTestDir.toString() + "/app";

        Path jobXmlPath = new Path(appPath, "workflow.xml");
        fileSystem.create(jobXmlPath);
        FsPermission permissions = new FsPermission(FsAction.READ_WRITE, FsAction.READ, FsAction.NONE);
        fileSystem.setPermission(jobXmlPath, permissions);

        AuthorizationService as = services.get(AuthorizationService.class);
        assertNotNull(as);
        as.authorizeForGroup("u", "g");
        assertNotNull(as.getDefaultGroup("u"));
        as.authorizeForAdmin("admin", false);
        as.authorizeForAdmin("admin", true);
        try {
            as.authorizeForAdmin("u", true);
            fail();
        }
        catch (AuthorizationException ex) {
        }
        try {
            as.authorizeForAdmin("u", true);
            fail();
        }
        catch (AuthorizationException ex) {
        }

        try {
            as.authorizeForApp("u", "g", appPath);
            fail();
        }
        catch (AuthorizationException ex) {
        }
        as.authorizeForApp(System.getProperty("user.name"), "others", appPath);

        as.authorizeForJob("u", jobId, false);
        as.authorizeForJob("u", jobId, true);
        //Because of group support and all users belong to same group
        as.authorizeForJob("blah", jobId, true);
    }

    public void testDefaultGroup() throws Exception {
        AuthorizationService as = services.get(AuthorizationService.class);
        assertNotNull(as);
        assertNotNull(as.getDefaultGroup("u"));
    }

    public void testErrors() throws Exception {
        services.setService(ForTestAuthorizationService.class);
        AuthorizationService as = services.get(AuthorizationService.class);

        try {
            as.authorizeForGroup("u", "g");
            fail();
        }
        catch (AuthorizationException ex) {
            assertEquals(ErrorCode.E0502, ex.getErrorCode());
        }
        try {
            as.authorizeForAdmin("u", true);
            fail();
        }
        catch (AuthorizationException ex) {
            assertEquals(ErrorCode.E0503, ex.getErrorCode());
        }
        try {
            Path app = new Path(getFsTestCaseDir(), "w");
            as.authorizeForApp("u", "users", app.toString());
            fail();
        }
        catch (AuthorizationException ex) {
            assertEquals(ErrorCode.E0504, ex.getErrorCode());
        }
        try {
            Path app = new Path(getFsTestCaseDir(), "w");
            getFileSystem().mkdirs(app);
            as.authorizeForApp("u", "users", app.toString());
            fail();
        }
        catch (AuthorizationException ex) {
            assertEquals(ErrorCode.E0505, ex.getErrorCode());
        }
        try {
            Path app = new Path(getFsTestCaseDir(), "w");
            Path wf = new Path(app, "workflow.xml");
            getFileSystem().mkdirs(wf);
            as.authorizeForApp("u", "users", app.toString());
            fail();
        }
        catch (AuthorizationException ex) {
            assertEquals(ErrorCode.E0506, ex.getErrorCode());
        }
        try {
            Path app = new Path(getFsTestCaseDir(), "ww");
            getFileSystem().mkdirs(app);
            Path wf = new Path(app, "workflow.xml");
            getFileSystem().create(wf).close();
            FsPermission fsPermission = new FsPermission(FsAction.READ, FsAction.NONE, FsAction.NONE);
            getFileSystem().setPermission(app, fsPermission);

            as.authorizeForApp("uu", "gg", app.toString());
            fail();
        }
        catch (AuthorizationException ex) {
            assertEquals(ErrorCode.E0507, ex.getErrorCode());
        }

        try {
            as.authorizeForJob("u", "1", true);
            fail();
        }
        catch (AuthorizationException ex) {
            assertEquals(ErrorCode.E0604, ex.getErrorCode());
        }

        services.setService(ForTestWorkflowStoreService.class);
        try {
            as.authorizeForJob("uu", "1", true);
            fail();
        }
        catch (AuthorizationException ex) {
            assertEquals(ErrorCode.E0508, ex.getErrorCode());
        }
    }

}
