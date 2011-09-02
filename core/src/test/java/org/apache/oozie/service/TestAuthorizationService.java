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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.DagEngine;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.ForTestingActionExecutor;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.workflow.WorkflowInstance;

/**
 * Tests the authorization service.
 */
public class TestAuthorizationService extends XDataTestCase {

    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        setSystemProperty(SchemaService.WF_CONF_EXT_SCHEMAS, "wf-ext-schema.xsd");

        Reader adminListReader = IOUtils.getResourceAsReader("adminusers.txt", -1);
        Writer adminListWriter = new FileWriter(new File(getTestCaseConfDir(), "adminusers.txt"));
        IOUtils.copyCharStream(adminListReader, adminListWriter);

        services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_CLASSES,
                 conf.get(Services.CONF_SERVICE_CLASSES) + "," + AuthorizationService.class.getName());
        services.init();
        services.getConf().setBoolean(AuthorizationService.CONF_SECURITY_ENABLED, true);
        services.get(AuthorizationService.class).init(services);
        services.get(ActionService.class).register(ForTestingActionExecutor.class);
        cleanUpDBTables();
    }

    @Override
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

        final DagEngine engine = new DagEngine(getTestUser(), "a");
        Configuration jobConf = new XConfiguration();
        jobConf.set(OozieClient.APP_PATH, getTestCaseDir() + File.separator + "workflow.xml");
        jobConf.set(OozieClient.USER_NAME, getTestUser());
        jobConf.set(OozieClient.GROUP_NAME, getTestGroup());
        injectKerberosInfo(jobConf);
        jobConf.set(OozieClient.LOG_TOKEN, "t");

        jobConf.set("external-status", "ok");
        jobConf.set("signal-value", "based_on_action_status");

        final String jobId = engine.submitJob(jobConf, true);

        FileSystem fileSystem = Services.get().get(HadoopAccessorService.class).
                createFileSystem(getTestUser(), getTestGroup(), getFileSystem().getUri(), jobConf);

        Path path = new Path(fileSystem.getWorkingDirectory(), getTestCaseDir().substring(1));
        Path fsTestDir = fileSystem.makeQualified(path);
        System.out.println(XLog.format("Setting FS testcase work dir[{0}]", fsTestDir));
        fileSystem.delete(fsTestDir, true);
        if (!fileSystem.mkdirs(path)) {
            throw new IOException(XLog.format("Could not create FS testcase dir [{0}]", fsTestDir));
        }

        String appPath = fsTestDir.toString() + "/app";

        Path jobXmlPath = new Path(appPath, "workflow.xml");
        fileSystem.create(jobXmlPath).close();
        fileSystem.setOwner(jobXmlPath, getTestUser(), getTestGroup());

        FsPermission permissions = new FsPermission(FsAction.READ_WRITE, FsAction.READ, FsAction.NONE);
        fileSystem.setPermission(jobXmlPath, permissions);

        AuthorizationService as = services.get(AuthorizationService.class);
        assertNotNull(as);
        as.authorizeForGroup(getTestUser(), getTestGroup());
        assertNotNull(as.getDefaultGroup(getTestUser()));
        as.authorizeForAdmin("admin", false);
        as.authorizeForAdmin("admin", true);
        try {
            as.authorizeForAdmin(getTestUser(), true);
            fail();
        }
        catch (AuthorizationException ex) {
        }
        try {
            as.authorizeForAdmin(getTestUser(), true);
            fail();
        }
        catch (AuthorizationException ex) {
        }

        as.authorizeForApp(getTestUser2(), getTestGroup(), appPath, jobConf);

        // this test fails in pre Hadoop 20S
        if (System.getProperty("hadoop20", "false").toLowerCase().equals("false")) {
            try {
                as.authorizeForApp(getTestUser3(), getTestGroup(), appPath, jobConf);
                fail();
            }
            catch (AuthorizationException ex) {
            }
        }

        as.authorizeForJob(getTestUser(), jobId, false);
        as.authorizeForJob(getTestUser(), jobId, true);
        //Because of group support and all users belong to same group
        as.authorizeForJob("blah", jobId, true);
    }

    public void testAuthorizationServiceForCoord() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, false, false);
        assertNotNull(job);
        AuthorizationService as = services.get(AuthorizationService.class);
        assertNotNull(as);
        as.authorizeForJob(getTestUser(), job.getId(), false);
        as.authorizeForJob(getTestUser(), job.getId(), true);
    }

    public void testAuthorizationServiceForBundle() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.PREP, false);
        assertNotNull(job);
        AuthorizationService as = services.get(AuthorizationService.class);
        assertNotNull(as);
        as.authorizeForJob(getTestUser(), job.getId(), false);
        as.authorizeForJob(getTestUser(), job.getId(), true);
    }

    public void testDefaultGroup() throws Exception {
        AuthorizationService as = services.get(AuthorizationService.class);
        assertNotNull(as);
        assertNotNull(as.getDefaultGroup(getTestUser()));
    }

    public void testErrors() throws Exception {
        services.setService(ForTestAuthorizationService.class);
        AuthorizationService as = services.get(AuthorizationService.class);

        Configuration conf = new Configuration();
        injectKerberosInfo(conf);
        FileSystem fileSystem = Services.get().get(HadoopAccessorService.class).
                createFileSystem(getTestUser(), getTestGroup(), getFileSystem().getUri(), conf);

        try {
            as.authorizeForGroup(getTestUser3(), getTestGroup());
            fail();
        }
        catch (AuthorizationException ex) {
            assertEquals(ErrorCode.E0502, ex.getErrorCode());
        }
        try {
            as.authorizeForAdmin(getTestUser(), true);
            fail();
        }
        catch (AuthorizationException ex) {
            assertEquals(ErrorCode.E0503, ex.getErrorCode());
        }
        try {
            Path app = new Path(getFsTestCaseDir(), "w");
            as.authorizeForApp(getTestUser(), getTestGroup(), app.toString(), conf);
            fail();
        }
        catch (AuthorizationException ex) {
            assertEquals(ErrorCode.E0504, ex.getErrorCode());
        }
        try {
            Path app = new Path(getFsTestCaseDir(), "w");
            fileSystem.mkdirs(app);
            as.authorizeForApp(getTestUser(), getTestGroup(), app.toString(), conf);
            fail();
        }
        catch (AuthorizationException ex) {
            assertEquals(ErrorCode.E0505, ex.getErrorCode());
        }
        try {
            Path app = new Path(getFsTestCaseDir(), "w");
            Path wf = new Path(app, "workflow.xml");
            fileSystem.mkdirs(wf);
            as.authorizeForApp(getTestUser(), getTestGroup(), app.toString(), conf);
            fail();
        }
        catch (AuthorizationException ex) {
            assertEquals(ErrorCode.E0506, ex.getErrorCode());
        }
        try {
            Path app = new Path(getFsTestCaseDir(), "ww");
            fileSystem.mkdirs(app);
            Path wf = new Path(app, "workflow.xml");
            fileSystem.create(wf).close();
            FsPermission fsPermission = new FsPermission(FsAction.READ, FsAction.NONE, FsAction.NONE);
            fileSystem.setPermission(app, fsPermission);

            as.authorizeForApp(getTestUser2(), getTestGroup() + "-invalid", app.toString(), conf);
            fail();
        }
        catch (AuthorizationException ex) {
            assertEquals(ErrorCode.E0507, ex.getErrorCode());
        }

        try {
            as.authorizeForJob(getTestUser(), "1", true);
            fail();
        }
        catch (AuthorizationException ex) {
            assertEquals(ErrorCode.E0604, ex.getErrorCode());
        }

        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);
        try {
            as.authorizeForJob(getTestUser3(), job.getId(), true);
            fail();
        }
        catch (AuthorizationException ex) {
            assertEquals(ErrorCode.E0508, ex.getErrorCode());
        }
    }

}
