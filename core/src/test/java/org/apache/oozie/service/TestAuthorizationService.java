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

package org.apache.oozie.service;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
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

    public static class DummyGroupsService extends GroupsService  {
        @Override
        public void init(Services services) {
        }

        @Override
        public List<String> getGroups(String user) throws IOException {
            if (getTestUser().equals(user)) {
                return Arrays.asList("users", getTestGroup());
            }
            if ("foo".equals(user)) {
                return Arrays.asList("users", "foogrp");
            }
            else {
                return Arrays.asList("users");
            }
        }

        @Override
        public void destroy() {
        }
    }
    private Services services;

    private void init(boolean useDefaultGroup, boolean useAdminUsersFile) throws Exception {
        boolean useAdminGroups = !useAdminUsersFile;
        init(useDefaultGroup, StringUtils.EMPTY, useAdminUsersFile, false, useAdminGroups);
    }

    private void init(boolean useDefaultGroup, String systemInfoAuthUsers,
                      boolean useAdminUsersFile, boolean useOozieSiteForAdminUsers, boolean useAdminGroups) throws
            Exception {
        setSystemProperty(SchemaService.WF_CONF_EXT_SCHEMAS, "wf-ext-schema.xsd");

        services = new Services();
        Configuration conf = services.getConf();
        if (useAdminUsersFile) {
            Reader adminListReader = IOUtils.getResourceAsReader("adminusers.txt", -1);
            Writer adminListWriter = new FileWriter(new File(getTestCaseConfDir(), "adminusers.txt"));
            IOUtils.copyCharStream(adminListReader, adminListWriter);
        }
        if (useAdminGroups) {
            conf.set(AuthorizationService.CONF_ADMIN_GROUPS, getTestGroup());
        }

        if (useOozieSiteForAdminUsers) {
            conf.set(AuthorizationService.CONF_ADMIN_USERS, getAdminUser());
        }

        conf.set(AuthorizationService.CONF_SYSTEM_INFO_AUTHORIZED_USERS, systemInfoAuthUsers);
        conf.set(Services.CONF_SERVICE_CLASSES,
                conf.get(Services.CONF_SERVICE_CLASSES) + "," + AuthorizationService.class.getName() + ","
                        + DummyGroupsService.class.getName());
        conf.set(AuthorizationService.CONF_DEFAULT_GROUP_AS_ACL, Boolean.toString(useDefaultGroup));
        conf.setBoolean(AuthorizationService.CONF_AUTHORIZATION_ENABLED, true);
        services.init();
        services.getConf().setBoolean(AuthorizationService.CONF_SECURITY_ENABLED, true);
        services.get(AuthorizationService.class).init(services);
        services.get(ActionService.class).registerAndInitExecutor(ForTestingActionExecutor.class);
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    /**
     * Tests the Authorization Service API.
     */
    public void testAuthorizationServiceUseDefaultGroup() throws Exception {
        _testAuthorizationService(true);
    }

    public void testAuthorizationServiceUseACLs() throws Exception {
        _testAuthorizationService(false);
    }

    private void _testAuthorizationService(boolean useDefaultGroup) throws Exception {
        init(useDefaultGroup, true);
        Reader reader = IOUtils.getResourceAsReader("wf-ext-schema-valid.xml", -1);
        Writer writer = new FileWriter(new File(getTestCaseDir(), "workflow.xml"));
        IOUtils.copyCharStream(reader, writer);

        final DagEngine engine = new DagEngine(getTestUser());
        Configuration jobConf = new XConfiguration();
        jobConf.set(OozieClient.APP_PATH, getTestCaseFileUri("workflow.xml"));
        jobConf.set(OozieClient.USER_NAME, getTestUser());
        if (useDefaultGroup) {
            jobConf.set(OozieClient.GROUP_NAME, getTestGroup());
        }
        else {
            jobConf.set(OozieClient.GROUP_NAME, getTestGroup() + ",foogrp");
        }

        jobConf.set(OozieClient.LOG_TOKEN, "t");

        jobConf.set("external-status", "ok");
        jobConf.set("signal-value", "based_on_action_status");

        final String jobId = engine.submitJob(jobConf, true);

        HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
        URI uri = getFileSystem().getUri();
        Configuration fsConf = has.createConfiguration(uri.getAuthority());
        FileSystem fileSystem = has.createFileSystem(getTestUser(), uri, fsConf);

        Path path = new Path(fileSystem.getWorkingDirectory(), UUID.randomUUID().toString());
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

        as.authorizeForApp(getTestUser2(), getTestGroup(), appPath, jobConf);

        try {
            as.authorizeForApp(getTestUser3(), getTestGroup(), appPath, jobConf);
            fail();
        }
        catch (AuthorizationException ex) {
        }

        as.authorizeForJob(getTestUser(), jobId, false);
        as.authorizeForJob(getTestUser(), jobId, true);
        if (!useDefaultGroup) {
            as.authorizeForJob("foo", jobId, true);
        }
        try {
            as.authorizeForJob("bar", jobId, true);
            fail();
        }
        catch (AuthorizationException ex) {
        }
    }

    public void testAuthorizationServiceForCoord() throws Exception {
        init(false, true);
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.PREP, false, false);
        assertNotNull(job);
        AuthorizationService as = services.get(AuthorizationService.class);
        assertNotNull(as);
        as.authorizeForJob(getTestUser(), job.getId(), false);
        as.authorizeForJob(getTestUser(), job.getId(), true);
    }

    public void testAuthorizationServiceForBundle() throws Exception {
        init(false, true);
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.PREP, false);
        assertNotNull(job);
        AuthorizationService as = services.get(AuthorizationService.class);
        assertNotNull(as);
        as.authorizeForJob(getTestUser(), job.getId(), false);
        as.authorizeForJob(getTestUser(), job.getId(), true);
    }

    public void testDefaultGroup() throws Exception {
        init(false, true);
        AuthorizationService as = services.get(AuthorizationService.class);
        assertNotNull(as);
        assertNotNull(as.getDefaultGroup(getTestUser()));
    }

    public void testErrors() throws Exception {
        init(false, true);
        services.setService(ForTestAuthorizationService.class);
        AuthorizationService as = services.get(AuthorizationService.class);

        Configuration conf = new Configuration();

        HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
        URI uri = getFileSystem().getUri();
        Configuration fsConf = has.createConfiguration(uri.getAuthority());
        FileSystem fileSystem = has.createFileSystem(getTestUser(), uri, fsConf);

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


    private void _testAdminUsers(boolean useAdminFile, String adminUser, String regularUser,
                                 boolean adminUserFromOozieSite, boolean useAdminGroup) throws Exception {
        init(true, StringUtils.EMPTY, useAdminFile, adminUserFromOozieSite, useAdminGroup );

        AuthorizationService as = services.get(AuthorizationService.class);
        as.authorizeForAdmin(adminUser, false);
        as.authorizeForAdmin(adminUser, true);
        try {
            as.authorizeForAdmin(regularUser, true);
            fail();
        }
        catch (AuthorizationException ex) {
        }
    }

    public void testAdminUsersWithAdminFile() throws Exception {
        _testAdminUsers(true, "admin", getTestUser(), false, false);
    }

    public void testAdminUsersWithAdminGroup() throws Exception {
        _testAdminUsers(false, getTestUser(), getTestUser2(), false, true);
    }

    public void testAuthorizedSystemInfoDefaultSuccess() throws Exception {
        //AuthorizationService.CONF_SYSTEM_INFO_AUTHORIZED_USERS is empty
        init(true, StringUtils.EMPTY, false, false, true);
        services.get(AuthorizationService.class).authorizeForSystemInfo("regularUser", "proxyUser");
    }

    public void testAuthorizedSystemInfoSuccess() throws Exception {
        //Set AuthorizationService.CONF_SYSTEM_INFO_AUTHORIZED_USERS to proxyUser,regularUser
        init(true, "proxyUser,regularUser", false, false, true);

        //Use proxyUser in request
        services.get(AuthorizationService.class).authorizeForSystemInfo("regularUser1", "proxyUser");

        //Use regularUser in request
        services.get(AuthorizationService.class).authorizeForSystemInfo("regularUser", "proxyUser1");

        //The proxy user and regular user used in the request are different. Proxy user belongs to one of the
        //admin groups in AuthorizationService.CONF_ADMIN_GROUPS
        services.get(AuthorizationService.class).authorizeForSystemInfo("regularUser1", getTestUser());
    }

    public void testAuthorizedSystemInfoFailure() throws Exception {
        init(true, "proxyUser,regularUser", false, false, true);
        try {
            services.get(AuthorizationService.class).authorizeForSystemInfo("regularUser1", "proxyUser1");
            fail("Should have thrown exception because regularUser1 or proxyUser1 are not authorized to access system info");
        }
        catch (AuthorizationException ex) {
            assertEquals("Exception message is different than expected",
                    "E0503: User [regularUser1] does not have admin " + "privileges", ex.getMessage());
        }
    }

    public void testWhenDefinedInConfigurationThenAdminPrivilegesAllowed() throws Exception {
        _testAdminUsers(false, getAdminUser(), getTestUser(), true, false);
    }

    public void testWhenDefinedInAdminFileAndConfigurationThenAllowBothAdmins() throws Exception {
        init(true, StringUtils.EMPTY, true, true, false );

        AuthorizationService as = services.get(AuthorizationService.class);
        as.authorizeForAdmin(getAdminUser(), false);
        as.authorizeForAdmin(getAdminUser(), true);
        as.authorizeForAdmin("admin", false);
        as.authorizeForAdmin("admin", true);
        try {
            as.authorizeForAdmin(getTestUser(), true);
            fail();
        }
        catch (AuthorizationException ex) {
        }


    }
}
