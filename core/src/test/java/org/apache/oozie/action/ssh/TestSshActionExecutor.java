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
package org.apache.oozie.action.ssh;

import org.apache.oozie.service.CallbackService;

import java.io.IOException;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction.Status;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.service.UUIDService.ApplicationType;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.PropertiesUtils;
import org.apache.oozie.util.XConfiguration;

public class TestSshActionExecutor extends XFsTestCase {

    private Services services;

    private class Context implements ActionExecutor.Context {
        private WorkflowActionBean action;
        private WorkflowJobBean workflow;

        public Context(WorkflowJobBean workflow, WorkflowActionBean action) {
            this.workflow = workflow;
            this.action = action;
        }

        public Configuration getProtoActionConf() {
            String s = workflow.getProtoActionConf();
            try {
                return new XConfiguration(new StringReader(s));
            }
            catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        public WorkflowJob getWorkflow() {
            return workflow;
        }

        public ELEvaluator getELEvaluator() {
            throw new UnsupportedOperationException();
        }

        public void setVar(String name, String value) {
            throw new UnsupportedOperationException();
        }

        public String getVar(String name) {
            throw new UnsupportedOperationException();
        }

        public boolean isRetry() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setExternalStatus(String externalStatus) {
            action.setExternalStatus(externalStatus);
        }

        @Override
        public void setEndData(Status status, String signalValue) {
            action.setEndData(status, signalValue);
        }

        @Override
        public void setExecutionData(String externalStatus, Properties actionData) {
            action.setExecutionData(externalStatus, actionData);
        }

        @Override
        public void setExecutionStats(String jsonStats){
            action.setExecutionStats(jsonStats);
        }

        @Override
        public void setExternalChildIDs(String externalChildIDs){
            action.setExternalChildIDs(externalChildIDs);
        }

        @Override
        public void setStartData(String externalId, String trackerUri, String consoleUrl) {
            action.setStartData(externalId, trackerUri, consoleUrl);
        }

        @Override
        public String getCallbackUrl(String externalStatusVar) {
            return Services.get().get(CallbackService.class).createCallBackUrl(action.getId(), externalStatusVar);
        }

        @Override
        public String getRecoveryId() {
            return action.getId();
        }

        public Path getActionDir() throws URISyntaxException, IOException {
            String name = getWorkflow().getId() + "/" + action.getName() + "--" + action.getType();
            FileSystem fs = getAppFileSystem();
            String actionDirPath = Services.get().getSystemId() + "/" + name;
            Path fqActionDir = new Path(fs.getHomeDirectory(), actionDirPath);
            return fqActionDir;
        }

        public FileSystem getAppFileSystem() throws IOException, URISyntaxException {
            return getFileSystem();
        }

        @Override
        public void setErrorInfo(String str, String exMsg) {
        }
    }


    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();

        XConfiguration conf = new XConfiguration();
        conf.setStrings(WorkflowAppService.HADOOP_USER, getTestUser());
        Path path = new Path(getNameNodeUri(), getTestCaseDir());
        FileSystem fs = getFileSystem();
        fs.delete(path, true);
    }

    protected String getActionXMLSchema() {
        return "uri:oozie-workflow:0.1";
    }

    public void testJobStart() throws ActionExecutorException {
        String baseDir = getTestCaseDir();
        Path appPath = new Path(getNameNodeUri(), baseDir);

        XConfiguration protoConf = new XConfiguration();
        protoConf.setStrings(WorkflowAppService.HADOOP_USER, getTestUser());


        XConfiguration wfConf = new XConfiguration();
        wfConf.set(OozieClient.APP_PATH, appPath.toString());

        WorkflowJobBean workflow = new WorkflowJobBean();
        workflow.setConf(wfConf.toXmlString());
        workflow.setAppPath(wfConf.get(OozieClient.APP_PATH));
        workflow.setProtoActionConf(protoConf.toXmlString());
        workflow.setId(Services.get().get(UUIDService.class).generateId(ApplicationType.WORKFLOW));

        final WorkflowActionBean action = new WorkflowActionBean();
        action.setId("actionId");
        action.setConf("<ssh xmlns='" + getActionXMLSchema() + "'>" +
                       "<host>localhost</host>" +
                       "<command>echo</command>" +
                       "<capture-output/>" +
                       "<args>\"prop1=something\"</args>" +
                       "</ssh>");
        action.setName("ssh");
        final SshActionExecutor ssh = new SshActionExecutor();
        final Context context = new Context(workflow, action);
        ssh.start(context, action);

        waitFor(30 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                ssh.check(context, action);
                return Status.DONE == action.getStatus();
            }
        });
        ssh.end(context, action);
        assertEquals(Status.OK, action.getStatus());
        assertEquals("something", PropertiesUtils.stringToProperties(action.getData()).getProperty("prop1"));
    }

    public void testJobRecover() throws ActionExecutorException, InterruptedException {
        String baseDir = getTestCaseDir();
        Path appPath = new Path(getNameNodeUri(), baseDir);

        XConfiguration protoConf = new XConfiguration();
        protoConf.setStrings(WorkflowAppService.HADOOP_USER, getTestUser());


        XConfiguration wfConf = new XConfiguration();
        wfConf.set(OozieClient.APP_PATH, appPath.toString());

        WorkflowJobBean workflow = new WorkflowJobBean();
        workflow.setConf(wfConf.toXmlString());
        workflow.setAppPath(wfConf.get(OozieClient.APP_PATH));
        workflow.setProtoActionConf(protoConf.toXmlString());
        workflow.setId(Services.get().get(UUIDService.class).generateId(ApplicationType.WORKFLOW));

        final WorkflowActionBean action = new WorkflowActionBean();
        action.setId("actionId");
        action.setConf("<ssh xmlns='" + getActionXMLSchema() + "'>" +
                       "<host>localhost</host>" +
                       "<command>echo</command>" +
                       "<capture-output/>" +
                       "<args>\"prop1=something\"</args>" +
                       "</ssh>");
        action.setName("ssh");
        final SshActionExecutor ssh = new SshActionExecutor();
        final Context context = new Context(workflow, action);
        ssh.start(context, action);

        Thread.sleep(200);
        final WorkflowActionBean action1 = new WorkflowActionBean();
        action1.setId("actionId");
        action1.setConf("<ssh xmlns='" + getActionXMLSchema() + "'>" +
                       "<host>localhost</host>" +
                       "<command>echo</command>" +
                       "<capture-output/>" +
                       "<args>\"prop1=nothing\"</args>" +
                       "</ssh>");
        action1.setName("ssh");
        final SshActionExecutor ssh1 = new SshActionExecutor();
        final Context context1 = new Context(workflow, action1);
        Thread.sleep(500);
        ssh1.start(context1, action1);
        assertEquals(action1.getExternalId(), action.getExternalId());

        waitFor(30 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                ssh.check(context1, action1);
                return Status.DONE == action1.getStatus();
            }
        });
        ssh1.end(context1, action1);
        assertEquals(Status.OK, action1.getStatus());
        assertEquals("something", PropertiesUtils.stringToProperties(action1.getData()).getProperty("prop1"));
    }


    // TODO Move this test case over to a new class. Conflict between this one
    // and testConnectionErrors. The property to replace the ssh user cannot be
    // reset in a good way during runtime.
    //
//    public void testOozieUserMismatch() throws ActionExecutorException {
//        String baseDir = getTestCaseDir();
//        Path appPath = new Path(getNameNodeUri(), baseDir);
//
//        Services.get().getConf().setBoolean(SshActionExecutor.CONF_SSH_ALLOW_USER_AT_HOST, false);
//        XConfiguration protoConf = new XConfiguration();
//        protoConf.setStrings(WorkflowAppService.HADOOP_USER, getTestUser());
//        protoConf.setStrings(WorkflowAppService.HADOOP_UGI, getTestUser() + "," + getTestGroup());
//
//        XConfiguration wfConf = new XConfiguration();
//        wfConf.set(OozieClient.APP_PATH, appPath.toString());
//
//        WorkflowJobBean workflow = new WorkflowJobBean();
//        workflow.setConf(wfConf.toXmlString());
//        workflow.setAppPath(wfConf.get(OozieClient.APP_PATH));
//        workflow.setProtoActionConf(protoConf.toXmlString());
//        workflow.setId("wfId");
//
//        final WorkflowActionBean action = new WorkflowActionBean();
//        action.setId("actionId_" + System.currentTimeMillis());
//        action.setConf("<ssh xmlns='" + getActionXMLSchema() + "'>" +
//                       "<host>invalid@localhost</host>" +
//                       "<command>echo</command>" +
//                       "<capture-output/>" +
//                       "<args>\"prop1=something\"</args>" +
//                       "</ssh>");
//        action.setName("ssh");
//
//        final SshActionExecutor ssh = new SshActionExecutor();
//
//        final Context context = new Context(workflow, action);
//        try {
//            ssh.start(context, action);
//            assertTrue(false);
//        } catch (ActionExecutorException ex) {
//            System.err.println("Caught exception, Error Code: " + ex.getErrorCode());
//            assertEquals(SshActionExecutor.ERR_USER_MISMATCH, ex.getErrorCode());
//        }
//    }

    public void testConnectionErrors() throws ActionExecutorException {
        String baseDir = getTestCaseDir();
        Path appPath = new Path(getNameNodeUri(), baseDir);

        XConfiguration protoConf = new XConfiguration();
        protoConf.setStrings(WorkflowAppService.HADOOP_USER, getTestUser());


        XConfiguration wfConf = new XConfiguration();
        wfConf.set(OozieClient.APP_PATH, appPath.toString());

        WorkflowJobBean workflow = new WorkflowJobBean();
        workflow.setConf(wfConf.toXmlString());
        workflow.setAppPath(wfConf.get(OozieClient.APP_PATH));
        workflow.setProtoActionConf(protoConf.toXmlString());
        workflow.setId(Services.get().get(UUIDService.class).generateId(ApplicationType.WORKFLOW));

        final WorkflowActionBean action = new WorkflowActionBean();
        action.setId("actionId");
        action.setConf("<ssh xmlns='" + getActionXMLSchema() + "'>" +
                "<host>blabla</host>" +
                "<command>echo</command>" +
                "<args>\"prop1=something\"</args>" +
                "</ssh>");
        action.setName("ssh");
        final SshActionExecutor ssh = new SshActionExecutor();
        final Context context = new Context(workflow, action);
        try {
            ssh.start(context, action);
        }
        catch (ActionExecutorException ex) {
            System.out.println("Testing COULD_NOT_RESOLVE_HOST");

            assertEquals("COULD_NOT_RESOLVE_HOST", ex.getErrorCode());
            assertEquals(ActionExecutorException.ErrorType.TRANSIENT, ex.getErrorType());
        }
        action.setConf("<ssh xmlns='" + getActionXMLSchema() + "'>" +
                "<host>11.11.11.11</host>" +
                "<command>echo</command>" +
                "<args>\"prop1=something\"</args>" +
                "</ssh>");
        try {
            ssh.start(context, action);
        }
        catch (ActionExecutorException ex) {
            System.out.println("Testing COULD_NOT_CONNECT");

            assertEquals("COULD_NOT_CONNECT", ex.getErrorCode());
            assertEquals(ActionExecutorException.ErrorType.TRANSIENT, ex.getErrorType());
        }
        action.setConf("<ssh xmlns='" + getActionXMLSchema() + "'>" +
                "<host>y@localhost</host>" +
                "<command>echo</command>" +
                "<args>\"prop1=something\"</args>" +
                "</ssh>");
        try {
            ssh.start(context, action);
        }
        catch (ActionExecutorException ex) {
            System.out.println("Testing AUTH_FAILED");

            assertEquals("AUTH_FAILED", ex.getErrorCode());
            assertEquals(ActionExecutorException.ErrorType.NON_TRANSIENT, ex.getErrorType());
        }
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }
}
