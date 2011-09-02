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
package org.apache.oozie.action.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.service.CallbackService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.XConfiguration;

import java.io.IOException;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public abstract class ActionExecutorTestCase extends XFsTestCase {

    protected void setUp() throws Exception {
        super.setUp();
        setSystemProps();
        new Services().init();
    }

    protected void setSystemProps() {
    }
    
    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    protected class Context implements ActionExecutor.Context {
        private WorkflowActionBean action;
        private WorkflowJobBean workflow;
        boolean started;
        boolean executed;
        boolean ended;
        private Map<String, String> vars = new HashMap<String, String>();

        public Context(WorkflowJobBean workflow, WorkflowActionBean action) {
            this.workflow = workflow;
            this.action = action;
        }

        public String getCallbackUrl(String externalStatusVar) {
            return Services.get().get(CallbackService.class).createCallBackUrl(action.getId(), externalStatusVar);
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

        public WorkflowAction getAction() {
            return action;
        }

        public ELEvaluator getELEvaluator() {
            throw new UnsupportedOperationException();
        }

        public void setVar(String name, String value) {
            if (value != null) {
                vars.put(name, value);
            }
            else {
                vars.remove(name);
            }
        }

        public String getVar(String name) {
            return vars.get(name);
        }

        public void setStartData(String externalId, String trackerUri, String consoleUrl) {
            action.setStartData(externalId, trackerUri, consoleUrl);
            started = true;
        }

        public void setExecutionData(String externalStatus, Properties actionData) {
            action.setExecutionData(externalStatus, actionData);
            executed = true;
        }

        public void setEndData(WorkflowAction.Status status, String signalValue) {
            action.setEndData(status, signalValue);
            ended = true;
        }

        public boolean isRetry() {
            throw new UnsupportedOperationException();
        }

        public boolean isStarted() {
            return started;
        }

        public boolean isExecuted() {
            return executed;
        }

        public boolean isEnded() {
            return ended;
        }

        public void setExternalStatus(String externalStatus) {
            action.setExternalStatus(externalStatus);
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
    }

    protected Path getAppPath() {
        Path baseDir = getFsTestCaseDir();
        return new Path(baseDir, "app");
    }

    protected XConfiguration getBaseProtoConf() {
        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());
        protoConf.set(WorkflowAppService.HADOOP_UGI, getTestUser() + "," + getTestGroup());
        protoConf.set(OozieClient.GROUP_NAME, getTestGroup());
        injectKerberosInfo(protoConf);        
        return protoConf;
    }

    // it contains one action with no configuration.
    protected WorkflowJobBean createBaseWorkflow(XConfiguration protoConf, String actionName) {
        Path appUri = getAppPath();
        XConfiguration wfConf = new XConfiguration();
        wfConf.set(OozieClient.USER_NAME, protoConf.get(OozieClient.USER_NAME));
        wfConf.set(OozieClient.GROUP_NAME, getTestGroup());
        wfConf.set(OozieClient.APP_PATH, appUri.toString());

        WorkflowJobBean workflow = new WorkflowJobBean();
        workflow.setAppPath(appUri.toString());
        workflow.setId(Services.get().get(UUIDService.class).generateId());
        workflow.setConf(wfConf.toXmlString());
        workflow.setUser(wfConf.get(OozieClient.USER_NAME));
        workflow.setGroup(wfConf.get(OozieClient.GROUP_NAME));
        workflow.setAuthToken("authToken");

        workflow.setProtoActionConf(protoConf.toXmlString());

        WorkflowActionBean action = new WorkflowActionBean();
        action.setName(actionName);
        action.setId(Services.get().get(UUIDService.class).generateChildId(workflow.getId(), actionName));
        workflow.getActions().add(action);
        return workflow;
    }

}