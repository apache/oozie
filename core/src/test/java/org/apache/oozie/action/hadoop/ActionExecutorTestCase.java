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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.DagELFunctions;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.service.CallbackService;
import org.apache.oozie.service.ELService;
import org.apache.oozie.service.LiteWorkflowStoreService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.service.WorkflowStoreService;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.WorkflowLib;
import org.apache.oozie.workflow.lite.EndNodeDef;
import org.apache.oozie.workflow.lite.LiteWorkflowApp;
import org.apache.oozie.workflow.lite.StartNodeDef;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public abstract class ActionExecutorTestCase extends XFsTestCase {

    @Override
    protected void setUp() throws Exception {
        beforeSetUp();
        super.setUp();
        setSystemProps();
        new Services().init();
    }

    protected void setSystemProps() throws Exception {
    }

    protected void beforeSetUp() throws Exception {
    }

    @Override
    protected void tearDown() throws Exception {
        if (Services.get() != null) {
            Services.get().destroy();
        }
        super.tearDown();
    }

    public class Context implements ActionExecutor.Context {
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
            ELEvaluator evaluator = Services.get().get(ELService.class).createEvaluator("workflow");
            DagELFunctions.configureEvaluator(evaluator, workflow, action);
            try {
                XConfiguration xconf = new XConfiguration(new StringReader(action.getConf()));
                for (Map.Entry<String, String> entry : xconf){
                    evaluator.setVariable(entry.getKey(), entry.getValue());
                }
            }
            catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            return evaluator;
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

        public String getExecutionStats() {
            return action.getExecutionStats();
        }

        public void setExecutionStats(String jsonStats) {
            action.setExecutionStats(jsonStats);
        }

        public String getExternalChildIDs() {
            return action.getExternalChildIDs();
        }

        public void setExternalChildIDs(String externalChildIDs) {
            action.setExternalChildIDs(externalChildIDs);
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

        @Override
        public void setErrorInfo(String str, String exMsg) {
            action.setErrorInfo(str, exMsg);
        }
    }

    protected Path getAppPath() {
        Path baseDir = getFsTestCaseDir();
        return new Path(baseDir, "app");
    }

    protected XConfiguration getBaseProtoConf() {
        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());

        return protoConf;
    }

    /**
     * Return a workflow job which contains one action with no configuration.
     *
     * @param protoConf
     * @param actionName
     * @return workflow job bean
     * @throws Exception
     */
    protected WorkflowJobBean createBaseWorkflow(XConfiguration protoConf, String actionName) throws Exception {
        Path appUri = new Path(getAppPath(), "workflow.xml");

        String content = "<workflow-app xmlns='uri:oozie:workflow:0.1'  xmlns:sla='uri:oozie:sla:0.1' name='no-op-wf'>";
        content += "<start to='end' />";
        content += "<end name='end' /></workflow-app>";
        writeToFile(content, getAppPath(), "workflow.xml");

        WorkflowApp app = new LiteWorkflowApp("testApp", "<workflow-app/>",
                                              new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class,
                                                               "end"))
                .addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));
        XConfiguration wfConf = new XConfiguration();
        wfConf.set(OozieClient.USER_NAME, getTestUser());
        wfConf.set(OozieClient.APP_PATH, appUri.toString());


        WorkflowJobBean workflow = createWorkflow(app, wfConf, protoConf);

        WorkflowActionBean action = new WorkflowActionBean();
        action.setName(actionName);
        action.setCred("null");
        action.setId(Services.get().get(UUIDService.class).generateChildId(workflow.getId(), actionName));
        workflow.getActions().add(action);
        return workflow;
    }

    /**
     * Return a workflow job which contains one action with no configuration and workflow contains credentials information.
     *
     * @param protoConf
     * @param actionName
     * @return workflow job bean
     * @throws Exception
     */
    protected WorkflowJobBean createBaseWorkflowWithCredentials(XConfiguration protoConf, String actionName)
            throws Exception {
        Path appUri = new Path(getAppPath(), "workflow.xml");
        Reader reader = IOUtils.getResourceAsReader("wf-credentials.xml", -1);
        String wfxml = IOUtils.getReaderAsString(reader, -1);

        writeToFile(wfxml, getAppPath(), "workflow.xml");

        WorkflowApp app = new LiteWorkflowApp("test-wf-cred", wfxml,
            new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "start")).
            addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));
        XConfiguration wfConf = new XConfiguration();
        wfConf.set(OozieClient.USER_NAME, getTestUser());
        wfConf.set(OozieClient.APP_PATH, appUri.toString());


        WorkflowJobBean workflow = createWorkflow(app, wfConf, protoConf);

        WorkflowActionBean action = new WorkflowActionBean();
        action.setName(actionName);
        action.setCred("null");
        action.setId(Services.get().get(UUIDService.class).generateChildId(workflow.getId(), actionName));
        workflow.getActions().add(action);
        return workflow;
    }

    private WorkflowJobBean createWorkflow(WorkflowApp app, Configuration conf, XConfiguration protoConf)
            throws Exception {
        WorkflowLib workflowLib = Services.get().get(WorkflowStoreService.class).getWorkflowLibWithNoDB();
        WorkflowInstance wfInstance;
        wfInstance = workflowLib.createInstance(app, conf);
        WorkflowJobBean workflow = new WorkflowJobBean();
        workflow.setId(wfInstance.getId());
        workflow.setAppName(app.getName());
        workflow.setAppPath(conf.get(OozieClient.APP_PATH));
        workflow.setConf(XmlUtils.prettyPrint(conf).toString());
        workflow.setProtoActionConf(XmlUtils.prettyPrint(protoConf).toString());
        workflow.setCreatedTime(new Date());
        workflow.setLogToken(conf.get(OozieClient.LOG_TOKEN, ""));
        workflow.setStatus(WorkflowJob.Status.PREP);
        workflow.setRun(0);
        workflow.setUser(conf.get(OozieClient.USER_NAME));
        workflow.setGroup(conf.get(OozieClient.GROUP_NAME));
        workflow.setWorkflowInstance(wfInstance);
        return workflow;
    }

    private void writeToFile(String content, Path appPath, String fileName) throws IOException {
        FileSystem fs = getFileSystem();
        Writer writer = new OutputStreamWriter(fs.create(new Path(appPath, fileName), true));
        writer.write(content);
        writer.close();
    }

}
