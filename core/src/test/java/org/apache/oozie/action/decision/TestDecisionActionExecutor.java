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
package org.apache.oozie.action.decision;

import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.service.CallbackService;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.service.Services;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;

public class TestDecisionActionExecutor extends XFsTestCase {

    private class Context implements ActionExecutor.Context {
        private WorkflowActionBean action;
        boolean executed;
        boolean ended;

        public Context(WorkflowActionBean action) {
            this.action = action;
        }

        public String getCallbackUrl(String externalStatusVar) {
            return Services.get().get(CallbackService.class).createCallBackUrl(action.getId(), externalStatusVar);
        }

        public WorkflowAction getAction() {
            return action;
        }

        public Configuration getProtoActionConf() {
            throw new UnsupportedOperationException();
        }

        public WorkflowJob getWorkflow() {
            throw new UnsupportedOperationException();
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

        public void setStartData(String externalId, String trackerUri, String consoleUrl) {
            action.setStartData(externalId, trackerUri, consoleUrl);
        }

        public void setExecutionData(String externalStatus, Properties actionData) {
            action.setExecutionData(externalStatus, actionData);
            executed = true;
        }

        public void setExecutionStats(String jsonStats) {
            action.setExecutionStats(jsonStats);
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
            throw new UnsupportedOperationException();
        }

        public boolean isExecuted() {
            throw new UnsupportedOperationException();
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
            // TODO Auto-generated method stub
            action.setErrorInfo(str, exMsg);
        }
    }

    public void testDecision() throws Exception {
        new Services().init();
        try {
            ActionExecutor decision = new DecisionActionExecutor();

            assertEquals(DecisionActionExecutor.ACTION_TYPE, decision.getType());

            WorkflowActionBean action = new WorkflowActionBean();
            action.setConf("<switch xmlns='uri:oozie:workflow:0.1'>" +
                "<case to='a'>true</case>" +
                "<case to='b'>true</case>" +
                "<case to='c'>false</case>" +
                "<default to='d'/></switch>");

            decision.start(new Context(action), action);
            assertEquals(WorkflowAction.Status.DONE, action.getStatus());
            decision.end(new Context(action), action);
            assertEquals(WorkflowAction.Status.OK, action.getStatus());
            assertEquals("a", action.getExternalStatus());

            action.setConf("<switch xmlns='uri:oozie:workflow:0.1'>" +
                "<case to='a'>false</case>" +
                "<case to='b'>true</case>" +
                "<case to='c'>false</case>" +
                "<default to='d'/></switch>");

            decision.start(new Context(action), action);
            assertEquals(WorkflowAction.Status.DONE, action.getStatus());
            decision.end(new Context(action), action);
            assertEquals(WorkflowAction.Status.OK, action.getStatus());
            assertEquals("b", action.getExternalStatus());

            action.setConf("<switch xmlns='uri:oozie:workflow:0.1'>" +
                "<case to='a'>false</case>" +
                "<case to='b'>false</case>" +
                "<case to='c'>false</case>" +
                "<default to='d'/></switch>");

            decision.start(new Context(action), action);
            assertEquals(WorkflowAction.Status.DONE, action.getStatus());
            decision.end(new Context(action), action);
            assertEquals(WorkflowAction.Status.OK, action.getStatus());
            assertEquals("d", action.getExternalStatus());

            try {
                action.setConf("<wrong>" +
                    "<case to='a'>false</case>" +
                    "<case to='b'>false</case>" +
                    "<case to='c'>false</case>" +
                    "<default to='d'/></switch>");

                decision.start(new Context(action), action);
                fail();
             }
             catch (ActionExecutorException ex) {
                assertEquals(ActionExecutorException.ErrorType.FAILED, ex.getErrorType());
                assertEquals(DecisionActionExecutor.XML_ERROR, ex.getErrorCode());
             }
             catch (Exception ex) {
                fail();
             }
        }
        finally {
            Services.get().destroy();
        }
    }
}
