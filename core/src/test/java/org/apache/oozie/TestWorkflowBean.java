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

package org.apache.oozie;

import org.apache.oozie.service.LiteWorkflowAppService;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.lite.LiteWorkflowApp;
import org.apache.oozie.workflow.lite.LiteWorkflowInstance;
import org.apache.oozie.WorkflowJobBean;
import org.apache.hadoop.conf.Configuration;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Map;

public class TestWorkflowBean extends XTestCase {

    //private static class MyWorkflowInstance implements WorkflowInstance {
    private static class MyWorkflowInstance extends LiteWorkflowInstance {
        private static final String TRANSITION_TO = "transition.to";
        private static String PATH_SEPARATOR = "/";
        private static String ROOT = PATH_SEPARATOR;
        private static String TRANSITION_SEPARATOR = "#";

        MyWorkflowInstance() {
        }

        public Configuration getConf() {
            return null;
        }

        public String getId() {
            return null;
        }

        public WorkflowApp getApp() {
            return null;
        }

        public boolean start() throws WorkflowException {
            return false;
        }

        public boolean signal(String path, String signaValue) throws WorkflowException {
            return false;
        }

        public void fail(String nodeName) throws WorkflowException {
        }

        public void kill() throws WorkflowException {
        }

        public void suspend() throws WorkflowException {
        }

        public void resume() throws WorkflowException {
        }

        public Status getStatus() {
            return null;
        }

        public void setVar(String name, String value) {
        }

        public String getVar(String name) {
            return null;
        }

        public Map<String, String> getAllVars() {
            return null;
        }

        public void setAllVars(Map<String, String> varMap) {
        }

        public void setTransientVar(String name, Object value) {
        }

        public Object getTransientVar(String name) {
            return null;
        }

        public String getTransition(String node) {
            return null;
        }
    }

    public void testWorkflow() {
        WorkflowJobBean workflow = new WorkflowJobBean();
        workflow.setLogToken("logToken");
        // workflow.setWorkflowInstance(new MyWorkflowInstance());
        workflow.setProtoActionConf("proto");
        assertEquals("logToken", workflow.getLogToken());
        // assertNotNull(workflow.getWorkflowInstance());
        assertEquals("proto", workflow.getProtoActionConf());
    }

    public void testEmptyWriteRead() throws Exception {
        WorkflowJobBean workflow = new WorkflowJobBean();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        workflow.write(dos);
        dos.close();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        workflow = new WorkflowJobBean();
        workflow.readFields(dis);

    }

    public void testFullWriteRead() throws Exception {
        //TODO
    }

}
