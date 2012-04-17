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
package org.apache.oozie.action.oozie;

import org.apache.oozie.action.hadoop.ActionExecutorTestCase;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import java.io.File;
import java.io.StringReader;
import java.io.Writer;
import java.io.OutputStreamWriter;

public class TestSubWorkflowActionExecutor extends ActionExecutorTestCase {
    private static final int JOB_TIMEOUT = 100 * 1000;

    public void testType() {
        SubWorkflowActionExecutor subWorkflow = new SubWorkflowActionExecutor();
        assertEquals(SubWorkflowActionExecutor.ACTION_TYPE, subWorkflow.getType());
    }

    public void testSubWorkflowConfCreation() throws Exception {
        SubWorkflowActionExecutor subWorkflow = new SubWorkflowActionExecutor();

        XConfiguration protoConf = getBaseProtoConf();
        WorkflowJobBean workflow = createBaseWorkflow(protoConf, "W");

        WorkflowActionBean action = (WorkflowActionBean) workflow.getActions().get(0);
        action.setConf("<sub-workflow xmlns='uri:oozie:workflow:0.1'>" +
                "      <app-path>hdfs://foo:9000/user/bar/workflow.xml</app-path>" +
                "      <configuration>" +
                "        <property>" +
                "          <name>a</name>" +
                "          <value>A</value>" +
                "        </property>" +
                "      </configuration>" +
                "</sub-workflow>");

        OozieClient oozieClient = subWorkflow.getWorkflowClient(new Context(workflow, action),
                                                                SubWorkflowActionExecutor.LOCAL);
        assertNotNull(oozieClient);

        oozieClient = subWorkflow.getWorkflowClient(new Context(workflow, action), "http://localhost:8080/oozie");

        assertNotNull(oozieClient);
    }

    private static final String APP1 = "<workflow-app xmlns='uri:oozie:workflow:0.1' name='app'>" +
            "<start to='end'/>" +
            "<end name='end'/>" +
            "</workflow-app>";

    public void testSubWorkflowStart() throws Exception {
        Path subWorkflowAppPath = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        Writer writer = new OutputStreamWriter(fs.create(new Path(subWorkflowAppPath, "workflow.xml")));
        writer.write(APP1);
        writer.close();

        XConfiguration protoConf = getBaseProtoConf();
        WorkflowJobBean workflow = createBaseWorkflow(protoConf, "W");

        final WorkflowActionBean action = (WorkflowActionBean) workflow.getActions().get(0);
        action.setConf("<sub-workflow xmlns='uri:oozie:workflow:0.1'>" +
                "      <app-path>" + subWorkflowAppPath + "</app-path>" +
                "      <configuration>" +
                "        <property>" +
                "          <name>a</name>" +
                "          <value>A</value>" +
                "        </property>" +
                "      </configuration>" +
                "</sub-workflow>");

        SubWorkflowActionExecutor subWorkflow = new SubWorkflowActionExecutor();
        subWorkflow.start(new Context(workflow, action), action);

        final OozieClient oozieClient = subWorkflow.getWorkflowClient(new Context(workflow, action),
                                                                      SubWorkflowActionExecutor.LOCAL);
        waitFor(JOB_TIMEOUT, new Predicate() {
            public boolean evaluate() throws Exception {
                return oozieClient.getJobInfo(action.getExternalId()).getStatus() == WorkflowJob.Status.SUCCEEDED;
            }
        });

        assertEquals(WorkflowJob.Status.SUCCEEDED, oozieClient.getJobInfo(action.getExternalId()).getStatus());

        subWorkflow.check(new Context(workflow, action), action);

        assertEquals(WorkflowAction.Status.DONE, action.getStatus());

        subWorkflow.end(new Context(workflow, action), action);

        assertEquals(WorkflowAction.Status.OK, action.getStatus());

    }

    public void testSubWorkflowRecovery() throws Exception {
        Path subWorkflowAppPath = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        Writer writer = new OutputStreamWriter(fs.create(new Path(subWorkflowAppPath, "workflow.xml")));
        writer.write(APP1);
        writer.close();

        XConfiguration protoConf = getBaseProtoConf();
        WorkflowJobBean workflow = createBaseWorkflow(protoConf, "W");

        final WorkflowActionBean action = (WorkflowActionBean) workflow.getActions().get(0);
        action.setConf("<sub-workflow xmlns='uri:oozie:workflow:0.1'>" +
                "      <app-path>" + subWorkflowAppPath + File.separator + "workflow.xml" + "</app-path>" +
                "      <configuration>" +
                "        <property>" +
                "          <name>a</name>" +
                "          <value>A</value>" +
                "        </property>" +
                "      </configuration>" +
                "</sub-workflow>");

        SubWorkflowActionExecutor subWorkflow = new SubWorkflowActionExecutor();
        subWorkflow.start(new Context(workflow, action), action);

        final OozieClient oozieClient = subWorkflow.getWorkflowClient(new Context(workflow, action),
                                                                      SubWorkflowActionExecutor.LOCAL);
        waitFor(JOB_TIMEOUT, new Predicate() {
            public boolean evaluate() throws Exception {
                return oozieClient.getJobInfo(action.getExternalId()).getStatus() == WorkflowJob.Status.SUCCEEDED;
            }
        });
        String extId = action.getExternalId();
        assertEquals(WorkflowJob.Status.SUCCEEDED, oozieClient.getJobInfo(extId).getStatus());
        WorkflowActionBean action1 = new WorkflowActionBean();
        action1.setId(action.getId());
        action1.setName(action.getName());
        action1.setConf("<sub-workflow xmlns='uri:oozie:workflow:0.1'>" +
                "      <app-path>wrongAppPath</app-path>" +
                "      <configuration>" +
                "        <property>" +
                "          <name>a</name>" +
                "          <value>A</value>" +
                "        </property>" +
                "      </configuration>" +
                "</sub-workflow>");
        subWorkflow.start(new Context(workflow, action1), action1);
        assertEquals(extId, action1.getExternalId());
        subWorkflow.check(new Context(workflow, action1), action1);
        assertEquals(WorkflowAction.Status.DONE, action1.getStatus());
        subWorkflow.end(new Context(workflow, action1), action1);
        assertEquals(WorkflowAction.Status.OK, action1.getStatus());
    }

    public void testConfigPropagation() throws Exception {
        Path subWorkflowAppPath = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        Writer writer = new OutputStreamWriter(fs.create(new Path(subWorkflowAppPath, "workflow.xml")));
        writer.write(APP1);
        writer.close();

        XConfiguration protoConf = getBaseProtoConf();
        WorkflowJobBean workflow = createBaseWorkflow(protoConf, "W");
        String defaultConf = workflow.getConf();
        XConfiguration newConf = new XConfiguration(new StringReader(defaultConf));
        newConf.set("abc", "xyz");
        workflow.setConf(newConf.toXmlString());

        final WorkflowActionBean action = (WorkflowActionBean) workflow.getActions().get(0);
        action.setConf("<sub-workflow xmlns='uri:oozie:workflow:0.1' name='subwf'>" +
                "      <app-path>" + subWorkflowAppPath + File.separator + "workflow.xml" + "</app-path>" +
                "      <propagate-configuration />" +
                "      <configuration>" +
                "        <property>" +
                "          <name>a</name>" +
                "          <value>A</value>" +
                "        </property>" +
                "      </configuration>" +
                "</sub-workflow>");

        SubWorkflowActionExecutor subWorkflow = new SubWorkflowActionExecutor();
        subWorkflow.start(new Context(workflow, action), action);

        final OozieClient oozieClient = subWorkflow.getWorkflowClient(new Context(workflow, action),
                                                                      SubWorkflowActionExecutor.LOCAL);
        waitFor(JOB_TIMEOUT, new Predicate() {
            public boolean evaluate() throws Exception {
                return oozieClient.getJobInfo(action.getExternalId()).getStatus() == WorkflowJob.Status.SUCCEEDED;
            }
        });

        assertEquals(WorkflowJob.Status.SUCCEEDED, oozieClient.getJobInfo(action.getExternalId()).getStatus());

        subWorkflow.check(new Context(workflow, action), action);

        assertEquals(WorkflowAction.Status.DONE, action.getStatus());

        subWorkflow.end(new Context(workflow, action), action);

        assertEquals(WorkflowAction.Status.OK, action.getStatus());

        WorkflowJob wf = oozieClient.getJobInfo(action.getExternalId());
        Configuration childConf = new XConfiguration(new StringReader(wf.getConf()));
        assertEquals("xyz", childConf.get("abc"));
    }

    public void testGetGroupFromParent() throws Exception {
        Path subWorkflowAppPath = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        Writer writer = new OutputStreamWriter(fs.create(new Path(subWorkflowAppPath, "workflow.xml")));
        writer.write(APP1);
        writer.close();

        XConfiguration protoConf = getBaseProtoConf();
        WorkflowJobBean workflow = createBaseWorkflow(protoConf, "W");
        String defaultConf = workflow.getConf();
        XConfiguration newConf = new XConfiguration(new StringReader(defaultConf));
        String actionConf = "<sub-workflow xmlns='uri:oozie:workflow:0.1' name='subwf'>" +
                "      <app-path>" + subWorkflowAppPath + File.separator + "workflow.xml" + "</app-path>" +
                "      <configuration>" +
                "        <property>" +
                "          <name>a</name>" +
                "          <value>A</value>" +
                "        </property>" +
                "      </configuration>" +
                "</sub-workflow>";

        final WorkflowActionBean action = (WorkflowActionBean) workflow.getActions().get(0);
        action.setConf(actionConf);

        // negative test
        SubWorkflowActionExecutor subWorkflow = new SubWorkflowActionExecutor();
        workflow.setConf(newConf.toXmlString());

        subWorkflow.start(new Context(workflow, action), action);

        OozieClient oozieClient = subWorkflow.getWorkflowClient(new Context(workflow, action),
                                                                      SubWorkflowActionExecutor.LOCAL);

        subWorkflow.check(new Context(workflow, action), action);
        subWorkflow.end(new Context(workflow, action), action);

        assertEquals(WorkflowAction.Status.OK, action.getStatus());

        WorkflowJob wf = oozieClient.getJobInfo(action.getExternalId());
        Configuration childConf = new XConfiguration(new StringReader(wf.getConf()));

        assertFalse(getTestGroup() == childConf.get(OozieClient.GROUP_NAME));

        // positive test
        newConf.set(OozieClient.GROUP_NAME, getTestGroup());
        workflow.setConf(newConf.toXmlString());
        WorkflowActionBean action1 = new WorkflowActionBean();
        action1.setConf(actionConf);
        action1.setId("W1");

        subWorkflow.start(new Context(workflow, action1), action1);

        oozieClient = subWorkflow.getWorkflowClient(new Context(workflow, action1),
                                                                      SubWorkflowActionExecutor.LOCAL);

        subWorkflow.check(new Context(workflow, action1), action1);
        subWorkflow.end(new Context(workflow, action1), action1);

        wf = oozieClient.getJobInfo(action1.getExternalId());
        childConf = new XConfiguration(new StringReader(wf.getConf()));
        assertEquals(getTestGroup(), childConf.get(OozieClient.GROUP_NAME));
    }

    public void testConfigNotPropagation() throws Exception {
        Path subWorkflowAppPath = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        Writer writer = new OutputStreamWriter(fs.create(new Path(subWorkflowAppPath, "workflow.xml")));
        writer.write(APP1);
        writer.close();

        XConfiguration protoConf = getBaseProtoConf();
        WorkflowJobBean workflow = createBaseWorkflow(protoConf, "W");
        String defaultConf = workflow.getConf();
        XConfiguration newConf = new XConfiguration(new StringReader(defaultConf));
        newConf.set("abc", "xyz");
        workflow.setConf(newConf.toXmlString());

        final WorkflowActionBean action = (WorkflowActionBean) workflow.getActions().get(0);
        action.setConf("<sub-workflow xmlns='uri:oozie:workflow:0.1' name='subwf'>" +
                "      <app-path>" + subWorkflowAppPath + File.separator + "workflow.xml" + "</app-path>" +
                "      <configuration>" +
                "        <property>" +
                "          <name>a</name>" +
                "          <value>A</value>" +
                "        </property>" +
                "      </configuration>" +
                "</sub-workflow>");

        SubWorkflowActionExecutor subWorkflow = new SubWorkflowActionExecutor();
        subWorkflow.start(new Context(workflow, action), action);

        final OozieClient oozieClient = subWorkflow.getWorkflowClient(new Context(workflow, action),
                                                                      SubWorkflowActionExecutor.LOCAL);
        waitFor(JOB_TIMEOUT, new Predicate() {
            public boolean evaluate() throws Exception {
                return oozieClient.getJobInfo(action.getExternalId()).getStatus() == WorkflowJob.Status.SUCCEEDED;
            }
        });

        assertEquals(WorkflowJob.Status.SUCCEEDED, oozieClient.getJobInfo(action.getExternalId()).getStatus());

        subWorkflow.check(new Context(workflow, action), action);

        assertEquals(WorkflowAction.Status.DONE, action.getStatus());

        subWorkflow.end(new Context(workflow, action), action);

        assertEquals(WorkflowAction.Status.OK, action.getStatus());

        WorkflowJob wf = oozieClient.getJobInfo(action.getExternalId());
        Configuration childConf = new XConfiguration(new StringReader(wf.getConf()));
        assertNull(childConf.get("abc"));
    }
}
