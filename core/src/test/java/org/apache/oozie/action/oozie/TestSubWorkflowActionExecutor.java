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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.hadoop.ActionExecutorTestCase;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.wf.SuspendXCommand;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.service.XLogService;
import org.apache.oozie.util.XConfiguration;

import java.io.*;
import java.net.URI;
import java.util.Properties;

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

        assertEquals(workflow.getId(), oozieClient.getJobInfo(action.getExternalId()).getParentId());
    }

    public void testSubWorkflowRecovery() throws Exception {
        Path subWorkflowAppPath = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        Path workflowPath = new Path(subWorkflowAppPath, "workflow.xml");
        Writer writer = new OutputStreamWriter(fs.create(workflowPath));
        writer.write(APP1);
        writer.close();

        XConfiguration protoConf = getBaseProtoConf();
        WorkflowJobBean workflow = createBaseWorkflow(protoConf, "W");

        final WorkflowActionBean action = (WorkflowActionBean) workflow.getActions().get(0);
        action.setConf("<sub-workflow xmlns='uri:oozie:workflow:0.1'>" +
                "      <app-path>" + workflowPath.toString() + "</app-path>" +
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
        assertEquals(workflow.getId(), oozieClient.getJobInfo(extId).getParentId());
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
        assertEquals(workflow.getId(), oozieClient.getJobInfo(extId).getParentId());
        subWorkflow.check(new Context(workflow, action1), action1);
        assertEquals(WorkflowAction.Status.DONE, action1.getStatus());
        subWorkflow.end(new Context(workflow, action1), action1);
        assertEquals(WorkflowAction.Status.OK, action1.getStatus());
    }

    public void testConfigPropagation() throws Exception {
        Path subWorkflowAppPath = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        Path workflowPath = new Path(subWorkflowAppPath, "workflow.xml");
        Writer writer = new OutputStreamWriter(fs.create(workflowPath));
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
                "      <app-path>" + workflowPath.toString() + "</app-path>" +
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
        Path workflowPath = new Path(subWorkflowAppPath, "workflow.xml");
        Writer writer = new OutputStreamWriter(fs.create(workflowPath));
        writer.write(APP1);
        writer.close();

        XConfiguration protoConf = getBaseProtoConf();
        final WorkflowJobBean workflow = createBaseWorkflow(protoConf, "W");
        String defaultConf = workflow.getConf();
        XConfiguration newConf = new XConfiguration(new StringReader(defaultConf));
        String actionConf = "<sub-workflow xmlns='uri:oozie:workflow:0.1' name='subwf'>" +
                "      <app-path>" + workflowPath.toString() + "</app-path>" +
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
        final SubWorkflowActionExecutor subWorkflow = new SubWorkflowActionExecutor();
        workflow.setConf(newConf.toXmlString());

        subWorkflow.start(new Context(workflow, action), action);

        OozieClient oozieClient = subWorkflow.getWorkflowClient(new Context(workflow, action),
                                                                      SubWorkflowActionExecutor.LOCAL);
        waitFor(5000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                subWorkflow.check(new Context(workflow, action), action);
                return action.getStatus() == WorkflowActionBean.Status.DONE;
            }
        });

        subWorkflow.check(new Context(workflow, action), action);
        subWorkflow.end(new Context(workflow, action), action);

        assertEquals(WorkflowAction.Status.OK, action.getStatus());

        WorkflowJob wf = oozieClient.getJobInfo(action.getExternalId());
        Configuration childConf = new XConfiguration(new StringReader(wf.getConf()));

        assertFalse(getTestGroup() == childConf.get(OozieClient.GROUP_NAME));

        // positive test
        newConf.set(OozieClient.GROUP_NAME, getTestGroup());
        workflow.setConf(newConf.toXmlString());
        final WorkflowActionBean action1 = new WorkflowActionBean();
        action1.setConf(actionConf);
        action1.setId("W1");

        subWorkflow.start(new Context(workflow, action1), action1);

        oozieClient = subWorkflow.getWorkflowClient(new Context(workflow, action1),
                                                                      SubWorkflowActionExecutor.LOCAL);

        waitFor(5000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                subWorkflow.check(new Context(workflow, action1), action1);
                return action1.getStatus() == WorkflowActionBean.Status.DONE;
            }
        });

        subWorkflow.check(new Context(workflow, action1), action1);
        subWorkflow.end(new Context(workflow, action1), action1);

        wf = oozieClient.getJobInfo(action1.getExternalId());
        childConf = new XConfiguration(new StringReader(wf.getConf()));
        assertEquals(getTestGroup(), childConf.get(OozieClient.GROUP_NAME));
    }

    public void testConfigNotPropagation() throws Exception {
        Path subWorkflowAppPath = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        Path workflowPath = new Path(subWorkflowAppPath, "workflow.xml");
        Writer writer = new OutputStreamWriter(fs.create(workflowPath));
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
                "      <app-path>" + workflowPath.toString() + "</app-path>" +
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

    public void testSubworkflowLib() throws Exception {
        XConfiguration protoConf = getBaseProtoConf();
        WorkflowJobBean workflow = createBaseWorkflow(protoConf, "W");
        FileSystem fs = getFileSystem();
        Path parentLibJar = new Path(getFsTestCaseDir(), "lib/parentLibrary.jar");
        fs.create(parentLibJar);
        assertTrue(fs.exists(parentLibJar));
        String defaultConf = workflow.getConf();
        XConfiguration newConf = new XConfiguration(new StringReader(defaultConf));
        newConf.set(OozieClient.LIBPATH, parentLibJar.getParent().toString());
        workflow.setConf(newConf.toXmlString());

        Path subWorkflowAppPath = new Path(getFsTestCaseDir().toString(), "subwf");
        Path workflowPath = new Path(subWorkflowAppPath, "workflow.xml");
        Writer writer = new OutputStreamWriter(fs.create(workflowPath));
        writer.write(APP1);
        writer.close();
        Path subwfLibJar = new Path(subWorkflowAppPath, "lib/subwfLibrary.jar");
        fs.create(subwfLibJar);
        assertTrue(fs.exists(subwfLibJar));

        final WorkflowActionBean action = (WorkflowActionBean) workflow.getActions().get(0);
        action.setConf("<sub-workflow xmlns='uri:oozie:workflow:0.1' name='subwf'>" +
                "      <app-path>" + workflowPath.toString() + "</app-path>" +
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

        WorkflowAppService wps = Services.get().get(WorkflowAppService.class);
        WorkflowJob wf = oozieClient.getJobInfo(action.getExternalId());
        Configuration childConf = new XConfiguration(new StringReader(wf.getConf()));
        childConf = wps.createProtoActionConf(childConf, true);
        assertEquals(childConf.get(WorkflowAppService.APP_LIB_PATH_LIST), subwfLibJar.toString());
    }

    public void testSubworkflowDepth() throws Exception {
        Path subWorkflowAppPath = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        Writer writer = new OutputStreamWriter(fs.create(new Path(subWorkflowAppPath, "workflow.xml")));
        // Infinitly recursive workflow
        
        String appStr = "<workflow-app xmlns=\"uri:oozie:workflow:0.4\" name=\"workflow\">" +
                "<start to=\"subwf\"/>" +
                "<action name=\"subwf\">" +
                "     <sub-workflow xmlns='uri:oozie:workflow:0.4'>" +
                "          <app-path>" + subWorkflowAppPath.toString() + "</app-path>" +
                "     </sub-workflow>" +
                "     <ok to=\"end\"/>" +
                "     <error to=\"fail\"/>" +
                "</action>" +
                "<kill name=\"fail\">" +
                "     <message>Sub workflow failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>" +
                "</kill>" +
                "<end name=\"end\"/>" +
                "</workflow-app>";
        writer.write(appStr);
        writer.close();

        try {
            Services.get().destroy();
            setSystemProperty(XLogService.LOG4J_FILE, "oozie-log4j.properties");
            LocalOozie.start();
            // Set the max depth to 3
            Services.get().getConf().setInt("oozie.action.subworkflow.max.depth", 3);
            final OozieClient wfClient = LocalOozie.getClient();
            Properties conf = wfClient.createConfiguration();
            conf.setProperty(OozieClient.APP_PATH, subWorkflowAppPath.toString());
            conf.setProperty(OozieClient.USER_NAME, getTestUser());
            final String jobId0 = wfClient.submit(conf);
            wfClient.start(jobId0);

            waitFor(20 * 1000, new Predicate() {
                @Override
                public boolean evaluate() throws Exception {
                    return wfClient.getJobInfo(jobId0).getStatus() == WorkflowJob.Status.KILLED;
                }
            });
            // All should be KILLED because our infinitly recusrive workflow hit the max
            assertEquals(WorkflowJob.Status.KILLED, wfClient.getJobInfo(jobId0).getStatus());
            String jobId1 = wfClient.getJobInfo(jobId0).getActions().get(1).getExternalId();
            assertEquals(WorkflowJob.Status.KILLED, wfClient.getJobInfo(jobId1).getStatus());
            String jobId2 = wfClient.getJobInfo(jobId1).getActions().get(1).getExternalId();
            assertEquals(WorkflowJob.Status.KILLED, wfClient.getJobInfo(jobId2).getStatus());
            String jobId3 = wfClient.getJobInfo(jobId2).getActions().get(1).getExternalId();
            assertEquals(WorkflowJob.Status.KILLED, wfClient.getJobInfo(jobId3).getStatus());
            String jobId4 = wfClient.getJobInfo(jobId3).getActions().get(1).getExternalId();
            // A fourth subworkflow shouldn't have been created because we set the max to 3
            assertNull(jobId4);
        }
        finally {
            LocalOozie.stop();
        }
    }

    public void testSubWorkflowSuspend() throws Exception {
        try {
            Path subWorkflowAppPath = getFsTestCaseDir();
            FileSystem fs = getFileSystem();
            Path subWorkflowPath = new Path(subWorkflowAppPath, "workflow.xml");
            Writer writer = new OutputStreamWriter(fs.create(subWorkflowPath));
            writer.write(getLazyWorkflow());
            writer.close();

            String workflowUri = getTestCaseFileUri("workflow.xml");
            String appXml = "<workflow-app xmlns=\"uri:oozie:workflow:0.4\" name=\"workflow\">" +
                    "<start to=\"subwf\"/>" +
                    "<action name=\"subwf\">" +
                    "     <sub-workflow xmlns='uri:oozie:workflow:0.4'>" +
                    "          <app-path>" + subWorkflowAppPath.toString() + "</app-path>" +
                    "     </sub-workflow>" +
                    "     <ok to=\"end\"/>" +
                    "     <error to=\"fail\"/>" +
                    "</action>" +
                    "<kill name=\"fail\">" +
                    "     <message>Sub workflow failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>" +
                    "</kill>" +
                    "<end name=\"end\"/>" +
                    "</workflow-app>";

            writeToFile(appXml, workflowUri);
            LocalOozie.start();
            final OozieClient wfClient = LocalOozie.getClient();
            Properties conf = wfClient.createConfiguration();
            conf.setProperty(OozieClient.APP_PATH, workflowUri);
            conf.setProperty(OozieClient.USER_NAME, getTestUser());
            conf.setProperty("appName", "var-app-name");
            final String jobId = wfClient.submit(conf);
            wfClient.start(jobId);

            waitFor(JOB_TIMEOUT, new Predicate() {
                public boolean evaluate() throws Exception {
                    return (wfClient.getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING) &&
                            (wfClient.getJobInfo(jobId).getActions().get(1).getStatus() == WorkflowAction.Status.RUNNING);
                }
            });
            WorkflowJob wf = wfClient.getJobInfo(jobId);
            // Suspending subworkflow
            new SuspendXCommand(wf.getActions().get(1).getExternalId()).call();
            // Check suspend for base workflow
            assertEquals(WorkflowJob.Status.SUSPENDED, wfClient.getJobInfo(jobId).getStatus());
            //Check suspend for sub workflow
            assertEquals(WorkflowJob.Status.SUSPENDED, wfClient.getJobInfo(wf.getActions().get(1).getExternalId()).getStatus());

        } finally {
            LocalOozie.stop();
        }

    }

    private void writeToFile(String appXml, String appPath) throws IOException {
        // TODO Auto-generated method stub
        File wf = new File(URI.create(appPath));
        PrintWriter out = null;
        try {
            out = new PrintWriter(new FileWriter(wf));
            out.println(appXml);
        }
        catch (IOException iex) {
            throw iex;
        }
        finally {
            if (out != null) {
                out.close();
            }
        }
    }

    public String getLazyWorkflow() {
        return  "<workflow-app xmlns='uri:oozie:workflow:0.3' name='app'>" +
                "<start to='java' />" +
                "       <action name='java'>" +
                "<java>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<main-class>" + JavaSleepAction.class.getName() + "</main-class>" +
                "<arg>exit0</arg>" +
                "</java>"
                + "<ok to='end' />"
                + "<error to='fail' />"
                + "</action>"
                + "<kill name='fail'>"
                + "<message>shell action fail, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>"
                + "</kill>"
                + "<end name='end' />"
                + "</workflow-app>";
    }

    public void testSubWorkflowRerun() throws Exception {
        try {
            Path subWorkflowAppPath = getFsTestCaseDir();
            FileSystem fs = getFileSystem();
            Path subWorkflowPath = new Path(subWorkflowAppPath, "workflow.xml");
            Writer writer = new OutputStreamWriter(fs.create(subWorkflowPath));
            writer.write(getLazyWorkflow());
            writer.close();
            String workflowUri = getTestCaseFileUri("workflow.xml");
            String appXml = "<workflow-app xmlns=\"uri:oozie:workflow:0.4\" name=\"workflow\">" +
                    "<start to=\"subwf\"/>" +
                    "<action name=\"subwf\">" +
                    "     <sub-workflow xmlns='uri:oozie:workflow:0.4'>" +
                    "          <app-path>" + subWorkflowAppPath.toString() + "</app-path>" +
                    "     </sub-workflow>" +
                    "     <ok to=\"end\"/>" +
                    "     <error to=\"fail\"/>" +
                    "</action>" +
                    "<kill name=\"fail\">" +
                    "     <message>Sub workflow failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>" +
                    "</kill>" +
                    "<end name=\"end\"/>" +
                    "</workflow-app>";

            writeToFile(appXml, workflowUri);
            LocalOozie.start();
            final OozieClient wfClient = LocalOozie.getClient();
            Properties conf = wfClient.createConfiguration();
            conf.setProperty(OozieClient.APP_PATH, workflowUri);
            conf.setProperty(OozieClient.USER_NAME, getTestUser());
            conf.setProperty("appName", "var-app-name");
            final String jobId = wfClient.submit(conf);
            wfClient.start(jobId);

            waitFor(JOB_TIMEOUT, new Predicate() {
                public boolean evaluate() throws Exception {
                    return (wfClient.getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING) &&
                            (wfClient.getJobInfo(jobId).getActions().get(1).getStatus() == WorkflowAction.Status.RUNNING);
                }
            });

            String subWorkflowExternalId = wfClient.getJobInfo(jobId).getActions().get(1).getExternalId();
            wfClient.kill(wfClient.getJobInfo(jobId).getActions().get(1).getExternalId());

            waitFor(JOB_TIMEOUT, new Predicate() {
                public boolean evaluate() throws Exception {
                    return (wfClient.getJobInfo(jobId).getStatus() == WorkflowJob.Status.KILLED) &&
                            (wfClient.getJobInfo(jobId).getActions().get(1).getStatus() == WorkflowAction.Status.ERROR);
                }
            });

            conf.setProperty(OozieClient.RERUN_FAIL_NODES, "true");
            wfClient.reRun(jobId,conf);

            waitFor(JOB_TIMEOUT, new Predicate() {
                public boolean evaluate() throws Exception {
                    return (wfClient.getJobInfo(jobId).getStatus() == WorkflowJob.Status.SUCCEEDED) &&
                            (wfClient.getJobInfo(jobId).getActions().get(2).getStatus() == WorkflowAction.Status.OK);

                }
            });

            WorkflowJob job = wfClient.getJobInfo(wfClient.getJobInfo(jobId).getActions().get(2).getExternalId());
            assertEquals(job.getStatus(), WorkflowJob.Status.SUCCEEDED);
            assertEquals(job.getId(), subWorkflowExternalId);

        } finally {
            LocalOozie.stop();
        }

    }
}
