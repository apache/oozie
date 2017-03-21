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

package org.apache.oozie.command.wf;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.net.URI;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor.WorkflowJobQuery;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.service.WorkflowStoreService;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.service.XLogService;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;

public class TestSubmitXCommand extends XDataTestCase {
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        setSystemProperty(XLogService.LOG4J_FILE, "oozie-log4j.properties");
        LocalOozie.start();
    }

    @Override
    protected void tearDown() throws Exception {
        LocalOozie.stop();
        super.tearDown();
    }

  public void testSubmitAppName() throws Exception {
      Configuration conf = new XConfiguration();
      String workflowUri = getTestCaseFileUri("workflow.xml");
      String appXml = "<workflow-app xmlns='uri:oozie:workflow:0.1' name='${appName}-foo'> " + "<start to='end' /> "
              + "<end name='end' /> " + "</workflow-app>";

      writeToFile(appXml, workflowUri);
      conf.set(OozieClient.APP_PATH, workflowUri);
      conf.set(OozieClient.USER_NAME, getTestUser());
      conf.set("appName", "var-app-name");
      SubmitXCommand sc = new SubmitXCommand(conf);
      String jobId = sc.call();
      WorkflowStoreService wss = Services.get().get(WorkflowStoreService.class);
      WorkflowStore ws = wss.create();
      WorkflowJobBean wfb = ws.getWorkflow(jobId, false);
      assertEquals("var-app-name-foo", wfb.getAppName());
  }

    public void testSubmitReservedVars() throws Exception {
        Configuration conf = new XConfiguration();
        String workflowUri = getTestCaseFileUri("workflow.xml");
        String appXml = "<workflow-app xmlns='uri:oozie:workflow:0.1' name='map-reduce-wf'> " + "<start to='end' /> "
                + "<end name='end' /> " + "</workflow-app>";

        writeToFile(appXml, workflowUri);
        conf.set(OozieClient.APP_PATH, workflowUri);
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("GB", "5");
        SubmitXCommand sc = new SubmitXCommand(conf);

        try {
            sc.call();
            fail("WF job submission should fail with reserved variable definitions.");
        }
        catch (CommandException ce) {

        }
    }

    public void testAppPathIsDir() throws Exception {
        Configuration conf = new XConfiguration();
        String workflowUri = getTestCaseFileUri("workflow.xml");
        String appXml = "<workflow-app xmlns='uri:oozie:workflow:0.1' name='map-reduce-wf'> " + "<start to='end' /> "
                + "<end name='end' /> " + "</workflow-app>";

        writeToFile(appXml, workflowUri);
        conf.set(OozieClient.APP_PATH, workflowUri);
        conf.set(OozieClient.USER_NAME, getTestUser());

        SubmitXCommand sc = new SubmitXCommand(conf);

        try {
            sc.call();
        }
        catch (CommandException ce) {
            fail("Should succeed");
        }
    }

    public void testSubmitLongXml() throws Exception {
        Configuration conf = new XConfiguration();
        String workflowUri = getTestCaseFileUri("workflow.xml");
            String actionXml = "<map-reduce>"
                    + "<job-tracker>${jobTracker}</job-tracker>"
                    + "<name-node>${nameNode}</name-node>"
                    + "        <prepare>"
                    + "          <delete path=\"${nameNode}/user/${wf:user()}/mr/${outputDir}\"/>"
                    + "        </prepare>"
                    + "        <configuration>"
                    + "          <property><name>bb</name><value>BB</value></property>"
                    + "          <property><name>cc</name><value>from_action</value></property>"
                    + "        </configuration>"
                    + "      </map-reduce>";
        String appXml = "<workflow-app xmlns='uri:oozie:workflow:0.5' name='too-long-wf'> " +
                "<global><configuration>"+generate64kData()+"</configuration></global><start to='mr-node' /> "
                + "    <action name=\"mr-node\">"
                + actionXml
                + "    <ok to=\"end\"/>"
                + "    <error to=\"end\"/>"
                + "</action>"
                + "<end name='end' /> " + "</workflow-app>";

        writeToFile(appXml, workflowUri);
        conf.set(OozieClient.APP_PATH, workflowUri);
        conf.set(OozieClient.USER_NAME, getTestUser());
        addBunchOfProperties(conf);

        SubmitXCommand sc = new SubmitXCommand(conf);
        sc.call();
    }

    private void addBunchOfProperties(Configuration conf) {
        int i=0;
        while(conf.size() < 10000){
            conf.set("ID"+i, i+"something");
            i++;
        }
    }

    private String generate64kData() {
       return "<property><name>radnom</name><value>"+ RandomStringUtils.randomAlphanumeric(70000)+"</value></property>";
    }

    public void testAppPathIsFile1() throws Exception {
        Configuration conf = new XConfiguration();
        String workflowUri = getTestCaseFileUri("workflow.xml");
        String appXml = "<workflow-app xmlns='uri:oozie:workflow:0.1' name='map-reduce-wf'> " + "<start to='end' /> "
                + "<end name='end' /> " + "</workflow-app>";

        writeToFile(appXml, workflowUri);
        conf.set(OozieClient.APP_PATH, workflowUri);
        conf.set(OozieClient.USER_NAME, getTestUser());

        SubmitXCommand sc = new SubmitXCommand(conf);

        try {
            sc.call();
        }
        catch (CommandException ce) {
            fail("Should succeed");
        }
    }

    public void testAppPathIsFile2() throws Exception {
        Configuration conf = new XConfiguration();
        String workflowUri = getTestCaseFileUri("workflow.xml");
        String appXml = "<workflow-app xmlns='uri:oozie:workflow:0.1' name='map-reduce-wf'> " + "<start to='end' /> "
                + "<end name='end' /> " + "</workflow-app>";

        writeToFile(appXml, workflowUri);
        conf.set(OozieClient.APP_PATH, workflowUri);
        conf.set(OozieClient.USER_NAME, getTestUser());

        SubmitXCommand sc = new SubmitXCommand(conf);

        try {
            sc.call();
        }
        catch (CommandException ce) {
            fail("Should succeed");
        }
    }

    public void testAppPathIsFileNegative() throws Exception {
        Configuration conf = new XConfiguration();
        String workflowUri = getTestCaseFileUri("test.xml");
        String appXml = "<workflow-app xmlns='uri:oozie:workflow:0.1' name='map-reduce-wf'> " + "<start to='end' /> "
                + "<end name='end' /> " + "</workflow-app>";

        writeToFile(appXml, workflowUri);
        conf.set(OozieClient.APP_PATH, getTestCaseFileUri("does_not_exist.xml"));
        conf.set(OozieClient.USER_NAME, getTestUser());
        SubmitXCommand sc = new SubmitXCommand(conf);

        try {
            sc.call();
            fail("should fail");
        }
        catch (CommandException ce) {

        }
    }

    public void testDryrunValidXml() throws Exception {
        Configuration conf = new XConfiguration();
        String workflowUri = getTestCaseFileUri("workflow.xml");
        String appXml = IOUtils.getResourceAsString("wf-schema-valid-global.xml", -1);
        writeToFile(appXml, workflowUri);
        conf.set(OozieClient.APP_PATH, workflowUri);
        conf.set(OozieClient.USER_NAME, getTestUser());
        SubmitXCommand sc = new SubmitXCommand(true, conf);
        try {
            sc.call();
            fail("Should fail with variable cannot be resolved");
        } catch (CommandException ex) {
            assertEquals(ErrorCode.E0803, ex.getErrorCode());
            assertEquals("E0803: IO error, variable [foo] cannot be resolved", ex.getMessage());
        }
        conf.set("foo", "foo");
        sc = new SubmitXCommand(true, conf);
        assertEquals("OK", sc.call());
    }

    public void testDryrunInvalidXml() throws Exception {
        Configuration conf = new XConfiguration();
        String workflowUri = getTestCaseFileUri("workflow.xml");
        String appXml = IOUtils.getResourceAsString("wf-loop1-invalid.xml", -1);
        writeToFile(appXml, workflowUri);
        conf.set(OozieClient.APP_PATH, workflowUri);
        conf.set(OozieClient.USER_NAME, getTestUser());
        SubmitXCommand sc = new SubmitXCommand(true, conf);
        try {
            sc.call();
            fail("Should have gotten E0707 because the XML has a loop");
        } catch (CommandException ce) {
            assertEquals(ErrorCode.E0707, ce.getErrorCode());
            assertEquals("E0707: Loop detected at parsing, node [a], path [:start:->a->c->a]", ce.getMessage());
        }

        conf = new XConfiguration();
        appXml = IOUtils.getResourceAsString("wf-transition-invalid.xml", -1);
        writeToFile(appXml, workflowUri);
        conf.set(OozieClient.APP_PATH, workflowUri);
        conf.set(OozieClient.USER_NAME, getTestUser());
        sc = new SubmitXCommand(true, conf);
        try {
            sc.call();
            fail("Should have gotten E0708 because the XML has an invalid transition");
        } catch (CommandException ce) {
            assertEquals(ErrorCode.E0708, ce.getErrorCode());
            assertEquals("E0708: Invalid transition, node [c] transition [f]", ce.getMessage());
        }

        conf = new XConfiguration();
        appXml = IOUtils.getResourceAsString("wf-schema-invalid.xml", -1);
        writeToFile(appXml, workflowUri);
        conf.set(OozieClient.APP_PATH, workflowUri);
        conf.set(OozieClient.USER_NAME, getTestUser());
        sc = new SubmitXCommand(true, conf);
        try {
            sc.call();
            fail("Should have gotten E0701 because the XML has an invalid element");
        } catch (CommandException ce) {
            assertEquals(ErrorCode.E0701, ce.getErrorCode());
            assertTrue(ce.getMessage().contains("XML schema error"));
            assertTrue(ce.getMessage().contains("starting with element 'xstart'"));
            assertTrue(ce.getMessage().contains("'{\"uri:oozie:workflow:0.1\":start}' is expected"));
        }
    }

    // It should not store Hadoop properties
    public void testProtoConfStorage() throws Exception {
        final OozieClient wfClient = LocalOozie.getClient();

        Configuration conf = new XConfiguration();
        String workflowUri = getTestCaseFileUri("workflow.xml");
        String appXml = "<workflow-app xmlns='uri:oozie:workflow:0.1' name='${appName}-foo'> " + "<start to='end' /> "
                + "<end name='end' /> " + "</workflow-app>";

        writeToFile(appXml, workflowUri);
        conf.set(OozieClient.APP_PATH, workflowUri);
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("appName", "var-app-name");
        SubmitXCommand sc = new SubmitXCommand(conf);
        final String jobId = sc.call();

        waitFor(15 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return wfClient.getJobInfo(jobId).getStatus() == WorkflowJob.Status.PREP;
            }
        });
        WorkflowJobBean wf = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW, jobId);
        XConfiguration protoConf = new XConfiguration(new StringReader(wf.getProtoActionConf()));
        // Hadoop 2 adds "mapreduce.job.user.name" in addition to "user.name"
        if (protoConf.get("mapreduce.job.user.name") != null) {
            assertEquals(3, protoConf.size());
        } else {
            assertEquals(2, protoConf.size());
        }
        assertNull(protoConf.get(WorkflowAppService.APP_LIB_PATH_LIST));

        new File(getTestCaseDir() + "/lib").mkdirs();
        File.createTempFile("parentLibrary", ".jar", new File(getTestCaseDir() + "/lib"));
        conf.set(OozieClient.APP_PATH, workflowUri);
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("appName", "var-app-name");
        sc = new SubmitXCommand(conf);
        final String jobId1 = sc.call();

        waitFor(15 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return wfClient.getJobInfo(jobId1).getStatus() == WorkflowJob.Status.PREP;
            }
        });
        wf = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW, jobId1);
        protoConf = new XConfiguration(new StringReader(wf.getProtoActionConf()));
        // Hadoop 2 adds "mapreduce.job.user.name" in addition to "user.name"
        if (protoConf.get("mapreduce.job.user.name") != null) {
            assertEquals(4, protoConf.size());
        } else {
            assertEquals(3, protoConf.size());
        }
        assertNotNull(protoConf.get(WorkflowAppService.APP_LIB_PATH_LIST));
    }

    public void testWFConfigDefaultVarResolve() throws Exception {
        final OozieClient wfClient = LocalOozie.getClient();
        OutputStream os = new FileOutputStream(getTestCaseDir() + "/config-default.xml");
        XConfiguration defaultConf = new XConfiguration();
        defaultConf.set("outputDir", "default-output-dir");
        defaultConf.set("foo.bar", "default-foo-bar");
        defaultConf.set("foobarRef", "${foo.bar}");
        defaultConf.set("key", "default_value");
        defaultConf.set("should_resolve", "${should.resolve}");
        defaultConf.set("mixed", "${nameNode}/${outputDir}");
        defaultConf.writeXml(os);
        os.close();

        String workflowUri = getTestCaseFileUri("workflow.xml");
        String actionXml = "<map-reduce>"
                + "<job-tracker>${jobTracker}</job-tracker>"
                + "<name-node>${nameNode}</name-node>"
                + "        <prepare>"
                + "          <delete path=\"${nameNode}/user/${wf:user()}/mr/${outputDir}\"/>"
                + "        </prepare>"
                + "        <configuration>"
                + "          <property><name>bb</name><value>BB</value></property>"
                + "          <property><name>cc</name><value>from_action</value></property>"
                + "        </configuration>"
                + "      </map-reduce>";
        String wfXml = "<workflow-app xmlns=\"uri:oozie:workflow:0.5\" name=\"map-reduce-wf\">"
                + "    <start to=\"mr-node\"/>"
                + "    <action name=\"mr-node\">"
                + actionXml
                + "    <ok to=\"end\"/>"
                + "    <error to=\"fail\"/>"
                + "</action>"
                + "<kill name=\"fail\">"
                + "    <message>Map/Reduce failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>"
                + "</kill>"
                + "<end name=\"end\"/>"
                + "</workflow-app>";

        writeToFile(wfXml, workflowUri);
        Configuration conf = new XConfiguration();
        conf.set("nameNode", getNameNodeUri());
        conf.set("jobTracker", getJobTrackerUri());
        conf.set("foobarRef", "foobarRef");
        conf.set("key", "job_prop_value");
        conf.set(OozieClient.APP_PATH, workflowUri);
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("should.resolve", "resolved");
        SubmitXCommand sc = new SubmitXCommand(conf);
        final String jobId = sc.call();
        new StartXCommand(jobId).call();
        waitFor(15 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return wfClient.getJobInfo(jobId).getStatus() == WorkflowJob.Status.KILLED;
            }
        });
        String actionId = jobId + "@mr-node";
        WorkflowActionBean action = WorkflowActionQueryExecutor.getInstance().get(WorkflowActionQueryExecutor.WorkflowActionQuery
                .GET_ACTION, actionId);
        Element eAction = XmlUtils.parseXml(action.getConf());
        Element eConf = eAction.getChild("configuration", eAction.getNamespace());
        Configuration actionConf = new XConfiguration(new StringReader(XmlUtils.prettyPrint(eConf).toString()));
        assertEquals("default-output-dir", actionConf.get("outputDir"));
        assertEquals("BB", actionConf.get("bb"));
        assertEquals("from_action", actionConf.get("cc"));
        assertEquals("resolved", actionConf.get("should_resolve"));
        assertEquals("default-foo-bar", actionConf.get("foo.bar"));
        assertEquals("default-foo-bar", actionConf.get("foobarRef"));
        assertEquals("default_value", actionConf.get("key"));
        assertEquals(getNameNodeUri()+"/default-output-dir", actionConf.get("mixed"));
    }


    private void writeToFile(String appXml, String appPath) throws IOException {
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
}
