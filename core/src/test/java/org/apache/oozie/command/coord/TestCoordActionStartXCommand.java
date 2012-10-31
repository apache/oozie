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
package org.apache.oozie.command.coord;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.hadoop.MapperReducerForTest;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.CoordinatorAction.Status;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.CoordActionGetForStartJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionInsertJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionsGetForJobJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;

public class TestCoordActionStartXCommand extends XDataTestCase {
    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        cleanUpDBTables();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    /**
     * Test the working of CoordActionStartXCommand with standard coord action
     * XML and then check the action
     *
     * @throws IOException
     * @throws JPAExecutorException
     * @throws CommandException
     */
    public void testActionStartCommand() throws IOException, JPAExecutorException, CommandException {
        String actionId = new Date().getTime() + "-COORD-ActionStartCommand-C@1";
        addRecordToActionTable(actionId, 1, null);
        new CoordActionStartXCommand(actionId, "me", "mytoken", "myjob").call();
        checkCoordAction(actionId);
    }

    /**
     * Coord action XML contains non-supported parameterized action name and
     * test that CoordActionStartXCommand stores error code and error message in
     * action's table during error handling
     *
     * @throws IOException
     * @throws JPAExecutorException
     * @throws CommandException
     */
    public void testActionStartWithErrorReported() throws IOException, JPAExecutorException, CommandException {
        String actionId = new Date().getTime() + "-COORD-ActionStartCommand-C@1";
        String wfApp = "<start to='${someParam}' />";
        addRecordToActionTable(actionId, 1, wfApp);
        new CoordActionStartXCommand(actionId, "me", "mytoken", "myjob").call();
        final JPAService jpaService = Services.get().get(JPAService.class);
        CoordinatorActionBean action = jpaService.execute(new CoordActionGetForStartJPAExecutor(actionId));
        if (action.getStatus() == CoordinatorAction.Status.SUBMITTED) {
            fail("Expected status was FAILED due to incorrect XML element");
        }
        assertEquals(action.getErrorCode(), ErrorCode.E0701.toString());
        assertTrue(action.getErrorMessage().contains(
                "XML schema error, cvc-pattern-valid: Value '${someParam}' "
                        + "is not facet-valid with respect to pattern"));
    }

    /**
     * Test : configuration contains url string which should be escaped before put into the evaluator.
     * If not escape, the error 'SAXParseException' will be thrown and workflow job will not be submitted.
     *
     * @throws Exception
     */
    public void testActionStartWithEscapeStrings() throws Exception {
        Date start = DateUtils.parseDateOozieTZ("2009-12-15T01:00Z");
        Date end = DateUtils.parseDateOozieTZ("2009-12-16T01:00Z");
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, false,
                false, 1);

        CoordinatorActionBean action = addRecordToCoordActionTable(coordJob.getId(), 1,
                CoordinatorAction.Status.SUBMITTED, "coord-action-start-escape-strings.xml", 0);

        String actionId = action.getId();
        new CoordActionStartXCommand(actionId, getTestUser(), "undef","myjob").call();

        final JPAService jpaService = Services.get().get(JPAService.class);
        action = jpaService.execute(new CoordActionGetJPAExecutor(actionId));

        if (action.getStatus() == CoordinatorAction.Status.SUBMITTED) {
            fail("CoordActionStartCommand didn't work because the status for action id" + actionId + " is :"
                    + action.getStatus() + " expected to be NOT SUBMITTED (i.e. RUNNING)");
        }

        final String wfId = action.getExternalId();

        waitFor(3000, new Predicate() {
            public boolean evaluate() throws Exception {
                List<WorkflowActionBean> wfActions = jpaService.execute(new WorkflowActionsGetForJobJPAExecutor(wfId));
                return wfActions.size() > 0;
            }
        });
        List<WorkflowActionBean> wfActions = jpaService.execute(new WorkflowActionsGetForJobJPAExecutor(wfId));
        assertTrue(wfActions.size() > 0);

        final String wfActionId = wfActions.get(0).getId();
        waitFor(20 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                WorkflowActionBean wfAction = jpaService.execute(new WorkflowActionGetJPAExecutor(wfActionId));
                return wfAction.getExternalId() != null;
            }
        });

        WorkflowActionBean wfAction = jpaService.execute(new WorkflowActionGetJPAExecutor(wfActionId));
        assertNotNull(wfAction.getExternalId());
    }

    @Override
    protected Configuration getCoordConf(Path coordAppPath) throws IOException {
        Path wfAppPath = new Path(getFsTestCaseDir(), "app");
        FileSystem fs = getFileSystem();
        fs.mkdirs(new Path(wfAppPath, "lib"));
        File jarFile = IOUtils.createJar(new File(getTestCaseDir()), "test.jar", MapperReducerForTest.class);
        InputStream is = new FileInputStream(jarFile);
        OutputStream os = fs.create(new Path(wfAppPath, "lib/test.jar"));
        IOUtils.copyStream(is, os);
        Path input = new Path(wfAppPath, "input");
        fs.mkdirs(input);
        Writer writer = new OutputStreamWriter(fs.create(new Path(input, "test.txt")));
        writer.write("hello");
        writer.close();

        final String APP1 = "<workflow-app xmlns='uri:oozie:workflow:0.1' name='app'>" +
                "<start to='end'/>" +
                "<end name='end'/>" +
                "</workflow-app>";
        String subWorkflowAppPath = new Path(wfAppPath, "subwf").toString();
        fs.mkdirs(new Path(wfAppPath, "subwf"));
        Writer writer2 = new OutputStreamWriter(fs.create(new Path(subWorkflowAppPath, "workflow.xml")));
        writer2.write(APP1);
        writer2.close();

        Reader reader = IOUtils.getResourceAsReader("wf-url-template.xml", -1);
        Writer writer1 = new OutputStreamWriter(fs.create(new Path(wfAppPath + "/workflow.xml")));
        IOUtils.copyCharStream(reader, writer1);

        Properties jobConf = new Properties();
        jobConf.setProperty(OozieClient.COORDINATOR_APP_PATH, coordAppPath.toString());
        jobConf.setProperty(OozieClient.USER_NAME, getTestUser());
        jobConf.setProperty(OozieClient.GROUP_NAME, getTestGroup());
        jobConf.setProperty("myJobTracker", getJobTrackerUri());
        jobConf.setProperty("myNameNode", getNameNodeUri());
        jobConf.setProperty("wfAppPath", wfAppPath.toString()+ File.separator + "workflow.xml");
        jobConf.setProperty("mrclass", MapperReducerForTest.class.getName());
        jobConf.setProperty("delPath", wfAppPath.toString() + "/output");
        jobConf.setProperty("subWfApp", wfAppPath.toString() + "/subwf/workflow.xml");


        return new XConfiguration(jobConf);
    }

    private void addRecordToActionTable(String actionId, int actionNum, String wfParam)
            throws IOException, JPAExecutorException {
        final JPAService jpaService = Services.get().get(JPAService.class);
        CoordinatorActionBean action = new CoordinatorActionBean();
        action.setJobId(actionId);
        action.setId(actionId);
        action.setActionNumber(actionNum);
        action.setNominalTime(new Date());
        action.setStatus(Status.SUBMITTED);
        String appPath = getTestCaseDir()+"/coord/no-op/";
        String actionXml = "<coordinator-app xmlns='uri:oozie:coordinator:0.2' xmlns:sla='uri:oozie:sla:0.1' name='NAME' " +
        		"frequency=\"1\" start='2009-02-01T01:00Z' end='2009-02-03T23:59Z' timezone='UTC' freq_timeunit='DAY' " +
        		"end_of_duration='NONE'  instance-number=\"1\" action-nominal-time=\"2009-02-01T01:00Z\">";
        actionXml += "<controls>";
        actionXml += "<timeout>10</timeout>";
        actionXml += "<concurrency>2</concurrency>";
        actionXml += "<execution>LIFO</execution>";
        actionXml += "</controls>";
        actionXml += "<input-events>";
        actionXml += "<data-in name='A' dataset='a'>";
        actionXml += "<dataset name='a' frequency='7' initial-instance='2009-02-01T01:00Z' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        actionXml += "<uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template>";
        actionXml += "</dataset>";
        actionXml += "<instance>${coord:latest(0)}</instance>";
        actionXml += "</data-in>";
        actionXml += "</input-events>";
        actionXml += "<output-events>";
        actionXml += "<data-out name='LOCAL_A' dataset='local_a'>";
        actionXml += "<dataset name='local_a' frequency='7' initial-instance='2009-02-01T01:00Z' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        actionXml += "<uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template>";
        actionXml += "</dataset>";
        actionXml += "<instance>${coord:current(-1)}</instance>";
        actionXml += "</data-out>";
        actionXml += "</output-events>";
        actionXml += "<action>";
        actionXml += "<workflow>";
        actionXml += "<app-path>file://" + appPath + "</app-path>";
        actionXml += "<configuration>";
        actionXml += "<property>";
        actionXml += "<name>inputA</name>";
        actionXml += "<value>file:///tmp/coord//US/2009/02/01</value>";
        actionXml += "</property>";
        actionXml += "<property>";
        actionXml += "<name>inputB</name>";
        actionXml += "<value>file:///tmp/coord//US/2009/02/01</value>";
        actionXml += "</property>";
        actionXml += "</configuration>";
        actionXml += "</workflow>";
        String slaXml = " <sla:info xmlns:sla='uri:oozie:sla:0.1'>" + " <sla:app-name>test-app</sla:app-name>"
                + " <sla:nominal-time>2009-03-06T10:00Z</sla:nominal-time>" + " <sla:should-start>5</sla:should-start>"
                + " <sla:should-end>120</sla:should-end>"
                + " <sla:notification-msg>Notifying User for nominal time : 2009-03-06T10:00Z </sla:notification-msg>"
                + " <sla:alert-contact>abc@example.com</sla:alert-contact>"
                + " <sla:dev-contact>abc@example.com</sla:dev-contact>"
                + " <sla:qa-contact>abc@example.com</sla:qa-contact>" + " <sla:se-contact>abc@example.com</sla:se-contact>"
                + "</sla:info>";
        actionXml += slaXml;
        actionXml += "</action>";
        actionXml += "</coordinator-app>";
        action.setActionXml(actionXml);
        action.setSlaXml(slaXml);

        String createdConf = "<configuration> ";
        createdConf += "<property> <name>execution_order</name> <value>LIFO</value> </property>";
        createdConf += "<property> <name>user.name</name> <value>" + getTestUser() + "</value> </property>";
        createdConf += "<property> <name>group.name</name> <value>other</value> </property>";
        createdConf += "<property> <name>app-path</name> " + "<value>file://" + appPath + "/</value> </property>";
        createdConf += "<property> <name>jobTracker</name> ";
        createdConf += "<value>localhost:9001</value></property>";
        createdConf += "<property> <name>nameNode</name> <value>hdfs://localhost:9000</value></property>";
        createdConf += "<property> <name>queueName</name> <value>default</value></property>";
        createdConf += "</configuration> ";

        action.setCreatedConf(createdConf);
        jpaService.execute(new CoordActionInsertJPAExecutor(action));
        String content = "<workflow-app xmlns='uri:oozie:workflow:0.2'  xmlns:sla='uri:oozie:sla:0.1' name='no-op-wf'>";
        if (wfParam != null) {
            if (!wfParam.isEmpty()) {
                content += wfParam;
            }
        }
        else {
            content += "<start to='end' />";
        }
        String slaXml2 = " <sla:info>"
                // + " <sla:client-id>axonite-blue</sla:client-id>"
                + " <sla:app-name>test-app</sla:app-name>" + " <sla:nominal-time>2009-03-06T10:00Z</sla:nominal-time>"
                + " <sla:should-start>5</sla:should-start>" + " <sla:should-end>${2 * HOURS}</sla:should-end>"
                + " <sla:notification-msg>Notifying User for nominal time : 2009-03-06T10:00Z </sla:notification-msg>"
                + " <sla:alert-contact>abc@example.com</sla:alert-contact>"
                + " <sla:dev-contact>abc@example.com</sla:dev-contact>"
                + " <sla:qa-contact>abc@example.com</sla:qa-contact>" + " <sla:se-contact>abc@example.com</sla:se-contact>"
                + "</sla:info>";
        content += "<end name='end' />" + slaXml2 + "</workflow-app>";
        writeToFile(content, appPath);
    }

    private void checkCoordAction(String actionId) {
        try {
            final JPAService jpaService = Services.get().get(JPAService.class);
            CoordinatorActionBean action = jpaService.execute(new CoordActionGetJPAExecutor(actionId));
            if (action.getStatus() == CoordinatorAction.Status.SUBMITTED) {
                fail("CoordActionStartCommand didn't work because the status for action id" + actionId + " is :"
                        + action.getStatus() + " expected to be NOT SUBMITTED (i.e. RUNNING)");
            }
            if (action.getExternalId() != null) {
                WorkflowJobBean wfJob = jpaService.execute(new WorkflowJobGetJPAExecutor(action.getExternalId()));
                assertEquals(wfJob.getParentId(), action.getId());
            }
        }
        catch (JPAExecutorException je) {
            fail("Action ID " + actionId + " was not stored properly in db");
        }
    }

    private void writeToFile(String content, String appPath) throws IOException {
        createDir(appPath);
        File wf = new File(appPath + "/workflow.xml");
        PrintWriter out = null;
        try {
            out = new PrintWriter(new FileWriter(wf));
            out.println(content);
        }
        catch (IOException iex) {
            iex.printStackTrace();
            throw iex;
        }
        finally {
            if (out != null) {
                out.close();
            }
        }

    }

    private void createDir(String dir) {
        Process pr;
        try {
            pr = Runtime.getRuntime().exec("mkdir -p " + dir + "/_SUCCESS");
            pr.waitFor();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
