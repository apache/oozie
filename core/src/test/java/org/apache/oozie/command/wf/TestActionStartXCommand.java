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

import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.Writer;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.hadoop.LauncherMapper;
import org.apache.oozie.action.hadoop.MapReduceActionExecutor;
import org.apache.oozie.action.hadoop.MapperReducerForTest;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.XCommand;
import org.apache.oozie.command.wf.ActionXCommand.ActionExecutorContext;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionInsertJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobInsertJPAExecutor;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.InstrumentationService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.lite.EndNodeDef;
import org.apache.oozie.workflow.lite.LiteWorkflowApp;
import org.apache.oozie.workflow.lite.StartNodeDef;
import org.jdom.Element;
import org.jdom.Namespace;

public class TestActionStartXCommand extends XDataTestCase {
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
     * Test : verify the PreconditionException is thrown when pending = true and action = PREP and job != RUNNING
     *
     * @throws Exception
     */
    public void testActionStartPreCondition1() throws Exception {
        Instrumentation inst = Services.get().get(InstrumentationService.class).get();

        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.FAILED, WorkflowInstance.Status.FAILED);
        WorkflowActionBean action = this.addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.PREP);

        assertNull(inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP));
        ActionStartXCommand startCmd = new ActionStartXCommand(action.getId(), "map-reduce");
        startCmd.call();

        // precondition failed because of pending = true and action = PREP and
        // job != RUNNING
        Long counterVal = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(
                startCmd.getName() + ".preconditionfailed").getValue();
        assertEquals(new Long(1), new Long(counterVal));
    }

    /**
     * Test : verify the PreconditionException is thrown when pending = true and action = START_RETRY and job != RUNNING
     *
     * @throws Exception
     */
    public void testActionStartPreCondition2() throws Exception {
        Instrumentation inst = Services.get().get(InstrumentationService.class).get();

        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.FAILED, WorkflowInstance.Status.FAILED);
        WorkflowActionBean action = this.addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.START_RETRY);

        assertNull(inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP));
        ActionStartXCommand startCmd = new ActionStartXCommand(action.getId(), "map-reduce");
        startCmd.call();

        // precondition failed because of pending = true and action =
        // START_RETRY and job != RUNNING
        Long counterVal = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(
                startCmd.getName() + ".preconditionfailed").getValue();
        assertEquals(new Long(1), new Long(counterVal));
    }

    /**
     * Test : verify the PreconditionException is thrown when pending = false
     *
     * @throws Exception
     */
    public void testActionStartPreCondition3() throws Exception {
        Instrumentation inst = Services.get().get(InstrumentationService.class).get();

        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action = super.addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.PREP);
        assertFalse(action.getPending());

        assertNull(inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP));
        ActionStartXCommand startCmd = new ActionStartXCommand(action.getId(), "map-reduce");
        startCmd.call();

        // precondition failed because of pending = false
        Long counterVal = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(
                startCmd.getName() + ".preconditionfailed").getValue();
        assertEquals(new Long(1), new Long(counterVal));
    }

    @SuppressWarnings("deprecation")
    public void testActionStart() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action = this.addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.PREP);
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(action.getId());

        new ActionStartXCommand(action.getId(), "map-reduce").call();
        action = jpaService.execute(wfActionGetCmd);
        assertNotNull(action.getExternalId());

        ActionExecutorContext context = new ActionXCommand.ActionExecutorContext(job, action, false, false);
        MapReduceActionExecutor actionExecutor = new MapReduceActionExecutor();
        JobConf conf = actionExecutor.createBaseHadoopConf(context, XmlUtils.parseXml(action.getConf()));
        String user = conf.get("user.name");
        String group = conf.get("group.name");
        JobClient jobClient = Services.get().get(HadoopAccessorService.class).createJobClient(user, conf);

        String launcherId = action.getExternalId();

        final RunningJob launcherJob = jobClient.getJob(JobID.forName(launcherId));

        waitFor(120 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return launcherJob.isComplete();
            }
        });
        assertTrue(launcherJob.isSuccessful());
        assertTrue(LauncherMapper.hasIdSwap(launcherJob));
    }

    public void testActionReuseWfJobAppPath() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        WorkflowJobBean job = this.addRecordToWfJobTableWithCustomAppPath(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action = this.addRecordToWfActionTableWithAppPathConfig(job.getId(), "1", WorkflowAction.Status.PREP);
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(action.getId());

        new ActionStartXCommand(action.getId(), "map-reduce").call();
        action = jpaService.execute(wfActionGetCmd);
        assertNotNull(action.getExternalId());

        Element actionXml = XmlUtils.parseXml(action.getConf());
        Namespace ns = actionXml.getNamespace();
        Element configElem = actionXml.getChild("configuration", ns);
        String strConf = XmlUtils.prettyPrint(configElem).toString();
        XConfiguration inlineConf = new XConfiguration(new StringReader(strConf));
        String workDir = inlineConf.get("work.dir", null);
        assertNotNull(workDir);
        assertFalse(workDir.contains("workflow.xml"));
    }

    /**
     * Test workflow action with CDATA section and string configuration that needs to be escaped can be submitted without
     * throwing exceptions.
     * <p/>
     * Escaped string needs to be 'escaped' before converting to XML Document, otherwise,
     * exception will be thrown.
     * @see org.apache.oozie.DagELFunctions#configureEvaluator(org.apache.oozie.util.ELEvaluator.ELEvaluator evaluator, org.apache.oozie.WorkflowJobBean, org.apache.oozie.WorkflowActionBean)
     *
     * @throws Exception thrown if failed to execute test case
     */
    public void testActionWithEscapedStringAndCDATA() throws Exception {
        // create workflow job and action beans with escaped parameters and CDATA value
        JPAService jpaService = Services.get().get(JPAService.class);
        WorkflowJobBean job = this.addRecordToWfJobTableWithEscapedStringAndCDATA(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action = this.addRecordToWfActionTableWithEscapedStringAndCDATA(job.getId(), WorkflowAction.Status.PREP);
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(action.getId());

        // start workflow action
        new ActionStartXCommand(action.getId(), "map-reduce").call();
        action = jpaService.execute(wfActionGetCmd);
        assertNotNull(action.getExternalId());

        ActionExecutorContext context = new ActionXCommand.ActionExecutorContext(job, action, false, false);
        MapReduceActionExecutor actionExecutor = new MapReduceActionExecutor();
        JobConf conf = actionExecutor.createBaseHadoopConf(context, XmlUtils.parseXml(action.getConf()));
        String user = conf.get("user.name");
        String group = conf.get("group.name");
        JobClient jobClient = Services.get().get(HadoopAccessorService.class).createJobClient(user, conf);

        String launcherId = action.getExternalId();

        // retrieve launcher job
        final RunningJob launcherJob = jobClient.getJob(JobID.forName(launcherId));

        // time out after 120 seconds unless launcher job succeeds
        waitFor(240 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return launcherJob.isComplete();
            }
        });
        // check if launcher job succeeds
        assertTrue(launcherJob.isSuccessful());
        assertTrue(LauncherMapper.hasIdSwap(launcherJob));
    }

    /**
     * Create workflow job with custom application path
     *
     * @param jobStatus workflow job status
     * @param instanceStatus workflow instance status
     * @return workflow job bean
     * @throws Exception thrown if failed to create workflow job
     */
    protected WorkflowJobBean addRecordToWfJobTableWithCustomAppPath(WorkflowJob.Status jobStatus, WorkflowInstance.Status instanceStatus)
    throws Exception {
        WorkflowApp app = new LiteWorkflowApp("testApp", "<workflow-app/>", new StartNodeDef("end"))
        .addNode(new EndNodeDef("end"));
        Configuration conf = new Configuration();
        Path appUri = getAppPath();
        conf.set(OozieClient.APP_PATH, appUri.toString());
        conf.set(OozieClient.LOG_TOKEN, "testToken");
        conf.set(OozieClient.USER_NAME, getTestUser());

        WorkflowJobBean wfBean = createWorkflow(app, conf, "auth", jobStatus, instanceStatus);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            WorkflowJobInsertJPAExecutor wfInsertCmd = new WorkflowJobInsertJPAExecutor(wfBean);
            jpaService.execute(wfInsertCmd);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test wf job record to table");
            throw je;
        }
        return wfBean;
    }

    /**
     * Create workflow action with custom action configuration
     *
     * @param wfId workflow id
     * @param actionName action name
     * @param status workflow action status
     * @return workflow action bean
     * @throws Exception thrown if failed to create workflow action
     */
    protected WorkflowActionBean addRecordToWfActionTableWithAppPathConfig(String wfId, String actionName, WorkflowAction.Status status)
            throws Exception {
        WorkflowActionBean action = createWorkflowActionWithAppPathConfig(wfId, status);
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            WorkflowActionInsertJPAExecutor actionInsertCmd = new WorkflowActionInsertJPAExecutor(action);
            jpaService.execute(actionInsertCmd);
        }
        catch (JPAExecutorException ce) {
            ce.printStackTrace();
            fail("Unable to insert the test wf action record to table");
            throw ce;
        }
        return action;
    }

    /**
     * Create workflow action with custom action configuration
     *
     * @param wfId workflow id
     * @param status workflow action status
     * @return workflow action bean
     * @throws Exception thrown if failed to create workflow action
     */
    protected WorkflowActionBean createWorkflowActionWithAppPathConfig(String wfId, WorkflowAction.Status status)
    throws Exception {
        WorkflowActionBean action = new WorkflowActionBean();
        String actionname = "testAction";
        action.setName(actionname);
        action.setCred("null");
        action.setId(Services.get().get(UUIDService.class).generateChildId(wfId, actionname));
        action.setJobId(wfId);
        action.setType("map-reduce");
        action.setTransition("transition");
        action.setStatus(status);
        action.setStartTime(new Date());
        action.setEndTime(new Date());
        action.setLastCheckTime(new Date());
        action.setPending();

        Path inputDir = new Path(getFsTestCaseDir(), "input");
        Path outputDir = new Path(getFsTestCaseDir(), "output");

        FileSystem fs = getFileSystem();
        Writer w = new OutputStreamWriter(fs.create(new Path(inputDir, "data.txt")));
        w.write("dummy\n");
        w.write("dummy\n");
        w.close();

        String actionXml = "<map-reduce>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
        + getNameNodeUri() + "</name-node>" + "<configuration>"
        + "<property><name>mapred.mapper.class</name><value>" + MapperReducerForTest.class.getName()
        + "</value></property>" +
        "<property><name>mapred.reducer.class</name><value>"
        + MapperReducerForTest.class.getName() + "</value></property>"
        + "<property><name>work.dir</name><value>${wf:appPath()}/sub</value></property>"
        + "<property><name>mapred.input.dir</name><value>" + inputDir.toString() + "</value></property>"
        + "<property><name>mapred.output.dir</name><value>" + outputDir.toString() + "</value></property>"
        + "</configuration>" + "</map-reduce>";
        action.setConf(actionXml);

        return action;
    }


    /* (non-Javadoc)
     * @see org.apache.oozie.test.XDataTestCase#addRecordToWfActionTable(java.lang.String, java.lang.String, org.apache.oozie.client.WorkflowAction.Status)
     */
    @Override
    protected WorkflowActionBean addRecordToWfActionTable(String wfId, String actionName, WorkflowAction.Status status)
            throws Exception {
        WorkflowActionBean action = createWorkflowActionSetPending(wfId, status);
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            WorkflowActionInsertJPAExecutor actionInsertCmd = new WorkflowActionInsertJPAExecutor(action);
            jpaService.execute(actionInsertCmd);
        }
        catch (JPAExecutorException ce) {
            ce.printStackTrace();
            fail("Unable to insert the test wf action record to table");
            throw ce;
        }
        return action;
    }

    /**
     * Create workflow action with pending true
     *
     * @param wfId workflow id
     * @param status workflow action status
     * @return workflow action bean
     * @throws Exception thrown if failed to create workflow action
     */
    protected WorkflowActionBean createWorkflowActionSetPending(String wfId, WorkflowAction.Status status)
            throws Exception {
        WorkflowActionBean action = new WorkflowActionBean();
        String actionname = "testAction";
        action.setName(actionname);
        action.setCred("null");
        action.setId(Services.get().get(UUIDService.class).generateChildId(wfId, actionname));
        action.setJobId(wfId);
        action.setType("map-reduce");
        action.setTransition("transition");
        action.setStatus(status);
        action.setStartTime(new Date());
        action.setEndTime(new Date());
        action.setLastCheckTime(new Date());
        action.setPending();

        Path inputDir = new Path(getFsTestCaseDir(), "input");
        Path outputDir = new Path(getFsTestCaseDir(), "output");

        FileSystem fs = getFileSystem();
        Writer w = new OutputStreamWriter(fs.create(new Path(inputDir, "data.txt")));
        w.write("dummy\n");
        w.write("dummy\n");
        w.close();

        String actionXml = "<map-reduce>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>" + "<configuration>"
                + "<property><name>mapred.mapper.class</name><value>" + MapperReducerForTest.class.getName()
                + "</value></property>" + "<property><name>mapred.reducer.class</name><value>"
                + MapperReducerForTest.class.getName() + "</value></property>"
                + "<property><name>mapred.input.dir</name><value>" + inputDir.toString() + "</value></property>"
                + "<property><name>mapred.output.dir</name><value>" + outputDir.toString() + "</value></property>"
                + "</configuration>" + "</map-reduce>";
        action.setConf(actionXml);

        return action;
    }

    /**
     * Create workflow job with action configuration with CDATA section and escaped string as value in parameter.
     *
     * @param jobStatus workflow job status
     * @param instanceStatus workflow instance status
     * @return workflow job bean
     * @throws Exception thrown if failed to create workflow job
     */
    private WorkflowJobBean addRecordToWfJobTableWithEscapedStringAndCDATA(WorkflowJob.Status jobStatus,
            WorkflowInstance.Status instanceStatus) throws Exception {
        WorkflowApp app = new LiteWorkflowApp("testApp", "<workflow-app/>", new StartNodeDef("end"))
                .addNode(new EndNodeDef("end"));
        Configuration conf = new Configuration();
        Path appUri = new Path(getAppPath(), "workflow.xml");
        conf.set(OozieClient.APP_PATH, appUri.toString());
        conf.set(OozieClient.LOG_TOKEN, "testToken");
        conf.set(OozieClient.USER_NAME, getTestUser());
        // The properties should not be escaped here. It will be escaped when set to configuration.
        conf.set("testAmpSign", "http://test.apache.com/a-webservices?urlSigner=signUrl&namespace=nova.proxy");
        conf.set("testCDATA",
                        "<![CDATA[?redirect=http%3A%2F%2Ftest.apache.com%2Fa-webservices%2Fv1%2FurlSigner%2FsignUrl&amp;namespace=nova.proxy&amp;keyDBHash=Vsy6n_C7K6NG0z4R2eBlKg--]]>");

        WorkflowJobBean wfBean = createWorkflow(app, conf, "auth", jobStatus, instanceStatus);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            WorkflowJobInsertJPAExecutor wfInsertCmd = new WorkflowJobInsertJPAExecutor(wfBean);
            jpaService.execute(wfInsertCmd);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test wf job record to table");
        }
        return wfBean;
    }

    /**
     * Create workflow action with action configuration with CDATA section and escaped string as value in parameter.
     *
     * @param wfId workflow job id
     * @param status action status
     * @return workflow action bean
     * @throws Exception thrown if failed to create workflow action
     */
    private WorkflowActionBean addRecordToWfActionTableWithEscapedStringAndCDATA(String wfId,
            WorkflowAction.Status status) throws Exception {
        WorkflowActionBean action = createWorkflowActionSetPendingWithEscapedStringAndCDATA(wfId, status);
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            WorkflowActionInsertJPAExecutor actionInsertCmd = new WorkflowActionInsertJPAExecutor(action);
            jpaService.execute(actionInsertCmd);
        }
        catch (JPAExecutorException ce) {
            ce.printStackTrace();
            fail("Unable to insert the test wf action record to table");
        }
        return action;
    }

    /**
     * Create workflow action with action configuration with CDATA section and escaped string as value in parameter.
     *
     * @param wfId workflow job id
     * @param status action status
     * @return workflow action bean
     * @throws Exception thrown if failed to create workflow action
     */
    private WorkflowActionBean createWorkflowActionSetPendingWithEscapedStringAndCDATA(String wfId,
            WorkflowAction.Status status) throws Exception {
        WorkflowActionBean action = new WorkflowActionBean();
        String actionname = "testAction";
        action.setName(actionname);
        action.setCred("null");
        action.setId(Services.get().get(UUIDService.class).generateChildId(wfId, actionname));
        action.setJobId(wfId);
        action.setType("map-reduce");
        action.setTransition("transition");
        action.setStatus(status);
        action.setStartTime(new Date());
        action.setEndTime(new Date());
        action.setLastCheckTime(new Date());
        action.setPending();

        Path inputDir = new Path(getFsTestCaseDir(), "input");
        Path outputDir = new Path(getFsTestCaseDir(), "output");

        FileSystem fs = getFileSystem();
        Writer w = new OutputStreamWriter(fs.create(new Path(inputDir, "data.txt")));
        w.write("dummy\n");
        w.write("dummy\n");
        w.close();

        String actionXml = "<map-reduce>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>" + "<configuration>"
                + "<property><name>mapred.mapper.class</name><value>" + MapperReducerForTest.class.getName()
                + "</value></property>" + "<property><name>mapred.reducer.class</name><value>"
                + MapperReducerForTest.class.getName() + "</value></property>"
                + "<property><name>mapred.input.dir</name><value>" + inputDir.toString() + "</value></property>"
                + "<property><name>mapred.output.dir</name><value>" + outputDir.toString() + "</value></property>"
                + "<property><name>test.ampsign</name><value>${testAmpSign}</value></property>"
                + "<property><name>test.cdata</name><value>${testCDATA}</value></property>" + "</configuration>"
                + "</map-reduce>";
        action.setConf(actionXml);

        return action;
    }


}
