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
import java.io.Writer;
import java.util.Date;

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
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.XCommand;
import org.apache.oozie.command.wf.ActionXCommand.ActionExecutorContext;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionInsertJPAExecutor;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.InstrumentationService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.workflow.WorkflowInstance;

public class TestActionCheckXCommand extends XDataTestCase {
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
     * Test : verify the PreconditionException is thrown when actionCheckDelay > 0
     *
     * @throws Exception
     */
    public void testActionCheckPreCondition1() throws Exception {
        Instrumentation inst = Services.get().get(InstrumentationService.class).get();

        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action = this.addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.PREP);

        ActionCheckXCommand checkCmd = new ActionCheckXCommand(action.getId(), 10);

        Long counterVal = new Long(0);

        try {
            counterVal = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(checkCmd.getName() + ".preconditionfailed").getValue();
        } catch (NullPointerException e){
            //counter might be null
        }

        assertEquals(new Long(0), new Long(counterVal));

        checkCmd.call();

        //precondition failed because of actionCheckDelay > 0
        counterVal = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(checkCmd.getName() + ".preconditionfailed").getValue();
        assertEquals(new Long(1), new Long(counterVal));
    }

    /**
     * Test : verify the PreconditionException is thrown when pending = false
     *
     * @throws Exception
     */
    public void testActionCheckPreCondition2() throws Exception {
        Instrumentation inst = Services.get().get(InstrumentationService.class).get();

        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action = super.addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.RUNNING);

        ActionCheckXCommand checkCmd = new ActionCheckXCommand(action.getId());

        Long counterVal = new Long(0);

        try {
            counterVal = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(checkCmd.getName() + ".preconditionfailed").getValue();
        } catch (NullPointerException e){
            //counter might be null
        }

        assertEquals(new Long(0), new Long(counterVal));

        checkCmd.call();

        //precondition failed because of pending = false
        counterVal = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(checkCmd.getName() + ".preconditionfailed").getValue();
        assertEquals(new Long(1), new Long(counterVal));
    }

    /**
     * Test : verify the PreconditionException is thrown when action != RUNNING
     *
     * @throws Exception
     */
    public void testActionCheckPreCondition3() throws Exception {
        Instrumentation inst = Services.get().get(InstrumentationService.class).get();

        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action = this.addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.PREP);

        ActionCheckXCommand checkCmd = new ActionCheckXCommand(action.getId());

        Long counterVal = new Long(0);

        try{
            counterVal = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(checkCmd.getName() + ".preconditionfailed").getValue();
        } catch (NullPointerException e){
            //counter might be null
        }

        assertEquals(new Long(0), new Long(counterVal));

        checkCmd.call();

        //precondition failed because of action != RUNNING
        counterVal = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(checkCmd.getName() + ".preconditionfailed").getValue();
        assertEquals(new Long(1), new Long(counterVal));
    }

    /**
     * Test : verify the PreconditionException is thrown when job != RUNNING
     *
     * @throws Exception
     */
    public void testActionCheckPreCondition4() throws Exception {
        Instrumentation inst = Services.get().get(InstrumentationService.class).get();

        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.FAILED, WorkflowInstance.Status.FAILED);
        WorkflowActionBean action = this.addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.PREP);

        ActionCheckXCommand checkCmd = new ActionCheckXCommand(action.getId());

        Long counterVal = new Long(0);

        try {
            counterVal = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(checkCmd.getName() + ".preconditionfailed").getValue();
        } catch (NullPointerException e){
            //counter might be null
        }

        assertEquals(new Long(0), new Long(counterVal));

        checkCmd.call();

        //precondition failed because of job != RUNNING
        counterVal = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(checkCmd.getName() + ".preconditionfailed").getValue();
        assertEquals(new Long(1), new Long(counterVal));
    }

    @SuppressWarnings("deprecation")
    public void testActionCheck() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action = this.addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.PREP);
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(action.getId());

        new ActionStartXCommand(action.getId(), "map-reduce").call();
        action = jpaService.execute(wfActionGetCmd);

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

        new ActionCheckXCommand(action.getId()).call();
        action = jpaService.execute(wfActionGetCmd);
        String mapperId = action.getExternalId();

        assertFalse(launcherId.equals(mapperId));

        final RunningJob mrJob = jobClient.getJob(JobID.forName(mapperId));

        waitFor(120 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return mrJob.isComplete();
            }
        });
        assertTrue(mrJob.isSuccessful());

        new ActionCheckXCommand(action.getId()).call();
        action = jpaService.execute(wfActionGetCmd);

        assertEquals("SUCCEEDED", action.getExternalStatus());

    }

    @Override
    protected WorkflowActionBean addRecordToWfActionTable(String wfId, String actionName, WorkflowAction.Status status) throws Exception {
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

    protected WorkflowActionBean createWorkflowActionSetPending(String wfId, WorkflowAction.Status status) throws Exception {
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

        String actionXml = "<map-reduce>" +
        "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
        "<name-node>" + getNameNodeUri() + "</name-node>" +
        "<configuration>" +
        "<property><name>mapred.mapper.class</name><value>" + MapperReducerForTest.class.getName() +
        "</value></property>" +
        "<property><name>mapred.reducer.class</name><value>" + MapperReducerForTest.class.getName() +
        "</value></property>" +
        "<property><name>mapred.input.dir</name><value>"+inputDir.toString()+"</value></property>" +
        "<property><name>mapred.output.dir</name><value>"+outputDir.toString()+"</value></property>" +
        "</configuration>" +
        "</map-reduce>";
        action.setConf(actionXml);

        return action;
    }

}
