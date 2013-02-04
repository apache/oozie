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
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.apache.oozie.service.ActionCheckerService;
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

    public void testActionCheckTransientDuringLauncher() throws Exception {
        // When using YARN, skip this test because it relies on shutting down the job tracker, which isn't used in YARN
        if (createJobConf().get("yarn.resourcemanager.address") != null) {
            return;
        }
        services.destroy();
        // Make the ActionCheckXCommand run more frequently so the test won't take as long
        setSystemProperty("oozie.service.ActionCheckerService.action.check.interval", "10");
        setSystemProperty("oozie.service.ActionCheckerService.action.check.delay", "20");
        // Make the max number of retries lower so the test won't take as long
        final int maxRetries = 2;
        setSystemProperty("oozie.action.retries.max", Integer.toString(maxRetries));
        services = new Services();
        services.init();

        final JPAService jpaService = Services.get().get(JPAService.class);
        WorkflowJobBean job0 = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        final String jobId = job0.getId();
        WorkflowActionBean action0 = this.addRecordToWfActionTable(jobId, "1", WorkflowAction.Status.PREP);
        final String actionId = action0.getId();
        final WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(actionId);

        new ActionStartXCommand(actionId, "map-reduce").call();
        final WorkflowActionBean action1 = jpaService.execute(wfActionGetCmd);
        String originalLauncherId = action1.getExternalId();

        // At this point, the launcher job has started (but not finished)
        // Now, shutdown the job tracker to pretend it has gone down during the launcher job
        executeWhileJobTrackerIsShutdown(new ShutdownJobTrackerExecutable() {
            @Override
            public void execute() throws Exception {
                assertEquals(0, action1.getRetries());
                new ActionCheckXCommand(actionId).call();

                waitFor(30 * 1000, new Predicate() {
                    @Override
                    public boolean evaluate() throws Exception {
                        WorkflowActionBean action1a = jpaService.execute(wfActionGetCmd);
                        return (action1a.getRetries() > 0);
                    }
                });
                waitFor(180 * 1000, new Predicate() {
                    @Override
                    public boolean evaluate() throws Exception {
                        WorkflowActionBean action1a = jpaService.execute(wfActionGetCmd);
                        return (action1a.getRetries() == 0);
                    }
                });
                WorkflowActionBean action1b = jpaService.execute(wfActionGetCmd);
                assertEquals(0, action1b.getRetries());
                assertEquals("START_MANUAL", action1b.getStatusStr());

                WorkflowJobBean job1 = jpaService.execute(new WorkflowJobGetJPAExecutor(jobId));
                assertEquals("SUSPENDED", job1.getStatusStr());

                // At this point, the action has gotten a transient error, even after maxRetries tries so the workflow has been
                // SUSPENDED
            }
        });
        // Now, lets bring the job tracker back up and resume the workflow (which will restart the current action)
        // It should now continue and finish with SUCCEEDED
        new ResumeXCommand(jobId).call();
        WorkflowJobBean job2 = jpaService.execute(new WorkflowJobGetJPAExecutor(jobId));
        assertEquals("RUNNING", job2.getStatusStr());

        ActionExecutorContext context = new ActionXCommand.ActionExecutorContext(job2, action1, false, false);
        WorkflowActionBean action2 = jpaService.execute(wfActionGetCmd);
        MapReduceActionExecutor actionExecutor = new MapReduceActionExecutor();
        JobConf conf = actionExecutor.createBaseHadoopConf(context, XmlUtils.parseXml(action2.getConf()));
        String user = conf.get("user.name");
        JobClient jobClient = Services.get().get(HadoopAccessorService.class).createJobClient(user, conf);

        new ActionCheckXCommand(actionId).call();
        WorkflowActionBean action3 = jpaService.execute(wfActionGetCmd);
        String launcherId = action3.getExternalId();
        assertFalse(originalLauncherId.equals(launcherId));

        final RunningJob launcherJob = jobClient.getJob(JobID.forName(launcherId));

        waitFor(120 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return launcherJob.isComplete();
            }
        });
        assertTrue(launcherJob.isSuccessful());
        assertTrue(LauncherMapper.hasIdSwap(launcherJob));

        new ActionCheckXCommand(actionId).call();
        WorkflowActionBean action4 = jpaService.execute(wfActionGetCmd);
        String mapperId = action4.getExternalId();

        assertFalse(launcherId.equals(mapperId));

        final RunningJob mrJob = jobClient.getJob(JobID.forName(mapperId));

        waitFor(120 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return mrJob.isComplete();
            }
        });
        assertTrue(mrJob.isSuccessful());

        new ActionCheckXCommand(actionId).call();
        WorkflowActionBean action5 = jpaService.execute(wfActionGetCmd);

        assertEquals("SUCCEEDED", action5.getExternalStatus());
    }

    public void testActionCheckTransientDuringMRAction() throws Exception {
        // When using YARN, skip this test because it relies on shutting down the job tracker, which isn't used in YARN
        if (createJobConf().get("yarn.resourcemanager.address") != null) {
            return;
        }
        services.destroy();
        // Make the ActionCheckXCommand run more frequently so the test won't take as long
        setSystemProperty("oozie.service.ActionCheckerService.action.check.interval", "10");
        setSystemProperty("oozie.service.ActionCheckerService.action.check.delay", "20");
        // Make the max number of retries lower so the test won't take as long
        final int maxRetries = 2;
        setSystemProperty("oozie.action.retries.max", Integer.toString(maxRetries));
        services = new Services();
        services.init();

        final JPAService jpaService = Services.get().get(JPAService.class);
        WorkflowJobBean job0 = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        final String jobId = job0.getId();
        WorkflowActionBean action0 = this.addRecordToWfActionTable(jobId, "1", WorkflowAction.Status.PREP);
        final String actionId = action0.getId();
        final WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(actionId);

        new ActionStartXCommand(actionId, "map-reduce").call();
        final WorkflowActionBean action1 = jpaService.execute(wfActionGetCmd);
        String originalLauncherId = action1.getExternalId();

        ActionExecutorContext context = new ActionXCommand.ActionExecutorContext(job0, action1, false, false);
        MapReduceActionExecutor actionExecutor = new MapReduceActionExecutor();
        JobConf conf = actionExecutor.createBaseHadoopConf(context, XmlUtils.parseXml(action1.getConf()));
        String user = conf.get("user.name");
        JobClient jobClient = Services.get().get(HadoopAccessorService.class).createJobClient(user, conf);

        final RunningJob launcherJob = jobClient.getJob(JobID.forName(originalLauncherId));

        waitFor(120 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return launcherJob.isComplete();
            }
        });
        assertTrue(launcherJob.isSuccessful());
        assertTrue(LauncherMapper.hasIdSwap(launcherJob));

        new ActionCheckXCommand(action1.getId()).call();
        WorkflowActionBean action2 = jpaService.execute(wfActionGetCmd);
        String originalMapperId = action2.getExternalId();

        assertFalse(originalLauncherId.equals(originalMapperId));

        // At this point, the launcher job has finished and the map-reduce action has started (but not finished)
        // Now, shutdown the job tracker to pretend it has gone down during the map-reduce job
        executeWhileJobTrackerIsShutdown(new ShutdownJobTrackerExecutable() {
            @Override
            public void execute() throws Exception {
                assertEquals(0, action1.getRetries());
                new ActionCheckXCommand(actionId).call();

                waitFor(30 * 1000, new Predicate() {
                    @Override
                    public boolean evaluate() throws Exception {
                        WorkflowActionBean action1a = jpaService.execute(wfActionGetCmd);
                        return (action1a.getRetries() > 0);
                    }
                });
                waitFor(180 * 1000, new Predicate() {
                    @Override
                    public boolean evaluate() throws Exception {
                        WorkflowActionBean action1a = jpaService.execute(wfActionGetCmd);
                        return (action1a.getRetries() == 0);
                    }
                });
                WorkflowActionBean action1b = jpaService.execute(wfActionGetCmd);
                assertEquals(0, action1b.getRetries());
                assertEquals("START_MANUAL", action1b.getStatusStr());

                WorkflowJobBean job1 = jpaService.execute(new WorkflowJobGetJPAExecutor(jobId));
                assertEquals("SUSPENDED", job1.getStatusStr());

                // At this point, the action has gotten a transient error, even after maxRetries tries so the workflow has been
                // SUSPENDED
            }
        });
        // Now, lets bring the job tracker back up and resume the workflow (which will restart the current action)
        // It should now continue and finish with SUCCEEDED
        new ResumeXCommand(jobId).call();
        WorkflowJobBean job2 = jpaService.execute(new WorkflowJobGetJPAExecutor(jobId));
        assertEquals("RUNNING", job2.getStatusStr());

        sleep(500);

        new ActionCheckXCommand(actionId).call();
        WorkflowActionBean action3 = jpaService.execute(wfActionGetCmd);
        String launcherId = action3.getExternalId();

        assertFalse(originalLauncherId.equals(launcherId));
        assertFalse(originalMapperId.equals(launcherId));

        final RunningJob launcherJob2 = jobClient.getJob(JobID.forName(launcherId));

        waitFor(120 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return launcherJob2.isComplete();
            }
        });
        assertTrue(launcherJob2.isSuccessful());
        assertTrue(LauncherMapper.hasIdSwap(launcherJob2));

        new ActionCheckXCommand(actionId).call();
        WorkflowActionBean action4 = jpaService.execute(wfActionGetCmd);
        String mapperId = action4.getExternalId();
        assertFalse(originalMapperId.equals(mapperId));

        assertFalse(launcherId.equals(mapperId));

        final RunningJob mrJob = jobClient.getJob(JobID.forName(mapperId));

        waitFor(120 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return mrJob.isComplete();
            }
        });
        assertTrue(mrJob.isSuccessful());

        new ActionCheckXCommand(actionId).call();
        WorkflowActionBean action5 = jpaService.execute(wfActionGetCmd);

        assertEquals("SUCCEEDED", action5.getExternalStatus());
    }

    /**
     * This test case verifies if getRetryInterval picks up the
     * overridden value.
     *
     * @throws Exception
     */
    public void testCheckInterval() throws Exception {
        long testedValue = 10;
        Services.get().getConf().setLong(ActionCheckerService.CONF_ACTION_CHECK_INTERVAL,
                testedValue);

        WorkflowJobBean job0 = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        final String jobId = job0.getId();
        WorkflowActionBean action0 = this.addRecordToWfActionTable(jobId, "1", WorkflowAction.Status.RUNNING);
        final String actionId = action0.getId();

        ActionCheckXCommand checkCommand = new ActionCheckXCommand(actionId);
        checkCommand.call();
        long effectiveValue = checkCommand.getRetryInterval();
        assertEquals(testedValue, effectiveValue);
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
        "<prepare><delete path=\"" + outputDir.toString() + "\"/></prepare>" +
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
