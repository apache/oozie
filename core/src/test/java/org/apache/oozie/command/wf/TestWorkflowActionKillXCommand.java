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

import java.io.StringReader;
import java.net.URI;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.hadoop.MapperReducerForTest;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionInsertJPAExecutor;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.test.XTestCase.Predicate;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.workflow.WorkflowInstance;

public class TestWorkflowActionKillXCommand extends XDataTestCase {
    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    /**
     * Test : kill action successfully.
     *
     * @throws Exception
     */
    public void testWfActionKillSuccess() throws Exception {
        String externalJobID = launchSleepJob(1000);
        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.KILLED, WorkflowInstance.Status.KILLED);
        WorkflowActionBean action = this.addRecordToWfActionTable(job.getId(), externalJobID, "1",
                WorkflowAction.Status.KILLED,null);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(action.getId());

        action = jpaService.execute(wfActionGetCmd);
        assertEquals(WorkflowAction.Status.KILLED, action.getStatus());
        assertEquals("RUNNING", action.getExternalStatus());

        new ActionKillXCommand(action.getId()).call();

        action = jpaService.execute(wfActionGetCmd);
        assertEquals(WorkflowAction.Status.KILLED, action.getStatus());
        assertEquals("KILLED", action.getExternalStatus());
    }

    /**
     * Test : kill a non-killed action. Will throw the exception from {@link
     * WorkflowActionKillXCommand.verifyPrecondition()}
     *
     * @throws Exception
     */
    public void testWfActionKillFailed() throws Exception {
        String externalJobID = launchSleepJob(1000);
        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action = this.addRecordToWfActionTable(job.getId(), externalJobID, "1",
                WorkflowAction.Status.RUNNING,null);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(action.getId());

        action = jpaService.execute(wfActionGetCmd);
        assertEquals(action.getStatus(), WorkflowAction.Status.RUNNING);
        assertEquals(action.getExternalStatus(), "RUNNING");

        new ActionKillXCommand(action.getId()).call();

        // action is not in KILLED, action status must not change
        action = jpaService.execute(wfActionGetCmd);
        assertEquals(action.getStatus(), WorkflowAction.Status.RUNNING);
        assertEquals(action.getExternalStatus(), "RUNNING");
    }

    public void testWfActionKillChildJob() throws Exception {
        String externalJobID = launchSleepJob(1000);
        String childId = launchSleepJob(1000000);

        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.KILLED, WorkflowInstance.Status.KILLED);
        WorkflowActionBean action = this.addRecordToWfActionTable(job.getId(), externalJobID, "1",
                WorkflowAction.Status.KILLED, childId);

        new ActionKillXCommand(action.getId()).call();
        JobClient jobClient = createJobClient();

        final RunningJob mrJob = jobClient.getJob(JobID.forName(childId));
        waitFor(60 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return mrJob.isComplete();
            }
        });
        assertEquals(mrJob.getJobState(), JobStatus.KILLED);
    }

    protected WorkflowActionBean addRecordToWfActionTable(String wfId, String externalJobID, String actionName,
            WorkflowAction.Status status, String childID) throws Exception {
        WorkflowActionBean action = new WorkflowActionBean();
        action.setId(Services.get().get(UUIDService.class).generateChildId(wfId, actionName));
        action.setJobId(wfId);
        action.setName(actionName);
        action.setType("map-reduce");
        action.setStatus(status);
        action.setStartTime(new Date());
        action.setEndTime(new Date());
        action.setLastCheckTime(new Date());
        action.setPending();
        action.setExternalId(externalJobID);
        action.setExternalStatus("RUNNING");
        action.setExternalChildIDs(childID);

        String actionXml = "<map-reduce>" +
        "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
        "<name-node>" + getNameNodeUri() + "</name-node>" +
        "<configuration>" +
        "<property><name>mapred.mapper.class</name><value>" + MapperReducerForTest.class.getName() +
        "</value></property>" +
        "<property><name>mapred.reducer.class</name><value>" + MapperReducerForTest.class.getName() +
        "</value></property>" +
        "<property><name>mapred.input.dir</name><value>inputDir</value></property>" +
        "<property><name>mapred.output.dir</name><value>outputDir</value></property>" +
        "</configuration>" +
        "</map-reduce>";
        action.setConf(actionXml);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            WorkflowActionInsertJPAExecutor actionInsertExe = new WorkflowActionInsertJPAExecutor(action);
            jpaService.execute(actionInsertExe);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test wf action record to table");
            throw je;
        }
        return action;
    }

    private String launchSleepJob(int sleep) throws Exception {
        JobConf jobConf = Services.get().get(HadoopAccessorService.class)
                .createJobConf(new URI(getNameNodeUri()).getAuthority());
        JobClient jobClient = createJobClient();

        SleepJob sleepjob = new SleepJob();
        sleepjob.setConf(jobConf);
        jobConf = sleepjob.setupJobConf(1, 1, sleep, 1, sleep, 1);

        final RunningJob runningJob = jobClient.submitJob(jobConf);
        return runningJob.getID().toString();
    }

}
