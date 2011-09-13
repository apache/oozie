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

import java.util.Date;

import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.WorkflowActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.LiteWorkflowStoreService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.service.UUIDService.ApplicationType;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.workflow.WorkflowInstance;

public class TestWorkflowKillXCommand extends XDataTestCase {
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
     * Test : kill RUNNING job and PREP action successfully.
     *
     * @throws Exception
     */
    public void testWfKillSuccess1() throws Exception {
        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action = this.addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.PREP);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(job.getId());
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(action.getId());

        job = jpaService.execute(wfJobGetCmd);
        action = jpaService.execute(wfActionGetCmd);
        assertEquals(job.getStatus(), WorkflowJob.Status.RUNNING);
        assertEquals(action.getStatus(), WorkflowAction.Status.PREP);
        WorkflowInstance wfInstance = job.getWorkflowInstance();
        assertEquals(wfInstance.getStatus(), WorkflowInstance.Status.RUNNING);

        new KillXCommand(job.getId()).call();

        job = jpaService.execute(wfJobGetCmd);
        action = jpaService.execute(wfActionGetCmd);
        assertEquals(job.getStatus(), WorkflowJob.Status.KILLED);
        assertEquals(action.getStatus(), WorkflowAction.Status.KILLED);
        wfInstance = job.getWorkflowInstance();
        assertEquals(wfInstance.getStatus(), WorkflowInstance.Status.KILLED);
    }

    /**
     * Test : kill RUNNING job and RUNNING action successfully.
     *
     * @throws Exception
     */
    public void testWfKillSuccess2() throws Exception {
        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action = this.addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.RUNNING);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(job.getId());
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(action.getId());

        job = jpaService.execute(wfJobGetCmd);
        action = jpaService.execute(wfActionGetCmd);
        assertEquals(job.getStatus(), WorkflowJob.Status.RUNNING);
        assertEquals(action.getStatus(), WorkflowAction.Status.RUNNING);
        WorkflowInstance wfInstance = job.getWorkflowInstance();
        assertEquals(wfInstance.getStatus(), WorkflowInstance.Status.RUNNING);

        new KillXCommand(job.getId()).call();

        job = jpaService.execute(wfJobGetCmd);
        action = jpaService.execute(wfActionGetCmd);
        assertEquals(job.getStatus(), WorkflowJob.Status.KILLED);
        assertEquals(action.getStatus(), WorkflowAction.Status.KILLED);
        wfInstance = job.getWorkflowInstance();
        assertEquals(wfInstance.getStatus(), WorkflowInstance.Status.KILLED);
    }

    /**
     * Test : kill RUNNING job after NodeDef upgrade.
     *
     * @throws Exception
     */
    public void testWfKillSuccessAfterNodeDefUpgrade() throws Exception {
        services.destroy();

        setSystemProperty(LiteWorkflowStoreService.CONF_NODE_DEF_VERSION, LiteWorkflowStoreService.NODE_DEF_VERSION_0);
        services = new Services();
        services.init();

        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action = this.addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.PREP);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(job.getId());
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(action.getId());

        job = jpaService.execute(wfJobGetCmd);
        action = jpaService.execute(wfActionGetCmd);
        assertEquals(job.getStatus(), WorkflowJob.Status.RUNNING);
        assertEquals(action.getStatus(), WorkflowAction.Status.PREP);
        WorkflowInstance wfInstance = job.getWorkflowInstance();
        assertEquals(wfInstance.getStatus(), WorkflowInstance.Status.RUNNING);

        services.destroy();

        Thread.sleep(5000);

        setSystemProperty(LiteWorkflowStoreService.CONF_NODE_DEF_VERSION, LiteWorkflowStoreService.NODE_DEF_VERSION_1);
        services = new Services();
        services.init();

        Thread.sleep(5000);

        new KillXCommand(job.getId()).call();

        jpaService = Services.get().get(JPAService.class);
        job = jpaService.execute(wfJobGetCmd);
        action = jpaService.execute(wfActionGetCmd);
        assertEquals(job.getStatus(), WorkflowJob.Status.KILLED);
        assertEquals(action.getStatus(), WorkflowAction.Status.KILLED);
        wfInstance = job.getWorkflowInstance();
        assertEquals(wfInstance.getStatus(), WorkflowInstance.Status.KILLED);
    }


    public void testChildId() throws Exception {
        setSystemProperty(UUIDService.CONF_GENERATOR, "counter");
        Services services = new Services();
        services.init();
        UUIDService uuid = services.get(UUIDService.class);
        String id = uuid.generateId(ApplicationType.WORKFLOW);
        String childId = uuid.generateChildId(id, "a");
        assertEquals(id, uuid.getId(childId));
        assertEquals("a", uuid.getChildName(childId));
        services.destroy();

        setSystemProperty(UUIDService.CONF_GENERATOR, "random");
        services = new Services();
        services.init();
        uuid = services.get(UUIDService.class);
        id = uuid.generateId(ApplicationType.WORKFLOW);
        childId = uuid.generateChildId(id, "a");
        assertEquals(id, uuid.getId(childId));
        assertEquals("a", uuid.getChildName(childId));
        services.destroy();
    }

    /**
     * Test : kill job but failed to kill an already successful action.
     *
     * @throws Exception
     */
    public void testWfKillFailed() throws Exception {

        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action = this.addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.OK);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(job.getId());
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(action.getId());

        job = jpaService.execute(wfJobGetCmd);
        action = jpaService.execute(wfActionGetCmd);
        assertEquals(job.getStatus(), WorkflowJob.Status.RUNNING);
        assertEquals(action.getStatus(), WorkflowAction.Status.OK);
        WorkflowInstance wfInstance = job.getWorkflowInstance();
        assertEquals(wfInstance.getStatus(), WorkflowInstance.Status.RUNNING);

        new KillXCommand(job.getId()).call();

        job = jpaService.execute(wfJobGetCmd);
        action = jpaService.execute(wfActionGetCmd);
        assertEquals(job.getStatus(), WorkflowJob.Status.KILLED);
        assertEquals(action.getStatus(), WorkflowAction.Status.OK);
        wfInstance = job.getWorkflowInstance();
        assertEquals(wfInstance.getStatus(), WorkflowInstance.Status.KILLED);
    }

    /**
     * Test : kill job failed to load the job.
     *
     * @throws Exception
     */
    public void testWfKillFailedToLoadJob() throws Exception {
        final String testWfId = "0000001-" + new Date().getTime() + "-testWfKill-W";

        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action = this.addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.OK);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        WorkflowJobGetJPAExecutor wfJobGetCmd = new WorkflowJobGetJPAExecutor(job.getId());
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(action.getId());

        job = jpaService.execute(wfJobGetCmd);
        action = jpaService.execute(wfActionGetCmd);
        assertEquals(job.getStatus(), WorkflowJob.Status.RUNNING);
        assertEquals(action.getStatus(), WorkflowAction.Status.OK);
        WorkflowInstance wfInstance = job.getWorkflowInstance();
        assertEquals(wfInstance.getStatus(), WorkflowInstance.Status.RUNNING);

        try {
            new KillXCommand(testWfId).call();
            fail("Job doesn't exist. Should fail.");
        } catch (CommandException ce) {
        }
    }



}
