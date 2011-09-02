/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.service;

import java.io.FileWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.List;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.DagEngine;
import org.apache.oozie.ForTestingActionExecutor;
import org.apache.oozie.service.ActionCheckerService.ActionCheckRunnable;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.service.WorkflowSchemaService;
import org.apache.oozie.service.WorkflowStoreService;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;

/**
 * Test cases for the Action Checker Service.
 *
 */
public class TestActionCheckerService extends XTestCase {

    private Services services;

    @Override
    protected void setUp()throws Exception {
        super.setUp();
        setSystemProperty(WorkflowSchemaService.CONF_EXT_SCHEMAS, "wf-ext-schema.xsd");
        services = new Services();
        cleanUpDB(services.getConf());
        services.init();
        services.get(ActionService.class).register(ForTestingActionExecutor.class);
    }

    protected void tearDown()throws Exception {
        services.destroy();
        super.tearDown();
    }

    /**
     * Tests functionality of the Action Checker Service Runnable.
     * </p>
     * Starts an action which behaves like an Async Action (Action and Job state
     * set to Running). Verifies the action status to be RUNNING.
     * </p>
     * Runs the ActionCheck runnable, and checks for thw job to complete.
     * 
     * @throws Exception
     */
    public void testActionCheckerService() throws Exception {
        Reader reader = IOUtils.getResourceAsReader("wf-ext-schema-valid.xml", -1);
        Writer writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
        IOUtils.copyCharStream(reader, writer);

        final DagEngine engine = new DagEngine("u", "a");
        Configuration conf = new XConfiguration();
        conf.set(OozieClient.APP_PATH, getTestCaseDir());
        conf.set(OozieClient.USER_NAME, "u");
        conf.set(OozieClient.GROUP_NAME, "g");
        conf.set(OozieClient.LOG_TOKEN, "t");

        conf.set("external-status", "ok");
        conf.set("signal-value", "based_on_action_status");
        conf.set("running-mode", "async");

        final String jobId = engine.submitJob(conf, true);
        Thread.sleep(200);

        waitFor(5000, new Predicate() {
            public boolean evaluate() throws Exception {
                return (engine.getJob(jobId).getStatus() == WorkflowJob.Status.RUNNING);
            }
        });
        String actionId = null;
        final WorkflowStore store = Services.get().get(WorkflowStoreService.class).create();
        List<WorkflowActionBean> actions = store.getActionsForWorkflow(jobId, false);
        WorkflowActionBean action = actions.get(0);
        actionId = action.getId();
        assertEquals(WorkflowActionBean.Status.RUNNING, action.getStatus());
        store.close();

        Thread.sleep(2000);
        Runnable actionCheckRunnable = new ActionCheckRunnable(0);
        actionCheckRunnable.run();       

        waitFor(20000, new Predicate() {
            public boolean evaluate() throws Exception {
                return (engine.getJob(jobId).getStatus() == WorkflowJob.Status.SUCCEEDED);
            }
        });

        final WorkflowStore store2 = Services.get().get(WorkflowStoreService.class).create();
        List<WorkflowActionBean> actions2 = store2.getActionsForWorkflow(jobId, false);
        WorkflowActionBean action2 = actions2.get(0);
        assertEquals(WorkflowActionBean.Status.OK, action2.getStatus());
        store2.close();
    }

    /**
     * Tests the delayed check functionality of the Action Check Service
     * Runnable.
     * </p>
     * Starts an action which behaves like an Async Action (Action and Job state
     * set to Running). Verifies the action status to be RUNNING.
     * </p>
     * Updates the last check time to now, and attempts to run the
     * ActionCheckRunnable with the delay configured to 20 seconds.
     * 
     * @throws Exception
     */
    public void testActionCheckerServiceDelay() throws Exception {
        Reader reader = IOUtils.getResourceAsReader("wf-ext-schema-valid.xml", -1);
        Writer writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
        IOUtils.copyCharStream(reader, writer);

        final DagEngine engine = new DagEngine("u", "a");
        Configuration conf = new XConfiguration();
        conf.set(OozieClient.APP_PATH, getTestCaseDir());
        conf.set(OozieClient.USER_NAME, "u");
        conf.set(OozieClient.GROUP_NAME, "g");
        conf.set(OozieClient.LOG_TOKEN, "t");

        conf.set("external-status", "ok");
        conf.set("signal-value", "based_on_action_status");
        conf.set("running-mode", "async");

        final String jobId = engine.submitJob(conf, true);
        Thread.sleep(200);


        waitFor(5000, new Predicate() {
            public boolean evaluate() throws Exception {
                return (engine.getJob(jobId).getStatus() == WorkflowJob.Status.RUNNING);
            }
        });

        Thread.sleep(100);
        final WorkflowStore store = Services.get().get(WorkflowStoreService.class).create();
        List<WorkflowActionBean> actions = store.getActionsForWorkflow(jobId, false);
        WorkflowActionBean action = actions.get(0);
        assertEquals(WorkflowActionBean.Status.RUNNING, action.getStatus());

        action.setLastCheckTime(new Date());
        store.updateAction(action);
        store.commit();
        store.close();

        int actionCheckDelay = 20;

        Runnable actionCheckRunnable = new ActionCheckRunnable(actionCheckDelay);
        actionCheckRunnable.run();

        Thread.sleep(3000);
        final WorkflowStore store2 = Services.get().get(WorkflowStoreService.class).create();
        List<WorkflowActionBean> actions2 = store2.getActionsForWorkflow(jobId, false);
        WorkflowActionBean action2 = actions2.get(0);
        assertEquals(WorkflowActionBean.Status.RUNNING, action2.getStatus());
        store2.close();
        assertEquals(WorkflowJob.Status.RUNNING, engine.getJob(jobId).getStatus());
    }
}