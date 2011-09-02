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
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.DagEngine;
import org.apache.oozie.ForTestingActionExecutor;
import org.apache.oozie.service.ActionRecoveryService.ActionRecoveryRunnable;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.service.WorkflowSchemaService;
import org.apache.oozie.service.WorkflowStoreService;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;

public class TestActionRecoveryService extends XTestCase {
    private Services services;

    @Override
    protected void setUp()throws Exception {
        super.setUp();
        setSystemProperty(WorkflowSchemaService.CONF_EXT_SCHEMAS, "wf-ext-schema.xsd");
        services = new Services();
        services.init();
        services.get(ActionService.class).register(ForTestingActionExecutor.class);
    }

    protected void tearDown()throws Exception {
        services.destroy();
        super.tearDown();
    }

    /**
     * Tests functionality of the Recovery Service Runnable command.
     * </p>
     * Starts an action which behaves like an Async Action (Action and Job state
     * set to Running). Changes the action configuration to run in sync mode and
     * updates the store. Runs the recovery runnable, and ensures the state of
     * the action and job have not changed.
     * </p>
     * Changes the state of the action from RUNNING to PREP and updates the
     * store. Again, runs the recovery runnable and ensures the state changes to
     * OK and the job completes successfully.
     * 
     * @throws Exception
     */
    public void testRecoveryService() throws Exception {
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

        String actionConf = action.getConf();
        String fixedActionConf = actionConf.replaceAll("async", "sync");
        action.setConf(fixedActionConf);
        store.updateAction(action);
        store.commit();
        store.close();

        Runnable recoveryRunnable = new ActionRecoveryRunnable(0);

        recoveryRunnable.run();
        Thread.sleep(3000);

        final WorkflowStore store2 = Services.get().get(WorkflowStoreService.class).create();
        assertEquals(WorkflowJob.Status.RUNNING, engine.getJob(jobId).getStatus());
        List<WorkflowActionBean> actions2 = store2.getActionsForWorkflow(jobId, false);
        WorkflowActionBean action2 = actions2.get(0);
        assertEquals(WorkflowActionBean.Status.RUNNING, action2.getStatus());
        action.setStatus(WorkflowActionBean.Status.PREP);
        store2.updateAction(action);
        store2.commit();
        store2.close();

        recoveryRunnable.run();
        Thread.sleep(3000);

        final WorkflowStore store3 = Services.get().get(WorkflowStoreService.class).create();
        assertEquals(WorkflowJob.Status.SUCCEEDED, engine.getJob(jobId).getStatus());
        List<WorkflowActionBean> actions3 = store3.getActionsForWorkflow(jobId, false);
        WorkflowActionBean action3 = actions3.get(0);
        assertEquals(WorkflowActionBean.Status.OK, action3.getStatus());
        store3.close();
    }
}
