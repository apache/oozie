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
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.DagEngine;
import org.apache.oozie.DagEngineException;
import org.apache.oozie.ForTestingActionExecutor;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.command.wf.PurgeCommand;
import org.apache.oozie.service.PurgeService.PurgeRunnable;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.service.WorkflowSchemaService;
import org.apache.oozie.service.WorkflowStoreService;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;

/**
 * Test cases for checking the correct functionality of the PurgeService.
 */
public class TestPurgeService extends XTestCase {
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
     * Tests the {@link org.apache.oozie.service.PurgeService}.
     * </p>
     * Creates and runs a new job to completion. Attempts to purge jobs older
     * than a day. Verifies the presence of the job in the system.
     * </p>
     * Sets the end date for the same job to make it qualify for the purge
     * criteria. Calls the purge service, and ensure the job does not exist in
     * the system.
     */
    public void testPurgeService() throws Exception {
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

        final String jobId = engine.submitJob(conf, true);

        waitFor(5000, new Predicate() {
            public boolean evaluate() throws Exception {
                return (engine.getJob(jobId).getStatus() == WorkflowJob.Status.SUCCEEDED);
            }
        });

        assertEquals(WorkflowJob.Status.SUCCEEDED, engine.getJob(jobId).getStatus());

        new PurgeCommand(1).call();
        assertEquals(WorkflowJob.Status.SUCCEEDED, engine.getJob(jobId).getStatus());

        final WorkflowStore store = Services.get().get(WorkflowStoreService.class).create();
        WorkflowJobBean wfBean = store.getWorkflow(jobId, true);
        Date endDate = new Date(System.currentTimeMillis() - 2 * 24 * 60 * 60 * 1000);
        wfBean.setEndTime(endDate);
        store.updateWorkflow(wfBean);
        store.commit();
        store.close();

        Runnable purgeRunnable = new PurgeRunnable(1);
        purgeRunnable.run();

        waitFor(5000, new Predicate() {
            public boolean evaluate() throws Exception {
                try {
                    engine.getJob(jobId).getStatus();
                }
                catch (Exception ex) {
                    return true;
                }
                return false;
            }
        });

        try {
            engine.getJob(jobId).getStatus();
            assertTrue(false);
        }
        catch (Exception ex) {
            assertEquals(ex.getClass(), DagEngineException.class);
            DagEngineException dex = (DagEngineException) ex;
            assertEquals(ErrorCode.E0604, dex.getErrorCode());
        }
    }
}
