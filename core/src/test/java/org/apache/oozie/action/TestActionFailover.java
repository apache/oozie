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
package org.apache.oozie.action;


import java.io.OutputStreamWriter;
import java.util.List;
import java.io.File;
import java.io.FileWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.FaultInjection;
import org.apache.oozie.command.wf.ActionStartXCommand;
import org.apache.oozie.command.SkipCommitFaultInjection;
import org.apache.oozie.service.WorkflowStoreService;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.util.IOUtils;
import org.mortbay.io.WriterOutputStream;

public class TestActionFailover extends XFsTestCase {

    @Override
    protected void setUp() throws Exception {
        Class c = ActionStartXCommand.class;
        super.setUp();
        LocalOozie.start();
    }

    @Override
    protected void tearDown() throws Exception {
        FaultInjection.deactivate("org.apache.oozie.command.SkipCommitFaultInjection");
        LocalOozie.stop();
        super.tearDown();
    }

    public void testFsFailover() throws Exception {
        Path wf = new Path(getFsTestCaseDir(), "workflow.xml");
        Reader reader = IOUtils.getResourceAsReader("failover-fs-wf.xml", -1);
        Writer writer = new OutputStreamWriter(getFileSystem().create(wf));
        IOUtils.copyCharStream(reader, writer);

        final OozieClient wfClient = LocalOozie.getClient();
        Properties conf = wfClient.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, wf.toString());
        conf.setProperty(OozieClient.USER_NAME, getTestUser());
        conf.setProperty(OozieClient.GROUP_NAME, getTestGroup());

        final Path source = new Path(getFsTestCaseDir(), "fsfailover-source");
        getFileSystem().mkdirs(source);
        final Path target = new Path(getFsTestCaseDir().toString(), "fsfailover-target");
        conf.setProperty("source", source.toString());
        conf.setProperty("target", target.toUri().getPath());

        setSystemProperty(FaultInjection.FAULT_INJECTION, "true");
        setSystemProperty(SkipCommitFaultInjection.ACTION_FAILOVER_FAULT_INJECTION, "true");

        final String jobId1 = wfClient.submit(conf);
        wfClient.start(jobId1);

        waitFor(10 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return getFileSystem().exists(target);
            }
        });
        assertTrue(getFileSystem().exists(target));

        waitFor(10 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return FaultInjection.isActive("org.apache.oozie.command.SkipCommitFaultInjection");
            }
        });
        assertFalse(FaultInjection.isActive("org.apache.oozie.command.SkipCommitFaultInjection"));

        assertEquals(WorkflowJob.Status.RUNNING, wfClient.getJobInfo(jobId1).getStatus());

        WorkflowStore store = Services.get().get(WorkflowStoreService.class).create();

        List<WorkflowActionBean> actions = store.getActionsForWorkflow(jobId1, false);
        WorkflowActionBean action = actions.get(0);
        assertEquals(WorkflowAction.Status.PREP, action.getStatus());

        setSystemProperty(FaultInjection.FAULT_INJECTION, "false");
        setSystemProperty(SkipCommitFaultInjection.ACTION_FAILOVER_FAULT_INJECTION, "false");

        ActionStartXCommand actionStartCommand = new ActionStartXCommand(action.getId(), action.getType());
        actionStartCommand.call();

        store = Services.get().get(WorkflowStoreService.class).create();
        actions = store.getActionsForWorkflow(jobId1, false);
        action = actions.get(0);
        assertEquals(WorkflowAction.Status.DONE, action.getStatus());

        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return wfClient.getJobInfo(jobId1).getStatus() == WorkflowJob.Status.SUCCEEDED;
            }
        });
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfClient.getJobInfo(jobId1).getStatus());

        final String jobId2 = wfClient.submit(conf);

        wfClient.start(jobId2);
        waitFor(10 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return wfClient.getJobInfo(jobId2).getStatus() == WorkflowJob.Status.KILLED;
            }
        });
        assertEquals(WorkflowJob.Status.KILLED, wfClient.getJobInfo(jobId2).getStatus());
    }
}
