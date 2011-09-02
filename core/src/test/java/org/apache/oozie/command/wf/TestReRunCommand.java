/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.command.wf;

import java.util.Properties;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

import org.apache.hadoop.fs.Path;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.service.XLogService;

public class TestReRunCommand extends XFsTestCase {
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        setSystemProperty(XLogService.LOG4J_FILE_ENV, "oozie-log4j.properties");
        LocalOozie.start();
    }

    @Override
    protected void tearDown() throws Exception {
        LocalOozie.stop();
        super.tearDown();
    }

    public void testRerun() throws IOException, OozieClientException {
        Reader reader = IOUtils.getResourceAsReader("rerun-wf.xml", -1);
        Writer writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
        IOUtils.copyCharStream(reader, writer);

        Path path = getFsTestCaseDir();

        getFileSystem().mkdirs(new Path(path, "p2"));

        final OozieClient wfClient = LocalOozie.getClient();
        Properties conf = wfClient.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, getTestCaseDir());
        conf.setProperty(OozieClient.USER_NAME, getTestUser());
        injectKerberosInfo(conf);

        conf.setProperty("nnbase", path.toString());
        conf.setProperty("base", path.toUri().getPath());

        final String jobId1 = wfClient.submit(conf);
        wfClient.start(jobId1);
        waitFor(15 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return wfClient.getJobInfo(jobId1).getStatus() == WorkflowJob.Status.KILLED;
            }
        });
        assertEquals(WorkflowJob.Status.KILLED, wfClient.getJobInfo(jobId1).getStatus());

        //Skip a non-executed node
        conf.setProperty(OozieClient.RERUN_SKIP_NODES, "fs1,fs2,dec3");

        boolean failed = false;
        try {
            wfClient.reRun(jobId1, conf);
        }
        catch (OozieClientException e) {
            failed = true;
            assertTrue(e.getCause().getMessage().contains(ErrorCode.E0807.toString()));
        }
        assertEquals(true, failed);

        //Skip executed nodes
        getFileSystem().delete(new Path(path, "p2"), true);
        conf.setProperty(OozieClient.RERUN_SKIP_NODES, "fs1");

        wfClient.reRun(jobId1, conf);
        waitFor(15 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return wfClient.getJobInfo(jobId1).getStatus() == WorkflowJob.Status.SUCCEEDED;
            }
        });
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfClient.getJobInfo(jobId1).getStatus());
    }

    public void testRedeploy() throws IOException, OozieClientException {
        Reader reader = IOUtils.getResourceAsReader("rerun-elerr-wf.xml", -1);
        Writer writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
        IOUtils.copyCharStream(reader, writer);

        final OozieClient wfClient = LocalOozie.getClient();
        Properties conf = wfClient.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, getTestCaseDir());
        conf.setProperty(OozieClient.USER_NAME, getTestUser());
        injectKerberosInfo(conf);

        conf.setProperty("inPath", getFsTestCaseDir().toString());
        conf.setProperty("checkDir", getFsTestCaseDir().toString() + "/check");

        final String jobId1 = wfClient.submit(conf);
        wfClient.start(jobId1);
        waitFor(15 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return wfClient.getJobInfo(jobId1).getStatus() == WorkflowJob.Status.FAILED;
            }
        });
        assertEquals(WorkflowJob.Status.FAILED, wfClient.getJobInfo(jobId1).getStatus());

        reader = IOUtils.getResourceAsReader("rerun-el-wf.xml", -1);
        writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
        IOUtils.copyCharStream(reader, writer);

        conf.setProperty(OozieClient.RERUN_SKIP_NODES, "hdfs11");
        wfClient.reRun(jobId1, conf);
        waitFor(15 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return wfClient.getJobInfo(jobId1).getStatus() == WorkflowJob.Status.SUCCEEDED;
            }
        });
        assertEquals(WorkflowJob.Status.SUCCEEDED, wfClient.getJobInfo(jobId1).getStatus());
    }
}
