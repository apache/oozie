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

package org.apache.oozie.test;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.XLogService;
import org.apache.oozie.servlet.V2ValidateServlet;

import java.io.*;
import java.util.Properties;

public abstract class WorkflowTestCase extends MiniOozieTestCase {

    @Override
    protected void setUp() throws Exception {
        System.setProperty(XLogService.LOG4J_FILE, "oozie-log4j.properties");
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    protected void submitAndAssert(final String workflowXml, final WorkflowJob.Status terminalStatus)
            throws OozieClientException, IOException {
        final WorkflowJob finishedWorkflowJob = new WorkflowJobBuilder()
                .submit(workflowXml)
                .start()
                .waitForSucceeded()
                .build();

        assertNotNull(finishedWorkflowJob);
        assertEquals(terminalStatus, finishedWorkflowJob.getStatus());
    }

    protected void validate(final String workflowXml) throws IOException, OozieClientException {
        new WorkflowJobBuilder()
                .validate(workflowXml);
    }

    protected void runWorkflowFromFile(final String workflowFileName, final Properties additionalWorkflowProperties)
            throws IOException, OozieClientException {
        final FileSystem fs = getFileSystem();
        final Path appPath = new Path(getFsTestCaseDir(), "app");
        fs.mkdirs(appPath);
        fs.mkdirs(new Path(appPath, "lib"));

        final Reader reader = getResourceAsReader(workflowFileName, -1);
        final Writer writer = new OutputStreamWriter(fs.create(new Path(appPath, "workflow.xml")));
        copyCharStream(reader, writer);
        writer.close();
        reader.close();

        final Path path = getFsTestCaseDir();

        final OozieClient oozieClient = LocalOozie.getClient();

        final Properties conf = oozieClient.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, new Path(appPath, "workflow.xml").toString());
        conf.setProperty(OozieClient.USER_NAME, getTestUser());
        conf.setProperty("nameNodeBasePath", path.toString());
        conf.setProperty("base", path.toUri().getPath());
        conf.setProperty("nameNode", getNameNodeUri());
        conf.setProperty("jobTracker", getJobTrackerUri());

        for (final String additionalKey : additionalWorkflowProperties.stringPropertyNames()) {
            conf.setProperty(additionalKey, additionalWorkflowProperties.getProperty(additionalKey));
        }

        final String jobId = oozieClient.submit(conf);
        assertNotNull(jobId);

        WorkflowJob wf = oozieClient.getJobInfo(jobId);
        assertNotNull(wf);
        assertEquals(WorkflowJob.Status.PREP, wf.getStatus());

        oozieClient.start(jobId);

        waitFor(15_000, new Predicate() {
            public boolean evaluate() throws Exception {
                final WorkflowJob wf = oozieClient.getJobInfo(jobId);
                return wf.getStatus() == WorkflowJob.Status.SUCCEEDED;
            }
        });

        wf = oozieClient.getJobInfo(jobId);
        assertNotNull(wf);
        assertEquals(WorkflowJob.Status.SUCCEEDED, wf.getStatus());
    }

    /**
     * Return a classpath resource as a stream.
     * <p/>
     *
     * @param path classpath for the resource.
     * @param maxLen max content length allowed.
     * @return the stream for the resource.
     * @throws IOException thrown if the resource could not be read.
     */
    private InputStream getResourceAsStream(final String path, final int maxLen) throws IOException {
        final InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
        if (is == null) {
            throw new IllegalArgumentException("resource " + path + " not found");
        }
        return is;
    }

    /**
     * Return a classpath resource as a reader.
     * <p/>
     * It is assumed that the resource is a text resource.
     *
     * @param path classpath for the resource.
     * @param maxLen max content length allowed.
     * @return the reader for the resource.
     * @throws IOException thrown if the resource could not be read.
     */
    private Reader getResourceAsReader(final String path, final int maxLen) throws IOException {
        return new InputStreamReader(getResourceAsStream(path, maxLen));
    }

    /**
     * Copies an char input stream into an char output stream.
     *
     * @param reader reader to copy from.
     * @param writer writer to copy to.
     * @throws IOException thrown if the copy failed.
     */
    private void copyCharStream(final Reader reader, final Writer writer) throws IOException {
        final char[] buffer = new char[4096];
        int read;
        while ((read = reader.read(buffer)) > -1) {
            writer.write(buffer, 0, read);
        }
    }

    private class WorkflowJobBuilder {
        private final FileSystem dfs;
        private final Path appPath;
        private final OozieClient oozieClient = LocalOozie.getClient();
        private String workflowJobId;
        private WorkflowJob workflowJob;
        private final Path localPath;

        private WorkflowJobBuilder() throws IOException {
            this.dfs = getFileSystem();
            this.appPath = new Path(getFsTestCaseDir(), "app");
            this.localPath = new Path(File.createTempFile(appPath.getName(), "workflow.xml").toString());

            dfs.mkdirs(appPath);
            dfs.mkdirs(new Path(appPath, "lib"));
        }

        private WorkflowJobBuilder submit(final String workflowXml) throws IOException, OozieClientException {
            writeToDFS(workflowXml);

            final Properties conf = createAndResolveConfiguration();

            workflowJobId = oozieClient.submit(conf);

            assertNotNull(workflowJobId);

            return this;
        }

        private WorkflowJobBuilder validate(final String workflowXml) throws IOException, OozieClientException {
            final String result = oozieClient.validateXML(workflowXml);

            assertEquals("not a valid workflow xml", V2ValidateServlet.VALID_WORKFLOW_APP, result);

            return this;
        }

        private void writeToDFS(final String workflowXml) throws IOException {
            try (final Writer writer = new OutputStreamWriter(dfs.create(getDFSWorkflowPath()))) {
                writer.write(workflowXml);
                writer.flush();
            }
        }

        private Properties createAndResolveConfiguration() {
            final OozieClient wc = LocalOozie.getClient();

            final Properties conf = wc.createConfiguration();
            conf.setProperty(OozieClient.APP_PATH, getDFSWorkflowPath().toString());
            conf.setProperty(OozieClient.USER_NAME, getTestUser());
            conf.setProperty("nameNodeBasePath", getFsTestCaseDir().toString());
            conf.setProperty("base", getFsTestCaseDir().toUri().getPath());
            conf.setProperty("nameNode", getNameNodeUri());
            conf.setProperty("jobTracker", getJobTrackerUri());
            return conf;
        }

        private void writeToLocalFile(final String workflowXml) throws IOException {
            try (final Writer writer = new FileWriter(localPath.toString())) {
                writer.write(workflowXml);
                writer.flush();
            }
        }

        private Path getDFSWorkflowPath() {
            return new Path(appPath, "workflow.xml");
        }

        private WorkflowJobBuilder start() throws OozieClientException {
            workflowJob = oozieClient.getJobInfo(workflowJobId);

            assertNotNull(workflowJob);
            assertEquals(WorkflowJob.Status.PREP, workflowJob.getStatus());

            oozieClient.start(workflowJobId);

            workflowJob = oozieClient.getJobInfo(workflowJobId);

            assertEquals(WorkflowJob.Status.RUNNING, workflowJob.getStatus());

            return this;
        }

        private WorkflowJobBuilder waitForSucceeded() throws OozieClientException {
            waitFor(15_000, new Predicate() {
                public boolean evaluate() throws Exception {
                    final WorkflowJob wf = oozieClient.getJobInfo(workflowJobId);
                    return wf.getStatus() == WorkflowJob.Status.SUCCEEDED;
                }
            });

            workflowJob = oozieClient.getJobInfo(workflowJobId);

            return this;
        }

        private WorkflowJob build() {
            return workflowJob;
        }
    }
}
