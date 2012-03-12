/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.test;

import org.apache.oozie.service.XLogService;
import org.apache.oozie.test.MiniOozieTestCase;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.local.LocalOozie;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.io.OutputStreamWriter;
import java.util.Properties;

/**
 * MiniOozie unit test
 */
public class WorkflowTest extends MiniOozieTestCase {

    @Override
    protected void setUp() throws Exception {
        System.setProperty(XLogService.LOG4J_FILE, "oozie-log4j.properties");
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testWorkflowRun() throws Exception {
        String wfApp = "<workflow-app xmlns='uri:oozie:workflow:0.1' name='test-wf'>" + "    <start to='end'/>"
                + "    <end name='end'/>" + "</workflow-app>";

        FileSystem fs = getFileSystem();
        Path appPath = new Path(getFsTestCaseDir(), "app");
        fs.mkdirs(appPath);
        fs.mkdirs(new Path(appPath, "lib"));

        Writer writer = new OutputStreamWriter(fs.create(new Path(appPath, "workflow.xml")));
        writer.write(wfApp);
        writer.close();

        final OozieClient wc = LocalOozie.getClient();

        Properties conf = wc.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, appPath.toString() + File.separator + "workflow.xml");
        conf.setProperty(OozieClient.USER_NAME, getTestUser());
        conf.setProperty(OozieClient.GROUP_NAME, getTestGroup());


        final String jobId = wc.submit(conf);
        assertNotNull(jobId);

        WorkflowJob wf = wc.getJobInfo(jobId);
        assertNotNull(wf);
        assertEquals(WorkflowJob.Status.PREP, wf.getStatus());

        wc.start(jobId);

        waitFor(1000, new Predicate() {
            public boolean evaluate() throws Exception {
                WorkflowJob wf = wc.getJobInfo(jobId);
                return wf.getStatus() == WorkflowJob.Status.SUCCEEDED;
            }
        });

        wf = wc.getJobInfo(jobId);
        assertNotNull(wf);
        assertEquals(WorkflowJob.Status.SUCCEEDED, wf.getStatus());

    }

    public void testWorkflowRunFromFile() throws Exception {
        FileSystem fs = getFileSystem();
        Path appPath = new Path(getFsTestCaseDir(), "app");
        fs.mkdirs(appPath);
        fs.mkdirs(new Path(appPath, "lib"));

        Reader reader = getResourceAsReader("wf-test.xml", -1);
        Writer writer = new OutputStreamWriter(fs.create(new Path(appPath, "workflow.xml")));
        copyCharStream(reader, writer);
        writer.close();
        reader.close();

        Path path = getFsTestCaseDir();

        final OozieClient wc = LocalOozie.getClient();

        Properties conf = wc.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, appPath.toString() + File.separator + "workflow.xml");
        conf.setProperty(OozieClient.USER_NAME, getTestUser());
        conf.setProperty(OozieClient.GROUP_NAME, getTestGroup());
        conf.setProperty("nnbase", path.toString());
        conf.setProperty("base", path.toUri().getPath());



        final String jobId = wc.submit(conf);
        assertNotNull(jobId);

        WorkflowJob wf = wc.getJobInfo(jobId);
        assertNotNull(wf);
        assertEquals(WorkflowJob.Status.PREP, wf.getStatus());

        wc.start(jobId);

        waitFor(15 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                WorkflowJob wf = wc.getJobInfo(jobId);
                return wf.getStatus() == WorkflowJob.Status.SUCCEEDED;
            }
        });

        wf = wc.getJobInfo(jobId);
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
    public InputStream getResourceAsStream(String path, int maxLen) throws IOException {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
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
    public Reader getResourceAsReader(String path, int maxLen) throws IOException {
        return new InputStreamReader(getResourceAsStream(path, maxLen));
    }

    /**
     * Copies an char input stream into an char output stream.
     *
     * @param reader reader to copy from.
     * @param writer writer to copy to.
     * @throws IOException thrown if the copy failed.
     */
    public void copyCharStream(Reader reader, Writer writer) throws IOException {
        char[] buffer = new char[4096];
        int read;
        while ((read = reader.read(buffer)) > -1) {
            writer.write(buffer, 0, read);
        }
    }

}
