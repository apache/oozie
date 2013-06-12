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
package org.apache.oozie;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URI;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.DagXLogInfoService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.XLogService;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLogStreamer.Filter;

public class TestCoordinatorEngineStreamLog extends XFsTestCase {
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

    private void writeToFile(String appXml, String appPath) throws Exception {
        File wf = new File(new URI(appPath).getPath());
        PrintWriter out = null;
        try {
            out = new PrintWriter(new FileWriter(wf));
            out.println(appXml);
        }
        catch (IOException iex) {
            throw iex;
        }
        finally {
            if (out != null) {
                out.close();
            }
        }
    }

    static class DummyXLogService extends XLogService {
        Filter filter;

        @Override
        public void streamLog(Filter filter1, Date startTime, Date endTime, Writer writer) throws IOException {
            filter = filter1;
        }
    }

    private CoordinatorEngine createCoordinatorEngine() {
        return new CoordinatorEngine(getTestUser());
    }

    /**
     * The log streaming itself is tested in
     * {@link org.apache.oozie.service.TestXLogService}. Here we test only the
     * fields that are injected into
     * {@link org.apache.oozie.util.XLogStreamer.Filter} upon
     * {@link CoordinatorEngine#streamLog(String, Writer)} invocation.
     */
    public void testStreamLog2() throws Exception {
        CoordinatorEngine ce = createCoordinatorEngine();
        String jobId = runJobsImpl(ce);
        ce.streamLog(jobId, new StringWriter()/* writer is unused */);

        DummyXLogService service = (DummyXLogService) services.get(XLogService.class);
        Filter filter = service.filter;

        assertEquals(filter.getFilterParams().get(DagXLogInfoService.JOB), jobId);
    }

    /**
     * Test method org.apache.oozie.CoordinatorEngine.streamLog(String, String,
     * String, Writer) with null 2nd and 3rd arguments.
     */
    public void testStreamLog4NullNull() throws Exception {
        CoordinatorEngine ce = createCoordinatorEngine();
        String jobId = runJobsImpl(ce);
        ce.streamLog(jobId, null, null, new StringWriter()/* writer is unused */);

        DummyXLogService service = (DummyXLogService) services.get(XLogService.class);
        Filter filter = service.filter;

        assertEquals(filter.getFilterParams().get(DagXLogInfoService.JOB), jobId);
    }

    /**
     * Test method org.apache.oozie.CoordinatorEngine.streamLog(String, String,
     * String, Writer) with RestConstants.JOB_LOG_ACTION and non-null 2nd
     * argument.
     */
    public void testStreamLog4JobLogAction() throws Exception {
        CoordinatorEngine ce = createCoordinatorEngine();
        String jobId = runJobsImpl(ce);

        ce.streamLog(jobId, "678, 123-127, 946", RestConstants.JOB_LOG_ACTION, new StringWriter()/* unused */);

        DummyXLogService service = (DummyXLogService) services.get(XLogService.class);
        Filter filter = service.filter;

        assertEquals(jobId, filter.getFilterParams().get(DagXLogInfoService.JOB));
        assertEquals("(" + jobId + "@678|" + jobId + "@123|" + jobId + "@124|" + jobId + "@125|" + jobId + "@126|" + jobId
                + "@127|" + jobId + "@946)", filter.getFilterParams().get(DagXLogInfoService.ACTION));
    }

    /**
     * Test method org.apache.oozie.CoordinatorEngine.streamLog(String, String,
     * String, Writer) with RestConstants.JOB_LOG_DATE.
     */
    public void testStreamLog4JobLogDate() throws Exception {
        CoordinatorEngine ce = createCoordinatorEngine();
        final String jobId = runJobsImpl(ce);

        CoordinatorJobBean cjb = ce.getCoordJob(jobId);
        Date createdDate = cjb.getCreatedTime();
        Date endDate = new Date();
        assertTrue(endDate.after(createdDate));

        long middle = (createdDate.getTime() + endDate.getTime()) / 2;
        Date middleDate = new Date(middle);

        ce.streamLog(jobId, DateUtils.formatDateOozieTZ(createdDate) + "::" + DateUtils.formatDateOozieTZ(middleDate) + ","
                + DateUtils.formatDateOozieTZ(middleDate) + "::" + DateUtils.formatDateOozieTZ(endDate),
                RestConstants.JOB_LOG_DATE, new StringWriter()/* unused */);

        DummyXLogService service = (DummyXLogService) services.get(XLogService.class);
        Filter filter = service.filter;

        assertEquals(jobId, filter.getFilterParams().get(DagXLogInfoService.JOB));
        final String action = filter.getFilterParams().get(DagXLogInfoService.ACTION);
        assertEquals("(" + jobId + "@1|" + jobId + "@2)", action);
    }

    private String runJobsImpl(final CoordinatorEngine ce) throws Exception {
        services.setService(DummyXLogService.class);
        // need to re-define the parameters that are cleared upon the service reset:
        new DagXLogInfoService().init(services);
        services.init();

        Configuration conf = new XConfiguration();

        final String appPath = "file://" + getTestCaseDir() + File.separator + "coordinator.xml";
        final long now = System.currentTimeMillis();
        final String start = DateUtils.formatDateOozieTZ(new Date(now));
        long e = now + 1000 * 119;
        final String end = DateUtils.formatDateOozieTZ(new Date(e));

        String wfXml = IOUtils.getResourceAsString("wf-no-op.xml", -1);
        writeToFile(wfXml, getFsTestCaseDir(), "workflow.xml");

        String appXml = "<coordinator-app name=\"NAME\" frequency=\"${coord:minutes(1)}\" start=\"" + start + "\" end=\"" + end
                + "\" timezone=\"UTC\" " + "xmlns=\"uri:oozie:coordinator:0.1\"> " + "<controls> " + "  <timeout>10</timeout> "
                + "  <concurrency>1</concurrency> " + "  <execution>LIFO</execution> " + "</controls> " + "<action> "
                + "  <workflow> " + "  <app-path>" + getFsTestCaseDir() + "/workflow.xml</app-path>"
                + "  <configuration> <property> <name>inputA</name> <value>valueA</value> </property> "
                + "  <property> <name>inputB</name> <value>valueB</value> " + "  </property></configuration> " + "</workflow>"
                + "</action> " + "</coordinator-app>";
        writeToFile(appXml, appPath);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());

        final String jobId = ce.submitJob(conf, true);
        waitFor(1000 * 119, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                try {
                    List<CoordinatorAction> actions = ce.getCoordJob(jobId).getActions();
                    if (actions.size() < 1) {
                        return false;
                    }
                    for (CoordinatorAction action : actions) {
                        CoordinatorAction.Status actionStatus = action.getStatus();
                        if (actionStatus != CoordinatorAction.Status.SUCCEEDED) {
                            return false;
                        }
                    }
                    return true;
                }
                catch (Exception ex) {
                    ex.printStackTrace();
                    return false;
                }
            }
        });
        // Assert all the actions are succeeded (useful for waitFor() timeout case):
        final List<CoordinatorAction> actions = ce.getCoordJob(jobId).getActions();
        for (CoordinatorAction action: actions) {
          assertEquals(CoordinatorAction.Status.SUCCEEDED, action.getStatus());
        }
        return jobId;
    }

    private void writeToFile(String content, Path appPath, String fileName) throws IOException {
        FileSystem fs = getFileSystem();
        Writer writer = new OutputStreamWriter(fs.create(new Path(appPath, fileName), true));
        writer.write(content);
        writer.close();
    }
}
