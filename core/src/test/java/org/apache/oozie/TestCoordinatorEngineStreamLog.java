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
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.DagXLogInfoService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.XLogStreamingService;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.XLogFilter;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XLogUserFilterParam;
import org.apache.oozie.util.XConfiguration;

public class TestCoordinatorEngineStreamLog extends XFsTestCase {
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

    static class DummyXLogStreamingService extends XLogStreamingService {
        XLogFilter filter;
        Date startTime;
        Date endTime;

        @Override
        public void streamLog(XLogFilter filter1, Date startTime, Date endTime, Writer writer, Map<String, String[]> params)
                throws IOException {
            filter = filter1;
            this.startTime = startTime;
            this.endTime = endTime;
        }
    }

    private CoordinatorEngine createCoordinatorEngine() {
        return new CoordinatorEngine(getTestUser());
    }

    public void testCoordLogStreaming() throws Exception {
        CoordinatorEngine ce = createCoordinatorEngine();
        final String jobId = runJobsImpl(ce, 6);

        CoordinatorJobBean cjb = ce.getCoordJob(jobId);
        Date createdDate = cjb.getCreatedTime();
        Date endDate = new Date();
        assertTrue(endDate.after(createdDate));

        List<CoordinatorAction> list = cjb.getActions();
        Collections.sort(list, new Comparator<CoordinatorAction>() {
            public int compare(CoordinatorAction a, CoordinatorAction b) {
                return a.getId().compareTo(b.getId());
            }
        });
        
        endDate = new Date();
        
        Thread.sleep(2000);
        // Test 1.to test if fields are injected
        ce.streamLog(jobId, new StringWriter(), new HashMap<String, String[]>());
        DummyXLogStreamingService service = (DummyXLogStreamingService) services.get(XLogStreamingService.class);
        XLogFilter filter = service.filter;
        assertEquals(filter.getFilterParams().get(DagXLogInfoService.JOB), jobId);
        assertTrue(endDate.before(service.endTime));


        // Test2
        // * Test method org.apache.oozie.CoordinatorEngine.streamLog(String,
        // String,
        // * String, Writer) with null 2nd and 3rd arguments.
        // */
        ce.streamLog(jobId, null, null, new StringWriter(), new HashMap<String, String[]>());
        service = (DummyXLogStreamingService) services.get(XLogStreamingService.class);
        filter = service.filter;
        assertEquals(filter.getFilterParams().get(DagXLogInfoService.JOB), jobId);

        // Test 3
        // * Test method org.apache.oozie.CoordinatorEngine.streamLog(String,
        // String,
        // * String, Writer) with RestConstants.JOB_LOG_ACTION and non-null 2nd
        // * argument.

        ce.streamLog(jobId, "1, 3-4, 6", RestConstants.JOB_LOG_ACTION, new StringWriter(),
                new HashMap<String, String[]>());

        service = (DummyXLogStreamingService) services.get(XLogStreamingService.class);
        filter = service.filter;
        assertEquals(jobId, filter.getFilterParams().get(DagXLogInfoService.JOB));
        assertEquals("(" + jobId + "@1|" + jobId + "@3|" + jobId + "@4|" + jobId + "@6)",
                filter.getFilterParams().get(DagXLogInfoService.ACTION));

        // Test 4. testing with date range
        long middle = (createdDate.getTime() + endDate.getTime()) / 2;
        Date middleDate = new Date(middle);
        ce.streamLog(jobId, DateUtils.formatDateOozieTZ(createdDate) + "::" + DateUtils.formatDateOozieTZ(middleDate)
                + "," + DateUtils.formatDateOozieTZ(middleDate) + "::" + DateUtils.formatDateOozieTZ(endDate),
                RestConstants.JOB_LOG_DATE, new StringWriter(), new HashMap<String, String[]>());
        service = (DummyXLogStreamingService) services.get(XLogStreamingService.class);
        filter = service.filter;
        assertEquals(jobId, filter.getFilterParams().get(DagXLogInfoService.JOB));
        final String action = filter.getFilterParams().get(DagXLogInfoService.ACTION);
        assertEquals("(" + jobId + "@1|" + jobId + "@2|" + jobId + "@3|" + jobId + "@4|" + jobId + "@5|" + jobId
                + "@6)", action);

        // Test 5 testing with action list range
        ce.streamLog(jobId, "2-4", RestConstants.JOB_LOG_ACTION, new StringWriter(), new HashMap<String, String[]>());
        service = (DummyXLogStreamingService) services.get(XLogStreamingService.class);
        assertEquals(list.get(1).getCreatedTime(), service.startTime);
        assertEquals(list.get(3).getLastModifiedTime(), service.endTime);

        // Test 6, testing with 1 action list
        ce.streamLog(jobId, "5", RestConstants.JOB_LOG_ACTION, new StringWriter(), new HashMap<String, String[]>());
        service = (DummyXLogStreamingService) services.get(XLogStreamingService.class);
        assertEquals(list.get(4).getCreatedTime(), service.startTime);
        assertEquals(list.get(4).getLastModifiedTime(), service.endTime);

        // Test 7, testing with 1 action list + range
        ce.streamLog(jobId, "1,2-4,5", RestConstants.JOB_LOG_ACTION, new StringWriter(),
                new HashMap<String, String[]>());
        service = (DummyXLogStreamingService) services.get(XLogStreamingService.class);
        assertEquals(list.get(0).getCreatedTime(), service.startTime);
        assertEquals(list.get(4).getLastModifiedTime(), service.endTime);

        // Test 8, testing with out order range
        ce.streamLog(jobId, "5,3-4,1", RestConstants.JOB_LOG_ACTION, new StringWriter(),
                new HashMap<String, String[]>());
        service = (DummyXLogStreamingService) services.get(XLogStreamingService.class);
        assertEquals(list.get(0).getCreatedTime(), service.startTime);
        assertEquals(list.get(4).getLastModifiedTime(), service.endTime);


        // Test 9, testing with date range
        ce.streamLog(
                jobId,
                DateUtils.formatDateOozieTZ(list.get(1).getCreatedTime()) + "::"
                        + DateUtils.formatDateOozieTZ(list.get(4).getLastModifiedTime()) + ",",
                RestConstants.JOB_LOG_DATE, new StringWriter(), new HashMap<String, String[]>());
        service = (DummyXLogStreamingService) services.get(XLogStreamingService.class);
        assertEquals(list.get(1).getCreatedTime().toString(), service.startTime.toString());
        assertEquals(list.get(4).getLastModifiedTime().toString(), service.endTime.toString());

        // Test 10, testing with multiple date range
        ce.streamLog(
                jobId,
                DateUtils.formatDateOozieTZ(list.get(1).getCreatedTime()) + "::"
                        + DateUtils.formatDateOozieTZ(list.get(2).getLastModifiedTime()) + ","
                        + DateUtils.formatDateOozieTZ(list.get(3).getCreatedTime()) + "::"
                        + DateUtils.formatDateOozieTZ(list.get(5).getLastModifiedTime()), RestConstants.JOB_LOG_DATE,
                new StringWriter(), new HashMap<String, String[]>());
        service = (DummyXLogStreamingService) services.get(XLogStreamingService.class);
        assertEquals(list.get(1).getCreatedTime().toString(), service.startTime.toString());
        assertEquals(list.get(5).getLastModifiedTime().toString(), service.endTime.toString());

        // Test 11, testing -scope option with Max Count
        Services.get().getConf().setInt(CoordinatorEngine.COORD_ACTIONS_LOG_MAX_COUNT, 1);
        ce = createCoordinatorEngine();
        try {
            ce.streamLog(jobId, "1-3", RestConstants.JOB_LOG_ACTION, new StringWriter(), new HashMap<String, String[]>());
        } catch (XException e){
            assertEquals(e.getErrorCode(), ErrorCode.E0302);
            assertTrue(e.getMessage().indexOf("Retrieving log of too many coordinator actions") != -1);
        }

        // Test 12, testing -date option with Max Count
        try {
            ce.streamLog(jobId, DateUtils.formatDateOozieTZ(createdDate) + "::" + DateUtils.formatDateOozieTZ(endDate),
                RestConstants.JOB_LOG_DATE, new StringWriter(),new HashMap<String, String[]>());
        } catch (XException e) {
            assertEquals(e.getErrorCode(), ErrorCode.E0302);
            assertTrue(e.getMessage().indexOf("Retrieving log of too many coordinator actions") != -1);
        }
    }

    private String runJobsImpl(final CoordinatorEngine ce, int count) throws Exception {
        try {
            Services.get().getConf().setBoolean("oozie.service.coord.check.maximum.frequency", false);
            services.setService(DummyXLogStreamingService.class);
            // need to re-define the parameters that are cleared upon the service
            // reset:
            new DagXLogInfoService().init(services);

            Configuration conf = new XConfiguration();

            final String appPath = getTestCaseFileUri("coordinator.xml");
            final long now = System.currentTimeMillis();
            final String start = DateUtils.formatDateOozieTZ(new Date(now));
            long e = now + 1000 * 60 * count;
            final String end = DateUtils.formatDateOozieTZ(new Date(e));

            String wfXml = IOUtils.getResourceAsString("wf-no-op.xml", -1);
            writeToFile(wfXml, getFsTestCaseDir(), "workflow.xml");

            String appXml = "<coordinator-app name=\"NAME\" frequency=\"${coord:minutes(1)}\" start=\"" + start
                    + "\" end=\"" + end + "\" timezone=\"UTC\" " + "xmlns=\"uri:oozie:coordinator:0.1\"> " + "<controls> "
                    + "  <timeout>1</timeout> " + "  <concurrency>1</concurrency> " + "  <execution>LIFO</execution> "
                    + "</controls> " + "<action> " + "  <workflow> " + "  <app-path>" + getFsTestCaseDir()
                    + "/workflow.xml</app-path>"
                    + "  <configuration> <property> <name>inputA</name> <value>valueA</value> </property> "
                    + "  <property> <name>inputB</name> <value>valueB</value> " + "  </property></configuration> "
                    + "</workflow>" + "</action> " + "</coordinator-app>";
            writeToFile(appXml, appPath);
            conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
            conf.set(OozieClient.USER_NAME, getTestUser());

            final String jobId = ce.submitJob(conf, true);
            waitFor(1000 * 60 * count, new Predicate() {
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
            // Assert all the actions are succeeded (useful for waitFor() timeout
            // case):
            final List<CoordinatorAction> actions = ce.getCoordJob(jobId).getActions();
            for (CoordinatorAction action : actions) {
                assertEquals(CoordinatorAction.Status.SUCCEEDED, action.getStatus());
            }
            return jobId;
        } finally {
            Services.get().getConf().setBoolean("oozie.service.coord.check.maximum.frequency", true);
        }
    }

    private void writeToFile(String content, Path appPath, String fileName) throws IOException {
        FileSystem fs = getFileSystem();
        Writer writer = new OutputStreamWriter(fs.create(new Path(appPath, fileName), true));
        writer.write(content);
        writer.close();
    }
}
