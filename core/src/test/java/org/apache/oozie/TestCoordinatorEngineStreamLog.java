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

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.service.DagXLogInfoService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.XLogStreamingService;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.XLogFilter;
import org.apache.oozie.util.XLogStreamer;

public class TestCoordinatorEngineStreamLog extends XDataTestCase {
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

    static class DummyXLogStreamingService extends XLogStreamingService {
        XLogFilter filter;
        Date startTime;
        Date endTime;

        @Override
        public void streamLog(XLogStreamer logStreamer, Date startTime, Date endTime, Writer writer)
                throws IOException {
            filter = logStreamer.getXLogFilter();
            this.startTime = startTime;
            this.endTime = endTime;
        }
    }

    private CoordinatorEngine createCoordinatorEngine() {
        return new CoordinatorEngine(getTestUser());
    }

    public void testCoordLogStreaming() throws Exception {
        services.setService(DummyXLogStreamingService.class);
        new DagXLogInfoService().init(services);
        CoordinatorEngine ce = createCoordinatorEngine();
        final String jobId = createJobs(6);

        CoordinatorJobBean cjb = ce.getCoordJob(jobId);
        Date createdDate = cjb.getCreatedTime();
        Date endDate = cjb.getEndTime();
        assertTrue(endDate.after(createdDate));

        List<CoordinatorAction> list = cjb.getActions();
        Collections.sort(list, new Comparator<CoordinatorAction>() {
            public int compare(CoordinatorAction a, CoordinatorAction b) {
                return a.getId().compareTo(b.getId());
            }
        });

        // Test 1.to test if fields are injected
        ce.streamLog(jobId, new StringWriter(), new HashMap<String, String[]>());
        DummyXLogStreamingService service = (DummyXLogStreamingService) services.get(XLogStreamingService.class);
        XLogFilter filter = service.filter;
        assertEquals(filter.getFilterParams().get(DagXLogInfoService.JOB), jobId);
        assertEquals(cjb.getCreatedTime(), service.startTime);
        assertEquals(cjb.getLastModifiedTime(), service.endTime);


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

    private String createJobs(int numActions) throws Exception {
        long time = System.currentTimeMillis();
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, true);
        job.setCreatedTime(new Date(time));
        time += 1000 * 60;
        for (int i = 1; i <= numActions; i++) {
            CoordinatorActionBean action = createCoordAction(job.getId(), i, CoordinatorAction.Status.SUCCEEDED,
                    "coord-action-get.xml", 0, new Date(time));
            action.setCreatedTime(new Date(time));
            time += 1000 * 60;
            action.setLastModifiedTime(new Date(time));
            time += 1000 * 60;
            addRecordToCoordActionTable(action, null);
        }
        job.setEndTime(new Date(time));
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQueryExecutor.CoordJobQuery.UPDATE_COORD_JOB, job);
        return job.getId();
    }
}
