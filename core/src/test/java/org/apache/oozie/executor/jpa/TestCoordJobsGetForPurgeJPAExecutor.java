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

package org.apache.oozie.executor.jpa;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;

import org.apache.hadoop.fs.Path;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.CoordinatorJob.Execution;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;

public class TestCoordJobsGetForPurgeJPAExecutor extends XFsTestCase {
    Services services;

    /* (non-Javadoc)
     * @see org.apache.oozie.test.XFsTestCase#setUp()
     */
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        LocalOozie.start();

    }

    /* (non-Javadoc)
     * @see org.apache.oozie.test.XFsTestCase#tearDown()
     */
    @Override
    protected void tearDown() throws Exception {
        LocalOozie.stop();
        super.tearDown();
    }

    public void testCoordJobsGetForPurgeJPAExecutorWithParent() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        String jobId1 = "00001-" + new Date().getTime() + "-TestCoordJobsGetForPurgeJPAExecutor-C";
        insertJob(jobId1, CoordinatorJob.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-01-01T01:00Z"));
        String jobId2 = "00002-" + new Date().getTime() + "-TestCoordJobsGetForPurgeJPAExecutor-C";
        CoordinatorJobBean job2 = insertJob(jobId2, CoordinatorJob.Status.SUCCEEDED,
                DateUtils.parseDateOozieTZ("2011-01-01T01:00Z"));
        job2.setBundleId("some_bundle_parent_id");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB_BUNDLEID, job2);

        CoordJobsGetForPurgeJPAExecutor executor = new CoordJobsGetForPurgeJPAExecutor(10, 50);
        List<String> jobList = jpaService.execute(executor);
        // job2 shouldn't be in the list because it has a parent
        assertEquals(1, jobList.size());
        assertEquals(jobId1, jobList.get(0));
    }

    public void testCoordJobsGetForPurgeJPAExecutorTooMany() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        String jobId1 = "00001-" + new Date().getTime() + "-TestCoordJobsGetForPurgeJPAExecutor-C";
        insertJob(jobId1, CoordinatorJob.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-01-01T01:00Z"));
        String jobId2 = "00002-" + new Date().getTime() + "-TestCoordJobsGetForPurgeJPAExecutor-C";
        insertJob(jobId2, CoordinatorJob.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-01-01T01:00Z"));
        String jobId3 = "00003-" + new Date().getTime() + "-TestCoordJobsGetForPurgeJPAExecutor-C";
        insertJob(jobId3, CoordinatorJob.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-01-01T01:00Z"));
        String jobId4 = "00004-" + new Date().getTime() + "-TestCoordJobsGetForPurgeJPAExecutor-C";
        insertJob(jobId4, CoordinatorJob.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-01-01T01:00Z"));
        String jobId5 = "00005-" + new Date().getTime() + "-TestCoordJobsGetForPurgeJPAExecutor-C";
        insertJob(jobId5, CoordinatorJob.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-01-01T01:00Z"));

        List<String> list = new ArrayList<String>();
        // Get the first 3
        list.addAll(jpaService.execute(new CoordJobsGetForPurgeJPAExecutor(1, 3)));
        assertEquals(3, list.size());
        // Get the next 3 (though there's only 2 more)
        list.addAll(jpaService.execute(new CoordJobsGetForPurgeJPAExecutor(1, 3, 3)));
        assertEquals(5, list.size());
        checkCoordinators(list, jobId1, jobId2, jobId3, jobId4, jobId5);
    }

    private CoordinatorJobBean insertJob(String jobId, CoordinatorJob.Status status, Date d) throws Exception {
        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String appXml = getCoordJobXml(appPath);

        CoordinatorJobBean coordJob = new CoordinatorJobBean();
        coordJob.setId(jobId);
        coordJob.setAppName("COORD-TEST");
        coordJob.setAppPath(appPath.toString());
        coordJob.setStatus(status);
        coordJob.setCreatedTime(d);
        coordJob.setLastModifiedTime(d);
        coordJob.setUser(getTestUser());
        coordJob.setGroup(getTestGroup());

        Properties conf = getCoordConf(appPath);
        String confStr = XmlUtils.writePropToString(conf);

        coordJob.setConf(confStr);
        coordJob.setJobXml(appXml);
        coordJob.setLastActionNumber(0);
        coordJob.setFrequency("1");
        coordJob.setExecutionOrder(Execution.FIFO);
        coordJob.setConcurrency(1);
        try {
            coordJob.setStartTime(DateUtils.parseDateOozieTZ("2009-12-15T01:00Z"));
            coordJob.setEndTime(DateUtils.parseDateOozieTZ("2009-12-17T01:00Z"));
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not set Date/time");
        }

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordJobInsertJPAExecutor coordInsertCmd = new CoordJobInsertJPAExecutor(coordJob);
            jpaService.execute(coordInsertCmd);
        }
        catch (JPAExecutorException ce) {
            ce.printStackTrace();
            fail("Unable to insert the test job record to table");
            throw ce;
        }
        return coordJob;
    }

    private String getCoordJobXml(Path appPath) {
        String inputTemplate = appPath + "/coord-input/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
        inputTemplate = Matcher.quoteReplacement(inputTemplate);
        String outputTemplate = appPath + "/coord-input/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
        outputTemplate = Matcher.quoteReplacement(outputTemplate);
        try {
            Reader reader = IOUtils.getResourceAsReader("coord-job-get.xml", -1);
            String appXml = IOUtils.getReaderAsString(reader, -1);
            appXml = appXml.replaceAll("#inputTemplate", inputTemplate);
            appXml = appXml.replaceAll("#outputTemplate", outputTemplate);
            return appXml;
        }
        catch (IOException ioe) {
            throw new RuntimeException(XLog.format("Could not get coord-rerun-job.xml", ioe));
        }
    }

    private Properties getCoordConf(Path appPath) {
        Path wfAppPath = new Path(getFsTestCaseDir(), "workflow");
        final OozieClient coordClient = LocalOozie.getCoordClient();
        Properties conf = coordClient.createConfiguration();
        conf.setProperty(OozieClient.COORDINATOR_APP_PATH, appPath.toString());
        conf.setProperty("jobTracker", getJobTrackerUri());
        conf.setProperty("nameNode", getNameNodeUri());
        conf.setProperty("wfAppPath", wfAppPath.toString());
        conf.remove("user.name");
        conf.setProperty("user.name", getTestUser());

        return conf;
    }

    private void checkCoordinators(List<String> coords, String... coordJobIDs) {
        assertEquals(coordJobIDs.length, coords.size());
        Arrays.sort(coordJobIDs);
        Collections.sort(coords);

        for (int i = 0; i < coordJobIDs.length; i++) {
            assertEquals(coordJobIDs[i], coords.get(i));
        }
    }
}
