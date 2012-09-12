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
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;

import org.apache.hadoop.fs.Path;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.CoordinatorJob.Execution;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;

public class TestCoordJobsToBeMaterializedJPAExecutor extends XFsTestCase {
    Services services;

    /* (non-Javadoc)
     * @see org.apache.oozie.test.XFsTestCase#setUp()
     */
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        LocalOozie.start();
        cleanUpDBTables();
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.test.XFsTestCase#tearDown()
     */
    @Override
    protected void tearDown() throws Exception {
        LocalOozie.stop();
        super.tearDown();
    }

    public void testCoordJobsToBeMaterializedCommand() throws Exception {
        String jobId = "00000-" + new Date().getTime() + "-TestCoordJobsToBeMaterializedJPAExecutor-C";
        insertJob(jobId, CoordinatorJob.Status.PREP);
        _testCoordJobsToBeMaterialized();
    }

    private void _testCoordJobsToBeMaterialized() {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        try {
            Date d1 = new Date();
            Date d2 = new Date(d1.getTime() + 1000);
            CoordJobsToBeMaterializedJPAExecutor cmatcmd = new CoordJobsToBeMaterializedJPAExecutor(d2, 50);
            List<CoordinatorJobBean> jobList = jpaService.execute(cmatcmd);

            if (jobList.size() == 0) {
                fail("Test of getCoordinatorJobsToBeMaterialized returned no records. Date =" + d2);
            }
            // Assumption: no other older records are there
            d2 = new Date(d1.getTime() - 86400000L * 365L);
            CoordJobsToBeMaterializedJPAExecutor cmatcmdnew = new CoordJobsToBeMaterializedJPAExecutor(d2, 50);
            jobList = jpaService.execute(cmatcmdnew);

            /*
             * if(jobList.size() > 0){ fail("Test of
             * getCoordinatorJobsToBeMaterialized returned some records while
             * expecting no records = " + d2); }
             */

        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Unable to Get Materialized Jobs ");
        }
    }

    private void insertJob(String jobId, CoordinatorJob.Status status) throws Exception {
        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String appXml = getCoordJobXml(appPath);

        CoordinatorJobBean coordJob = new CoordinatorJobBean();
        coordJob.setId(jobId);
        coordJob.setAppName("COORD-TEST");
        coordJob.setAppPath(appPath.toString());
        coordJob.setStatus(status);
        coordJob.setCreatedTime(new Date());
        coordJob.setLastModifiedTime(new Date());
        coordJob.setUser(getTestUser());
        coordJob.setGroup(getTestGroup());
        coordJob.setAuthToken("notoken");

        Properties conf = getCoordConf(appPath);
        String confStr = XmlUtils.writePropToString(conf);

        coordJob.setConf(confStr);
        coordJob.setJobXml(appXml);
        coordJob.setLastActionNumber(0);
        coordJob.setFrequency(1);
        coordJob.setExecution(Execution.FIFO);
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
}
