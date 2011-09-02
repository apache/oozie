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
package org.apache.oozie.command.coord;

import java.sql.Timestamp;
import java.util.Date;

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.util.DateUtils;

public class TestCoordJobMatLookupXCommand extends CoordXTestCase {

    public void testMatLookupCommand1() throws Exception {
        String jobId = "0000000-" + new Date().getTime()
                + "-testMatLookupCommand-C";
        
        Date start = DateUtils.parseDateUTC("2009-02-01T01:00Z");
        Date end = DateUtils.parseDateUTC("2009-02-03T23:59Z");
        addRecordToJobTable(jobId, start, end);
        new CoordJobMatLookupXCommand(jobId, 3600).call();
        checkCoordJobs(jobId, CoordinatorJob.Status.PREMATER);
    }
    
    // Test a coordinator job that will run in far future,
    // materialization should not happen.
    public void testMatLookupCommand2() throws Exception {
        String jobId = "0000000-" + new Date().getTime()
                + "-testMatLookupCommand-C";
        
        Date start = DateUtils.parseDateUTC("2099-02-01T01:00Z");
        Date end = DateUtils.parseDateUTC("2099-02-03T23:59Z");
        addRecordToJobTable(jobId, start, end);
        new CoordJobMatLookupXCommand(jobId, 3600).call();
        checkCoordJobs(jobId, CoordinatorJob.Status.PREP);
    }
    
    // Test a coordinator job that will run within 5 minutes from now,
    // materilization should happen.
    public void testMatLookupCommand3() throws Exception {
        String jobId = "0000000-" + new Date().getTime()
                + "-testMatLookupCommand-C";
        
        Date start = DateUtils.toDate(new Timestamp(System.currentTimeMillis() + 180 * 1000));
        Date end = DateUtils.parseDateUTC("2099-02-03T23:59Z");
        addRecordToJobTable(jobId, start, end);
        new CoordJobMatLookupXCommand(jobId, 3600).call();
        checkCoordJobs(jobId, CoordinatorJob.Status.PREMATER);
    }
    
    // Test a coordinator job that will run beyond 5 minutes from now,
    // materilization should not happen.
    public void testMatLookupCommand4() throws Exception {
        String jobId = "0000000-" + new Date().getTime()
                + "-testMatLookupCommand-C";
        
        Date start = DateUtils.toDate(new Timestamp(System.currentTimeMillis() + 360 * 1000));
        Date end = DateUtils.parseDateUTC("2099-02-03T23:59Z");
        addRecordToJobTable(jobId, start, end);
        new CoordJobMatLookupXCommand(jobId, 3600).call();
        checkCoordJobs(jobId, CoordinatorJob.Status.PREP);
    }

    private void checkCoordJobs(String jobId, CoordinatorJob.Status expectedStatus) {
        try {
            CoordinatorJobBean job = jpaService.execute(new CoordJobGetJPAExecutor(jobId));
            if (job.getStatus() != expectedStatus) {
                fail("CoordJobMatLookupCommand didn't work because the status for job id"
                        + jobId + " is : " + job.getStatusStr() + "; however expected status is : " + expectedStatus.toString());
            }
        }
        catch (JPAExecutorException se) {
            fail("Job ID " + jobId + " was not stored properly in db");
        }
    }

    private void addRecordToJobTable(String jobId, Date start, Date end) throws CommandException {
        CoordinatorJobBean coordJob = new CoordinatorJobBean();
        coordJob.setId(jobId);
        coordJob.setAppName("testApp");
        coordJob.setAppPath("testAppPath");
        coordJob.setStatus(CoordinatorJob.Status.PREP);
        coordJob.setCreatedTime(new Date()); // TODO: Do we need that?
        coordJob.setLastModifiedTime(new Date());
        coordJob.setUser("testUser");
        coordJob.setGroup("testGroup");

        String confStr = "<configuration></configuration>";
        coordJob.setConf(confStr);
        String startDateStr = null, endDateStr = null;
        try {
            startDateStr = DateUtils.formatDateUTC(start);
            endDateStr = DateUtils.formatDateUTC(end);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Could not format dates");   
        }
        String appXml = "<coordinator-app xmlns='uri:oozie:coordinator:0.1' name='NAME' frequency=\"1\" start='" + startDateStr + "' end='" + endDateStr + "'";
        
        appXml += "<controls>";
        appXml += "<timeout>10</timeout>";
        appXml += "<concurrency>2</concurrency>";
        appXml += "<execution>LIFO</execution>";
        appXml += "</controls>";
        appXml += "<action>";
        appXml += "<workflow>";
        appXml += "<app-path>hdfs:///tmp/workflows/</app-path>";
        appXml += "</workflow>";
        appXml += "</action>";
        appXml += "</coordinator-app>";
        coordJob.setJobXml(appXml);
        coordJob.setLastActionNumber(0);
        coordJob.setFrequency(1);
        try {
            coordJob.setStartTime(start);
            coordJob.setEndTime(end);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not set Date/time");
        }

        try {
            jpaService.execute(new CoordJobInsertJPAExecutor(coordJob));
        }
        catch (JPAExecutorException ex) {
            ex.printStackTrace();
            fail("Unable to insert the test job record to table");
        }
    }

}