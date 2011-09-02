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

import java.io.IOException;
import java.util.Date;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.CoordinatorJob.Execution;
import org.apache.oozie.client.CoordinatorJob.Timeunit;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.XConfiguration;

public class TestCoordActionInputCheckXCommand extends CoordXTestCase {
    public void testActionInputCheck() throws Exception {
        String jobId = "0000000-" + new Date().getTime() + "-testActionInputCheck-C";
        addRecordToJobTable(jobId);

        Date startTime = DateUtils.parseDateUTC("2009-02-01T23:59Z");
        Date endTime = DateUtils.parseDateUTC("2009-02-02T23:59Z");
        new CoordActionMaterializeXCommand(jobId, startTime, endTime).call();
        createDir(getTestCaseDir() + "/2009/29/");
        createDir(getTestCaseDir() + "/2009/15/");
        new CoordActionInputCheckXCommand(jobId + "@1").call();
        checkCoordAction(jobId + "@1");
    }

    private void addRecordToJobTable(String jobId) throws CommandException {
        CoordinatorJobBean coordJob = new CoordinatorJobBean();
        coordJob.setId(jobId);
        coordJob.setAppName("testApp");
        coordJob.setAppPath("testAppPath");
        coordJob.setStatus(CoordinatorJob.Status.PREMATER);
        coordJob.setCreatedTime(new Date());
        coordJob.setLastModifiedTime(new Date());
        coordJob.setUser("testUser");
        coordJob.setGroup("testGroup");
        coordJob.setAuthToken("notoken");
        coordJob.setTimeZone("UTC");
        coordJob.setTimeUnit(Timeunit.DAY);

        String testDir = getTestCaseDir();

        XConfiguration jobConf = new XConfiguration();
        jobConf.set(OozieClient.USER_NAME, getTestUser());
        jobConf.set(OozieClient.GROUP_NAME, getTestGroup());
        injectKerberosInfo(jobConf);
        String confStr = jobConf.toXmlString(false);
        coordJob.setConf(confStr);
        String appXml = "<coordinator-app xmlns='uri:oozie:coordinator:0.1' name='NAME' frequency=\"1\" start='2009-02-01T01:00Z' end='2009-02-03T23:59Z' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        appXml += "<controls>";
        appXml += "<timeout>10</timeout>";
        appXml += "<concurrency>2</concurrency>";
        appXml += "<execution>LIFO</execution>";
        appXml += "</controls>";
        appXml += "<input-events>";
        appXml += "<data-in name='A' dataset='a'>";
        appXml += "<dataset name='a' frequency='7' initial-instance='2009-01-01T01:00Z' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        appXml += "<uri-template>file://" + testDir + "/${YEAR}/${DAY}</uri-template>";
        appXml += "</dataset>";
        appXml += "<start-instance>${coord:current(-3)}</start-instance>";
        appXml += "<end-instance>${coord:current(0)}</end-instance>";
        appXml += "</data-in>";
        appXml += "</input-events>";
        appXml += "<output-events>";
        appXml += "<data-out name='LOCAL_A' dataset='local_a'>";
        appXml += "<dataset name='local_a' frequency='7' initial-instance='2009-01-01T01:00Z' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        appXml += "<uri-template>file://" + testDir + "/${YEAR}/${DAY}</uri-template>";
        appXml += "</dataset>";
        appXml += "<start-instance>${coord:current(-3)}</start-instance>";
        appXml += "<instance>${coord:current(0)}</instance>";
        appXml += "</data-out>";
        appXml += "</output-events>";
        appXml += "<action>";
        appXml += "<workflow>";
        appXml += "<app-path>hdfs:///tmp/workflows/</app-path>";
        appXml += "<configuration>";
        appXml += "<property>";
        appXml += "<name>inputA</name>";
        appXml += "<value>${coord:dataIn('A')}</value>";
        appXml += "</property>";
        appXml += "<property>";
        appXml += "<name>inputB</name>";
        appXml += "<value>${coord:dataOut('LOCAL_A')}</value>";
        appXml += "</property>";
        appXml += "</configuration>";
        appXml += "</workflow>";
        appXml += "</action>";
        appXml += "</coordinator-app>";
        coordJob.setJobXml(appXml);
        coordJob.setLastActionNumber(0);
        coordJob.setFrequency(1);
        coordJob.setExecution(Execution.FIFO);
        coordJob.setConcurrency(1);
        try {
            coordJob.setEndTime(DateUtils.parseDateUTC("2009-02-02T23:59Z"));
            coordJob.setStartTime(DateUtils.parseDateUTC("2009-02-01T23:59Z"));
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not set Date/time");
        }

        JPAService jpaService = Services.get().get(JPAService.class);
        if (jpaService != null) {
            try {
                jpaService.execute(new CoordJobInsertJPAExecutor(coordJob));
            }
            catch (JPAExecutorException e) {
                throw new CommandException(e);
            }
        }
        else {
            fail("Unable to insert the test job record to table");
        }
    }

    private void checkCoordAction(String actionId) {
        try {
            CoordinatorActionBean action = jpaService.execute(new CoordActionGetJPAExecutor(actionId));
            System.out.println("missingDeps " + action.getMissingDependencies() + " Xml " + action.getActionXml());
            if (action.getMissingDependencies().indexOf("/2009/29/") >= 0) {
                fail("directory should be resolved :" + action.getMissingDependencies());
            }
            if (action.getMissingDependencies().indexOf("/2009/15/") < 0) {
                fail("directory should NOT be resolved :" + action.getMissingDependencies());
            }
        }
        catch (JPAExecutorException se) {
            fail("Action ID " + actionId + " was not stored properly in db");
        }
    }

    private void createDir(String dir) {
        Process pr;
        try {
            pr = Runtime.getRuntime().exec("mkdir -p " + dir + "/_SUCCESS");
            pr.waitFor();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
