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
package org.apache.oozie.command.coord;

import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.CoordinatorJob.Execution;
import org.apache.oozie.client.CoordinatorJob.Timeunit;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.CallableQueueService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.XLogService;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;

public class TestCoordActionInputCheckXCommand extends XDataTestCase {
    protected Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        setSystemProperty(XLogService.LOG4J_FILE, "oozie-log4j.properties");
        services = new Services();
        services.init();
        cleanUpDBTables();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public class MyCoordActionInputCheckXCommand extends CoordActionInputCheckXCommand {
        long executed = 0;
        int wait;

        public MyCoordActionInputCheckXCommand(String actionId, int wait, String entityKey) {
            super(actionId, entityKey);
            this.wait = wait;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Type:").append(getType());
            sb.append(",Priority:").append(getPriority());
            return sb.toString();
        }

        @Override
        protected Void execute() throws CommandException {
            try {
                Thread.sleep(wait);
            }
            catch (InterruptedException e) {
            }
            executed = System.currentTimeMillis();
            return null;
        }

    }

    public void testCoordActionInputCheckXCommandUniqueness() throws Exception {
        Date startTime = DateUtils.parseDateUTC("2009-02-01T23:59Z");
        Date endTime = DateUtils.parseDateUTC("2009-02-02T23:59Z");
        CoordinatorJobBean job = addRecordToCoordJobTableForWaiting("coord-job-for-action-input-check.xml",
                CoordinatorJob.Status.RUNNING, startTime, endTime, false, true, 3);

        CoordinatorActionBean action1 = addRecordToCoordActionTableForWaiting(job.getId(), 1,
                CoordinatorAction.Status.WAITING, "coord-action-for-action-input-check.xml");

        createDir(getTestCaseDir() + "/2009/29/");
        createDir(getTestCaseDir() + "/2009/22/");
        createDir(getTestCaseDir() + "/2009/15/");
        createDir(getTestCaseDir() + "/2009/08/");

        final MyCoordActionInputCheckXCommand callable1 = new MyCoordActionInputCheckXCommand(action1.getId(), 100, "1");
        final MyCoordActionInputCheckXCommand callable2 = new MyCoordActionInputCheckXCommand(action1.getId(), 100, "2");
        final MyCoordActionInputCheckXCommand callable3 = new MyCoordActionInputCheckXCommand(action1.getId(), 100, "3");

        List<MyCoordActionInputCheckXCommand> callables = Arrays.asList(callable1, callable2, callable3);

        CallableQueueService queueservice = services.get(CallableQueueService.class);

        for (MyCoordActionInputCheckXCommand c : callables) {
            queueservice.queue(c);
        }

        waitFor(200, new Predicate() {
            public boolean evaluate() throws Exception {
                return callable1.executed != 0 && callable2.executed == 0 && callable3.executed == 0;
            }
        });

        assertTrue(callable1.executed != 0);
        assertTrue(callable2.executed == 0);
        assertTrue(callable3.executed == 0);
    }

    public void testActionInputCheck() throws Exception {
        String jobId = "0000000-" + new Date().getTime() + "-TestCoordActionInputCheckXCommand-C";
        Date startTime = DateUtils.parseDateUTC("2009-02-01T23:59Z");
        Date endTime = DateUtils.parseDateUTC("2009-02-02T23:59Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(jobId, startTime, endTime);
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
        createDir(getTestCaseDir() + "/2009/29/");
        createDir(getTestCaseDir() + "/2009/15/");
        new CoordActionInputCheckXCommand(job.getId() + "@1", job.getId()).call();
        checkCoordAction(job.getId() + "@1");
    }

    /**
     * This test case verifies if getCoordInputCheckRequeueInterval picks up the
     * overridden value. In reality, the value could be overridden in
     * oozie-site.xml.
     *
     * @throws Exception
     */
    public void testRequeueInterval() throws Exception {
        /*
         * Create a dummy Coordinator Job to pass to
         * CoordActionInputCheckXCommand constructor.
         */
        String jobId = "0000000-" + new Date().getTime() + "-TestCoordActionInputCheckXCommand-C";
        Date startTime = DateUtils.parseDateUTC("2009-02-01T23:59Z");
        Date endTime = DateUtils.parseDateUTC("2009-02-02T23:59Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(jobId, startTime, endTime);
        /* Override the property value for testing purpose only. */
        long testedValue = 12000;
        Services.get().getConf().setLong(CoordActionInputCheckXCommand.CONF_COORD_INPUT_CHECK_REQUEUE_INTERVAL,
                testedValue);

        CoordActionInputCheckXCommand caicc = new CoordActionInputCheckXCommand(job.getId() + "@1", job.getId());

        long effectiveValue = caicc.getCoordInputCheckRequeueInterval();
        // Verify if two values are same.
        assertEquals(testedValue, effectiveValue);
    }

    protected CoordinatorJobBean addRecordToCoordJobTableForWaiting(String testFileName, CoordinatorJob.Status status,
            Date start, Date end, boolean pending, boolean doneMatd, int lastActionNum) throws Exception {

        String testDir = getTestCaseDir();
        CoordinatorJobBean coordJob = createCoordJob(testFileName, status, start, end, pending, doneMatd, lastActionNum);
        String appXml = getCoordJobXmlForWaiting(testFileName, testDir);
        coordJob.setJobXml(appXml);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordJobInsertJPAExecutor coordInsertCmd = new CoordJobInsertJPAExecutor(coordJob);
            jpaService.execute(coordInsertCmd);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test coord job record to table");
            throw je;
        }

        return coordJob;
    }

    protected String getCoordJobXmlForWaiting(String testFileName, String testDir) {
        try {
            Reader reader = IOUtils.getResourceAsReader(testFileName, -1);
            String appXml = IOUtils.getReaderAsString(reader, -1);
            appXml = appXml.replaceAll("#testDir", testDir);
            return appXml;
        }
        catch (IOException ioe) {
            throw new RuntimeException(XLog.format("Could not get " + testFileName, ioe));
        }
    }

    protected CoordinatorActionBean addRecordToCoordActionTableForWaiting(String jobId, int actionNum,
            CoordinatorAction.Status status, String resourceXmlName) throws Exception {
        CoordinatorActionBean action = createCoordAction(jobId, actionNum, status, resourceXmlName, 0);
        String testDir = getTestCaseDir();
        String missDeps = "file://#testDir/2009/29/_SUCCESS#file://#testDir/2009/22/_SUCCESS#file://#testDir/2009/15/_SUCCESS#file://#testDir/2009/08/_SUCCESS";
        missDeps = missDeps.replaceAll("#testDir", testDir);
        action.setMissingDependencies(missDeps);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordActionInsertJPAExecutor coordActionInsertCmd = new CoordActionInsertJPAExecutor(action);
            jpaService.execute(coordActionInsertCmd);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test coord action record to table");
            throw je;
        }
        return action;
    }

    private CoordinatorJobBean addRecordToCoordJobTable(String jobId, Date start, Date end) throws CommandException {
        CoordinatorJobBean coordJob = new CoordinatorJobBean();
        coordJob.setId(jobId);
        coordJob.setAppName("testApp");
        coordJob.setAppPath("testAppPath");
        coordJob.setStatus(CoordinatorJob.Status.RUNNING);
        coordJob.setCreatedTime(new Date());
        coordJob.setLastModifiedTime(new Date());
        coordJob.setUser("testUser");
        coordJob.setGroup("testGroup");
        coordJob.setAuthToken("notoken");
        coordJob.setTimeZone("UTC");
        coordJob.setTimeUnit(Timeunit.DAY);
        coordJob.setMatThrottling(2);
        try {
            coordJob.setStartTime(start);
            coordJob.setEndTime(end);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not set Date/time");
        }

        String testDir = getTestCaseDir();

        XConfiguration jobConf = new XConfiguration();
        jobConf.set(OozieClient.USER_NAME, getTestUser());

        String confStr = jobConf.toXmlString(false);
        coordJob.setConf(confStr);
        String appXml = "<coordinator-app xmlns='uri:oozie:coordinator:0.2' name='NAME' frequency=\"1\" start='2009-02-01T01:00Z' end='2009-02-03T23:59Z' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
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
        return coordJob;
    }

    private void checkCoordAction(String actionId) {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
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
