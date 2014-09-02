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
import java.io.StringReader;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.executor.jpa.CoordJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class TestCoordCommandUtils extends XDataTestCase {
    protected Services services;

    protected String getProcessingTZ() {
        return DateUtils.OOZIE_PROCESSING_TIMEZONE_DEFAULT;
    }

    private String hcatServer;

    @Before
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        setSystemProperty(DateUtils.OOZIE_PROCESSING_TIMEZONE_KEY, getProcessingTZ());
        services = super.setupServicesForHCatalog();
        services.init();
        hcatServer = getMetastoreAuthority();
    }

    @After
    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    @Test
    public void testDryRunPushDependencies() {
        try {
            CoordinatorJobBean job = addRecordToCoordJobTableForWaiting("coord-job-for-action-input-check.xml",
                    CoordinatorJob.Status.RUNNING, false, true);
            Path appPath = new Path(getFsTestCaseDir(), "coord");
            // actionXml only to check whether coord conf got resolved or not
            String actionXml = getCoordActionXml(appPath, "coord-action-for-action-input-check.xml");
            CoordinatorActionBean actionBean = createCoordinatorActionBean(job);

            String db = "default";
            String table = "tablename";
            String hcatDependency = getPushMissingDependencies(db, table);
            actionBean.setPushMissingDependencies(hcatDependency);

            Element eAction = createActionElement(actionXml);
            String newactionXml = CoordCommandUtils.dryRunCoord(eAction, actionBean);
            eAction = XmlUtils.parseXml(newactionXml);

            Element configElem = eAction.getChild("action", eAction.getNamespace())
                    .getChild("workflow", eAction.getNamespace()).getChild("configuration", eAction.getNamespace());
            List<?> elementList = configElem.getChildren("property", configElem.getNamespace());
            Element e1 = (Element) elementList.get(0);
            Element e2 = (Element) elementList.get(1);
            // Make sure conf is not resolved as dependencies are not met
            assertEquals("${coord:dataIn('A')}", e1.getChild("value", e1.getNamespace()).getValue());
            assertEquals("${coord:dataOut('LOCAL_A')}", e2.getChild("value", e2.getNamespace()).getValue());

            // Make the dependencies available
            populateTable(db, table);
            newactionXml = CoordCommandUtils.dryRunCoord(eAction, actionBean);

            eAction = XmlUtils.parseXml(newactionXml);
            configElem = eAction.getChild("action", eAction.getNamespace())
                    .getChild("workflow", eAction.getNamespace()).getChild("configuration", eAction.getNamespace());
            elementList = configElem.getChildren("property", configElem.getNamespace());
            e1 = (Element) elementList.get(0);
            e2 = (Element) elementList.get(1);
            // Check for resolved conf
            assertEquals(
                    "file://,testDir/2009/29,file://,testDir/2009/22,file://,testDir/2009/15,file://,testDir/2009/08",
                    e1.getChild("value", e1.getNamespace()).getValue());
            assertEquals("file://,testDir/2009/29", e2.getChild("value", e1.getNamespace()).getValue());

        }
        catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

    }

    @Test
    public void testDryRunPullDeps() {

        try {
            CoordinatorJobBean job = addRecordToCoordJobTableForWaiting("coord-job-for-matd-hcat.xml",
                    CoordinatorJob.Status.RUNNING, false, true);

            Path appPath = new Path(getFsTestCaseDir(), "coord");
            // actionXml only to check whether coord conf got resolved or not
            String actionXml = getCoordActionXml(appPath, "coord-action-for-action-input-check.xml");

            CoordinatorActionBean actionBean = createCoordinatorActionBean(job);
            String testDir = getTestCaseDir();
            String missDeps = getPullMissingDependencies(testDir);
            actionBean.setMissingDependencies(missDeps);

            Element eAction = createActionElement(actionXml);

            String newactionXml = CoordCommandUtils.dryRunCoord(eAction, actionBean);

            eAction = XmlUtils.parseXml(newactionXml);
            Element configElem = eAction.getChild("action", eAction.getNamespace())
                    .getChild("workflow", eAction.getNamespace()).getChild("configuration", eAction.getNamespace());
            List<?> elementList = configElem.getChildren("property", configElem.getNamespace());
            Element e1 = (Element) elementList.get(0);
            Element e2 = (Element) elementList.get(1);
            // Make sure conf is not resolved as dependencies are not met
            assertEquals("${coord:dataIn('A')}", e1.getChild("value", e1.getNamespace()).getValue());
            assertEquals("${coord:dataOut('LOCAL_A')}", e2.getChild("value", e2.getNamespace()).getValue());

            // Make the dependencies available
            createDir(testDir + "/2009/29/");
            createDir(testDir + "/2009/22/");
            createDir(testDir + "/2009/15/");
            createDir(testDir + "/2009/08/");
            sleep(1000);

            newactionXml = CoordCommandUtils.dryRunCoord(eAction, actionBean);

            eAction = XmlUtils.parseXml(newactionXml);
            configElem = eAction.getChild("action", eAction.getNamespace())
                    .getChild("workflow", eAction.getNamespace()).getChild("configuration", eAction.getNamespace());
            elementList = configElem.getChildren("property", configElem.getNamespace());
            e1 = (Element) elementList.get(0);
            e2 = (Element) elementList.get(1);
            // Check for resolved conf
            assertEquals(
                    "file://,testDir/2009/29,file://,testDir/2009/22,file://,testDir/2009/15,file://,testDir/2009/08",
                    e1.getChild("value", e1.getNamespace()).getValue());
            assertEquals("file://,testDir/2009/29", e2.getChild("value", e1.getNamespace()).getValue());

        }
        catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

    }

    @Test
    public void testDryRunPullAndPushDeps() {

        try {
            CoordinatorJobBean job = addRecordToCoordJobTableForWaiting("coord-job-for-matd-hcat.xml",
                    CoordinatorJob.Status.RUNNING, false, true);

            Path appPath = new Path(getFsTestCaseDir(), "coord");
            // actionXml only to check whether coord conf got resolved or not
            String actionXml = getCoordActionXml(appPath, "coord-action-for-action-input-check.xml");

            CoordinatorActionBean actionBean = createCoordinatorActionBean(job);
            String testDir = getTestCaseDir();
            String missDeps = getPullMissingDependencies(testDir);
            actionBean.setMissingDependencies(missDeps);

            String db = "default";
            String table = "tablename";
            String hcatDependency = getPushMissingDependencies(db, table);

            actionBean.setPushMissingDependencies(hcatDependency);

            // Make only pull dependencies available
            createDir(getTestCaseDir() + "/2009/29/");
            createDir(getTestCaseDir() + "/2009/22/");
            createDir(getTestCaseDir() + "/2009/15/");
            createDir(getTestCaseDir() + "/2009/08/");
            sleep(1000);

            Element eAction = createActionElement(actionXml);

            String newactionXml = CoordCommandUtils.dryRunCoord(eAction, actionBean);

            eAction = XmlUtils.parseXml(newactionXml);

            Element configElem = eAction.getChild("action", eAction.getNamespace())
                    .getChild("workflow", eAction.getNamespace()).getChild("configuration", eAction.getNamespace());
            List<?> elementList = configElem.getChildren("property", configElem.getNamespace());
            Element e1 = (Element) elementList.get(0);
            Element e2 = (Element) elementList.get(1);
            // Make sure conf is not resolved as pull dependencies are met but
            // push deps are not met
            assertEquals("${coord:dataIn('A')}", e1.getChild("value", e1.getNamespace()).getValue());
            assertEquals("${coord:dataOut('LOCAL_A')}", e2.getChild("value", e2.getNamespace()).getValue());

            populateTable(db, table);
            newactionXml = CoordCommandUtils.dryRunCoord(eAction, actionBean);

            eAction = XmlUtils.parseXml(newactionXml);
            configElem = eAction.getChild("action", eAction.getNamespace())
                    .getChild("workflow", eAction.getNamespace()).getChild("configuration", eAction.getNamespace());
            elementList = configElem.getChildren("property", configElem.getNamespace());
            e1 = (Element) elementList.get(0);
            e2 = (Element) elementList.get(1);
            // Check for resolved conf
            assertEquals(
                    "file://,testDir/2009/29,file://,testDir/2009/22,file://,testDir/2009/15,file://,testDir/2009/08",
                    e1.getChild("value", e1.getNamespace()).getValue());
            assertEquals("file://,testDir/2009/29", e2.getChild("value", e1.getNamespace()).getValue());

        }
        catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    public void testGetNextValidActionTime() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2013-07-18T00:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2013-07-18T01:00Z");

        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime,
                "10,20 * * * *");
        Date actionTime = DateUtils.parseDateOozieTZ("2013-07-18T00:15Z");
        Date expectedDate = DateUtils.parseDateOozieTZ("2013-07-18T00:20Z");
        Date retDate = CoordCommandUtils.getNextValidActionTimeForCronFrequency(actionTime, job);
        assertEquals(expectedDate, retDate);

        job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, "10/20 * * 5-7 4,5");
        actionTime = DateUtils.parseDateOozieTZ("2013-07-18T00:15Z");
        expectedDate = DateUtils.parseDateOozieTZ("2013-07-18T00:30Z");
        retDate = CoordCommandUtils.getNextValidActionTimeForCronFrequency(actionTime, job);
        assertEquals(expectedDate, retDate);

        job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, "20-30 * 20 5-7 4,5");
        actionTime = DateUtils.parseDateOozieTZ("2013-07-18T00:20Z");
        expectedDate = DateUtils.parseDateOozieTZ("2013-07-18T00:21Z");
        retDate = CoordCommandUtils.getNextValidActionTimeForCronFrequency(actionTime, job);
        assertEquals(expectedDate, retDate);

        job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, "30 * 20 5-7 ?");
        actionTime = DateUtils.parseDateOozieTZ("2013-07-18T00:20Z");
        expectedDate = DateUtils.parseDateOozieTZ("2013-07-20T00:30Z");
        retDate = CoordCommandUtils.getNextValidActionTimeForCronFrequency(actionTime, job);
        assertEquals(expectedDate, retDate);

        job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, "0 9-16 * * 2-6");
        actionTime = DateUtils.parseDateOozieTZ("2013-07-20T00:20Z");
        expectedDate = DateUtils.parseDateOozieTZ("2013-07-22T09:00Z");
        retDate = CoordCommandUtils.getNextValidActionTimeForCronFrequency(actionTime, job);
        assertEquals(expectedDate, retDate);
        retDate = CoordCommandUtils.getNextValidActionTimeForCronFrequency(retDate, job);
        expectedDate = DateUtils.parseDateOozieTZ("2013-07-22T10:00Z");
        assertEquals(expectedDate, retDate);

        job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, "20-30 * * 1 *");
        actionTime = DateUtils.parseDateOozieTZ("2013-07-18T00:20Z");
        expectedDate = DateUtils.parseDateOozieTZ("2014-01-01T00:20Z");
        retDate = CoordCommandUtils.getNextValidActionTimeForCronFrequency(actionTime, job);
        assertEquals(expectedDate, retDate);

        job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, "20-30 10 * * MON,WED");
        actionTime = DateUtils.parseDateOozieTZ("2013-07-18T00:20Z");
        expectedDate = DateUtils.parseDateOozieTZ("2013-07-22T10:20Z");
        retDate = CoordCommandUtils.getNextValidActionTimeForCronFrequency(actionTime, job);
        assertEquals(expectedDate, retDate);
    }

    @Test
    public void testCoordAbsolute() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTableForWaiting("coord-dataset-absolute.xml",
                CoordinatorJob.Status.RUNNING, false, true);
        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String actionXml = getCoordActionXml(appPath, "coord-dataset-absolute.xml");
        CoordinatorActionBean actionBean = createCoordinatorActionBean(job);
        Configuration jobConf = new XConfiguration(new StringReader(job.getConf()));
        Element eAction = createActionElement(actionXml);
        jobConf.set("startInstance", "coord:absolute(2009-08-20T01:00Z)");
        jobConf.set("endInstance", "coord:current(2)");
        String output = CoordCommandUtils.materializeOneInstance("jobId", true, eAction,
                DateUtils.parseDateOozieTZ("2009-08-20T01:00Z"), DateUtils.parseDateOozieTZ("2009-08-20T01:00Z"), 1,
                jobConf, actionBean);
        eAction = XmlUtils.parseXml(output);
        List<?> elementList = ((Element) eAction.getChildren("input-events", eAction.getNamespace()).get(0))
                .getChildren();
        Element e1 = (Element) elementList.get(0);
        Element e2 = (Element) elementList.get(1);

        // startInstance = coord:absolute(2009-08-20T01:00Z) which is current(0)
        // and endInstance = coord:current(2).
        assertEquals(e1.getChild("uris", e1.getNamespace()).getTextTrim(),
                "hdfs:///tmp/workflows/2009/09/03;region=us#hdfs:///tmp/workflows/2009/08/27;"
                        + "region=us#hdfs:///tmp/workflows/2009/08/20;region=us");

        // Test parameterized with startInstance =
        // coord:absolute(2009-08-20T01:00Z) which is current (0) and
        // endInstance = coord:current(2)
        assertEquals(e2.getChild("uris", e1.getNamespace()).getTextTrim(),
                "hdfs:///tmp/workflows/2009/09/03;region=us#hdfs:///tmp/workflows/2009/08/27;"
                        + "region=us#hdfs:///tmp/workflows/2009/08/20;region=us");

        // Test when start instance < nominal time. 2009-08-20T01:00Z is
        // current(-3)

        jobConf.set("startInstance", "coord:absolute(2009-08-20T01:00Z)");
        jobConf.set("endInstance", "coord:current(2)");
        eAction = createActionElement(actionXml);
        output = CoordCommandUtils.materializeOneInstance("jobId", true, eAction,
                DateUtils.parseDateOozieTZ("2009-09-10T01:00Z"), DateUtils.parseDateOozieTZ("2009-09-08T01:00Z"), 1,
                jobConf, actionBean);
        eAction = XmlUtils.parseXml(output);
        elementList = ((Element) eAction.getChildren("input-events", eAction.getNamespace()).get(0)).getChildren();
        e1 = (Element) elementList.get(1);
        assertEquals(e1.getChild("uris", e1.getNamespace()).getTextTrim(),
                "hdfs:///tmp/workflows/2009/09/24;region=us#hdfs:///tmp/workflows/2009/09/17;region=us#"
                        + "hdfs:///tmp/workflows/2009/09/10;region=us#hdfs:///tmp/workflows/2009/09/03;region=us#"
                        + "hdfs:///tmp/workflows/2009/08/27;region=us#hdfs:///tmp/workflows/2009/08/20;region=us");

        // // Test when start instance > nominal time. 2009-08-20T01:00Z is
        // current(1)
        jobConf.set("startInstance", "coord:absolute(2009-08-20T01:00Z)");
        jobConf.set("endInstance", "coord:current(2)");
        eAction = createActionElement(actionXml);
        output = CoordCommandUtils.materializeOneInstance("jobId", true, eAction,
                DateUtils.parseDateOozieTZ("2009-08-14T01:00Z"), DateUtils.parseDateOozieTZ("2009-08-14T01:00Z"), 1,
                jobConf, actionBean);
        eAction = XmlUtils.parseXml(output);
        elementList = ((Element) eAction.getChildren("input-events", eAction.getNamespace()).get(0)).getChildren();
        e1 = (Element) elementList.get(1);
        assertEquals(e1.getChild("uris", e1.getNamespace()).getTextTrim(),
                "hdfs:///tmp/workflows/2009/08/27;region=us#hdfs:///tmp/workflows/2009/08/20;region=us");

        try {
            // Test start instance > end instance. start = 2009-08-27T01:00Z
            // which is current(3)
            jobConf.set("startInstance", "coord:absolute(2009-08-27T01:00Z)");
            jobConf.set("endInstance", "coord:current(2)");
            eAction = createActionElement(actionXml);
            output = CoordCommandUtils.materializeOneInstance("jobId", true, eAction,
                    DateUtils.parseDateOozieTZ("2009-08-06T01:00Z"), DateUtils.parseDateOozieTZ("2009-08-16T01:00Z"),
                    1, jobConf, actionBean);
            eAction = XmlUtils.parseXml(output);
            fail("Should throw exception. Start-instance > end-instance ");
        }
        catch (Exception e) {
            assertTrue(e.getCause().getMessage()
                    .contains("start-instance should be equal or earlier than the end-instance"));
        }

        try {
            // Test start instance < initial instance. initial instance =
            // 2009-08-06T01:00Z and start = 2009-07-01T01:00Z
            jobConf.set("startInstance", "coord:absolute(2009-07-01T01:00Z)");
            jobConf.set("endInstance", "coord:current(2)");
            eAction = createActionElement(actionXml);
            output = CoordCommandUtils.materializeOneInstance("jobId", true, eAction,
                    DateUtils.parseDateOozieTZ("2009-08-06T01:00Z"), DateUtils.parseDateOozieTZ("2009-08-16T01:00Z"),
                    1, jobConf, actionBean);
            eAction = XmlUtils.parseXml(output);
            fail("Should throw exception. Start-instance > end-instance ");
        }
        catch (Exception e) {
            assertTrue(e.getCause().getMessage()
                    .contains("intial-instance should be equal or earlier than the start-instance"));
        }

        try {
            // Test Exception. Start-instance as absolute and end-instance as
            // latest.
            jobConf.set("startInstance", "coord:absolute(2009-08-20T01:00Z)");
            jobConf.set("endInstance", "coord:latest(2)");
            eAction = createActionElement(actionXml);
            output = CoordCommandUtils.materializeOneInstance("jobId", true, eAction,
                    DateUtils.parseDateOozieTZ("2009-08-20T01:00Z"), DateUtils.parseDateOozieTZ("2009-08-20T01:00Z"),
                    1, jobConf, actionBean);
            eAction = XmlUtils.parseXml(output);
            fail("Should throw exception. Start-instance is absolute and end-instance is latest");
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains(
                    "Only start-instance as absolute and end-instance as current is supported"));
        }
    }

    protected CoordinatorJobBean addRecordToCoordJobTable(CoordinatorJob.Status status, Date startTime, Date endTime,
            String freq) throws Exception {
        CoordinatorJobBean coordJob = createCoordJob(status, startTime, endTime, false, false, 0);
        coordJob.setStartTime(startTime);
        coordJob.setEndTime(endTime);
        coordJob.setFrequency(freq);
        coordJob.setTimeUnit(CoordinatorJob.Timeunit.MINUTE);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordJobInsertJPAExecutor coordInsertCmd = new CoordJobInsertJPAExecutor(coordJob);
            jpaService.execute(coordInsertCmd);
        }
        catch (JPAExecutorException ex) {
            ex.printStackTrace();
            fail("Unable to insert the test coord job record to table");
            throw ex;
        }

        return coordJob;
    }

    private String getPullMissingDependencies(String testDir) {
        String missDeps = getTestCaseFileUri("2009/29/_SUCCESS") + "#"
                + getTestCaseFileUri("2009/22/_SUCCESS") + "#"
                + getTestCaseFileUri("2009/15/_SUCCESS") + "#"
                + getTestCaseFileUri("2009/08/_SUCCESS") + "#";
        missDeps = missDeps.replaceAll("#testDir", testDir);
        return missDeps;
    }

    private String getPushMissingDependencies(String db, String table) throws Exception {
        String newHCatDependency1 = "hcat://" + hcatServer + "/" + db + "/" + table + "/dt=20120412;country=brazil";
        String newHCatDependency2 = "hcat://" + hcatServer + "/" + db + "/" + table + "/dt=20120430;country=usa";
        String newHCatDependency = newHCatDependency1 + CoordELFunctions.INSTANCE_SEPARATOR + newHCatDependency2;
        dropTable(db, table, true);
        dropDatabase(db, true);
        createDatabase(db);
        createTable(db, table, "dt,country");
        return newHCatDependency;
    }

    private Element createActionElement(String actionXml) throws JDOMException, ParseException {
        Element eAction = XmlUtils.parseXml(actionXml);
        eAction.removeAttribute("start");
        eAction.removeAttribute("end");
        eAction.setAttribute("instance-number", Integer.toString(1));
        eAction.setAttribute("action-nominal-time",
                DateUtils.formatDateOozieTZ(DateUtils.parseDateOozieTZ("2009-09-08T01:00Z")));
        eAction.setAttribute("action-actual-time", DateUtils.formatDateOozieTZ(new Date()));
        return eAction;
    }

    private CoordinatorActionBean createCoordinatorActionBean(CoordinatorJob job) throws IOException {
        CoordinatorActionBean actionBean = new CoordinatorActionBean();
        String actionId = Services.get().get(UUIDService.class).generateChildId(job.getId(), "1");
        actionBean.setJobId(job.getId());
        actionBean.setId(actionId);
        Configuration jobConf = new XConfiguration(new StringReader(job.getConf()));
        actionBean.setRunConf(XmlUtils.prettyPrint(jobConf).toString());
        return actionBean;
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

    private void populateTable(String db, String table) throws Exception {
        addPartition(db, table, "dt=20120430;country=usa");
        addPartition(db, table, "dt=20120412;country=brazil");
        addPartition(db, table, "dt=20120413;country=brazil");
    }

}
