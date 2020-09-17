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

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
    public void testCoordOffset() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTableForWaiting("coord-dataset-offset.xml",
                CoordinatorJob.Status.RUNNING, false, true);
        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String actionXml = getCoordActionXml(appPath, "coord-dataset-offset.xml");
        actionXml = actionXml.replace("-unit-", "DAY");
        actionXml = actionXml.replace("-frequency-", "1");
        CoordinatorActionBean actionBean = createCoordinatorActionBean(job);
        Configuration jobConf = new XConfiguration(new StringReader(job.getConf()));
        Element eAction = createActionElement(actionXml);
        jobConf.set("startInstance", "coord:offset(-4,DAY)");
        jobConf.set("endInstance", "coord:offset(0,DAY)");
        String output = CoordCommandUtils.materializeOneInstance("jobId", true, eAction,
                DateUtils.parseDateOozieTZ("2009-08-20T10:00Z"), DateUtils.parseDateOozieTZ("2009-08-20T10:00Z"), 1,
                jobConf, actionBean);
        eAction = XmlUtils.parseXml(output);
        Element e = (Element) ((Element) eAction.getChildren("input-events", eAction.getNamespace()).get(0))
                .getChildren().get(0);
        assertEquals(e.getChild("uris", e.getNamespace()).getTextTrim(),
                "hdfs:///tmp/workflows/2009/08/20/01;region=us#hdfs:///tmp/workflows/2009/08/19/01;region=us#"
                        + "hdfs:///tmp/workflows/2009/08/18/01;region=us#hdfs:///tmp/workflows/2009/08/17/01;"
                        + "region=us#hdfs:///tmp/workflows/2009/08/16/01;region=us");

        jobConf.set("startInstance", "coord:offset(-4,HOUR)");
        jobConf.set("endInstance", "coord:offset(0,HOUR)");
        actionXml = getCoordActionXml(appPath, "coord-dataset-offset.xml");
        actionXml = actionXml.replace("-unit-", "MINUTE");
        actionXml = actionXml.replace("-frequency-", "60");
        eAction = createActionElement(actionXml);

        output = CoordCommandUtils.materializeOneInstance("jobId", true, eAction,
                DateUtils.parseDateOozieTZ("2009-08-20T01:00Z"), DateUtils.parseDateOozieTZ("2009-08-20T01:00Z"), 1,
                jobConf, actionBean);
        eAction = XmlUtils.parseXml(output);
        e = (Element) ((Element) eAction.getChildren("input-events", eAction.getNamespace()).get(0))
                .getChildren().get(0);
        assertEquals(e.getChild("uris", e.getNamespace()).getTextTrim(),
                "hdfs:///tmp/workflows/2009/08/20/01;region=us#hdfs:///tmp/workflows/2009/08/20/00;region=us#"
                + "hdfs:///tmp/workflows/2009/08/19/23;region=us#hdfs:///tmp/workflows/2009/08/19/22;region=us#"
                + "hdfs:///tmp/workflows/2009/08/19/21;region=us");

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
                    .contains("initial-instance should be equal or earlier than the start-instance"));
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
                    "Only start-instance as absolute/endOfMonths/endOfWeeks/endOfDays"
                            + " and end-instance as current is supported."));
        }
    }

    @Test
    public void testComputeNextNominalTime() throws Exception {
        // The 10th minute of every hour
        CoordinatorJobBean job = new CoordinatorJobBean();
        CoordinatorActionBean action = new CoordinatorActionBean();
        job.setFrequency("10 * * * *");
        job.setTimeUnit(CoordinatorJob.Timeunit.CRON);
        job.setTimeZone("America/Los_Angeles");
        job.setStartTime(DateUtils.parseDateOozieTZ("2015-07-21T08:00Z"));
        job.setEndTime(DateUtils.parseDateOozieTZ("2015-07-21T13:50Z"));
        job.setJobXml("<coordinator-app name=\"test\" end_of_duration='NONE'></coordinator-app>");
        // @5 occurs at the 10th minute of the 5th hour after the starting time (12:10)
        action.setNominalTime(DateUtils.parseDateOozieTZ("2015-07-21T12:10Z"));
        action.setActionNumber(5);
        // The next nominal time should be at the 10th minute of the next hour (13:10)
        assertEquals(DateUtils.parseDateOozieTZ("2015-07-21T13:10Z"), CoordCommandUtils.computeNextNominalTime(job, action));
        job.setEndTime(DateUtils.parseDateOozieTZ("2015-07-21T13:00Z"));
        assertNull(CoordCommandUtils.computeNextNominalTime(job, action));

        // Every 10 hours
        job = new CoordinatorJobBean();
        action = new CoordinatorActionBean();
        job.setFrequency("10");
        job.setTimeUnit(CoordinatorJob.Timeunit.HOUR);
        job.setTimeZone("America/Los_Angeles");
        job.setStartTime(DateUtils.parseDateOozieTZ("2015-07-21T14:00Z"));
        job.setEndTime(DateUtils.parseDateOozieTZ("2015-07-25T23:00Z"));
        job.setJobXml("<coordinator-app name=\"test\" end_of_duration='NONE'></coordinator-app>");
        // @5 occurs 50 hours after the start time (23T06:00)
        action.setNominalTime(DateUtils.parseDateOozieTZ("2015-07-23T06:00Z"));
        action.setActionNumber(5);
        // The next nominal time should be 10 hours after that (23T16:00)
        assertEquals(DateUtils.parseDateOozieTZ("2015-07-23T16:00Z"), CoordCommandUtils.computeNextNominalTime(job, action));
        job.setEndTime(DateUtils.parseDateOozieTZ("2015-07-23T09:00Z"));
        assertNull(CoordCommandUtils.computeNextNominalTime(job, action));

        // Every 10 days, at the end of the day
        job = new CoordinatorJobBean();
        action = new CoordinatorActionBean();
        job.setFrequency("10");
        job.setTimeUnit(CoordinatorJob.Timeunit.END_OF_DAY);
        job.setTimeZone("America/Los_Angeles");
        job.setStartTime(DateUtils.parseDateOozieTZ("2015-07-21T14:00Z"));
        job.setEndTime(DateUtils.parseDateOozieTZ("2015-09-21T23:00Z"));
        job.setJobXml("<coordinator-app name=\"test\" end_of_duration='END_OF_DAY'></coordinator-app>");
        // @5 occurs at 50 days, at the end of the day, after the starting time (08-31T07:00Z)
        // (Timezone is America/Los_Angeles so end of day is -0700)
        action.setNominalTime(DateUtils.parseDateOozieTZ("2015-08-31T07:00Z"));
        action.setActionNumber(5);
        // The next nominal time should be 10 days after that, at the end of the day
        assertEquals(DateUtils.parseDateOozieTZ("2015-09-10T07:00Z"), CoordCommandUtils.computeNextNominalTime(job, action));
        job.setEndTime(DateUtils.parseDateOozieTZ("2015-09-08T23:00Z"));
        assertNull(CoordCommandUtils.computeNextNominalTime(job, action));
    }

    @Test
    public void testCoordEndOfMonthsParamerized() throws Exception {
        List<?> elementList = getSyncDatasetEvents("coord-dataset-endOfMonths.xml", "coord:endOfMonths(-1)",
                "coord:current(0)", "2009-08-20T01:00Z", "2009-08-20T01:00Z");
        Element e1 = (Element) elementList.get(0);
        Element e2 = (Element) elementList.get(1);

        // startInstance = coord:endOfMonths(-1) i.e.2009-08-01T01:00Z and
        // endInstance = coord:current(0) i.e. 2009-08-20T01:00Z
        Calendar start = DateUtils.getCalendar("2009-08-01T01:00Z", DateUtils.UTC);
        checkUris(e1.getChild("uris", e1.getNamespace()).getTextTrim(), (Calendar) start.clone(), Calendar.DATE, 1,
                "YYYY/MM/dd");

        // Test parameterized
        // startInstance = coord:endOfMonths(-1) i.e.2009-08-01T01:00Z and
        // endInstance = coord:current(0) i.e. 2009-08-20T01:00Z
        checkUris(e2.getChild("uris", e2.getNamespace()).getTextTrim(), start, Calendar.DATE, 1, "YYYY/MM/dd");
    }

    public void testCoordEndOfMonthsStartingFromPrevMonth() throws Exception {
        List<?> elementList = getSyncDatasetEvents("coord-dataset-endOfMonths.xml", "coord:endOfMonths(-2)",
                "coord:current(0)", "2009-08-20T01:00Z", "2009-08-20T01:00Z");
        Element e2 = (Element) elementList.get(1);
        e2 = (Element) elementList.get(1);

        // startInstance = coord:endOfMonths(-2) i.e.2009-07-01T01:00Z and
        // endInstance = coord:current(0) i.e. 2009-08-20T01:00Z
        Calendar start = DateUtils.getCalendar("2009-07-01T01:00Z", DateUtils.UTC);
        checkUris(e2.getChild("uris", e2.getNamespace()).getTextTrim(), start, Calendar.DATE, 1, "YYYY/MM/dd");
    }

    public void testCoordEndOfMonthsFailOnStartInstanceIsLater() throws Exception {
        try {
            // start instance = 2009-10-01T01:00Z
            // end instance = 2009-08-01T01:00Z
            getSyncDatasetEvents("coord-dataset-endOfMonths.xml", "coord:endOfMonths(1)",
                    "coord:current(0)", "2009-08-06T01:00Z", "2009-08-16T01:00Z");
            fail("Should throw exception because end is earlier than start-instance");
        }
        catch (Exception e) {
            assertTrue(e.getCause().getMessage()
                    .contains("start-instance should be equal or earlier than the end-instance"));
        }
    }

    public void testCoordEndOfMonthsFailOnInitialInstanceIsLater() throws Exception {
        try {
            // start instance = 2009-06-01T01:00Z
            // initial instance = 2009-08-06T01:00Z
            getSyncDatasetEvents("coord-dataset-endOfMonths.xml", "coord:endOfMonths(-3)", "coord:current(0)",
                "2009-08-06T01:00Z", "2009-08-16T01:00Z");
            fail("Should throw exception because initial instance is later than start instance");
        }
        catch (Exception e) {
            e.printStackTrace(System.out);
            assertTrue(e.getCause().getMessage()
                    .contains("initial-instance should be equal or earlier than the start-instance"));
        }
    }

    public void testCoordEndOfMonthsFailOnLatestAsEndInstance() throws Exception {
        try {
            getSyncDatasetEvents("coord-dataset-endOfMonths.xml", "coord:endOfMonths(-1)",
                "coord:latest(2)", "2009-08-20T01:00Z", "2009-08-20T01:00Z");
            fail("Should throw exception because latest is not allowed as end-instance");
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains(
                    "Only start-instance as absolute/endOfMonths/endOfWeeks/endOfDays"
                            + " and end-instance as current is supported."));
        }
    }

    public void testCoordEndOfMonthsInitialInstaceNotInPhase() throws Exception {
        try {
            getSyncDatasetEvents("out-of-phase-coordinator.xml", "${coord:endOfMonths(-1)}", "${coord:current(0)}",
                    "2009-08-20T18:00Z", "2009-08-20T18:00Z");
            fail("Should throw exception because initial instance is not in phase with start instance");
        }
        catch (Exception e) {
            e.printStackTrace(System.out);
            assertTrue(e.getCause().getMessage().contains("initial-instance is not in phase with start-instance"));
        }
    }

    @Test
    public void testCoordEndOfWeeksParamerized() throws Exception {
        String nominalTime = "2009-08-20T01:00Z";
        String startTime = nominalTime;
        List<?> elementList = getSyncDatasetEvents("coord-dataset-endOfWeeks.xml", "coord:endOfWeeks(-1)",
                "coord:current(0)", nominalTime, startTime);
        Element e1 = (Element) elementList.get(0);
        Element e2 = (Element) elementList.get(1);

        // startInstance = coord:endOfWeeks(-1)
        // endInstance = coord:current(0) i.e. 2009-08-20T01:00Z
        Calendar start = DateUtils.getCalendar(nominalTime, DateUtils.UTC);
        start.add(Calendar.DAY_OF_WEEK, start.getFirstDayOfWeek() - start.get(Calendar.DAY_OF_WEEK));
        checkUris(e1.getChild("uris", e1.getNamespace()).getTextTrim(), (Calendar) start.clone(), Calendar.DATE, 1,
                "YYYY/MM/dd");

        // Test parameterized
        // startInstance = coord:endOfWeeks(-1)
        // endInstance = coord:current(0) i.e. 2009-08-20T01:00Z
        checkUris(e2.getChild("uris", e2.getNamespace()).getTextTrim(), start, Calendar.DATE, 1, "YYYY/MM/dd");
    }

    public void testCoordEndOfWeeksStartingFromPrevWeek() throws Exception {
        List<?> elementList = getSyncDatasetEvents("coord-dataset-endOfWeeks.xml", "coord:endOfWeeks(-2)",
                "coord:current(0)", "2009-08-20T01:00Z", "2009-08-20T01:00Z");
        Element e2 = (Element) elementList.get(1);
        e2 = (Element) elementList.get(1);

        // startInstance = coord:endOfWeeks(-2)
        // endInstance = coord:current(0) i.e. 2009-08-20T01:00Z
        Calendar start = DateUtils.getCalendar("2009-08-10T01:00Z", DateUtils.UTC);
        start.add(Calendar.DAY_OF_WEEK, start.getFirstDayOfWeek() - start.get(Calendar.DAY_OF_WEEK));
        checkUris(e2.getChild("uris", e2.getNamespace()).getTextTrim(), start, Calendar.DATE, 1, "YYYY/MM/dd");
    }

    public void testCoordEndOfWeeksFailOnStartInstanceIsLater() throws Exception {
        try {
            // start instance = coord:endOfWeeks(1) i.e. 2009-08-30T01:00Z
            // end instance = coord:current(0) i.e. 2009-08-20T01:00Z
            getSyncDatasetEvents("coord-dataset-endOfWeeks.xml", "coord:endOfWeeks(1)", "coord:current(0)",
                    "2009-08-20T01:00Z", "2009-08-20T01:00Z");
            fail("Should throw exception because end is earlier than start-instance");
        }
        catch (Exception e) {
            assertTrue(e.getCause().getMessage()
                    .contains("start-instance should be equal or earlier than the end-instance"));
        }
    }

    public void testCoordEndOfWeeksFailOnInitialInstanceIsLater() throws Exception {
        try {
            // start instance = coord:endOfWeeks(-11) i.e. 2009-05-31T01:00Z
            // initial instance = 2009-06-06T01:00Z
            getSyncDatasetEvents("coord-dataset-endOfWeeks.xml", "coord:endOfWeeks(-11)", "coord:current(0)",
                    "2009-08-06T01:00Z", "2009-08-16T01:00Z");
            fail("Should throw exception because initial instance is later than start instance");
        }
        catch (Exception e) {
            e.printStackTrace(System.out);
            assertTrue(e.getCause().getMessage()
                    .contains("initial-instance should be equal or earlier than the start-instance"));
        }
    }

    public void testCoordEndOfWeeksFailOnLatestAsEndInstance() throws Exception {
        try {
            getSyncDatasetEvents("coord-dataset-endOfWeeks.xml", "coord:endOfWeeks(-1)", "coord:latest(2)",
                    "2009-08-20T01:00Z", "2009-08-20T01:00Z");
            fail("Should throw exception because latest is not allowed as end-instance");
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains(
                    "Only start-instance as absolute/endOfMonths/endOfWeeks/endOfDays"
                            + " and end-instance as current is supported."));
        }
    }

    public void testCoordEndOfWeeksInitialInstaceNotInPhase() throws Exception {
        try {
            getSyncDatasetEvents("out-of-phase-coordinator.xml", "${coord:endOfWeeks(-1)}", "${coord:current(0)}",
                    "2009-08-20T18:00Z", "2009-08-20T18:00Z");
            fail("Should throw exception because initial instance is not in phase with start instance");
        }
        catch (Exception e) {
            assertTrue(e.getCause().getMessage().contains("initial-instance is not in phase with start-instance"));
        }
    }

    @Test
    public void testCoordEndOfDaysParameterized() throws Exception {
        List<?> elementList = getSyncDatasetEvents("coord-dataset-endOfDays.xml",
                "coord:endOfDays(-1)",
                "coord:current(0)", "2009-08-20T18:00Z", "2009-08-20T18:00Z");
        Element e1 = (Element) elementList.get(0);
        Element e2 = (Element) elementList.get(1);

        // startInstance = coord:endOfDays(-1) i.e 2009-08-20T01:00Z and
        // endInstance = coord:current(0) i.e. 2009-08-20T18:00Z
        Calendar start = DateUtils.getCalendar("2009-08-20T00:00Z", DateUtils.UTC);
        checkUris(e1.getChild("uris", e1.getNamespace()).getTextTrim(), (Calendar) start.clone(), Calendar.MINUTE, 30,
                "YYYY/MM/dd/HH/mm");

        // Test parameterized
        // startInstance = coord:endOfDays(-1) i.e 2009-08-20T01:00Z and
        // endInstance = coord:current(0) i.e. 2009-08-20T01:00Z
        checkUris(e2.getChild("uris", e2.getNamespace()).getTextTrim(), start, Calendar.MINUTE, 30,
                "YYYY/MM/dd/HH/mm");
    }

    public void testCoordEndOfDaysStartingFromPrevDay() throws Exception {
        List<?> elementList = getSyncDatasetEvents("coord-dataset-endOfDays.xml", "coord:endOfDays(-2)",
                "coord:current(0)", "2009-08-20T18:00Z", "2009-08-20T18:00Z");
        Element e2 = (Element) elementList.get(1);

        // startInstance = coord:endOfDays(-2) i.e 2009-08-19T01:00Z and
        // endInstance = coord:current(0) i.e. 2009-08-20T01:00Z
        Calendar start = DateUtils.getCalendar("2009-08-19T00:00Z", DateUtils.UTC);
        checkUris(e2.getChild("uris", e2.getNamespace()).getTextTrim(), start, Calendar.MINUTE, 30,
                "YYYY/MM/dd/HH/mm");
    }

    public void testCoordEndOfDaysFailOnStartInstanceIsLater() throws Exception {
        try {
            // start instance = coord:endOfDays(1) i.e. 2009-08-08T00:00Z
            // end instance = coord:current(0) i.e. 2009-08-06T18:00Z
            getSyncDatasetEvents("coord-dataset-endOfDays.xml", "coord:endOfDays(1)",
                "coord:current(0)", "2009-08-06T18:00Z", "2009-08-16T18:00Z");
        }
        catch (Exception e) {
            assertTrue(e.getCause().getMessage()
                    .contains("start-instance should be equal or earlier than the end-instance"));
        }
    }

    public void testCoordEndOfDaysFailOnInitialInstanceIsLater() throws Exception {
        try {
            // start instance = coord:endOfDays(-11) i.e. 2009-07-27T00:00Z
            // initial instance = 2009-08-01T01:00Z
            getSyncDatasetEvents("coord-dataset-endOfDays.xml", "coord:endOfDays(-11)", "coord:current(0)",
                    "2009-08-06T18:00Z", "2009-08-16T18:00Z");
            fail("Should throw exception because initial instance is later than start instance");
        }
        catch (Exception e) {
            assertTrue(e.getCause().getMessage()
                    .contains("initial-instance should be equal or earlier than the start-instance"));
        }
    }

    public void testCoordEndOfDaysFailOnLatestAsEndInstance() throws Exception {
        try {
            getSyncDatasetEvents("coord-dataset-endOfDays.xml", "coord:endOfDays(-1)",
                "coord:latest(2)", "2009-08-20T18:00Z", "2009-08-20T18:00Z");
            fail("Should throw exception. Start-instance is coord:endOfDays and end-instance is latest");
        }
        catch (Exception e) {
            assertTrue(e.getMessage()
                    .contains(
                            "Only start-instance as absolute/endOfMonths/endOfWeeks/endOfDays"
                                    + " and end-instance as current is supported."));
        }
    }

    public void testCoordEndOfDaysInitialInstaceNotInPhase() throws Exception {
        try {
            getSyncDatasetEvents("out-of-phase-coordinator.xml", "${coord:endOfDays(-1)}", "${coord:current(0)}"
                    , "2009-08-20T18:00Z", "2009-08-20T18:00Z");
            fail("Should throw exception because initial instance is not in phase with start instance");
        }
        catch (Exception e) {
            assertTrue(e.getCause().getMessage().contains("initial-instance is not in phase with start-instance"));
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

    private void checkUris(String uris, Calendar start, int freqUnit ,int freq, String format) {
        //String format = "YYYY/MM/dd";
        String[] allUris = uris.split("#");
        String pathFormat = "hdfs:///tmp/workflows/%s;region=us";
        for (int i = allUris.length - 1; i >= 0; i--) {
            String path = String.format(pathFormat, DateUtils.formatDateCustom(start.getTime(), format));
            assertEquals(allUris[i], path);
            start.add(freqUnit, freq);
        }
    }

    private List<?> getSyncDatasetEvents(String coordXml, String startInstanceParam, String endInstanceParam,
            String nominalTime, String actualTime) throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTableForWaiting(coordXml, CoordinatorJob.Status.RUNNING, false,
                true);
        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String actionXml = getCoordActionXml(appPath, coordXml);
        CoordinatorActionBean actionBean = createCoordinatorActionBean(job);
        Configuration jobConf = new XConfiguration(new StringReader(job.getConf()));
        Element eAction = createActionElement(actionXml);
        jobConf.set("startInstance", startInstanceParam);
        jobConf.set("endInstance", endInstanceParam);
        String output = CoordCommandUtils.materializeOneInstance("jobId", true, eAction,
                DateUtils.parseDateOozieTZ(nominalTime), DateUtils.parseDateOozieTZ(actualTime), 1, jobConf,
                actionBean);
        eAction = XmlUtils.parseXml(output);
        List<?> elementList = ((Element) eAction.getChildren("input-events", eAction.getNamespace()).get(0))
                .getChildren();
        return elementList;
    }

}
