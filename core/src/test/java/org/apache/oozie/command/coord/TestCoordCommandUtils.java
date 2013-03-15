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
        cleanUpDBTables();
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

    private String getPullMissingDependencies(String testDir) {
        String missDeps = "file://#testDir/2009/29/_SUCCESS#file://#testDir/2009/22/_SUCCESS#file"
                + "://#testDir/2009/15/_SUCCESS#file://#testDir/2009/08/_SUCCESS";
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
