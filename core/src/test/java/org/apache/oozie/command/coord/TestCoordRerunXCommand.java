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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.io.StringReader;
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.action.oozie.JavaSleepAction;
import org.apache.oozie.client.*;
import org.apache.oozie.client.CoordinatorJob.Execution;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.dependency.URIHandler;
import org.apache.oozie.executor.jpa.CoordActionInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.executor.jpa.CoordJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.SchemaService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.StatusTransitService;
import org.apache.oozie.service.URIHandlerService;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.util.XConfiguration;
import org.jdom.Element;
import org.jdom.JDOMException;

public class TestCoordRerunXCommand extends XDataTestCase {
    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        LocalOozie.start();
    }

    @Override
    protected void tearDown() throws Exception {
        LocalOozie.stop();
        services.destroy();
        super.tearDown();
    }

    /**
     * Test : rerun <jobId> -action 1 -nocleanup
     *
     * @throws Exception
     */
    public void testCoordRerunActions1() throws Exception {
        final String jobId = "0000000-" + new Date().getTime() + "-testCoordRerun-C";
        final int actionNum = 1;
        final String actionId = jobId + "@" + actionNum;
        try {
            addRecordToJobTable(jobId, CoordinatorJob.Status.SUCCEEDED);
            addRecordToActionTable(jobId, actionNum, actionId, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action1.xml");
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }

        CoordinatorActionBean action1 = getCoordinatorAction(actionId);
        assertEquals(action1.getStatus(), CoordinatorAction.Status.SUCCEEDED);

        final OozieClient coordClient = LocalOozie.getCoordClient();
        coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_SCOPE_ACTION, Integer.toString(actionNum), false, true);

        CoordinatorActionBean action2 = getCoordinatorAction(actionId);

        assertNotSame(action2.getStatus(), CoordinatorAction.Status.SUCCEEDED);
        assertNull(action2.getExternalId());
    }

    /**
     * Test : rerun <jobId> -action 1-2 -nocleanup
     *
     * @throws Exception
     */
    public void testCoordRerunActions2() throws Exception {
        final String jobId = "0000000-" + new Date().getTime() + "-testCoordRerun-C";
        final int actionNum1 = 1;
        final int actionNum2 = 2;
        final String actionId1 = jobId + "@" + actionNum1;
        final String actionId2 = jobId + "@" + actionNum2;
        try {
            addRecordToJobTable(jobId, CoordinatorJob.Status.SUCCEEDED);
            addRecordToActionTable(jobId, actionNum1, actionId1, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action1.xml");
            addRecordToActionTable(jobId, actionNum2, actionId2, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action2.xml");
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }

        String rerunScope = Integer.toString(actionNum1) + "-" + Integer.toString(actionNum2);

        final OozieClient coordClient = LocalOozie.getCoordClient();
        coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_SCOPE_ACTION, rerunScope, false, true);

        CoordinatorActionBean action1 = getCoordinatorAction(actionId1);
        assertNotSame(action1.getStatus(), CoordinatorAction.Status.SUCCEEDED);
        CoordinatorActionBean action2 = getCoordinatorAction(actionId2);
        assertNotSame(action2.getStatus(), CoordinatorAction.Status.SUCCEEDED);
    }

    /**
     * Test : rerun <jobId> -action 1,2 -nocleanup
     *
     * @throws Exception
     */
    public void testCoordRerunActions3() throws Exception {
        final String jobId = "0000000-" + new Date().getTime() + "-testCoordRerun-C";
        final int actionNum1 = 1;
        final int actionNum2 = 2;
        final String actionId1 = jobId + "@" + actionNum1;
        final String actionId2 = jobId + "@" + actionNum2;
        try {
            addRecordToJobTable(jobId, CoordinatorJob.Status.SUCCEEDED);
            addRecordToActionTable(jobId, actionNum1, actionId1, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action1.xml");
            addRecordToActionTable(jobId, actionNum2, actionId2, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action2.xml");
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }
        String rerunScope = Integer.toString(actionNum1) + "," + Integer.toString(actionNum2);

        final OozieClient coordClient = LocalOozie.getCoordClient();
        coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_SCOPE_ACTION, rerunScope, false, true);

        CoordinatorActionBean action1 = getCoordinatorAction(actionId1);
        assertNotSame(action1.getStatus(), CoordinatorAction.Status.SUCCEEDED);
        CoordinatorActionBean action2 = getCoordinatorAction(actionId2);
        assertNotSame(action2.getStatus(), CoordinatorAction.Status.SUCCEEDED);
    }

    /**
     * Negative Test : rerun <jobId> -action 1-3 -nocleanup. Only 2 actions is in db.
     *
     * @throws Exception
     */
    public void testCoordRerunActionsNeg1() throws Exception {
        final String jobId = "0000000-" + new Date().getTime() + "-testCoordRerun-C";
        final int actionNum1 = 1;
        final int actionNum2 = 2;
        final String actionId1 = jobId + "@" + actionNum1;
        final String actionId2 = jobId + "@" + actionNum2;
        try {
            addRecordToJobTable(jobId, CoordinatorJob.Status.SUCCEEDED);
            addRecordToActionTable(jobId, actionNum1, actionId1, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action1.xml");
            addRecordToActionTable(jobId, actionNum2, actionId2, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action2.xml");
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }

        String rerunScope = "1-3";
        try {
            final OozieClient coordClient = LocalOozie.getCoordClient();
            coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_SCOPE_ACTION, rerunScope, false, true);
        }
        catch (OozieClientException ex) {
        }
    }

    /**
     * Negative Test : rerun <jobId> -action 1 -nocleanup. Action is not in terminal state.
     *
     * @throws Exception
     */
    public void testCoordRerunActionsNeg2() throws Exception {
        final String jobId = "0000000-" + new Date().getTime() + "-testCoordRerun-C";
        final int actionNum = 1;
        final String actionId = jobId + "@" + actionNum;
        try {
            addRecordToJobTable(jobId, CoordinatorJob.Status.SUCCEEDED);
            addRecordToActionTable(jobId, actionNum, actionId, CoordinatorAction.Status.RUNNING,
                    "coord-rerun-action1.xml");
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }

        try {
            final OozieClient coordClient = LocalOozie.getCoordClient();
            coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_SCOPE_ACTION, Integer.toString(actionNum), false,
                    true);
            fail("Exception expected because action is not in terminal state.");
        }
        catch (OozieClientException ex) {
            if (!ex.getErrorCode().equals(ErrorCode.E1018.toString())) {
                fail("Error code should be E1018 when action is not in terminal state.");
            }
        }

        CoordinatorActionBean action2 = getCoordinatorAction(actionId);
        assertEquals(action2.getStatus(), CoordinatorAction.Status.RUNNING);
    }

    /**
     * Test : rerun <jobId> -date 2009-12-15T17:00Z -nocleanup
     *
     * @throws Exception
     */
    public void testCoordRerunDate1() throws Exception {
        final String jobId = "0000000-" + new Date().getTime() + "-testCoordRerun-C";
        final int actionNum = 1;
        final String actionId = jobId + "@" + actionNum;
        try {
            addRecordToJobTable(jobId, CoordinatorJob.Status.SUCCEEDED);
            addRecordToActionTable(jobId, actionNum, actionId, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action1.xml");
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }

        CoordinatorActionBean action1 = getCoordinatorAction(actionId);
        assertEquals(action1.getStatus(), CoordinatorAction.Status.SUCCEEDED);

        final OozieClient coordClient = LocalOozie.getCoordClient();
        coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_SCOPE_DATE, "2009-12-15T01:00Z", false, true);

        CoordinatorActionBean action2 = getCoordinatorAction(actionId);
        assertNotSame(action2.getStatus(), CoordinatorAction.Status.SUCCEEDED);
    }

    /**
     * Test : rerun <jobId> -date 2009-12-15T01:00Z::2009-12-16T01:00Z -nocleanup
     *
     * @throws Exception
     */
    public void testCoordRerunDate2() throws Exception {
        final String jobId = "0000000-" + new Date().getTime() + "-testCoordRerun-C";
        final int actionNum1 = 1;
        final int actionNum2 = 2;
        final String actionId1 = jobId + "@" + actionNum1;
        final String actionId2 = jobId + "@" + actionNum2;
        try {
            addRecordToJobTable(jobId, CoordinatorJob.Status.SUCCEEDED);
            addRecordToActionTable(jobId, actionNum1, actionId1, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action1.xml");
            addRecordToActionTable(jobId, actionNum2, actionId2, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action2.xml");
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }

        String rerunScope = "2009-12-15T01:00Z" + "::" + "2009-12-16T01:00Z";

        final OozieClient coordClient = LocalOozie.getCoordClient();
        coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_SCOPE_DATE, rerunScope, false, true);

        CoordinatorActionBean action1 = getCoordinatorAction(actionId1);
        assertNotSame(action1.getStatus(), CoordinatorAction.Status.SUCCEEDED);
        CoordinatorActionBean action2 = getCoordinatorAction(actionId2);
        assertNotSame(action2.getStatus(), CoordinatorAction.Status.SUCCEEDED);
    }

    /**
     * Test : rerun <jobId> -date 2009-12-15T01:00Z,2009-12-16T01:00Z -nocleanup
     *
     * @throws Exception
     */
    public void testCoordRerunDate3() throws Exception {
        final String jobId = "0000000-" + new Date().getTime() + "-testCoordRerun-C";
        final int actionNum1 = 1;
        final int actionNum2 = 2;
        final String actionId1 = jobId + "@" + actionNum1;
        final String actionId2 = jobId + "@" + actionNum2;
        try {
            addRecordToJobTable(jobId, CoordinatorJob.Status.SUCCEEDED);
            addRecordToActionTable(jobId, actionNum1, actionId1, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action1.xml");
            addRecordToActionTable(jobId, actionNum2, actionId2, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action2.xml");
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }

        String rerunScope = "2009-12-15T01:00Z" + "," + "2009-12-16T01:00Z";

        final OozieClient coordClient = LocalOozie.getCoordClient();
        coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_SCOPE_DATE, rerunScope, false, true);

        CoordinatorActionBean action1 = getCoordinatorAction(actionId1);
        assertNotSame(action1.getStatus(), CoordinatorAction.Status.SUCCEEDED);
        CoordinatorActionBean action2 = getCoordinatorAction(actionId2);
        assertNotSame(action2.getStatus(), CoordinatorAction.Status.SUCCEEDED);
    }

    /**
     * Test : rerun <jobId> -date 2009-12-15T01:00Z::2009-12-17T01:00Z -nocleanup 2009-12-17T01:00Z is not in the action
     * list, but Oozie will tolerate this.
     *
     * @throws Exception
     */
    public void testCoordRerunDate4() throws Exception {
        final String jobId = "0000000-" + new Date().getTime() + "-testCoordRerun-C";
        final int actionNum1 = 1;
        final int actionNum2 = 2;
        final String actionId1 = jobId + "@" + actionNum1;
        final String actionId2 = jobId + "@" + actionNum2;
        try {
            addRecordToJobTable(jobId, CoordinatorJob.Status.SUCCEEDED);
            addRecordToActionTable(jobId, actionNum1, actionId1, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action1.xml");
            addRecordToActionTable(jobId, actionNum2, actionId2, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action2.xml");
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }

        String rerunScope = "2009-12-15T01:00Z" + "::" + "2009-12-17T01:00Z";

        final OozieClient coordClient = LocalOozie.getCoordClient();
        coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_SCOPE_DATE, rerunScope, false, true);

        CoordinatorActionBean action1 = getCoordinatorAction(actionId1);
        assertNotSame(action1.getStatus(), CoordinatorAction.Status.SUCCEEDED);
        CoordinatorActionBean action2 = getCoordinatorAction(actionId2);
        assertNotSame(action2.getStatus(), CoordinatorAction.Status.SUCCEEDED);
    }

    /**
     * Test : rerun <jobId> -date 2009-12-15T01:00Z,2009-12-16T01:00Z,2009-12-17T01:00Z -nocleanup 2009-12-17T01:00Z is
     * not in the action list, Oozie will not tolerate this when comma is used.
     *
     * @throws Exception
     */
    public void testCoordRerunDateNeg() throws Exception {
        final String jobId = "0000000-" + new Date().getTime() + "-testCoordRerun-C";
        final int actionNum1 = 1;
        final int actionNum2 = 2;
        final String actionId1 = jobId + "@" + actionNum1;
        final String actionId2 = jobId + "@" + actionNum2;
        try {
            addRecordToJobTable(jobId, CoordinatorJob.Status.SUCCEEDED);
            addRecordToActionTable(jobId, actionNum1, actionId1, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action1.xml");
            addRecordToActionTable(jobId, actionNum2, actionId2, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action2.xml");
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }
        String rerunScope = "2009-12-15T01:00Z,2009-12-16T01:00Z,2009-12-17T01:00Z";
        try {
            final OozieClient coordClient = LocalOozie.getCoordClient();
            coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_SCOPE_DATE, rerunScope, false, true);
            fail("Exception expected because one action is missing from db.");
        }
        catch (OozieClientException ex) {
            assertTrue(ex.getErrorCode() == ErrorCode.E0605.toString());
        }
    }

    /**
     * Test : rerun <jobId> -action 1 -nocleanup -refresh
     *
     * @throws Exception
     */
    public void testCoordRerunRefresh() throws Exception {
        final String jobId = "0000000-" + new Date().getTime() + "-testCoordRerun-C";
        final int actionNum = 1;
        final String actionId = jobId + "@" + actionNum;
        try {
            addRecordToJobTable(jobId, CoordinatorJob.Status.SUCCEEDED);
            addRecordToActionTable(jobId, actionNum, actionId, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action1.xml");
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }
        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String inputDir = appPath.toString() + "/coord-input/2010/07/09/01/00";
        FileSystem fs = getFileSystem();
        fs.mkdirs(new Path(inputDir));
        fs.create(new Path(inputDir, "_SUCCESS"), true);

        final OozieClient coordClient = LocalOozie.getCoordClient();
        coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_SCOPE_ACTION, Integer.toString(actionNum), true, true);

        CoordinatorActionBean action2 = getCoordinatorAction(actionId);
        assertNotSame(action2.getStatus(), CoordinatorAction.Status.SUCCEEDED);

        waitFor(120 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                CoordinatorAction bean = coordClient.getCoordActionInfo(actionId);
                return (bean.getStatus() == CoordinatorAction.Status.READY || bean.getStatus() == CoordinatorAction.Status.SUBMITTED);
            }
        });

        CoordinatorActionBean action3 = getCoordinatorAction(actionId);
        String actionXml = action3.getActionXml();
        System.out.println("After refresh, action xml= " + actionXml);

        Element eAction = XmlUtils.parseXml(actionXml);
        String[] urls = getActionXmlUrls(eAction, getTestUser(), getTestGroup());

        /*        if (urls != null) {
                    assertEquals(inputDir, urls[0]);
                }
                else {
                    fail("After refresh, latest() should get the inputDir:" + inputDir);
                }*/
    }

    /**
     * Test : nocleanup option in dataset
     *
     * @throws Exception
     */
    public void testCoordRerunCleanupOption() throws Exception {
        final String jobId = "0000000-" + new Date().getTime() + "-testCoordRerun-C";
        final int actionNum = 1;
        final String actionId = jobId + "@" + actionNum;
        try {
            addRecordToJobTable(jobId, CoordinatorJob.Status.SUCCEEDED);
            addRecordToActionTable(jobId, actionNum, actionId, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action4.xml");
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }
        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String outputDir = appPath.toString() + "/coord-input/2009/12/14/11/00";
        Path success = new Path(outputDir, "_SUCCESS");
        FileSystem fs = getFileSystem();
        fs.mkdirs(new Path(outputDir));
        fs.create(success, true);
        // before cleanup
        assertTrue(fs.exists(success));
        long beforeModifiedTime = fs.getFileStatus(success).getModificationTime();

        final OozieClient coordClient = LocalOozie.getCoordClient();
        coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_SCOPE_ACTION, Integer.toString(actionNum), false, false);

        CoordinatorActionBean action2 = getCoordinatorAction(actionId);
        assertNotSame(action2.getStatus(), CoordinatorAction.Status.SUCCEEDED);

        waitFor(120 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                CoordinatorAction bean = coordClient.getCoordActionInfo(actionId);
                return (bean.getStatus() == CoordinatorAction.Status.WAITING || bean.getStatus() == CoordinatorAction.Status.READY);
            }
        });

        // after cleanup
        assertTrue(fs.exists(success));
        long afterModifiedTime = fs.getFileStatus(success).getModificationTime();
        assertEquals(beforeModifiedTime, afterModifiedTime);
    }

    /**
     * Test : rerun <jobId> -action 1
     *
     * @throws Exception
     */
    public void testCoordRerunCleanup() throws Exception {
        final String jobId = "0000000-" + new Date().getTime() + "-testCoordRerun-C";
        final int actionNum = 1;
        final String actionId = jobId + "@" + actionNum;
        try {
            addRecordToJobTable(jobId, CoordinatorJob.Status.SUCCEEDED);
            addRecordToActionTable(jobId, actionNum, actionId, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action1.xml");
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }
        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String outputDir = appPath.toString() + "/coord-input/2009/12/14/11/00";
        Path success = new Path(outputDir, "_SUCCESS");
        FileSystem fs = getFileSystem();
        fs.mkdirs(new Path(outputDir));
        fs.create(success, true);
        // before cleanup
        assertTrue(fs.exists(success));

        final OozieClient coordClient = LocalOozie.getCoordClient();
        coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_SCOPE_ACTION, Integer.toString(actionNum), false, false);

        CoordinatorActionBean action2 = getCoordinatorAction(actionId);
        assertNotSame(action2.getStatus(), CoordinatorAction.Status.SUCCEEDED);

        waitFor(120 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                CoordinatorAction bean = coordClient.getCoordActionInfo(actionId);
                return (bean.getStatus() == CoordinatorAction.Status.WAITING || bean.getStatus() == CoordinatorAction.Status.READY);
            }
        });

        // after cleanup
        assertFalse(fs.exists(success));
    }

    /**
     * Test : rerun with refresh option when input dependency is hcat partition
     *
     * @throws Exception
     */
    public void testCoordRerunCleanupForHCat() throws Exception {

        services = super.setupServicesForHCatalog();
        services.init();

        final String jobId = "0000000-" + new Date().getTime() + "-testCoordRerun-C";
        final int actionNum = 1;
        final String actionId = jobId + "@" + actionNum;
        try {
            addRecordToJobTable(jobId, CoordinatorJob.Status.SUCCEEDED);
            addRecordToActionTable(jobId, actionNum, actionId, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action1.xml", true);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }

        String db = "mydb";
        String table = "mytable";
        String server = getHCatalogServer().getMetastoreAuthority();
        String newHCatDependency = "hcat://" + server + "/" + db + "/" + table + "/ds=2009121411;region=usa";

        dropTable(db, table, true);
        dropDatabase(db, true);
        createDatabase(db);
        createTable(db, table, "ds,region");
        addPartition(db, table, "ds=2009121411;region=usa");

        // before cleanup
        Configuration conf = new Configuration();
        URIHandler handler = services.get(URIHandlerService.class).getURIHandler(newHCatDependency);
        assertTrue(handler.exists(new URI(newHCatDependency), conf, getTestUser()));

        final OozieClient coordClient = LocalOozie.getCoordClient();
        coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_SCOPE_ACTION, Integer.toString(actionNum), false, false);

        CoordinatorActionBean action2 = CoordActionQueryExecutor.getInstance().get(CoordActionQuery.GET_COORD_ACTION, actionId);
        assertNotSame(action2.getStatus(), CoordinatorAction.Status.SUCCEEDED);

        waitFor(120 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                CoordinatorAction bean = coordClient.getCoordActionInfo(actionId);
                return (bean.getStatus() == CoordinatorAction.Status.WAITING || bean.getStatus() == CoordinatorAction.Status.READY);
            }
        });

        // after cleanup
        assertFalse(handler.exists(new URI(newHCatDependency), conf, getTestUser()));
    }

    /**
     * Test : rerun <jobId> -action 1 with no output-event
     *
     * @throws Exception
     */
    public void testCoordRerunCleanupNoOutputEvents() throws Exception {
        final String jobId = "0000000-" + new Date().getTime() + "-testCoordRerun-C";
        final int actionNum = 1;
        final String actionId = jobId + "@" + actionNum;
        try {
            addRecordToJobTable(jobId, CoordinatorJob.Status.SUCCEEDED);
            addRecordToActionTable(jobId, actionNum, actionId, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action3.xml");
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }

        final OozieClient coordClient = LocalOozie.getCoordClient();
        coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_SCOPE_ACTION, Integer.toString(actionNum), false, false);
        CoordinatorActionBean action2 = getCoordinatorAction(actionId);
        assertNotSame(action2.getStatus(), CoordinatorAction.Status.SUCCEEDED);
        waitFor(120 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                CoordinatorAction bean = coordClient.getCoordActionInfo(actionId);
                return (bean.getStatus() == CoordinatorAction.Status.WAITING || bean.getStatus() == CoordinatorAction.Status.READY);
            }
        });
        CoordinatorAction bean = coordClient.getCoordActionInfo(actionId);
        assertTrue(bean.getStatus() == CoordinatorAction.Status.WAITING
                || bean.getStatus() == CoordinatorAction.Status.READY);
    }

    /**
     * Test : Rerun FAILED coordinator job
     *
     * @throws Exception
     */
    public void testCoordRerunInFailed() throws Exception {
        CoordinatorJobBean job = this.addRecordToCoordJobTable(Job.Status.FAILED, false, false);

        job = getCoordinatorJob(job.getId());
        assertEquals(Job.Status.FAILED, job.getStatus());

        try {
            new CoordRerunXCommand(job.getId(), RestConstants.JOB_COORD_SCOPE_DATE, "2009-12-15T01:00Z", false, true,
                    false, null).call();
            fail("Coordinator job is FAILED, rerun should throw exception");
        }
        catch (CommandException ce) {
        }
    }


    /**
     * Test : Rerun DONEWITHERROR coordinator job
     *
     * @throws Exception
     */
    public void testCoordRerunInDoneWithError() throws Exception {
        Services.get().destroy();
        setSystemProperty(StatusTransitService.CONF_BACKWARD_SUPPORT_FOR_STATES_WITHOUT_ERROR, "false");
        services = new Services();
        services.init();
        CoordinatorJobBean job = this.addRecordToCoordJobTable(Job.Status.DONEWITHERROR, false, false);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.FAILED, "coord-rerun-action1.xml", 0);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        job = getCoordinatorJob(job.getId());
        assertEquals(Job.Status.DONEWITHERROR, job.getStatus());

        new CoordRerunXCommand(job.getId(), RestConstants.JOB_COORD_SCOPE_DATE, "2009-12-15T01:00Z", false, true, false,
                null).call();
        job = getCoordinatorJob(job.getId());
        assertEquals(Job.Status.RUNNINGWITHERROR, job.getStatus());

    }
    /**
     * Test : Rerun paused coordinator job
     *
     * @throws Exception
     */
    public void testCoordRerunInPaused() throws Exception {
        Date curr = new Date();
        Date pauseTime = new Date(curr.getTime() - 1000);
        CoordinatorJobBean job = this.addRecordToCoordJobTableWithPausedTime(Job.Status.PAUSED, false, false, pauseTime);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED, "coord-rerun-action1.xml", 0);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        job = getCoordinatorJob(job.getId());
        assertEquals(Job.Status.PAUSED, job.getStatus());

        new CoordRerunXCommand(job.getId(), RestConstants.JOB_COORD_SCOPE_DATE, "2009-12-15T01:00Z", false, true, false,
                null).call();

        job = getCoordinatorJob(job.getId());
        assertEquals(Job.Status.PAUSED, job.getStatus());
        assertNotNull(job.getPauseTime());
    }

    /**
     * Test : Rerun PAUSEDWITHERROR coordinator job
     *
     * @throws Exception
     */
    public void testCoordRerunInPausedWithError() throws Exception {
        Date curr = new Date();
        Date pauseTime = new Date(curr.getTime() - 1000);
        CoordinatorJobBean job = this.addRecordToCoordJobTableWithPausedTime(Job.Status.PAUSEDWITHERROR, false, false, pauseTime);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.FAILED, "coord-rerun-action1.xml", 0);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        job = getCoordinatorJob(job.getId());
        assertEquals(Job.Status.PAUSEDWITHERROR, job.getStatus());

        new CoordRerunXCommand(job.getId(), RestConstants.JOB_COORD_SCOPE_DATE, "2009-12-15T01:00Z", false, true, false,
                null).call();

        job = getCoordinatorJob(job.getId());
        assertEquals(Job.Status.PAUSEDWITHERROR, job.getStatus());
        assertNotNull(job.getPauseTime());
    }

    /**
     * rerun <jobId> -action 1 -nocleanup. Coordinator job is killed, but actions are able to rerun.
     *
     * @throws Exception
     */
    public void testCoordRerunKilledCoord() throws Exception {
        final String jobId = "0000000-" + new Date().getTime() + "-testCoordRerun-C";
        final int actionNum = 1;
        final String actionId = jobId + "@" + actionNum;
        try {
            addRecordToJobTable(jobId, CoordinatorJob.Status.KILLED);
            addRecordToActionTable(jobId, actionNum, actionId, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action1.xml");
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }

        try {
            final OozieClient coordClient = LocalOozie.getCoordClient();
            coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_SCOPE_ACTION, Integer.toString(actionNum), false,
                    true);
        }
        catch (OozieClientException ex) {
            ex.printStackTrace();
            fail("Coord rerun failed");
        }

        CoordinatorActionBean action2 = getCoordinatorAction(actionId);
        assertEquals(action2.getStatus(), CoordinatorAction.Status.WAITING);
    }

    /*
     * Test rerun of failed action
     */
    public void testCoordRerunFailedCoordAction() throws Exception {
        final String jobId = "0000000-" + new Date().getTime() + "-testCoordRerun-C";
        final int actionNum = 1;
        final String actionId = jobId + "@" + actionNum;
        try {
            addRecordToJobTable(jobId, CoordinatorJob.Status.RUNNING);
            addRecordToActionTable(jobId, actionNum, actionId, CoordinatorAction.Status.FAILED,
                    "coord-rerun-action1.xml");
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }

        try {
            final OozieClient coordClient = LocalOozie.getCoordClient();
            coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_SCOPE_ACTION, Integer.toString(actionNum), false,
                    true);
        }
        catch (OozieClientException ex) {
            ex.printStackTrace();
            fail("Coord rerun failed");
        }
        CoordinatorActionBean action2 = getCoordinatorAction(actionId);
        assertEquals(action2.getStatus(), CoordinatorAction.Status.WAITING);
        assertEquals(action2.getErrorCode(), "");
        assertEquals(action2.getErrorMessage(), "");
    }

    /**
     * Tests functionality of the coord rerun for backward support is true. </p> Insert a coordinator job with SUCCEEDED
     * and coordinator actions with pending false, but one of action is FAILED.
     * Set oozie.service.StatusTransitService.backward.support.for.coord.status=true
     * and use uri:oozie:coordinator:0.1 namespace, then, rerun the coord job for action 1 and action 2.
     *
     * @throws Exception
     */
    public void testCoordRerunForBackwardSupport1() throws Exception {
        Services.get().destroy();
        setSystemProperty(StatusTransitService.CONF_BACKWARD_SUPPORT_FOR_COORD_STATUS, "true");
        services = new Services();
        services.init();

        Date start = DateUtils.parseDateOozieTZ("2009-02-01T01:00Z");
        Date end = DateUtils.parseDateOozieTZ("2009-02-02T23:59Z");
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, start, end, false,
                true, 3);

        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        coordJob.setAppNamespace(SchemaService.COORDINATOR_NAMESPACE_URI_1);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB_APPNAMESPACE, coordJob);

        CoordinatorActionBean action1 = addRecordToCoordActionTable(coordJob.getId(), 1,
                CoordinatorAction.Status.FAILED, "coord-rerun-action1.xml", 0);
        CoordinatorActionBean action2 = addRecordToCoordActionTable(coordJob.getId(), 2,
                CoordinatorAction.Status.SUCCEEDED, "coord-rerun-action1.xml", 0);
        CoordinatorActionBean action3 = addRecordToCoordActionTable(coordJob.getId(), 3,
                CoordinatorAction.Status.SUCCEEDED, "coord-rerun-action1.xml", 0);

        String rerunScope = Integer.toString(1) + "-" + Integer.toString(2);

        final OozieClient coordClient = LocalOozie.getCoordClient();
        coordClient.reRunCoord(coordJob.getId(), RestConstants.JOB_COORD_SCOPE_ACTION, rerunScope, false, true);

        coordJob = getCoordinatorJob(coordJob.getId());
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob.getStatus());

        action1 = getCoordinatorAction(action1.getId());
        assertNotSame(action1.getStatus(), CoordinatorAction.Status.FAILED);

        action2 = getCoordinatorAction(action2.getId());
        assertNotSame(action2.getStatus(), CoordinatorAction.Status.SUCCEEDED);
    }

    /**
     * Tests functionality of the coord rerun for backward support is true. </p> Insert a coordinator job with SUSPENDED
     * and coordinator actions with pending false, but one of action is FAILED.
     * Set oozie.service.StatusTransitService.backward.support.for.coord.status=true
     * and use uri:oozie:coordinator:0.1 namespace, then, rerun the coord job for action 1 and action 2.
     *
     * @throws Exception
     */
    public void testCoordRerunForBackwardSupport2() throws Exception {
        Services.get().destroy();
        setSystemProperty(StatusTransitService.CONF_BACKWARD_SUPPORT_FOR_COORD_STATUS, "true");
        services = new Services();
        services.init();

        Date start = DateUtils.parseDateOozieTZ("2009-02-01T01:00Z");
        Date end = DateUtils.parseDateOozieTZ("2009-02-02T23:59Z");
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUSPENDED, start, end, false,
                true, 3);

        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        coordJob.setAppNamespace(SchemaService.COORDINATOR_NAMESPACE_URI_1);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB_APPNAMESPACE, coordJob);

        CoordinatorActionBean action1 = addRecordToCoordActionTable(coordJob.getId(), 1,
                CoordinatorAction.Status.FAILED, "coord-rerun-action1.xml", 0);
        CoordinatorActionBean action2 = addRecordToCoordActionTable(coordJob.getId(), 2,
                CoordinatorAction.Status.SUCCEEDED, "coord-rerun-action1.xml", 0);
        CoordinatorActionBean action3 = addRecordToCoordActionTable(coordJob.getId(), 3,
                CoordinatorAction.Status.SUCCEEDED, "coord-rerun-action1.xml", 0);

        String rerunScope = Integer.toString(1) + "-" + Integer.toString(2);

        final OozieClient coordClient = LocalOozie.getCoordClient();
        coordClient.reRunCoord(coordJob.getId(), RestConstants.JOB_COORD_SCOPE_ACTION, rerunScope, false, true);

        coordJob = getCoordinatorJob(coordJob.getId());
        assertEquals(CoordinatorJob.Status.SUSPENDED, coordJob.getStatus());

        action1 = getCoordinatorAction(action1.getId());
        assertNotSame(action1.getStatus(), CoordinatorAction.Status.FAILED);

        action2 = getCoordinatorAction(action2.getId());
        assertNotSame(action2.getStatus(), CoordinatorAction.Status.SUCCEEDED);
    }

    /**
     * Tests functionality of the coord rerun for backward support is true. </p> Insert a coordinator job with SUCCEEDED
     * and coordinator actions with pending false. However, job's doneMaterialization is false to imitate update from
     * 2.x to 3.0.1.
     * Set oozie.service.StatusTransitService.backward.support.for.coord.status=true
     * and use uri:oozie:coordinator:0.1 namespace, then, rerun the coord job for action 1 and action 2.
     *
     * @throws Exception
     */
    public void testCoordRerunForBackwardSupport3() throws Exception {
        Services.get().destroy();
        setSystemProperty(StatusTransitService.CONF_BACKWARD_SUPPORT_FOR_COORD_STATUS, "true");
        services = new Services();
        services.init();

        Date start = DateUtils.parseDateOozieTZ("2009-02-01T01:00Z");
        Date end = DateUtils.parseDateOozieTZ("2009-02-02T23:59Z");
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, start, end, false,
                false, 3);

        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        coordJob.setAppNamespace(SchemaService.COORDINATOR_NAMESPACE_URI_1);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB_APPNAMESPACE, coordJob);

        CoordinatorActionBean action1 = addRecordToCoordActionTable(coordJob.getId(), 1,
                CoordinatorAction.Status.SUCCEEDED, "coord-rerun-action1.xml", 0);
        CoordinatorActionBean action2 = addRecordToCoordActionTable(coordJob.getId(), 2,
                CoordinatorAction.Status.SUCCEEDED, "coord-rerun-action1.xml", 0);
        CoordinatorActionBean action3 = addRecordToCoordActionTable(coordJob.getId(), 3,
                CoordinatorAction.Status.SUCCEEDED, "coord-rerun-action1.xml", 0);

        String rerunScope = Integer.toString(1) + "-" + Integer.toString(2);

        final OozieClient coordClient = LocalOozie.getCoordClient();
        coordClient.reRunCoord(coordJob.getId(), RestConstants.JOB_COORD_SCOPE_ACTION, rerunScope, false, true);

        coordJob = getCoordinatorJob(coordJob.getId());
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob.getStatus());

        action1 = getCoordinatorAction(action1.getId());
        assertNotSame(action1.getStatus(), CoordinatorAction.Status.SUCCEEDED);

        action2 = getCoordinatorAction(action2.getId());
        assertNotSame(action2.getStatus(), CoordinatorAction.Status.SUCCEEDED);
    }

    protected CoordinatorJobBean addRecordToCoordJobTableWithPausedTime(CoordinatorJob.Status status, boolean pending,
            boolean doneMatd, Date pausedTime) throws Exception {
        CoordinatorJobBean coordJob = createCoordJob(status, pending, doneMatd);
        coordJob.setPauseTime(pausedTime);
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

    private void addRecordToJobTable(String jobId, CoordinatorJob.Status status)
            throws StoreException, IOException {
        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String appXml = getCoordJobXml(appPath);

        FileSystem fs = getFileSystem();

        Writer writer = new OutputStreamWriter(fs.create(new Path(appPath + "/coordinator.xml")));
        byte[] bytes = appXml.getBytes("UTF-8");
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        Reader reader2 = new InputStreamReader(bais);
        IOUtils.copyCharStream(reader2, writer);

        CoordinatorJobBean coordJob = new CoordinatorJobBean();
        coordJob.setId(jobId);
        coordJob.setAppName("COORD-TEST");
        coordJob.setAppPath(appPath.toString());
        coordJob.setStatus(status);
        coordJob.setCreatedTime(new Date());
        coordJob.setLastModifiedTime(new Date());
        coordJob.setUser(getTestUser());
        coordJob.setGroup(getTestGroup());

        Properties conf = getCoordProp(appPath);
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
            addRecordToCoordJobTable(coordJob);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Unable to insert the test job record to table");
            throw new StoreException(ErrorCode.E1019, e.getMessage());
        }

    }

    private void addRecordToActionTable(String jobId, int actionNum, String actionId,
            CoordinatorAction.Status status, String resourceXmlName) throws StoreException, IOException {
        addRecordToActionTable(jobId, actionNum, actionId, status, resourceXmlName, false);
    }

    private void addRecordToActionTable(String jobId, int actionNum, String actionId,
            CoordinatorAction.Status status, String resourceXmlName, boolean isHCatDep) throws StoreException,
            IOException {
        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String actionXml = null;
        if(isHCatDep != true) {
            actionXml = getCoordActionXml(appPath, resourceXmlName);
        } else {
            actionXml = getCoordActionXmlForHCat(appPath, resourceXmlName);
        }
        String actionNomialTime = getActionNomialTime(actionXml);

        CoordinatorActionBean action = new CoordinatorActionBean();
        action.setJobId(jobId);
        action.setId(actionId);
        action.setActionNumber(actionNum);
        try {
            action.setNominalTime(DateUtils.parseDateOozieTZ(actionNomialTime));
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Unable to get action nominal time");
            throw new IOException(e);
        }
        action.setLastModifiedTime(new Date());
        action.setStatus(status);
        action.setActionXml(actionXml);

        Properties conf = getCoordProp(appPath);
        String createdConf = XmlUtils.writePropToString(conf);

        action.setCreatedConf(createdConf);

        if (status.equals(CoordinatorAction.Status.FAILED)) {
            action.setErrorCode("E1000");
            action.setErrorMessage("Error");
        }

        try {
            addRecordToCoordActionTable(action, null);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Unable to insert the test job record to table");
            throw new StoreException(ErrorCode.E1019, e.getMessage());
        }

    }

    private Properties getCoordProp(Path appPath) throws IOException {
        Path wfAppPath = new Path(getFsTestCaseDir(), "workflow");
        final OozieClient coordClient = LocalOozie.getCoordClient();
        Properties conf = coordClient.createConfiguration();
        conf.setProperty(OozieClient.COORDINATOR_APP_PATH, appPath.toString());
        conf.setProperty("jobTracker", getJobTrackerUri());
        conf.setProperty("nameNode", getNameNodeUri());
        conf.setProperty("wfAppPath", wfAppPath.toString());
        conf.remove("user.name");
        conf.setProperty("user.name", getTestUser());


        String content = "<workflow-app xmlns='uri:oozie:workflow:0.1'  xmlns:sla='uri:oozie:sla:0.1' name='no-op-wf'>";
        content += "<start to='end' />";
        content += "<end name='end' /></workflow-app>";
        writeToFile(content, wfAppPath, "workflow.xml");

        return conf;
    }

    @SuppressWarnings("unchecked")
    private String[] getActionXmlUrls(Element eAction, String user, String group) {
        Element outputList = eAction.getChild("input-events", eAction.getNamespace());
        for (Element data : (List<Element>) outputList.getChildren("data-in", eAction.getNamespace())) {
            if (data.getChild("uris", data.getNamespace()) != null) {
                String uris = data.getChild("uris", data.getNamespace()).getTextTrim();
                if (uris != null) {
                    String[] uriArr = uris.split(CoordELFunctions.INSTANCE_SEPARATOR);
                    return uriArr;
                }
            }
        }
        return null;
    }

    @Override
    protected String getCoordJobXml(Path appPath) {
        String inputTemplate = appPath + "/coord-input/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
        inputTemplate = Matcher.quoteReplacement(inputTemplate);
        String outputTemplate = appPath + "/coord-input/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
        outputTemplate = Matcher.quoteReplacement(outputTemplate);
        try {
            Reader reader = IOUtils.getResourceAsReader("coord-rerun-job.xml", -1);
            String appXml = IOUtils.getReaderAsString(reader, -1);
            appXml = appXml.replaceAll("#inputTemplate", inputTemplate);
            appXml = appXml.replaceAll("#outputTemplate", outputTemplate);
            return appXml;
        }
        catch (IOException ioe) {
            throw new RuntimeException(XLog.format("Could not get coord-rerun-job.xml", ioe));
        }
    }

    @Override
    protected String getCoordActionXml(Path appPath, String resourceXmlName) {
        String inputTemplate = appPath + "/coord-input/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
        inputTemplate = Matcher.quoteReplacement(inputTemplate);
        String outputTemplate = appPath + "/coord-input/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
        outputTemplate = Matcher.quoteReplacement(outputTemplate);

        String inputDir = appPath + "/coord-input/2010/07/05/01/00";
        inputDir = Matcher.quoteReplacement(inputDir);
        String outputDir = appPath + "/coord-input/2009/12/14/11/00";
        outputDir = Matcher.quoteReplacement(outputDir);
        try {
            Reader reader = IOUtils.getResourceAsReader(resourceXmlName, -1);
            String appXml = IOUtils.getReaderAsString(reader, -1);
            appXml = appXml.replaceAll("#inputTemplate", inputTemplate);
            appXml = appXml.replaceAll("#outputTemplate", outputTemplate);
            appXml = appXml.replaceAll("#inputDir", inputDir);
            appXml = appXml.replaceAll("#outputDir", outputDir);
            return appXml;
        }
        catch (IOException ioe) {
            throw new RuntimeException(XLog.format("Could not get " + resourceXmlName, ioe));
        }
    }

    protected String getCoordActionXmlForHCat(Path appPath, String resourceXmlName) {
        String hcatServer = getHCatalogServer().getMetastoreAuthority();
        String inputTemplate = "hcat://" + hcatServer + "/mydb/mytable/ds=${YEAR}${MONTH}${DAY}${HOUR};region=usa";
        inputTemplate = Matcher.quoteReplacement(inputTemplate);
        String outputTemplate = "hcat://" + hcatServer + "/mydb/mytable/ds=${YEAR}${MONTH}${DAY}${HOUR};region=usa";
        outputTemplate = Matcher.quoteReplacement(outputTemplate);
        String inputDir = "hcat://" + hcatServer + "/mydb/mytable/ds=2010070501;region=usa";
        inputDir = Matcher.quoteReplacement(inputDir);
        String outputDir = "hcat://" + hcatServer + "/mydb/mytable/ds=2009121411;region=usa";
        outputDir = Matcher.quoteReplacement(outputDir);
        try {
            Reader reader = IOUtils.getResourceAsReader(resourceXmlName, -1);
            String appXml = IOUtils.getReaderAsString(reader, -1);
            appXml = appXml.replaceAll("#inputTemplate", inputTemplate);
            appXml = appXml.replaceAll("#outputTemplate", outputTemplate);
            appXml = appXml.replaceAll("#inputDir", inputDir);
            appXml = appXml.replaceAll("#outputDir", outputDir);
            return appXml;
        }
        catch (IOException ioe) {
            throw new RuntimeException(XLog.format("Could not get " + resourceXmlName, ioe));
        }
    }

    private String getActionNomialTime(String actionXml) {
        Element eAction;
        try {
            eAction = XmlUtils.parseXml(actionXml);
        }
        catch (JDOMException je) {
            throw new RuntimeException(XLog.format("Could not parse actionXml :" + actionXml, je));
        }
        String actionNomialTime = eAction.getAttributeValue("action-nominal-time");

        return actionNomialTime;
    }

    /**
     * It will verify the Action status running when workflow triggered.
     * @throws Exception
     */
    public void testActionStatusRunningWithWorkflow() throws Exception {
        Date start = DateUtils.parseDateOozieTZ("2009-12-15T01:00Z");
        Date end = DateUtils.parseDateOozieTZ("2009-12-16T01:00Z");
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, false,
                false, 1);

        CoordinatorActionBean action = addRecordToWithLazyAction(coordJob.getId(), 1,
                CoordinatorAction.Status.SUBMITTED, "coord-rerun-action1.xml");

        String actionId = action.getId();
        new CoordActionStartXCommand(actionId, getTestUser(), "myapp", "myjob").call();

        action = getCoordinatorAction(actionId);

        if (action.getStatus() == CoordinatorAction.Status.SUBMITTED) {
            fail("CoordActionStartCommand didn't work because the status for action id" + actionId + " is :"
                    + action.getStatus() + " expected to be NOT SUBMITTED (i.e. RUNNING)");
        }

        final String wfId = action.getExternalId();

        final OozieClient wfClient = LocalOozie.getClient();

        waitFor(15 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return wfClient.getJobInfo(wfId).getStatus() == WorkflowJob.Status.RUNNING;
            }
        });

        wfClient.kill(wfId);

        waitFor(15 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return wfClient.getJobInfo(wfId).getStatus() == WorkflowJob.Status.KILLED;
            }
        });
        assertEquals(WorkflowJob.Status.KILLED, wfClient.getJobInfo(wfId).getStatus());

        Properties conf = wfClient.createConfiguration();
        conf.setProperty(OozieClient.RERUN_FAIL_NODES, "true");
        wfClient.reRun(wfId,conf);

        waitFor(15 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return wfClient.getJobInfo(wfId).getStatus() == WorkflowJob.Status.RUNNING;
            }
        });

        assertEquals(WorkflowJob.Status.RUNNING, wfClient.getJobInfo(wfId).getStatus());
        OozieClient coordActionClient = LocalOozie.getCoordClient();
        assertEquals(CoordinatorAction.Status.RUNNING,coordActionClient.getCoordActionInfo(actionId).getStatus());
    }

    private CoordinatorActionBean addRecordToWithLazyAction
            (String jobId, int actionNum, CoordinatorAction.Status status, String resourceXmlName) throws IOException {
        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String actionXml = getCoordActionXml(appPath, resourceXmlName);
        String actionNomialTime = getActionNomialTime(actionXml);

        CoordinatorActionBean action = new CoordinatorActionBean();
        action.setJobId(jobId);
        action.setId(Services.get().get(UUIDService.class).generateChildId(jobId, actionNum + ""));
        action.setActionNumber(actionNum);
        try {
            action.setNominalTime(DateUtils.parseDateOozieTZ(actionNomialTime));
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Unable to get action nominal time");
            throw new IOException(e);
        }
        action.setLastModifiedTime(new Date());
        action.setStatus(status);
        action.setActionXml(actionXml);

        Properties conf = getLazyWorkflowProp(appPath);
        String createdConf = XmlUtils.writePropToString(conf);
        action.setCreatedConf(createdConf);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordActionInsertJPAExecutor coordActionInsertCmd = new CoordActionInsertJPAExecutor(action);
        try {
            jpaService.execute(coordActionInsertCmd);
        } catch (JPAExecutorException e) {
            e.printStackTrace();
            fail("Unable to insert the test coord action record to table");
        }
        return action;
    }

    private Properties getLazyWorkflowProp(Path appPath) throws IOException {
        Path wfAppPath = new Path(getFsTestCaseDir(), "workflow");
        final OozieClient coordClient = LocalOozie.getCoordClient();
        Properties conf = coordClient.createConfiguration();
        conf.setProperty(OozieClient.COORDINATOR_APP_PATH, appPath.toString());
        conf.setProperty("jobTracker", getJobTrackerUri());
        conf.setProperty("nameNode", getNameNodeUri());
        conf.setProperty("wfAppPath", wfAppPath.toString());
        conf.remove("user.name");
        conf.setProperty("user.name", getTestUser());
        writeToFile(getLazyWorkflow(), wfAppPath, "workflow.xml");
        return conf;
    }
    public String getLazyWorkflow() {
        return  "<workflow-app xmlns='uri:oozie:workflow:0.1'  xmlns:sla='uri:oozie:sla:0.1' name='no-op-wf'>" +
                "<start to='java' />" +
                "       <action name='java'>" +
                "<java>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<main-class>" + JavaSleepAction.class.getName() + "</main-class>" +
                "<arg>exit0</arg>" +
                "</java>"
                + "<ok to='end' />"
                + "<error to='fail' />"
                + "</action>"
                + "<kill name='fail'>"
                + "<message>shell action fail, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>"
                + "</kill>"
                + "<end name='end' />"
                + "</workflow-app>";
    }

    /**
     * Tests -failed option of rerun. If failed option is provided it should rerun the old workflow of an action
     * otherwise it should run the new workflow.
     * @throws Exception
     */
    public void testCoordRerunWithFailedOption() throws Exception {
        Date start = DateUtils.parseDateOozieTZ("2009-02-01T01:00Z");
        Date end = DateUtils.parseDateOozieTZ("2009-02-01T23:59Z");
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, false,
                false, 1);

        CoordinatorActionBean action = addRecordToWithLazyAction(coordJob.getId(), 1,
                CoordinatorAction.Status.SUBMITTED, "coord-rerun-action1.xml");
        final String actionId = action.getId();
        new CoordActionStartXCommand(actionId, getTestUser(), "myapp", "myjob").call();

        action = getCoordinatorAction(actionId);

        if (action.getStatus() == CoordinatorAction.Status.SUBMITTED) {
            fail("CoordActionStartCommand didn't work because the status for action id" + actionId + " is :"
                    + action.getStatus() + " expected to be NOT SUBMITTED (i.e. RUNNING)");
        }

        final OozieClient coordClient = LocalOozie.getCoordClient();
        final OozieClient wclient = LocalOozie.getClient();
        waitFor(15*1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return (coordClient.getCoordActionInfo(actionId).getStatus() == CoordinatorAction.Status.RUNNING);
            }
        });

        wclient.kill(coordClient.getCoordActionInfo(actionId).getExternalId());

        waitFor(150*1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return (coordClient.getCoordActionInfo(actionId).getStatus() == CoordinatorAction.Status.KILLED);
            }
        });

        String externalId = coordClient.getCoordActionInfo(actionId).getExternalId();

        coordClient.reRunCoord(coordJob.getId(), RestConstants.JOB_COORD_SCOPE_ACTION, "1", false, true, true,
                new Properties());

        waitFor(150 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return (coordClient.getCoordActionInfo(actionId).getStatus() == CoordinatorAction.Status.SUCCEEDED);
            }
        });
        assertEquals(externalId,coordClient.getCoordActionInfo(actionId).getExternalId());

        coordClient.reRunCoord(coordJob.getId(), RestConstants.JOB_COORD_SCOPE_ACTION, "1", false, true, false,
                new Properties());

        waitFor(150*1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return (coordClient.getCoordActionInfo(actionId).getStatus() == CoordinatorAction.Status.SUCCEEDED);
            }
        });
        assertNotSame(externalId,coordClient.getCoordActionInfo(actionId).getExternalId());
    }

    /**
     *  Passing config of workflow during rerun of coordinator.
     * @throws Exception
     */
    public void testCoordRerunWithConfOption() throws Exception {
        Date start = DateUtils.parseDateOozieTZ("2009-02-01T01:00Z");
        Date end = DateUtils.parseDateOozieTZ("2009-02-01T23:59Z");
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, false,
                false, 1);

        CoordinatorActionBean action = addRecordToWithLazyAction(coordJob.getId(), 1,
                CoordinatorAction.Status.SUBMITTED, "coord-rerun-action1.xml");
        final String actionId = action.getId();
        new CoordActionStartXCommand(actionId, getTestUser(), "myapp", "myjob").call();

        action = getCoordinatorAction(actionId);

        if (action.getStatus() == CoordinatorAction.Status.SUBMITTED) {
            fail("CoordActionStartCommand didn't work because the status for action id" + actionId + " is :"
                    + action.getStatus() + " expected to be NOT SUBMITTED (i.e. RUNNING)");
        }

        final OozieClient coordClient = LocalOozie.getCoordClient();
        final OozieClient wclient = LocalOozie.getClient();
        waitFor(15*1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return (coordClient.getCoordActionInfo(actionId).getStatus() == CoordinatorAction.Status.RUNNING);
            }
        });

        wclient.kill(coordClient.getCoordActionInfo(actionId).getExternalId());

        waitFor(150*1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return (coordClient.getCoordActionInfo(actionId).getStatus() == CoordinatorAction.Status.KILLED);
            }
        });

        Properties prop = new Properties();

        // Passing props to coordinator which will be passed to workflow rerun as well.
        prop.setProperty("workflowConf", "foo");
        coordClient.reRunCoord(coordJob.getId(), RestConstants.JOB_COORD_SCOPE_ACTION, "1", false, true, true,
                prop);

        waitFor(150 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return (coordClient.getCoordActionInfo(actionId).getStatus() == CoordinatorAction.Status.SUCCEEDED);
            }
        });

        WorkflowJob wfJob = wclient.getJobInfo(coordClient.getCoordActionInfo(actionId).getExternalId());
        Configuration conf = new XConfiguration(new StringReader(wfJob.getConf()));
        assertEquals(prop.get("workflowConf"), conf.get("workflowConf"));
    }
}
