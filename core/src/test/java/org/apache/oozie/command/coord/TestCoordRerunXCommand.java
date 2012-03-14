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
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.CoordinatorJob.Execution;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.SchemaService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.StatusTransitService;
import org.apache.oozie.service.StoreService;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.JDOMException;

public class TestCoordRerunXCommand extends XDataTestCase {
    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        cleanUpDBTables();
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
        CoordinatorStore store = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store.beginTrx();
        try {
            addRecordToJobTable(jobId, store, CoordinatorJob.Status.SUCCEEDED);
            addRecordToActionTable(jobId, actionNum, actionId, store, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action1.xml");
            store.commitTrx();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }
        finally {
            store.closeTrx();
        }

        CoordinatorStore store1 = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store1.beginTrx();
        CoordinatorActionBean action1 = store1.getCoordinatorAction(actionId, false);
        assertEquals(action1.getStatus(), CoordinatorAction.Status.SUCCEEDED);
        store1.commitTrx();
        store1.closeTrx();

        final OozieClient coordClient = LocalOozie.getCoordClient();
        coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_RERUN_ACTION, Integer.toString(actionNum), false, true);

        CoordinatorStore store2 = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store2.beginTrx();
        CoordinatorActionBean action2 = store2.getCoordinatorAction(actionId, false);
        assertNotSame(action2.getStatus(), CoordinatorAction.Status.SUCCEEDED);
        store2.commitTrx();
        store2.closeTrx();
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
        CoordinatorStore store = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store.beginTrx();
        try {
            addRecordToJobTable(jobId, store, CoordinatorJob.Status.SUCCEEDED);
            addRecordToActionTable(jobId, actionNum1, actionId1, store, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action1.xml");
            addRecordToActionTable(jobId, actionNum2, actionId2, store, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action2.xml");
            store.commitTrx();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }
        finally {
            store.closeTrx();
        }

        String rerunScope = Integer.toString(actionNum1) + "-" + Integer.toString(actionNum2);

        final OozieClient coordClient = LocalOozie.getCoordClient();
        coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_RERUN_ACTION, rerunScope, false, true);

        CoordinatorStore store1 = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store1.beginTrx();
        CoordinatorActionBean action1 = store1.getCoordinatorAction(actionId1, false);
        assertNotSame(action1.getStatus(), CoordinatorAction.Status.SUCCEEDED);
        CoordinatorActionBean action2 = store1.getCoordinatorAction(actionId2, false);
        assertNotSame(action2.getStatus(), CoordinatorAction.Status.SUCCEEDED);
        store1.commitTrx();
        store1.closeTrx();
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
        CoordinatorStore store = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store.beginTrx();
        try {
            addRecordToJobTable(jobId, store, CoordinatorJob.Status.SUCCEEDED);
            addRecordToActionTable(jobId, actionNum1, actionId1, store, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action1.xml");
            addRecordToActionTable(jobId, actionNum2, actionId2, store, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action2.xml");
            store.commitTrx();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }
        finally {
            store.closeTrx();
        }
        String rerunScope = Integer.toString(actionNum1) + "," + Integer.toString(actionNum2);

        final OozieClient coordClient = LocalOozie.getCoordClient();
        coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_RERUN_ACTION, rerunScope, false, true);

        CoordinatorStore store1 = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store1.beginTrx();
        CoordinatorActionBean action1 = store1.getCoordinatorAction(actionId1, false);
        assertNotSame(action1.getStatus(), CoordinatorAction.Status.SUCCEEDED);
        CoordinatorActionBean action2 = store1.getCoordinatorAction(actionId2, false);
        assertNotSame(action2.getStatus(), CoordinatorAction.Status.SUCCEEDED);
        store1.commitTrx();
        store1.closeTrx();
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
        CoordinatorStore store = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store.beginTrx();
        try {
            addRecordToJobTable(jobId, store, CoordinatorJob.Status.SUCCEEDED);
            addRecordToActionTable(jobId, actionNum1, actionId1, store, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action1.xml");
            addRecordToActionTable(jobId, actionNum2, actionId2, store, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action2.xml");
            store.commitTrx();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }
        finally {
            store.closeTrx();
        }

        String rerunScope = "1-3";
        try {
            final OozieClient coordClient = LocalOozie.getCoordClient();
            coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_RERUN_ACTION, rerunScope, false, true);
            fail("Exception expected because one action is missing from db.");
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
        CoordinatorStore store = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store.beginTrx();
        try {
            addRecordToJobTable(jobId, store, CoordinatorJob.Status.SUCCEEDED);
            addRecordToActionTable(jobId, actionNum, actionId, store, CoordinatorAction.Status.RUNNING,
                    "coord-rerun-action1.xml");
            store.commitTrx();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }
        finally {
            store.closeTrx();
        }

        try {
            final OozieClient coordClient = LocalOozie.getCoordClient();
            coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_RERUN_ACTION, Integer.toString(actionNum), false,
                    true);
            fail("Exception expected because action is not in terminal state.");
        }
        catch (OozieClientException ex) {
            if (!ex.getErrorCode().equals(ErrorCode.E1018.toString())) {
                fail("Error code should be E1018 when action is not in terminal state.");
            }
        }

        CoordinatorStore store2 = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store2.beginTrx();
        CoordinatorActionBean action2 = store2.getCoordinatorAction(actionId, false);
        assertEquals(action2.getStatus(), CoordinatorAction.Status.RUNNING);
        store2.commitTrx();
        store2.closeTrx();
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
        CoordinatorStore store = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store.beginTrx();
        try {
            addRecordToJobTable(jobId, store, CoordinatorJob.Status.SUCCEEDED);
            addRecordToActionTable(jobId, actionNum, actionId, store, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action1.xml");
            store.commitTrx();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }
        finally {
            store.closeTrx();
        }

        CoordinatorStore store1 = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store1.beginTrx();
        CoordinatorActionBean action1 = store1.getCoordinatorAction(actionId, false);
        assertEquals(action1.getStatus(), CoordinatorAction.Status.SUCCEEDED);
        store1.commitTrx();
        store1.closeTrx();

        final OozieClient coordClient = LocalOozie.getCoordClient();
        coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_RERUN_DATE, "2009-12-15T01:00Z", false, true);

        CoordinatorStore store2 = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store2.beginTrx();
        CoordinatorActionBean action2 = store2.getCoordinatorAction(actionId, false);
        assertNotSame(action2.getStatus(), CoordinatorAction.Status.SUCCEEDED);
        store2.commitTrx();
        store2.closeTrx();
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
        CoordinatorStore store = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store.beginTrx();
        try {
            addRecordToJobTable(jobId, store, CoordinatorJob.Status.SUCCEEDED);
            addRecordToActionTable(jobId, actionNum1, actionId1, store, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action1.xml");
            addRecordToActionTable(jobId, actionNum2, actionId2, store, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action2.xml");
            store.commitTrx();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }
        finally {
            store.closeTrx();
        }

        String rerunScope = "2009-12-15T01:00Z" + "::" + "2009-12-16T01:00Z";

        final OozieClient coordClient = LocalOozie.getCoordClient();
        coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_RERUN_DATE, rerunScope, false, true);

        CoordinatorStore store1 = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store1.beginTrx();
        CoordinatorActionBean action1 = store1.getCoordinatorAction(actionId1, false);
        assertNotSame(action1.getStatus(), CoordinatorAction.Status.SUCCEEDED);
        CoordinatorActionBean action2 = store1.getCoordinatorAction(actionId2, false);
        assertNotSame(action2.getStatus(), CoordinatorAction.Status.SUCCEEDED);
        store1.commitTrx();
        store1.closeTrx();
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
        CoordinatorStore store = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store.beginTrx();
        try {
            addRecordToJobTable(jobId, store, CoordinatorJob.Status.SUCCEEDED);
            addRecordToActionTable(jobId, actionNum1, actionId1, store, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action1.xml");
            addRecordToActionTable(jobId, actionNum2, actionId2, store, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action2.xml");
            store.commitTrx();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }
        finally {
            store.closeTrx();
        }

        String rerunScope = "2009-12-15T01:00Z" + "," + "2009-12-16T01:00Z";

        final OozieClient coordClient = LocalOozie.getCoordClient();
        coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_RERUN_DATE, rerunScope, false, true);

        CoordinatorStore store1 = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store1.beginTrx();
        CoordinatorActionBean action1 = store1.getCoordinatorAction(actionId1, false);
        assertNotSame(action1.getStatus(), CoordinatorAction.Status.SUCCEEDED);
        CoordinatorActionBean action2 = store1.getCoordinatorAction(actionId2, false);
        assertNotSame(action2.getStatus(), CoordinatorAction.Status.SUCCEEDED);
        store1.commitTrx();
        store1.closeTrx();
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
        CoordinatorStore store = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store.beginTrx();
        try {
            addRecordToJobTable(jobId, store, CoordinatorJob.Status.SUCCEEDED);
            addRecordToActionTable(jobId, actionNum1, actionId1, store, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action1.xml");
            addRecordToActionTable(jobId, actionNum2, actionId2, store, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action2.xml");
            store.commitTrx();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }
        finally {
            store.closeTrx();
        }

        String rerunScope = "2009-12-15T01:00Z" + "::" + "2009-12-17T01:00Z";

        final OozieClient coordClient = LocalOozie.getCoordClient();
        coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_RERUN_DATE, rerunScope, false, true);

        CoordinatorStore store1 = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store1.beginTrx();
        CoordinatorActionBean action1 = store1.getCoordinatorAction(actionId1, false);
        assertNotSame(action1.getStatus(), CoordinatorAction.Status.SUCCEEDED);
        CoordinatorActionBean action2 = store1.getCoordinatorAction(actionId2, false);
        assertNotSame(action2.getStatus(), CoordinatorAction.Status.SUCCEEDED);
        store1.commitTrx();
        store1.closeTrx();
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
        CoordinatorStore store = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store.beginTrx();
        try {
            addRecordToJobTable(jobId, store, CoordinatorJob.Status.SUCCEEDED);
            addRecordToActionTable(jobId, actionNum1, actionId1, store, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action1.xml");
            addRecordToActionTable(jobId, actionNum2, actionId2, store, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action2.xml");
            store.commitTrx();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }
        finally {
            store.closeTrx();
        }
        String rerunScope = "2009-12-15T01:00Z,2009-12-16T01:00Z,2009-12-17T01:00Z";
        try {
            final OozieClient coordClient = LocalOozie.getCoordClient();
            coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_RERUN_DATE, rerunScope, false, true);
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
        CoordinatorStore store = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store.beginTrx();
        try {
            addRecordToJobTable(jobId, store, CoordinatorJob.Status.SUCCEEDED);
            addRecordToActionTable(jobId, actionNum, actionId, store, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action1.xml");
            store.commitTrx();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }
        finally {
            store.closeTrx();
        }
        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String inputDir = appPath.toString() + "/coord-input/2010/07/09/01/00";
        FileSystem fs = getFileSystem();
        fs.mkdirs(new Path(inputDir));
        fs.create(new Path(inputDir, "_SUCCESS"), true);

        final OozieClient coordClient = LocalOozie.getCoordClient();
        coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_RERUN_ACTION, Integer.toString(actionNum), true, true);

        CoordinatorStore store2 = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store2.beginTrx();
        CoordinatorActionBean action2 = store2.getCoordinatorAction(actionId, false);
        assertNotSame(action2.getStatus(), CoordinatorAction.Status.SUCCEEDED);
        store2.commitTrx();
        store2.closeTrx();

        waitFor(120 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                CoordinatorAction bean = coordClient.getCoordActionInfo(actionId);
                return (bean.getStatus() == CoordinatorAction.Status.READY || bean.getStatus() == CoordinatorAction.Status.SUBMITTED);
            }
        });

        CoordinatorStore store3 = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store3.beginTrx();
        CoordinatorActionBean action3 = store3.getCoordinatorAction(actionId, false);
        String actionXml = action3.getActionXml();
        System.out.println("After refresh, action xml= " + actionXml);

        Element eAction = XmlUtils.parseXml(actionXml);
        String[] urls = getActionXmlUrls(eAction, getTestUser(), getTestGroup());
        store3.commitTrx();
        store3.closeTrx();

        /*        if (urls != null) {
                    assertEquals(inputDir, urls[0]);
                }
                else {
                    fail("After refresh, latest() should get the inputDir:" + inputDir);
                }*/
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
        CoordinatorStore store = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store.beginTrx();
        try {
            addRecordToJobTable(jobId, store, CoordinatorJob.Status.SUCCEEDED);
            addRecordToActionTable(jobId, actionNum, actionId, store, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action1.xml");
            store.commitTrx();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }
        finally {
            store.closeTrx();
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
        coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_RERUN_ACTION, Integer.toString(actionNum), false, false);

        CoordinatorStore store2 = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store2.beginTrx();
        CoordinatorActionBean action2 = store2.getCoordinatorAction(actionId, false);
        assertNotSame(action2.getStatus(), CoordinatorAction.Status.SUCCEEDED);
        store2.commitTrx();
        store2.closeTrx();

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
     * Test : rerun <jobId> -action 1 with no output-event
     *
     * @throws Exception
     */
    public void testCoordRerunCleanupNoOutputEvents() throws Exception {
        final String jobId = "0000000-" + new Date().getTime() + "-testCoordRerun-C";
        final int actionNum = 1;
        final String actionId = jobId + "@" + actionNum;
        CoordinatorStore store = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store.beginTrx();
        try {
            addRecordToJobTable(jobId, store, CoordinatorJob.Status.SUCCEEDED);
            addRecordToActionTable(jobId, actionNum, actionId, store, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action3.xml");
            store.commitTrx();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }
        finally {
            store.closeTrx();
        }

        final OozieClient coordClient = LocalOozie.getCoordClient();
        coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_RERUN_ACTION, Integer.toString(actionNum), false, false);
        CoordinatorStore store2 = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store2.beginTrx();
        CoordinatorActionBean action2 = store2.getCoordinatorAction(actionId, false);
        assertNotSame(action2.getStatus(), CoordinatorAction.Status.SUCCEEDED);
        store2.commitTrx();
        store2.closeTrx();
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

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetJPAExecutor coordJobGetExecutor = new CoordJobGetJPAExecutor(job.getId());
        job = jpaService.execute(coordJobGetExecutor);
        assertEquals(Job.Status.FAILED, job.getStatus());

        try {
            new CoordRerunXCommand(job.getId(), RestConstants.JOB_COORD_RERUN_DATE, "2009-12-15T01:00Z", false, true)
                    .call();
            fail("Coordinator job is FAILED, rerun should throw exception");
        }
        catch (CommandException ce) {
        }
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
        CoordJobGetJPAExecutor coordJobGetExecutor = new CoordJobGetJPAExecutor(job.getId());
        job = jpaService.execute(coordJobGetExecutor);
        assertEquals(Job.Status.PAUSED, job.getStatus());

        new CoordRerunXCommand(job.getId(), RestConstants.JOB_COORD_RERUN_DATE, "2009-12-15T01:00Z", false, true)
                .call();

        job = jpaService.execute(coordJobGetExecutor);
        assertEquals(Job.Status.PAUSED, job.getStatus());
        assertNotNull(job.getPauseTime());
    }

    /**
     * Negative Test : rerun <jobId> -action 1 -nocleanup. Coordinator job is killed, so no actions are able to rerun.
     *
     * @throws Exception
     */
    public void testCoordRerunNeg() throws Exception {
        final String jobId = "0000000-" + new Date().getTime() + "-testCoordRerun-C";
        final int actionNum = 1;
        final String actionId = jobId + "@" + actionNum;
        CoordinatorStore store = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store.beginTrx();
        try {
            addRecordToJobTable(jobId, store, CoordinatorJob.Status.KILLED);
            addRecordToActionTable(jobId, actionNum, actionId, store, CoordinatorAction.Status.SUCCEEDED,
                    "coord-rerun-action1.xml");
            store.commitTrx();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not update db.");
        }
        finally {
            store.closeTrx();
        }

        try {
            final OozieClient coordClient = LocalOozie.getCoordClient();
            coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_RERUN_ACTION, Integer.toString(actionNum), false,
                    true);
            fail("Exception expected because action is not in terminal state.");
        }
        catch (OozieClientException ex) {
            if (!ex.getErrorCode().equals(ErrorCode.E1018.toString())) {
                fail("Error code should be E1018 when job is killed or failed.");
            }
        }

        CoordinatorStore store2 = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store2.beginTrx();
        CoordinatorActionBean action2 = store2.getCoordinatorAction(actionId, false);
        assertEquals(action2.getStatus(), CoordinatorAction.Status.SUCCEEDED);
        store2.commitTrx();
        store2.closeTrx();
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
        new Services().init();

        Date start = DateUtils.parseDateUTC("2009-02-01T01:00Z");
        Date end = DateUtils.parseDateUTC("2009-02-02T23:59Z");
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, start, end, false,
                true, 3);

        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        coordJob.setAppNamespace(SchemaService.COORDINATOR_NAMESPACE_URI_1);
        jpaService.execute(new CoordJobUpdateJPAExecutor(coordJob));

        CoordinatorActionBean action1 = addRecordToCoordActionTable(coordJob.getId(), 1,
                CoordinatorAction.Status.FAILED, "coord-rerun-action1.xml", 0);
        CoordinatorActionBean action2 = addRecordToCoordActionTable(coordJob.getId(), 2,
                CoordinatorAction.Status.SUCCEEDED, "coord-rerun-action1.xml", 0);
        CoordinatorActionBean action3 = addRecordToCoordActionTable(coordJob.getId(), 3,
                CoordinatorAction.Status.SUCCEEDED, "coord-rerun-action1.xml", 0);

        String rerunScope = Integer.toString(1) + "-" + Integer.toString(2);

        final OozieClient coordClient = LocalOozie.getCoordClient();
        coordClient.reRunCoord(coordJob.getId(), RestConstants.JOB_COORD_RERUN_ACTION, rerunScope, false, true);

        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(coordJob.getId());
        coordJob = jpaService.execute(coordJobGetCmd);
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob.getStatus());

        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(action1.getId());
        action1 = jpaService.execute(coordActionGetCmd);
        assertNotSame(action1.getStatus(), CoordinatorAction.Status.FAILED);

        coordActionGetCmd = new CoordActionGetJPAExecutor(action2.getId());
        action2 = jpaService.execute(coordActionGetCmd);
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
        new Services().init();

        Date start = DateUtils.parseDateUTC("2009-02-01T01:00Z");
        Date end = DateUtils.parseDateUTC("2009-02-02T23:59Z");
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUSPENDED, start, end, false,
                true, 3);

        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        coordJob.setAppNamespace(SchemaService.COORDINATOR_NAMESPACE_URI_1);
        jpaService.execute(new CoordJobUpdateJPAExecutor(coordJob));

        CoordinatorActionBean action1 = addRecordToCoordActionTable(coordJob.getId(), 1,
                CoordinatorAction.Status.FAILED, "coord-rerun-action1.xml", 0);
        CoordinatorActionBean action2 = addRecordToCoordActionTable(coordJob.getId(), 2,
                CoordinatorAction.Status.SUCCEEDED, "coord-rerun-action1.xml", 0);
        CoordinatorActionBean action3 = addRecordToCoordActionTable(coordJob.getId(), 3,
                CoordinatorAction.Status.SUCCEEDED, "coord-rerun-action1.xml", 0);

        String rerunScope = Integer.toString(1) + "-" + Integer.toString(2);

        final OozieClient coordClient = LocalOozie.getCoordClient();
        coordClient.reRunCoord(coordJob.getId(), RestConstants.JOB_COORD_RERUN_ACTION, rerunScope, false, true);

        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(coordJob.getId());
        coordJob = jpaService.execute(coordJobGetCmd);
        assertEquals(CoordinatorJob.Status.SUSPENDED, coordJob.getStatus());

        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(action1.getId());
        action1 = jpaService.execute(coordActionGetCmd);
        assertNotSame(action1.getStatus(), CoordinatorAction.Status.FAILED);

        coordActionGetCmd = new CoordActionGetJPAExecutor(action2.getId());
        action2 = jpaService.execute(coordActionGetCmd);
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
        new Services().init();

        Date start = DateUtils.parseDateUTC("2009-02-01T01:00Z");
        Date end = DateUtils.parseDateUTC("2009-02-02T23:59Z");
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, start, end, false,
                false, 3);

        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        coordJob.setAppNamespace(SchemaService.COORDINATOR_NAMESPACE_URI_1);
        jpaService.execute(new CoordJobUpdateJPAExecutor(coordJob));

        CoordinatorActionBean action1 = addRecordToCoordActionTable(coordJob.getId(), 1,
                CoordinatorAction.Status.SUCCEEDED, "coord-rerun-action1.xml", 0);
        CoordinatorActionBean action2 = addRecordToCoordActionTable(coordJob.getId(), 2,
                CoordinatorAction.Status.SUCCEEDED, "coord-rerun-action1.xml", 0);
        CoordinatorActionBean action3 = addRecordToCoordActionTable(coordJob.getId(), 3,
                CoordinatorAction.Status.SUCCEEDED, "coord-rerun-action1.xml", 0);

        String rerunScope = Integer.toString(1) + "-" + Integer.toString(2);

        final OozieClient coordClient = LocalOozie.getCoordClient();
        coordClient.reRunCoord(coordJob.getId(), RestConstants.JOB_COORD_RERUN_ACTION, rerunScope, false, true);

        CoordJobGetJPAExecutor coordJobGetCmd = new CoordJobGetJPAExecutor(coordJob.getId());
        coordJob = jpaService.execute(coordJobGetCmd);
        assertEquals(CoordinatorJob.Status.SUCCEEDED, coordJob.getStatus());

        CoordActionGetJPAExecutor coordActionGetCmd = new CoordActionGetJPAExecutor(action1.getId());
        action1 = jpaService.execute(coordActionGetCmd);
        assertNotSame(action1.getStatus(), CoordinatorAction.Status.SUCCEEDED);

        coordActionGetCmd = new CoordActionGetJPAExecutor(action2.getId());
        action2 = jpaService.execute(coordActionGetCmd);
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

    private void addRecordToJobTable(String jobId, CoordinatorStore store, CoordinatorJob.Status status)
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
        coordJob.setAuthToken("notoken");

        Properties conf = getCoordProp(appPath);
        String confStr = XmlUtils.writePropToString(conf);

        coordJob.setConf(confStr);
        coordJob.setJobXml(appXml);
        coordJob.setLastActionNumber(0);
        coordJob.setFrequency(1);
        coordJob.setExecution(Execution.FIFO);
        coordJob.setConcurrency(1);
        try {
            coordJob.setStartTime(DateUtils.parseDateUTC("2009-12-15T01:00Z"));
            coordJob.setEndTime(DateUtils.parseDateUTC("2009-12-17T01:00Z"));
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not set Date/time");
        }

        try {
            store.insertCoordinatorJob(coordJob);
        }
        catch (StoreException se) {
            se.printStackTrace();
            store.rollbackTrx();
            fail("Unable to insert the test job record to table");
            throw se;
        }
    }

    private void addRecordToActionTable(String jobId, int actionNum, String actionId, CoordinatorStore store,
            CoordinatorAction.Status status, String resourceXmlName) throws StoreException, IOException {
        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String actionXml = getCoordActionXml(appPath, resourceXmlName);
        String actionNomialTime = getActionNomialTime(actionXml);

        CoordinatorActionBean action = new CoordinatorActionBean();
        action.setJobId(jobId);
        action.setId(actionId);
        action.setActionNumber(actionNum);
        try {
            action.setNominalTime(DateUtils.parseDateUTC(actionNomialTime));
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

        try {
            store.insertCoordinatorAction(action);
        }
        catch (StoreException se) {
            se.printStackTrace();
            store.rollbackTrx();
            fail("Unable to insert the test job record to table");
            throw se;
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

    private void writeToFile(String content, Path appPath, String fileName) throws IOException {
        FileSystem fs = getFileSystem();
        Writer writer = new OutputStreamWriter(fs.create(new Path(appPath, fileName), true));
        writer.write(content);
        writer.close();
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
}
