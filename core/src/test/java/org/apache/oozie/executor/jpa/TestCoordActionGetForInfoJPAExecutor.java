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

import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;

public class TestCoordActionGetForInfoJPAExecutor extends XDataTestCase {
    Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testCoordActionGet() throws Exception {
        int actionNum = 1;
        String errorCode = "000";
        String errorMessage = "Dummy";
        String resourceXmlName = "coord-action-get.xml";
        String consoleUrl = "consoleUrl";
        String externalStatus = "externalStatus";
        String trackerUri = "trackerUri";
        String missingDeps = "missingDeps";
        String slaXml = "slaXml";

        Date createdTime = new Date();
        Date lastModifiedTime = new Date();

        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        CoordinatorActionBean action = createCoordAction(job.getId(), actionNum, CoordinatorAction.Status.WAITING,
                resourceXmlName, 0);
        // Add extra attributes for action
        action.setErrorCode(errorCode);
        action.setErrorMessage(errorMessage);
        action.setConsoleUrl(consoleUrl);
        action.setExternalStatus(externalStatus);
        action.setTrackerUri(trackerUri);
        action.setCreatedTime(createdTime);
        action.setMissingDependencies(missingDeps);
        action.setLastModifiedTime(lastModifiedTime);
        action.setSlaXml(slaXml);
        // Insert the action
        insertRecordCoordAction(action);

        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String actionXml = getCoordActionXml(appPath, resourceXmlName);
        String actionNominalTime = getActionNominalTime(actionXml);

        // Pass the expected values
        _testGetForInfo(action.getId(), job.getId(), CoordinatorAction.Status.WAITING, action.getId() + "_E",
                errorCode, errorMessage, consoleUrl, externalStatus, trackerUri, createdTime, missingDeps,
                DateUtils.parseDateOozieTZ(actionNominalTime), actionNum, lastModifiedTime);

       //services.destroy();
    }

    private void _testGetForInfo(String actionId, String jobId, CoordinatorAction.Status status, String extId,
            String errorCode, String errorMessage, String consoleUrl, String externalStatus, String trackerUri,
            Date createdTime, String missingDeps, Date nominalTime, int actionNumber, Date lastModifiedTime)
            throws Exception {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordActionGetForInfoJPAExecutor actionGetCmd = new CoordActionGetForInfoJPAExecutor(actionId);
            CoordinatorActionBean action = jpaService.execute(actionGetCmd);
            assertNotNull(action);
            // Check for expected values
            assertEquals(actionId, action.getId());
            assertEquals(jobId, action.getJobId());
            assertEquals(status, action.getStatus());
            assertEquals(extId, action.getExternalId());
            assertEquals(errorMessage, action.getErrorMessage());
            assertEquals(errorCode, action.getErrorCode());
            assertEquals(consoleUrl, action.getConsoleUrl());
            assertEquals(externalStatus, action.getExternalStatus());
            assertEquals(trackerUri, action.getTrackerUri());
            assertEquals(createdTime, action.getCreatedTime());
            assertEquals(missingDeps, action.getMissingDependencies());
            assertEquals(nominalTime, action.getNominalTime());
            assertEquals(actionNumber, action.getActionNumber());
            assertEquals(lastModifiedTime, action.getLastModifiedTime());

        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Unable to GET a record for COORD Action By actionId =" + actionId);
        }
    }

    public void testCoordActionGetAllColumns() throws Exception {
        services.destroy();
        setSystemProperty(CoordActionGetForInfoJPAExecutor.COORD_GET_ALL_COLS_FOR_ACTION, "true");
        services = new Services();
        services.init();
        String resourceXmlName = "coord-action-get.xml";
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        CoordinatorActionBean action = createCoordAction(job.getId(), 1, CoordinatorAction.Status.WAITING, resourceXmlName, 0);
        String slaXml = "slaXml";
        action.setSlaXml(slaXml);
        // Insert the action
        insertRecordCoordAction(action);
        _testGetForInfoAllActions(action.getId(), slaXml);
    }

    private void _testGetForInfoAllActions(String actionId, String slaXml) throws Exception{
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordActionGetForInfoJPAExecutor actionGetCmd = new CoordActionGetForInfoJPAExecutor(actionId);
            CoordinatorActionBean action = jpaService.execute(actionGetCmd);
            assertEquals(CoordinatorAction.Status.WAITING, action.getStatus());
            assertEquals(slaXml, action.getSlaXml());
            assertEquals(0, action.getPending());

    }
}
