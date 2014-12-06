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

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestCoordActionGetForCheckJPAExecutor extends XDataTestCase {
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
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        CoordinatorActionBean action = createCoordAction(job.getId(), actionNum, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0);
        action.setSlaXml(XDataTestCase.slaXml);
        // Insert the action
        insertRecordCoordAction(action);
        _testGetActionForCheck(action.getId(), job.getId(), CoordinatorAction.Status.WAITING, 0, action.getId() + "_E", XDataTestCase.slaXml);
    }

    private void _testGetActionForCheck(String actionId, String jobId, CoordinatorAction.Status status, int pending, String extId, String slaXml) throws Exception {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordActionGetForCheckJPAExecutor actionGetCmd = new CoordActionGetForCheckJPAExecutor(actionId);
            CoordinatorActionBean action = jpaService.execute(actionGetCmd);
            assertNotNull(action);
            assertEquals(actionId, action.getId());
            assertEquals(status, action.getStatus());
            assertEquals(pending, action.getPending());
            assertEquals(extId, action.getExternalId());
            assertEquals(slaXml, action.getSlaXml());
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Unable to GET a record for COORD Action By actionId =" + actionId);
        }
    }
}
