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

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class TestCoordActionUpdateForModifiedTimeJPAExecutor extends XDataTestCase {
    Services services;

    @Before
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        cleanUpDBTables();
    }

    @After
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    @Test
    public void testCoordActionUpdateModifiedTime() throws Exception {
        int actionNum = 1;
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), actionNum,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);
        _testCoordActionUpdateModifiedTime(action);
    }

    private void _testCoordActionUpdateModifiedTime(CoordinatorActionBean action) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        Date currentDate = new Date();
        assertTrue(currentDate.getTime() - action.getLastModifiedTime().getTime() > 0);
        // Call the JPAUpdate executor to execute the Update command
        CoordActionUpdateForModifiedTimeJPAExecutor coordUpdCmd = new CoordActionUpdateForModifiedTimeJPAExecutor(
                action);
        jpaService.execute(coordUpdCmd);

        CoordActionGetJPAExecutor coordGetCmd = new CoordActionGetJPAExecutor(action.getId());
        CoordinatorActionBean newAction = jpaService.execute(coordGetCmd);

        assertNotNull(newAction);
        assertTrue(newAction.getLastModifiedTime().getTime() - currentDate.getTime() > 0);
    }

}
