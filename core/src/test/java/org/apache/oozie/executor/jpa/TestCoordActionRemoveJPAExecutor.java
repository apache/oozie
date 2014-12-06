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
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestCoordActionRemoveJPAExecutor extends XDataTestCase {
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

    public void testCoordActionRemove() throws Exception {
        int actionNum = 1;
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), actionNum,
                CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0);
        _testCoordActionRemove(job.getId(), action.getId());
    }

    private void _testCoordActionRemove(String jobId, String actionId) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordActionRemoveJPAExecutor coordRmvCmd = new CoordActionRemoveJPAExecutor(actionId);
        jpaService.execute(coordRmvCmd);

        try {
            CoordActionGetJPAExecutor coordGetCmd = new CoordActionGetJPAExecutor(actionId);
            CoordinatorActionBean newAction = jpaService.execute(coordGetCmd);
            fail("Action " + actionId + " Should be removed");
        }
        catch (JPAExecutorException je) {

        }
    }

    public void testRunningActionDelete() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1,
                CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordActionRemoveJPAExecutor coordRmvCmd = new CoordActionRemoveJPAExecutor(action.getId());

        try {
            jpaService.execute(coordRmvCmd);
            fail("Should have thrown JPAExecutorException");
        } catch(JPAExecutorException e) {
            System.out.println(e.getErrorCode());
            e.printStackTrace();
            if(e.getErrorCode() != ErrorCode.E1022)
                fail("Error code should be E1022");
        }
    }
}
