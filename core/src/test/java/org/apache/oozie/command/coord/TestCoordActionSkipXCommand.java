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

import java.util.Date;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestCoordActionSkipXCommand extends XDataTestCase {
    private Services services;

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

    public void testVerifyPrecondition() throws Exception {
        Date startTime = new Date();
        Date endTime = new Date(startTime.getTime() + 1 * 60 * 1000);
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, false,
                true, 0);
        int actionNum = 1;
        for (CoordinatorAction.Status actionStatus : CoordinatorAction.Status.values()) {
            CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), actionNum, actionStatus,
                "coord-action-get.xml", 0);
            try {
                new CoordActionSkipXCommand(action, getTestUser(), "my-app-name").verifyPrecondition();
                if (!(actionStatus.equals(CoordinatorAction.Status.WAITING)
                        || actionStatus.equals(CoordinatorAction.Status.READY))) {
                    fail();
                }
            } catch (PreconditionException pe) {
                assertEquals(ErrorCode.E1100, pe.getErrorCode());
                assertTrue(pe.getMessage().endsWith("[" + actionStatus + "]]"));
            }
            actionNum++;
        }
    }

    public void testWaitingToSkipped() throws Exception {
        _testToSkipped(CoordinatorAction.Status.WAITING);
    }

    public void testReadyToSkipped() throws Exception {
        _testToSkipped(CoordinatorAction.Status.READY);
    }

    public void _testToSkipped(CoordinatorAction.Status actionStatus) throws Exception {
        Date startTime = new Date();
        Date endTime = new Date(startTime.getTime() + 1 * 60 * 1000);
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, false,
                true, 0);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, actionStatus,
                "coord-action-get.xml", 0);
        assertEquals(actionStatus, action.getStatus());
        new CoordActionSkipXCommand(action, getTestUser(), "my-app-name").call();
        action = CoordActionQueryExecutor.getInstance()
                .get(CoordActionQueryExecutor.CoordActionQuery.GET_COORD_ACTION, action.getId());
        assertEquals(CoordinatorAction.Status.SKIPPED, action.getStatus());
    }
}
