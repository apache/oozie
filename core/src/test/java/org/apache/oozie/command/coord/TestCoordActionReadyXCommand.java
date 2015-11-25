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

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob.Execution;
import org.apache.oozie.client.CoordinatorJob.Timeunit;
import org.apache.oozie.client.Job;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.junit.Test;

import java.util.Date;

public class TestCoordActionReadyXCommand extends XDataTestCase {
    protected Services services;

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

    @Test
    public void testActionsInREADYLastOnly() throws Exception {
        _testActionsInREADY(Execution.LAST_ONLY);
    }

    @Test
    public void testActionsInREADYNone() throws Exception {
        ConfigurationService.set("oozie.coord.execution.none.tolerance", "5");
        _testActionsInREADY(Execution.NONE);
    }

    private void _testActionsInREADY(Execution execPolicy) throws Exception {
        Date now = new Date();
        Date start = new Date(now.getTime() - 16 * 60 * 1000);
        Date end = new Date(now.getTime() + 20 * 60 * 1000);
        CoordinatorJobBean job = createCoordJob(Job.Status.RUNNING, start, end, true, true, 5);
        job.setStartTime(start);
        job.setEndTime(end);
        job.setExecutionOrder(execPolicy);
        job.setFrequency("5");
        job.setTimeUnit(Timeunit.MINUTE);
        // Concurrency=1 ensures that with the 1 RUNNING action, CoordActionReadyXCommand won't actually start another action
        // (CoordActionStartXCommand), which would throw an Exception because the DB only has partial info
        job.setConcurrency(1);
        addRecordToCoordJobTable(job);

        final CoordinatorActionBean action1 = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.RUNNING,
                "wf-no-op.xml", 1, start);
        final CoordinatorActionBean action2 = addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.READY,
                "wf-no-op.xml", 1, new Date(start.getTime() + 5 * 60 * 1000));
        final CoordinatorActionBean action3 = addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.READY,
                "wf-no-op.xml", 1, new Date(start.getTime() + 10 * 60 * 1000));
        final CoordinatorActionBean action4 = addRecordToCoordActionTable(job.getId(), 4, CoordinatorAction.Status.READY,
                "wf-no-op.xml", 1, new Date(start.getTime() + 15 * 60 * 1000));
        final CoordinatorActionBean action5 = addRecordToCoordActionTable(job.getId(), 5, CoordinatorAction.Status.WAITING,
                "wf-no-op.xml", 1, new Date(start.getTime() + 20 * 60 * 1000));

        checkCoordActionStatus(action1.getId(), CoordinatorAction.Status.RUNNING);
        checkCoordActionStatus(action2.getId(), CoordinatorAction.Status.READY);
        checkCoordActionStatus(action3.getId(), CoordinatorAction.Status.READY);
        checkCoordActionStatus(action4.getId(), CoordinatorAction.Status.READY);
        checkCoordActionStatus(action5.getId(), CoordinatorAction.Status.WAITING);

        new CoordActionReadyXCommand(job.getId()).call();
        checkCoordActionStatus(action1.getId(), CoordinatorAction.Status.RUNNING);
        checkCoordActionStatus(action2.getId(), CoordinatorAction.Status.SKIPPED);
        checkCoordActionStatus(action3.getId(), CoordinatorAction.Status.SKIPPED);
        checkCoordActionStatus(action4.getId(), CoordinatorAction.Status.READY);
        checkCoordActionStatus(action5.getId(), CoordinatorAction.Status.WAITING);
    }
}
