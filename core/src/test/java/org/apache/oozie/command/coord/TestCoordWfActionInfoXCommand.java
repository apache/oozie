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
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.CoordinatorWfActionBean;
import org.apache.oozie.client.CoordinatorWfAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class TestCoordWfActionInfoXCommand extends XDataTestCase {
    Services services;

    private CoordinatorJobBean coordJob;
    private List<WorkflowJobBean> wfJobs;
    private List<CoordinatorActionBean> coordActions;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();

        createTestData();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }
    /**
     * init the test case.
     * 1 coordJob
     * 5 coordAction created by the coordJob, while the 5th coordAction's workflow instance is null
     * 4 wfJob match the 1st ~ 4th coordAction
     * the 1st - 3rd wfAction has a wfAction named 'aa' each, but the 4th desn't.
     */
    private void createTestData() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull("Missing jpa service", jpaService);

        coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        wfJobs = new ArrayList<WorkflowJobBean>();
        coordActions = new ArrayList<CoordinatorActionBean>();

        for(int i = 0; i < 4; i++) {
            WorkflowJobBean wfJob = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
            wfJobs.add(wfJob);
        }
        for(int i = 0; i < 4; i++) {
            CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), (i+1),
                    CoordinatorAction.Status.SUCCEEDED,"coord-action-get.xml", wfJobs.get(i).getId(), "SUCCEEDED", 0);
            coordActions.add(coordAction);
        }

        //add a coordAction that doesnt create workflow instance yet
        CoordinatorActionBean coordAction = addRecordToCoordActionTable(coordJob.getId(), 5,
                CoordinatorAction.Status.SUCCEEDED,"coord-action-get.xml", null, null, 0);
        coordActions.add(coordAction);

        //set the NominalTime,in order to keep the order of the coordAction.
        for(int i = 0; i < 5; i++) {
            setCoordActionNominalTime(coordActions.get(i).getId(), (i+1) * 1000);
        }

        //create the case that the 4th wfJob doesnt have a action named "aa"
        for(int i = 0; i < 4; i++) {
            String name = (i == 3) ? "bb" : "aa";
            addRecordToWfActionTable(wfJobs.get(i).getId(), name, WorkflowAction.Status.DONE);
        }
    }

    public void testNormalCase() throws Exception {
        int offset = 2, len = 2;
        List<CoordinatorWfActionBean> coordWfActions = new CoordWfActionInfoXCommand(coordJob.getId(), "aa", offset, len).call();
        assertEquals(2, coordWfActions.size());
        List<String> wfIds = Arrays.asList(wfJobs.get(1).getId(), wfJobs.get(2).getId());

        for(int i = 0; i < coordWfActions.size(); i++) {
            CoordinatorWfActionBean coordWfAction = coordWfActions.get(i);
            WorkflowActionBean wfAction = coordWfAction.getAction();

            assertEquals(i + offset, coordWfActions.get(i).getActionNumber());
            assertEquals(wfIds.get(i), wfAction.getWfId());
            assertEquals(null, coordWfAction.getNullReason());
        }
    }

    public void testActionMissing() throws CommandException{
        List<CoordinatorWfActionBean> coordWfActions = new CoordWfActionInfoXCommand(coordJob.getId(), "aa", 2, 3).call();
        assertEquals(3, coordWfActions.size());

        assertEquals(wfJobs.get(1).getId(), coordWfActions.get(0).getAction().getWfId());
        assertEquals(wfJobs.get(2).getId(), coordWfActions.get(1).getAction().getWfId());
        CoordinatorWfActionBean coordWfAction = coordWfActions.get(2);
        assertEquals(4, coordWfAction.getActionNumber());
        assertEquals(null, coordWfAction.getAction());
        String expectNullReason = CoordinatorWfAction.NullReason.ACTION_NULL.getNullReason("aa", wfJobs.get(3).getId());
        assertEquals(expectNullReason, coordWfAction.getNullReason());
    }

    public void testWorkflowInstanceMissing() throws CommandException {
        List<CoordinatorWfActionBean> coordWfActions = new CoordWfActionInfoXCommand(coordJob.getId(), "aa", 2, 4).call();
        assertEquals(4, coordWfActions.size());

        CoordinatorWfActionBean coordWfAction = coordWfActions.get(3);
        assertEquals(5, coordWfAction.getActionNumber());
        assertEquals(null, coordWfAction.getAction());
        String expectNullReason = CoordinatorWfAction.NullReason.PARENT_NULL.getNullReason();
        assertEquals(expectNullReason, coordWfAction.getNullReason());
    }

    //test offset out of Range
    public void testOffsetOutOfRange() throws CommandException {
        List<CoordinatorWfActionBean> coordWfActions = new CoordWfActionInfoXCommand(coordJob.getId(), "aa", 6, 4).call();
        assertEquals(0, coordWfActions.size());
    }

    //test len out of Range
    public void testLenOutOfRange() throws CommandException {
        int offset = 2;
        List<CoordinatorWfActionBean> coordWfActions = new CoordWfActionInfoXCommand(coordJob.getId(), "aa", 2, 19).call();
        assertEquals(4, coordWfActions.size());

        for(int i = 0; i < coordWfActions.size(); i++) {
            assertEquals(i + offset, coordWfActions.get(i).getActionNumber());
        }
    }

    //test default offset and len
    private void _testDefaultOffsetAndLen() throws CommandException {
        List<CoordinatorWfActionBean> coordWfActions = new CoordWfActionInfoXCommand(coordJob.getId(), "aa").call();
        assertEquals(5, coordWfActions.size());

        for(int i = 0; i < coordWfActions.size(); i++) {
            assertEquals(i + 1, coordWfActions.get(i).getActionNumber());
        }
    }
}
