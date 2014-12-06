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
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.executor.jpa.CoordActionGetForCheckJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobGetForSLAJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobInsertJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.LiteWorkflowStoreService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.StatusTransitService;
import org.apache.oozie.service.WorkflowStoreService;
import org.apache.oozie.service.StatusTransitService.StatusTransitRunnable;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.WorkflowLib;
import org.apache.oozie.workflow.lite.EndNodeDef;
import org.apache.oozie.workflow.lite.LiteWorkflowApp;
import org.apache.oozie.workflow.lite.LiteWorkflowInstance;
import org.apache.oozie.workflow.lite.StartNodeDef;

/**
 * Test command for killing coordinator actions by a range of dates (nominal time)
 * or action number (Id part after "@").
 */
public class TestCoordActionsKillXCommand extends XDataTestCase {
    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        setSystemProperty(StatusTransitService.CONF_BACKWARD_SUPPORT_FOR_STATES_WITHOUT_ERROR, "false");
        services = new Services();
        services.init();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    /**
     * Test the working of CoordActionKillXCommand by passing range
     * as action ids
     *
     * @throws Exception
     */
    public void testActionKillCommandActionNumbers() throws Exception {
        JPAService jpaService = services.get(JPAService.class);
        String[] ids = createDBRecords();

        new CoordActionsKillXCommand(ids[0], "action", "1,3").call();
        CoordinatorActionBean action = jpaService.execute(new CoordActionGetForCheckJPAExecutor(ids[1]));
        assertEquals(CoordinatorAction.Status.KILLED, action.getStatus());

        sleep(100);
        WorkflowJobBean wf = jpaService.execute(new WorkflowJobGetForSLAJPAExecutor(ids[3]));
        assertEquals(WorkflowJob.Status.KILLED, wf.getStatus());

        CoordinatorJobBean job = jpaService.execute(new CoordJobGetJPAExecutor(ids[0]));
        assertEquals(CoordinatorJob.Status.RUNNING, job.getStatus());
        Runnable runnable = new StatusTransitRunnable();
        runnable.run();

        job = jpaService.execute(new CoordJobGetJPAExecutor(ids[0]));
        assertEquals(CoordinatorJob.Status.RUNNINGWITHERROR, job.getStatus());
    }

    /**
     * Test the working of CoordActionKillXCommand by passing range
     * as date
     *
     * @throws Exception
     */
    public void testActionKillCommandDate() throws Exception {
        JPAService jpaService = services.get(JPAService.class);
        String[] ids = createDBRecords();

        System.out.println(DateUtils.parseDateOozieTZ("2009-12-15T01:00Z"));
        System.out.println(DateUtils.parseDateOozieTZ("2009-12-15T02:00Z"));
        new CoordActionsKillXCommand(ids[0], "date", "2009-12-15T01:00Z::2009-12-15T02:00Z").call();
        CoordinatorActionBean action = jpaService.execute(new CoordActionGetForCheckJPAExecutor(ids[1]));
        System.out.println(action.getNominalTime());
        assertEquals(CoordinatorAction.Status.KILLED, action.getStatus());

        action = jpaService.execute(new CoordActionGetForCheckJPAExecutor(ids[2]));
        System.out.println(action.getNominalTime());
        assertEquals(CoordinatorAction.Status.KILLED, action.getStatus());

        sleep(100);
        WorkflowJobBean wf = jpaService.execute(new WorkflowJobGetForSLAJPAExecutor(ids[3]));
        assertEquals(WorkflowJob.Status.KILLED, wf.getStatus());

        CoordinatorJobBean job = jpaService.execute(new CoordJobGetJPAExecutor(ids[0]));
        assertEquals(CoordinatorJob.Status.RUNNING, job.getStatus());
        Runnable runnable = new StatusTransitRunnable();
        runnable.run();

        job = jpaService.execute(new CoordJobGetJPAExecutor(ids[0]));
        assertEquals(CoordinatorJob.Status.KILLED, job.getStatus());
    }

    private String[] createDBRecords() throws Exception {
        JPAService jpaService = services.get(JPAService.class);

        Date startTime = DateUtils.parseDateOozieTZ("2013-08-01T23:59Z");
        Date endTime = DateUtils.parseDateOozieTZ("2013-08-02T23:59Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, false,
                true, 0);

        CoordinatorActionBean action1 = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.RUNNING,
                "coord-action-get.xml", 0);
        CoordinatorActionBean action2 = addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.RUNNING,
                "coord-action-get.xml", 0);
        action2.setNominalTime(DateUtils.parseDateOozieTZ("2009-12-15T02:00Z"));
        action2.setExternalId(null);
        CoordActionQueryExecutor.getInstance().executeUpdate(CoordActionQuery.UPDATE_COORD_ACTION, action2);

        WorkflowJobBean wf = new WorkflowJobBean();
        WorkflowApp app = new LiteWorkflowApp("testApp", "<workflow-app/>", new StartNodeDef(
                LiteWorkflowStoreService.LiteControlNodeHandler.class, "end")).addNode(new EndNodeDef("end",
                LiteWorkflowStoreService.LiteControlNodeHandler.class));
        wf.setId(action1.getExternalId());
        wf.setStatus(WorkflowJob.Status.RUNNING);
        WorkflowLib workflowLib = Services.get().get(WorkflowStoreService.class).getWorkflowLibWithNoDB();
        WorkflowInstance wfInstance = workflowLib.createInstance(app, new XConfiguration());
        ((LiteWorkflowInstance) wfInstance).setStatus(WorkflowInstance.Status.RUNNING);
        wf.setWorkflowInstance(wfInstance);
        jpaService.execute(new WorkflowJobInsertJPAExecutor(wf));

        return new String[] { job.getId(), action1.getId(), action2.getId(), wf.getId() };
    }

}
