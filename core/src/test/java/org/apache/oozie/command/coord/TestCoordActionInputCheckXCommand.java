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

import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.CoordinatorJob.Execution;
import org.apache.oozie.client.CoordinatorJob.Timeunit;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.executor.jpa.CoordActionGetForInputCheckJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.executor.jpa.CoordJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.CallableQueueService;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.XLogService;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.junit.Test;

public class TestCoordActionInputCheckXCommand extends XDataTestCase {
    protected Services services;

    protected String getProcessingTZ() {
        return DateUtils.OOZIE_PROCESSING_TIMEZONE_DEFAULT;
    }

    private String TZ;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        setSystemProperty(XLogService.LOG4J_FILE, "oozie-log4j.properties");
        setSystemProperty(DateUtils.OOZIE_PROCESSING_TIMEZONE_KEY, getProcessingTZ());
        services = new Services();
        services.init();
        TZ = (getProcessingTZ().equals(DateUtils.OOZIE_PROCESSING_TIMEZONE_DEFAULT))
             ? "Z" : getProcessingTZ().substring(3);
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public class MyCoordActionInputCheckXCommand extends CoordActionInputCheckXCommand {
        long executed = 0;
        int wait;

        public MyCoordActionInputCheckXCommand(String actionId, int wait, String entityKey) {
            super(actionId, entityKey);
            this.wait = wait;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Type:").append(getType());
            sb.append(",Priority:").append(getPriority());
            return sb.toString();
        }

        @Override
        protected Void execute() throws CommandException {
            try {
                Thread.sleep(wait);
            }
            catch (InterruptedException e) {
            }
            executed = System.currentTimeMillis();
            return null;
        }

    }

    public void testCoordActionInputCheckXCommandUniqueness() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2009-02-01T23:59" + TZ);
        Date endTime = DateUtils.parseDateOozieTZ("2009-02-02T23:59" + TZ);
        CoordinatorJobBean job = addRecordToCoordJobTableForWaiting("coord-job-for-action-input-check.xml",
                CoordinatorJob.Status.RUNNING, startTime, endTime, false, true, 3);

        CoordinatorActionBean action1 = addRecordToCoordActionTableForWaiting(job.getId(), 1,
                CoordinatorAction.Status.WAITING, "coord-action-for-action-input-check.xml");

        createTestCaseSubDir("2009/01/29/_SUCCESS".split("/"));
        createTestCaseSubDir("2009/01/22/_SUCCESS".split("/"));
        createTestCaseSubDir("2009/01/15/_SUCCESS".split("/"));
        createTestCaseSubDir("2009/01/08/_SUCCESS".split("/"));

        final MyCoordActionInputCheckXCommand callable1 = new MyCoordActionInputCheckXCommand(action1.getId(), 100, "1");
        final MyCoordActionInputCheckXCommand callable2 = new MyCoordActionInputCheckXCommand(action1.getId(), 100, "2");
        final MyCoordActionInputCheckXCommand callable3 = new MyCoordActionInputCheckXCommand(action1.getId(), 100, "3");

        List<MyCoordActionInputCheckXCommand> callables = Arrays.asList(callable1, callable2, callable3);

        CallableQueueService queueservice = services.get(CallableQueueService.class);

        for (MyCoordActionInputCheckXCommand c : callables) {
            queueservice.queue(c);
        }

        waitFor(200, new Predicate() {
            public boolean evaluate() throws Exception {
                return callable1.executed != 0 && callable2.executed == 0 && callable3.executed == 0;
            }
        });

        assertTrue(callable1.executed != 0);
        assertTrue(callable2.executed == 0);
        assertTrue(callable3.executed == 0);
    }

    public void testActionInputCheck() throws Exception {
        String jobId = "0000000-" + new Date().getTime() + "-TestCoordActionInputCheckXCommand-C";
        Date startTime = DateUtils.parseDateOozieTZ("2009-02-01T23:59" + TZ);
        Date endTime = DateUtils.parseDateOozieTZ("2009-02-02T23:59" + TZ);
        CoordinatorJobBean job = addRecordToCoordJobTable(jobId, startTime, endTime);
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
        createTestCaseSubDir("2009/02/05/_SUCCESS".split("/"));
        createTestCaseSubDir("2009/01/15/_SUCCESS".split("/"));
        new CoordActionInputCheckXCommand(job.getId() + "@1", job.getId()).call();
        JPAService jpaService = Services.get().get(JPAService.class);
        CoordinatorActionBean action = jpaService.execute(new CoordActionGetJPAExecutor(job.getId() + "@1"));
        System.out.println("missingDeps " + action.getMissingDependencies() + " Xml " + action.getActionXml());
        if (action.getMissingDependencies().indexOf("/2009/02/05/") >= 0) {
            fail("directory should be resolved :" + action.getMissingDependencies());
        }
        if (action.getMissingDependencies().indexOf("/2009/01/15/") < 0) {
            fail("directory should NOT be resolved :" + action.getMissingDependencies());
        }
    }

    /**
     * Test to check that missing dependencies list starts from the first unavailable dependency in synchronous order
     * @throws Exception
     */
    public void testActionInputMissingDependencies() throws Exception {
        String jobId = "0000000-" + new Date().getTime() + "-TestCoordActionInputCheckXCommand-C";
        Date startTime = DateUtils.parseDateOozieTZ("2009-02-15T23:59" + TZ);
        Date endTime = DateUtils.parseDateOozieTZ("2009-02-16T23:59" + TZ);
        CoordinatorJobBean job = addRecordToCoordJobTable(jobId, startTime, endTime);
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();

        // providing some of the dataset dirs required as per coordinator
        // specification - /2009/02/19, /2009/02/12, /2009/02/05, /2009/01/29, /2009/01/22
        createTestCaseSubDir("2009/02/19/_SUCCESS".split("/"));
        createTestCaseSubDir("2009/01/29/_SUCCESS".split("/"));

        new CoordActionInputCheckXCommand(job.getId() + "@1", job.getId()).call();
        CoordinatorActionBean action = null;
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            action = jpaService.execute(new CoordActionGetJPAExecutor(job.getId() + "@1"));
        }
        catch (JPAExecutorException se) {
            fail("Action ID " + job.getId() + "@1" + " was not stored properly in db");
        }

        // Missing dependencies recorded by the coordinator action after input check
        String missDepsOrder = action.getMissingDependencies();
        // Expected missing dependencies are /2009/02/12, /2009/02/05, /2009/01/29, and /2009/01/22.

        int index = missDepsOrder.indexOf("/2009/02/19");
        if( index >= 0) {
            fail("Dependency should be available! current list: " + missDepsOrder);
        }
        // Case when /2009/01/29 exists but checking stops since dataset synchronously expected before i.e. /2009/02/05 is missing
        index = missDepsOrder.indexOf("/2009/01/29");
        if( index < 0) {
            fail("Data should have been in missing dependency list! current list: " + missDepsOrder);
        }
        index = missDepsOrder.indexOf("/2009/02/05");
        if( index < 0) {
            fail("Data should have been in missing dependency list! current list: " + missDepsOrder);
        }
    }

    public void testActionInputCheckLatestActionCreationTime() throws Exception {
        Services.get().getConf().setBoolean(CoordELFunctions.LATEST_EL_USE_CURRENT_TIME, false);

        String jobId = "0000000-" + new Date().getTime() + "-TestCoordActionInputCheckXCommand-C";
        Date startTime = DateUtils.parseDateOozieTZ("2009-02-15T23:59" + TZ);
        Date endTime = DateUtils.parseDateOozieTZ("2009-02-16T23:59" + TZ);
        CoordinatorJobBean job = addRecordToCoordJobTable(jobId, startTime, endTime, "latest");
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();

        JPAService jpaService = Services.get().get(JPAService.class);
        CoordinatorActionBean action = jpaService
                .execute(new CoordActionGetForInputCheckJPAExecutor(job.getId() + "@1"));
        assertEquals(CoordCommandUtils.RESOLVED_UNRESOLVED_SEPARATOR + "${coord:latestRange(-3,0)}",
                action.getMissingDependencies());

        // Update action creation time
        String actionXML = action.getActionXml();
        String actionCreationTime = "2009-02-15T01:00" + TZ;
        actionXML = actionXML.replaceAll("action-actual-time=\".*\">", "action-actual-time=\"" + actionCreationTime
                + "\">");
        action.setActionXml(actionXML);
        action.setCreatedTime(DateUtils.parseDateOozieTZ(actionCreationTime));
        CoordActionQueryExecutor.getInstance().executeUpdate(
                CoordActionQueryExecutor.CoordActionQuery.UPDATE_COORD_ACTION_FOR_INPUTCHECK,
                action);
        action = jpaService.execute(new CoordActionGetForInputCheckJPAExecutor(job.getId() + "@1"));
        assertTrue(action.getActionXml().contains("action-actual-time=\"2009-02-15T01:00")) ;

        Thread.sleep(1000);
        // providing some of the dataset dirs required as per coordinator specification with holes
        // before and after action creation time
        createTestCaseSubDir("2009/03/05/_SUCCESS".split("/"));
        createTestCaseSubDir("2009/02/19/_SUCCESS".split("/"));
        createTestCaseSubDir("2009/02/12/_SUCCESS".split("/"));
        createTestCaseSubDir("2009/02/05/_SUCCESS".split("/"));
        createTestCaseSubDir("2009/01/22/_SUCCESS".split("/"));
        createTestCaseSubDir("2009/01/08/_SUCCESS".split("/"));

        new CoordActionInputCheckXCommand(job.getId() + "@1", job.getId()).call();
        //Sleep for sometime as it gets requeued with 10ms delay on failure to acquire write lock
        Thread.sleep(1000);

        action = jpaService.execute(new CoordActionGetForInputCheckJPAExecutor(job.getId() + "@1"));
        actionXML = action.getActionXml();
        assertEquals("", action.getMissingDependencies());
        // Datasets only before action creation/actual time should be picked up.
        String resolvedList = getTestCaseFileUri("2009/02/12") + CoordELFunctions.INSTANCE_SEPARATOR
                + getTestCaseFileUri("2009/02/05") + CoordELFunctions.INSTANCE_SEPARATOR
                + getTestCaseFileUri("2009/01/22") + CoordELFunctions.INSTANCE_SEPARATOR
                + getTestCaseFileUri("2009/01/08");
        System.out.println("Expected: " + resolvedList);
        System.out.println("Actual: " +  actionXML.substring(actionXML.indexOf("<uris>") + 6, actionXML.indexOf("</uris>")));
        assertEquals(resolvedList, actionXML.substring(actionXML.indexOf("<uris>") + 6, actionXML.indexOf("</uris>")));
    }

    public void testActionInputCheckLatestActionCreationTimeWithPushDependency() throws Exception {
        setupServicesForHCatalog(services);
        Services.get().getConf().setBoolean(CoordELFunctions.LATEST_EL_USE_CURRENT_TIME, false);
        services.init();
        String jobId = "0000000-" + new Date().getTime() + "-TestCoordActionInputCheckXCommand-C";
        Date startTime = DateUtils.parseDateOozieTZ("2009-02-15T23:59" + TZ);
        Date endTime = DateUtils.parseDateOozieTZ("2009-02-16T23:59" + TZ);
        CoordinatorJobBean job = addRecordToCoordJobTable(jobId, startTime, endTime, "latest");
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();

        // Set push missing dependency
        JPAService jpaService = Services.get().get(JPAService.class);
        CoordinatorActionBean action = jpaService
                .execute(new CoordActionGetForInputCheckJPAExecutor(job.getId() + "@1"));
        final String pushMissingDependency = getTestCaseFileUri("2009/02/05");
        action.setPushMissingDependencies(pushMissingDependency);
        CoordActionQueryExecutor.getInstance().executeUpdate(
                CoordActionQueryExecutor.CoordActionQuery.UPDATE_COORD_ACTION_FOR_PUSH_INPUTCHECK,
                action);

        // Update action creation time
        String actionXML = action.getActionXml();
        String actionCreationTime = "2009-02-15T01:00" + TZ;
        actionXML = actionXML.replaceAll("action-actual-time=\".*\">", "action-actual-time=\"" + actionCreationTime
                + "\">");
        action.setActionXml(actionXML);
        action.setCreatedTime(DateUtils.parseDateOozieTZ(actionCreationTime));
        CoordActionQueryExecutor.getInstance().executeUpdate(
                CoordActionQueryExecutor.CoordActionQuery.UPDATE_COORD_ACTION_FOR_INPUTCHECK,
                action);
        action = jpaService.execute(new CoordActionGetForInputCheckJPAExecutor(job.getId() + "@1"));
        assertTrue(action.getActionXml().contains("action-actual-time=\"2009-02-15T01:00")) ;

        new CoordActionInputCheckXCommand(job.getId() + "@1", job.getId()).call();
        new CoordPushDependencyCheckXCommand(job.getId() + "@1").call();
        action = jpaService.execute(new CoordActionGetForInputCheckJPAExecutor(job.getId() + "@1"));
        assertEquals(CoordCommandUtils.RESOLVED_UNRESOLVED_SEPARATOR + "${coord:latestRange(-3,0)}",
                action.getMissingDependencies());
        assertEquals(pushMissingDependency, action.getPushMissingDependencies());

        // providing some of the dataset dirs required as per coordinator specification with holes
        // before and after action creation time
        createTestCaseSubDir("2009/03/05/_SUCCESS".split("/"));
        createTestCaseSubDir("2009/02/19/_SUCCESS".split("/"));
        createTestCaseSubDir("2009/02/12/_SUCCESS".split("/"));
        createTestCaseSubDir("2009/01/22/_SUCCESS".split("/"));
        createTestCaseSubDir("2009/01/08/_SUCCESS".split("/"));
        createTestCaseSubDir("2009/01/01/_SUCCESS".split("/"));

        // Run input check after making latest available
        new CoordActionInputCheckXCommand(job.getId() + "@1", job.getId()).call();
        action = jpaService.execute(new CoordActionGetForInputCheckJPAExecutor(job.getId() + "@1"));
        assertEquals(CoordCommandUtils.RESOLVED_UNRESOLVED_SEPARATOR + "${coord:latestRange(-3,0)}",
                action.getMissingDependencies());
        assertEquals(pushMissingDependency, action.getPushMissingDependencies());

        // Run input check after making push dependencies available
        createTestCaseSubDir("2009/02/05/_SUCCESS".split("/"));
        new CoordPushDependencyCheckXCommand(job.getId() + "@1").call();
        action = jpaService.execute(new CoordActionGetForInputCheckJPAExecutor(job.getId() + "@1"));
        assertEquals("", action.getPushMissingDependencies());
        checkCoordAction(job.getId() + "@1", CoordCommandUtils.RESOLVED_UNRESOLVED_SEPARATOR
                + "${coord:latestRange(-3,0)}", CoordinatorAction.Status.WAITING);
        new CoordActionInputCheckXCommand(job.getId() + "@1", job.getId()).call();
        //Sleep for sometime as it gets requeued with 10ms delay on failure to acquire write lock
        Thread.sleep(1000);

        action = jpaService.execute(new CoordActionGetForInputCheckJPAExecutor(job.getId() + "@1"));
        assertEquals("", action.getMissingDependencies());
        actionXML = action.getActionXml();
        // Datasets only before action creation/actual time should be picked up.
        String resolvedList = getTestCaseFileUri("2009/02/12") + CoordELFunctions.INSTANCE_SEPARATOR
                + getTestCaseFileUri("2009/02/05") + CoordELFunctions.INSTANCE_SEPARATOR
                + getTestCaseFileUri("2009/01/22") + CoordELFunctions.INSTANCE_SEPARATOR
                + getTestCaseFileUri("2009/01/08");
        System.out.println("Expected: " + resolvedList);
        System.out.println("Actual: " +  actionXML.substring(actionXML.indexOf("<uris>") + 6, actionXML.indexOf("</uris>")));
        assertEquals(resolvedList, actionXML.substring(actionXML.indexOf("<uris>") + 6, actionXML.indexOf("</uris>")));
    }

    public void testActionInputCheckLatestCurrentTime() throws Exception {
        Services.get().getConf().setBoolean(CoordELFunctions.LATEST_EL_USE_CURRENT_TIME, true);

        String jobId = "0000000-" + new Date().getTime() + "-TestCoordActionInputCheckXCommand-C";
        Date startTime = DateUtils.parseDateOozieTZ("2009-02-15T23:59" + TZ);
        Date endTime = DateUtils.parseDateOozieTZ("2009-02-16T23:59" + TZ);
        CoordinatorJobBean job = addRecordToCoordJobTable(jobId, startTime, endTime, "latest");
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();

        JPAService jpaService = Services.get().get(JPAService.class);
        CoordinatorActionBean action = jpaService
                .execute(new CoordActionGetForInputCheckJPAExecutor(job.getId() + "@1"));
        assertEquals(CoordCommandUtils.RESOLVED_UNRESOLVED_SEPARATOR + "${coord:latestRange(-3,0)}",
                action.getMissingDependencies());

        // Update action creation time
        String actionXML = action.getActionXml();
        String actionCreationTime = "2009-02-15T01:00" + TZ;
        actionXML = actionXML.replaceAll("action-actual-time=\".*\">", "action-actual-time=\"" + actionCreationTime
                + "\">");
        action.setActionXml(actionXML);
        action.setCreatedTime(DateUtils.parseDateOozieTZ(actionCreationTime));
        CoordActionQueryExecutor.getInstance().executeUpdate(
                CoordActionQueryExecutor.CoordActionQuery.UPDATE_COORD_ACTION_FOR_INPUTCHECK,
                action);
        action = jpaService.execute(new CoordActionGetForInputCheckJPAExecutor(job.getId() + "@1"));
        assertTrue(action.getActionXml().contains("action-actual-time=\"2009-02-15T01:00")) ;

        // providing some of the dataset dirs required as per coordinator
        // specification with holes
        // before and after action creation time
        createTestCaseSubDir("2009/03/05/_SUCCESS".split("/"));
        createTestCaseSubDir("2009/02/19/_SUCCESS".split("/"));
        createTestCaseSubDir("2009/02/12/_SUCCESS".split("/"));
        createTestCaseSubDir("2009/02/05/_SUCCESS".split("/"));
        createTestCaseSubDir("2009/01/22/_SUCCESS".split("/"));
        createTestCaseSubDir("2009/01/08/_SUCCESS".split("/"));

        new CoordActionInputCheckXCommand(job.getId() + "@1", job.getId()).call();
        //Sleep for sometime as it gets requeued with 10ms delay on failure to acquire write lock
        Thread.sleep(1000);

        action = jpaService.execute(new CoordActionGetJPAExecutor(job.getId() + "@1"));
        actionXML = action.getActionXml();
        assertEquals("", action.getMissingDependencies());
        // Datasets should be picked up based on current time and not action creation/actual time.
        String resolvedList = getTestCaseFileUri("2009/03/05") + CoordELFunctions.INSTANCE_SEPARATOR
                + getTestCaseFileUri("2009/02/19") + CoordELFunctions.INSTANCE_SEPARATOR
                + getTestCaseFileUri("2009/02/12") + CoordELFunctions.INSTANCE_SEPARATOR
                + getTestCaseFileUri("2009/02/05");
        assertEquals(resolvedList, actionXML.substring(actionXML.indexOf("<uris>") + 6, actionXML.indexOf("</uris>")));
    }

    public void testActionInputCheckLatestCurrentTimeWithPushDependency() throws Exception {
        setupServicesForHCatalog(services);
        Services.get().getConf().setBoolean(CoordELFunctions.LATEST_EL_USE_CURRENT_TIME, true);
        services.init();

        String jobId = "0000000-" + new Date().getTime() + "-TestCoordActionInputCheckXCommand-C";
        Date startTime = DateUtils.parseDateOozieTZ("2009-02-15T23:59" + TZ);
        Date endTime = DateUtils.parseDateOozieTZ("2009-02-16T23:59" + TZ);
        CoordinatorJobBean job = addRecordToCoordJobTable(jobId, startTime, endTime, "latest");
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();

        // Set push missing dependency
        JPAService jpaService = Services.get().get(JPAService.class);
        CoordinatorActionBean action = jpaService
                .execute(new CoordActionGetForInputCheckJPAExecutor(job.getId() + "@1"));
        final String pushMissingDependency = getTestCaseFileUri("2009/02/05");
        action.setPushMissingDependencies(pushMissingDependency);
        CoordActionQueryExecutor.getInstance().executeUpdate(
                CoordActionQueryExecutor.CoordActionQuery.UPDATE_COORD_ACTION_FOR_PUSH_INPUTCHECK,
                action);

        // Update action creation time
        String actionXML = action.getActionXml();
        String actionCreationTime = "2009-02-15T01:00" + TZ;
        actionXML = actionXML.replaceAll("action-actual-time=\".*\">", "action-actual-time=\"" + actionCreationTime
                + "\">");
        action.setActionXml(actionXML);
        action.setCreatedTime(DateUtils.parseDateOozieTZ(actionCreationTime));
        CoordActionQueryExecutor.getInstance().executeUpdate(
                CoordActionQueryExecutor.CoordActionQuery.UPDATE_COORD_ACTION_FOR_INPUTCHECK,
                action);
        action = jpaService.execute(new CoordActionGetForInputCheckJPAExecutor(job.getId() + "@1"));
        assertTrue(action.getActionXml().contains("action-actual-time=\"2009-02-15T01:00")) ;

        new CoordActionInputCheckXCommand(job.getId() + "@1", job.getId()).call();
        new CoordPushDependencyCheckXCommand(job.getId() + "@1").call();
        action = jpaService.execute(new CoordActionGetForInputCheckJPAExecutor(job.getId() + "@1"));
        assertEquals(CoordCommandUtils.RESOLVED_UNRESOLVED_SEPARATOR + "${coord:latestRange(-3,0)}",
                action.getMissingDependencies());
        assertEquals(pushMissingDependency, action.getPushMissingDependencies());

        // providing some of the dataset dirs required as per coordinator specification with holes
        // before and after action creation time
        createTestCaseSubDir("2009/03/05/_SUCCESS".split("/"));
        createTestCaseSubDir("2009/02/19/_SUCCESS".split("/"));
        createTestCaseSubDir("2009/02/12/_SUCCESS".split("/"));
        createTestCaseSubDir("2009/01/22/_SUCCESS".split("/"));
        createTestCaseSubDir("2009/01/08/_SUCCESS".split("/"));
        createTestCaseSubDir("2009/01/05/_SUCCESS".split("/"));

        // Run input check after making latest available
        new CoordActionInputCheckXCommand(job.getId() + "@1", job.getId()).call();
        action = jpaService.execute(new CoordActionGetForInputCheckJPAExecutor(job.getId() + "@1"));
        assertEquals(CoordCommandUtils.RESOLVED_UNRESOLVED_SEPARATOR + "${coord:latestRange(-3,0)}",
                action.getMissingDependencies());
        assertEquals(pushMissingDependency, action.getPushMissingDependencies());

        // Run input check after making push dependencies available
        createTestCaseSubDir("2009/02/05/_SUCCESS".split("/"));
        new CoordPushDependencyCheckXCommand(job.getId() + "@1").call();
        action = jpaService.execute(new CoordActionGetForInputCheckJPAExecutor(job.getId() + "@1"));
        assertEquals("", action.getPushMissingDependencies());
        checkCoordAction(job.getId() + "@1", CoordCommandUtils.RESOLVED_UNRESOLVED_SEPARATOR
                + "${coord:latestRange(-3,0)}", CoordinatorAction.Status.WAITING);
        new CoordActionInputCheckXCommand(job.getId() + "@1", job.getId()).call();
        //Sleep for sometime as it gets requeued with 10ms delay on failure to acquire write lock
        Thread.sleep(1000);

        action = jpaService.execute(new CoordActionGetForInputCheckJPAExecutor(job.getId() + "@1"));
        assertEquals("", action.getMissingDependencies());
        actionXML = action.getActionXml();
        // Datasets should be picked up based on current time and not action creation/actual time.
        String resolvedList = getTestCaseFileUri("2009/03/05") + CoordELFunctions.INSTANCE_SEPARATOR
                + getTestCaseFileUri("2009/02/19") + CoordELFunctions.INSTANCE_SEPARATOR
                + getTestCaseFileUri("2009/02/12") + CoordELFunctions.INSTANCE_SEPARATOR
                + getTestCaseFileUri("2009/02/05");
        System.out.println("Expected: " + resolvedList);
        System.out.println("Actual: " +  actionXML.substring(actionXML.indexOf("<uris>") + 6, actionXML.indexOf("</uris>")));
        assertEquals(resolvedList, actionXML.substring(actionXML.indexOf("<uris>") + 6, actionXML.indexOf("</uris>")));
    }

    public void testActionInputCheckFuture() throws Exception {
        String jobId = "0000000-" + new Date().getTime() + "-TestCoordActionInputCheckXCommand-C";
        Date startTime = DateUtils.parseDateOozieTZ("2009-02-15T23:59" + TZ);
        Date endTime = DateUtils.parseDateOozieTZ("2009-02-16T23:59" + TZ);
        CoordinatorJobBean job = addRecordToCoordJobTable(jobId, startTime, endTime, "future");
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();

        // providing some of the dataset dirs required as per coordinator specification with holes
        createTestCaseSubDir("2009/02/12/_SUCCESS".split("/"));
        createTestCaseSubDir("2009/02/26/_SUCCESS".split("/"));
        createTestCaseSubDir("2009/03/05/_SUCCESS".split("/"));
        createTestCaseSubDir("2009/03/26/_SUCCESS".split("/")); //limit is 5. So this should be ignored

        new CoordActionInputCheckXCommand(job.getId() + "@1", job.getId()).call();
        CoordinatorActionBean action = null;
        JPAService jpaService = Services.get().get(JPAService.class);
        try {
            action = jpaService.execute(new CoordActionGetJPAExecutor(job.getId() + "@1"));
        }
        catch (JPAExecutorException se) {
            fail("Action ID " + job.getId() + "@1" + " was not stored properly in db");
        }

        assertEquals(CoordCommandUtils.RESOLVED_UNRESOLVED_SEPARATOR + "${coord:futureRange(0,3,'5')}",
                action.getMissingDependencies());

        createTestCaseSubDir("2009/03/12/_SUCCESS".split("/"));

        new CoordActionInputCheckXCommand(job.getId() + "@1", job.getId()).call();
        try {
            action = jpaService.execute(new CoordActionGetJPAExecutor(job.getId() + "@1"));
        }
        catch (JPAExecutorException se) {
            fail("Action ID " + job.getId() + "@1" + " was not stored properly in db");
        }

        assertEquals("", action.getMissingDependencies());
        String actionXML = action.getActionXml();
        String resolvedList = getTestCaseFileUri("2009/02/12") + CoordELFunctions.INSTANCE_SEPARATOR
                + getTestCaseFileUri("2009/02/26") + CoordELFunctions.INSTANCE_SEPARATOR
                + getTestCaseFileUri("2009/03/05") + CoordELFunctions.INSTANCE_SEPARATOR
                + getTestCaseFileUri("2009/03/12");
        assertEquals(resolvedList, actionXML.substring(actionXML.indexOf("<uris>") + 6, actionXML.indexOf("</uris>")));
    }
    /**
     * Testing a non existing namenode path
     *
     * @throws Exception
     */
    public void testNonExistingNameNode() throws Exception {
        String jobId = "0000000-" + new Date().getTime() + "-TestCoordActionInputCheckXCommand-C";
        Date startTime = DateUtils.parseDateUTC("2009-02-01T23:59Z");
        Date endTime = DateUtils.parseDateUTC("2009-02-02T23:59Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(jobId, startTime, endTime);
        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
        CoordActionInputCheckXCommand caicc = new CoordActionInputCheckXCommand(job.getId() + "@1", job.getId());
        caicc.loadState();

        // Override the name node while list for testing purpose only.
        String[] whiteList = new String[1];
        whiteList[0] = "localhost:5330";
        services.destroy();
        setSystemProperty(HadoopAccessorService.NAME_NODE_WHITELIST, whiteList[0]);
        services = new Services();
        services.init();

        // setting the test path with nonExistDir
        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String inputDir = appPath.toString() + "/coord-input/2010/07/09/01/00";
        String nonExistDir = inputDir.replaceFirst("localhost", "nonExist");
        try {
            caicc.pathExists(nonExistDir, new XConfiguration(), getTestUser());
            fail("Should throw exception due to non-existent NN path. Therefore fail");
        }
        catch (IOException ioe) {
            assertEquals(caicc.getCoordActionErrorCode(), "E0901");
            assertTrue(caicc.getCoordActionErrorMsg().contains("not in Oozies whitelist"));
        }
    }

    /**
     * This test case verifies if getCoordInputCheckRequeueInterval picks up the
     * overridden value. In reality, the value could be overridden in
     * oozie-site.xml.
     *
     * @throws Exception
     */
    public void testRequeueInterval() throws Exception {
        /*
         * Create a dummy Coordinator Job to pass to
         * CoordActionInputCheckXCommand constructor.
         */
        String jobId = "0000000-" + new Date().getTime() + "-TestCoordActionInputCheckXCommand-C";
        Date startTime = DateUtils.parseDateOozieTZ("2009-02-01T23:59" + TZ);
        Date endTime = DateUtils.parseDateOozieTZ("2009-02-02T23:59" + TZ);
        CoordinatorJobBean job = addRecordToCoordJobTable(jobId, startTime, endTime);
        /* Override the property value for testing purpose only. */
        long testedValue = 12000;
        Services.get().getConf().setLong(CoordActionInputCheckXCommand.CONF_COORD_INPUT_CHECK_REQUEUE_INTERVAL,
                testedValue);

        CoordActionInputCheckXCommand caicc = new CoordActionInputCheckXCommand(job.getId() + "@1", job.getId());

        long effectiveValue = caicc.getCoordInputCheckRequeueInterval();
        // Verify if two values are same.
        assertEquals(testedValue, effectiveValue);
    }

    public void testResolveCoordConfiguration() {
        try {
            CoordinatorJobBean job = addRecordToCoordJobTableForWaiting("coord-job-for-action-input-check.xml",
                    CoordinatorJob.Status.RUNNING, false, true);

            CoordinatorActionBean action = addRecordToCoordActionTableForWaiting(job.getId(), 1,
                    CoordinatorAction.Status.WAITING, "coord-action-for-action-input-check.xml");

            createTestCaseSubDir("2009/01/29/_SUCCESS".split("/"));
            createTestCaseSubDir("2009/01/22/_SUCCESS".split("/"));
            createTestCaseSubDir("2009/01/15/_SUCCESS".split("/"));
            createTestCaseSubDir("2009/01/08/_SUCCESS".split("/"));
            sleep(3000);
            new CoordActionInputCheckXCommand(action.getId(), job.getId()).call();
            sleep(3000);
            final JPAService jpaService = Services.get().get(JPAService.class);
            CoordActionGetJPAExecutor coordGetCmd = new CoordActionGetJPAExecutor(action.getId());
            CoordinatorActionBean caBean = jpaService.execute(coordGetCmd);

            Element eAction = XmlUtils.parseXml(caBean.getActionXml());
            Element configElem = eAction.getChild("action", eAction.getNamespace())
                    .getChild("workflow", eAction.getNamespace()).getChild("configuration", eAction.getNamespace());
            List<?> elementList = configElem.getChildren("property", configElem.getNamespace());
            Element e1 = (Element) elementList.get(0);
            Element e2 = (Element) elementList.get(1);
            assertEquals(
                    "file://,testDir/2009/29,file://,testDir/2009/22,file://,testDir/2009/15,file://,testDir/2009/08",
                    e1.getChild("value", e1.getNamespace()).getValue());
            assertEquals("file://,testDir/2009/29", e2.getChild("value", e1.getNamespace()).getValue());

        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Unexpected exception");
        }
    }

    /**
     * This test verifies that for a coordinator with no input dependencies
     * action is not stuck in WAITING
     *
     * @throws Exception
     */
    public void testNoDatasetDependency() throws Exception {
        /*
         * create coordinator job
         */
        CoordinatorJobBean coordJob = new CoordinatorJobBean();
        coordJob.setId("0000000" + new Date().getTime() + "-TestCoordActionInputCheckXCommand-C");
        coordJob.setAppName("testApp");
        coordJob.setAppPath("testAppPath");
        coordJob.setStatus(CoordinatorJob.Status.RUNNING);
        coordJob.setCreatedTime(new Date());
        coordJob.setLastModifiedTime(new Date());
        coordJob.setUser("testUser");
        coordJob.setGroup("testGroup");
        coordJob.setTimeZone("UTC");
        coordJob.setTimeUnit(Timeunit.DAY);
        coordJob.setMatThrottling(2);
        try {
            coordJob.setStartTime(DateUtils.parseDateOozieTZ("2009-02-01T23:59" + TZ));
            coordJob.setEndTime(DateUtils.parseDateOozieTZ("2009-02-02T23:59" + TZ));
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not set Date/time");
        }
        XConfiguration jobConf = new XConfiguration();
        jobConf.set(OozieClient.USER_NAME, getTestUser());
        String confStr = jobConf.toXmlString(false);
        coordJob.setConf(confStr);
        String wfXml = IOUtils.getResourceAsString("wf-no-op.xml", -1);
        writeToFile(wfXml, getFsTestCaseDir(), "workflow.xml");
        String appXml = "<coordinator-app xmlns='uri:oozie:coordinator:0.2' name='NAME' frequency=\"1\" start='2009-02-01T01:00"
                + TZ + "' end='2009-02-03T23:59" + TZ + "' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        appXml += "<output-events>";
        appXml += "<data-out name='LOCAL_A' dataset='local_a'>";
        appXml += "<dataset name='local_a' frequency='7' initial-instance='2009-01-01T01:00" + TZ
                + "' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        appXml += "<uri-template>file://" + getFsTestCaseDir() + "/${YEAR}/${MONTH}/${DAY}</uri-template>";
        appXml += "</dataset>";
        appXml += "<start-instance>${coord:current(-3)}</start-instance>";
        appXml += "<instance>${coord:current(0)}</instance>";
        appXml += "</data-out>";
        appXml += "</output-events>";
        appXml += "<action>";
        appXml += "<workflow>";
        appXml += "<app-path>" + getFsTestCaseDir() + "/workflow.xml</app-path>";
        appXml += "</workflow>";
        appXml += "</action>";
        appXml += "</coordinator-app>";
        coordJob.setJobXml(appXml);
        coordJob.setLastActionNumber(0);
        coordJob.setFrequency("1");
        coordJob.setConcurrency(1);
        JPAService jpaService = Services.get().get(JPAService.class);
        if (jpaService != null) {
            try {
                jpaService.execute(new CoordJobInsertJPAExecutor(coordJob));
            }
            catch (JPAExecutorException e) {
                throw new CommandException(e);
            }
        }
        else {
            fail("Unable to insert the test job record to table");
        }
        new CoordMaterializeTransitionXCommand(coordJob.getId(), 3600).call();
        /*
         * check coord action READY
         */
        new CoordActionInputCheckXCommand(coordJob.getId() + "@1", coordJob.getId()).call();
        CoordinatorActionBean action = null;
        try {
            jpaService = Services.get().get(JPAService.class);
            action = jpaService.execute(new CoordActionGetJPAExecutor(coordJob.getId() + "@1"));
        }
        catch (JPAExecutorException se) {
            fail("Action ID " + coordJob.getId() + "@1" + " was not stored properly in db");
        }
        // for no dataset dependency, action will proceed directly to start
        // workflow synchronously after being READY (since n(actions) < concurrency)
        assertEquals(CoordinatorAction.Status.RUNNING, action.getStatus());

        action.setMissingDependencies("");
        action.setStatus(CoordinatorAction.Status.WAITING);
        action.setExternalId(null);
        try {
            jpaService = Services.get().get(JPAService.class);
            CoordActionQueryExecutor.getInstance().executeUpdate(CoordActionQuery.UPDATE_COORD_ACTION, action);
        }
        catch (JPAExecutorException se) {
            fail("Action ID " + coordJob.getId() + "@1" + " was not stored properly in db");
        }

        new CoordActionInputCheckXCommand(coordJob.getId() + "@1", coordJob.getId()).call();
        try {
            jpaService = Services.get().get(JPAService.class);
            action = jpaService.execute(new CoordActionGetJPAExecutor(coordJob.getId() + "@1"));
        }
        catch (JPAExecutorException se) {
            fail("Action ID " + coordJob.getId() + "@1" + " was not stored properly in db");
        }
        // for no dataset dependency, action will proceed directly to start
        // workflow synchronously after being READY (since n(actions) < concurrency)
        assertEquals(CoordinatorAction.Status.RUNNING, action.getStatus());
    }

    @Test
    public void testTimeout() throws Exception {
        String missingDeps = "hdfs:///dirx/filex";
        String actionId = addInitRecords(missingDeps, null, TZ);
        new CoordActionInputCheckXCommand(actionId, actionId.substring(0, actionId.indexOf("@"))).call();
        // Timeout is 10 mins. Change action creation time to before 12 min to make the action
        // timeout.
        long timeOutCreationTime = System.currentTimeMillis() - (12 * 60 * 1000);
        setCoordActionCreationTime(actionId, timeOutCreationTime);
        new CoordActionInputCheckXCommand(actionId, actionId.substring(0, actionId.indexOf("@"))).call();
        Thread.sleep(100);
        checkCoordAction(actionId, missingDeps, CoordinatorAction.Status.TIMEDOUT);
    }

    @Test
    public void testTimeoutWithException() throws Exception {
        String missingDeps = "nofs:///dirx/filex";
        String actionId = addInitRecords(missingDeps, null, TZ);
        try {
            new CoordActionInputCheckXCommand(actionId, actionId.substring(0, actionId.indexOf("@"))).call();
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("No FileSystem for scheme"));
        }
        // Timeout is 10 mins. Change action created time to before 12 min to make the action
        // timeout.
        long timeOutCreationTime = System.currentTimeMillis() - (12 * 60 * 1000);
        setCoordActionCreationTime(actionId, timeOutCreationTime);
        try {
            new CoordActionInputCheckXCommand(actionId, actionId.substring(0, actionId.indexOf("@"))).call();
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("No FileSystem for scheme"));
        }
        Thread.sleep(100);
        checkCoordAction(actionId, missingDeps, CoordinatorAction.Status.TIMEDOUT);
    }

    @Test
    public void testLastOnly() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTableForWaiting("coord-job-for-action-input-check.xml",
                CoordinatorJob.Status.RUNNING, false, true);
        job.setExecutionOrder(CoordinatorJobBean.Execution.LAST_ONLY);
        job.setFrequency("10");
        job.setTimeUnit(Timeunit.MINUTE);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQueryExecutor.CoordJobQuery.UPDATE_COORD_JOB, job);
        String missingDeps = "hdfs:///dirx/filex";

        // 1 hour in the past means next nominal time is 50 mins ago so should become SKIPPED
        String actionId1 = addInitRecords(missingDeps, null, TZ, job, 1);
        Date nomTime = new Date(new Date().getTime() - 60 * 60 * 1000);     // 1 hour ago
        setCoordActionNominalTime(actionId1, nomTime.getTime());
        new CoordActionInputCheckXCommand(actionId1, job.getId()).call();
        checkCoordActionStatus(actionId1, CoordinatorAction.Status.SKIPPED);

        // 1 hour in the future means next nominal time is 70 mins from now, so nothing should happen
        String actionId2 = addInitRecords(missingDeps, null, TZ, job, 2);
        nomTime = new Date(new Date().getTime() + 60 * 60 * 1000);          // 1 hour from now
        setCoordActionNominalTime(actionId2, nomTime.getTime());
        new CoordActionInputCheckXCommand(actionId2, job.getId()).call();
        checkCoordActionStatus(actionId2, CoordinatorAction.Status.WAITING);

        // 5 mins in the past means next nominal time is 5 mins from now, so nothing should happen
        String actionId3 = addInitRecords(missingDeps, null, TZ, job, 3);
        nomTime = new Date(new Date().getTime() - 5 * 60 * 1000);           // 5 minutes ago
        setCoordActionNominalTime(actionId3, nomTime.getTime());
        new CoordActionInputCheckXCommand(actionId3, job.getId()).call();
        checkCoordActionStatus(actionId3, CoordinatorAction.Status.WAITING);

        // 3 mins in the past means the next nominal time is 7 mins from now, but the end time is only 3 mins from now (which means
        // that the next nominal time would be after the end time), so nothing should happen
        Date endTime = new Date(new Date().getTime() + 3 * 60 * 1000);      // 3 minutes from now
        job.setEndTime(endTime);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQueryExecutor.CoordJobQuery.UPDATE_COORD_JOB, job);
        String actionId4 = addInitRecords(missingDeps, null, TZ, job, 4);
        nomTime = new Date(new Date().getTime() - 5 * 60 * 1000);           // 5 minutes ago
        setCoordActionNominalTime(actionId4, nomTime.getTime());
        new CoordActionInputCheckXCommand(actionId4, job.getId()).call();
        checkCoordActionStatus(actionId4, CoordinatorAction.Status.WAITING);

        // 1 hour in the past means the next nominal time is 50 mins ago, but the end time is 55 mins ago (which means that the next
        // nominal time would be after the end time but still in the past), so nothing should happen even though the action and the
        // next nominal time are both in the past
        endTime = new Date(new Date().getTime() - 55 * 60 * 1000);          // 55 mins ago
        job.setEndTime(endTime);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQueryExecutor.CoordJobQuery.UPDATE_COORD_JOB, job);
        String actionId5 = addInitRecords(missingDeps, null, TZ, job, 5);
        nomTime = new Date(new Date().getTime() - 60 * 60 * 1000);           // 1 hour ago
        setCoordActionNominalTime(actionId5, nomTime.getTime());
        new CoordActionInputCheckXCommand(actionId5, job.getId()).call();
        checkCoordActionStatus(actionId5, CoordinatorAction.Status.WAITING);
    }

    @Test
    public void testNone() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTableForWaiting("coord-job-for-action-input-check.xml",
                CoordinatorJob.Status.RUNNING, false, true);
        job.setExecutionOrder(CoordinatorJobBean.Execution.NONE);
        job.setFrequency("10");
        job.setTimeUnit(Timeunit.MINUTE);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQueryExecutor.CoordJobQuery.UPDATE_COORD_JOB, job);
        String missingDeps = "hdfs:///dirx/filex";

        // nominal time is one hour past. So aciton will be skipped
        String actionId1 = addInitRecords(missingDeps, null, TZ, job, 1);
        Date nomTime = new Date(new Date().getTime() - 60 * 60 * 1000);     // 1 hour ago
        setCoordActionNominalTime(actionId1, nomTime.getTime());
        new CoordActionInputCheckXCommand(actionId1, job.getId()).call();
        checkCoordActionStatus(actionId1, CoordinatorAction.Status.SKIPPED);

        // Nominal time is 60 minutes from now, action should be in WAITING stage
        String actionId2 = addInitRecords(missingDeps, null, TZ, job, 2);
        nomTime = new Date(new Date().getTime() + 60 * 60 * 1000);          // 1 hour from now
        setCoordActionNominalTime(actionId2, nomTime.getTime());
        new CoordActionInputCheckXCommand(actionId2, job.getId()).call();
        checkCoordActionStatus(actionId2, CoordinatorAction.Status.WAITING);

        // Nominal times of both actions are more than one minute past, so both will be skipped
        String actionId3 = addInitRecords(missingDeps, null, TZ, job, 3);
        nomTime = new Date(new Date().getTime() - 5 * 60 * 1000);           // 5 minutes ago
        setCoordActionNominalTime(actionId3, nomTime.getTime());
        new CoordActionInputCheckXCommand(actionId3, job.getId()).call();
        checkCoordActionStatus(actionId3, CoordinatorAction.Status.SKIPPED);

        String actionId4 = addInitRecords(missingDeps, null, TZ, job, 4);
        nomTime = new Date(new Date().getTime() - 2 * 60 * 1000);           // 2 minutes ago
        setCoordActionNominalTime(actionId4, nomTime.getTime());
        new CoordActionInputCheckXCommand(actionId4, job.getId()).call();
        checkCoordActionStatus(actionId4, CoordinatorAction.Status.SKIPPED);


    }

    protected CoordinatorJobBean addRecordToCoordJobTableForWaiting(String testFileName, CoordinatorJob.Status status,
            Date start, Date end, boolean pending, boolean doneMatd, int lastActionNum) throws Exception {

        String testDir = getTestCaseDir();
        CoordinatorJobBean coordJob = createCoordJob(testFileName, status, start, end, pending, doneMatd, lastActionNum);
        String appXml = getCoordJobXmlForWaiting(testFileName, testDir);
        coordJob.setJobXml(appXml);

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

    protected String getCoordJobXmlForWaiting(String testFileName, String testDir) {
        try {
            Reader reader = IOUtils.getResourceAsReader(testFileName, -1);
            String appXml = IOUtils.getReaderAsString(reader, -1);
            appXml = appXml.replaceAll("#testDir", testDir);
            return appXml;
        }
        catch (IOException ioe) {
            throw new RuntimeException(XLog.format("Could not get " + testFileName, ioe));
        }
    }

    protected CoordinatorActionBean addRecordToCoordActionTableForWaiting(String jobId, int actionNum,
            CoordinatorAction.Status status, String resourceXmlName) throws Exception {
        CoordinatorActionBean action = createCoordAction(jobId, actionNum, status, resourceXmlName, 0, TZ, null);
        String missDeps = getTestCaseFileUri("2009/01/29/_SUCCESS") + "#"
                + getTestCaseFileUri("2009/01/22/_SUCCESS") + "#"
                + getTestCaseFileUri("2009/01/15/_SUCCESS") + "#"
                + getTestCaseFileUri("2009/01/08/_SUCCESS");

        action.setMissingDependencies(missDeps);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordActionInsertJPAExecutor coordActionInsertCmd = new CoordActionInsertJPAExecutor(action);
            jpaService.execute(coordActionInsertCmd);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test coord action record to table");
            throw je;
        }
        return action;
    }

    private CoordinatorJobBean addRecordToCoordJobTable(String jobId, Date start, Date end) throws CommandException {
        return addRecordToCoordJobTable(jobId, start, end, "current");
    }

    private CoordinatorJobBean addRecordToCoordJobTable(String jobId, Date start, Date end,
            String dataInType) throws CommandException {
        CoordinatorJobBean coordJob = new CoordinatorJobBean();
        coordJob.setId(jobId);
        coordJob.setAppName("testApp");
        coordJob.setAppPath("testAppPath");
        coordJob.setStatus(CoordinatorJob.Status.RUNNING);
        coordJob.setCreatedTime(new Date());
        coordJob.setLastModifiedTime(new Date());
        coordJob.setUser("testUser");
        coordJob.setGroup("testGroup");
        coordJob.setTimeZone("UTC");
        coordJob.setTimeUnit(Timeunit.DAY);
        coordJob.setMatThrottling(2);
        try {
            coordJob.setStartTime(start);
            coordJob.setEndTime(end);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not set Date/time");
        }

        String testDir = getTestCaseDir();

        XConfiguration jobConf = new XConfiguration();
        jobConf.set(OozieClient.USER_NAME, getTestUser());

        String confStr = jobConf.toXmlString(false);
        coordJob.setConf(confStr);
        String appXml = "<coordinator-app xmlns='uri:oozie:coordinator:0.2' name='NAME' frequency=\"1\" start='2009-02-01T01:00" + TZ + "' end='2009-02-03T23:59" + TZ + "' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        appXml += "<controls>";
        appXml += "<timeout>10</timeout>";
        appXml += "<concurrency>2</concurrency>";
        appXml += "<execution>LIFO</execution>";
        appXml += "</controls>";
        appXml += "<input-events>";
        appXml += "<data-in name='A' dataset='a'>";
        appXml += "<dataset name='a' frequency='7' initial-instance='2009-01-01T01:00" + TZ + "' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        appXml += "<uri-template>" + getTestCaseFileUri("${YEAR}/${MONTH}/${DAY}" )+ "</uri-template>";
        appXml += "</dataset>";
        if (dataInType.equals("future")) {
            appXml += "<start-instance>${coord:" + dataInType + "(0,5)}</start-instance>";
            appXml += "<end-instance>${coord:" + dataInType + "(3,5)}</end-instance>";
        }
        else if (dataInType.equals("latest")) {
            appXml += "<start-instance>${coord:" + dataInType + "(-3)}</start-instance>";
            appXml += "<end-instance>${coord:" + dataInType + "(0)}</end-instance>";
        }
        else if (dataInType.equals("current")) {
            appXml += "<start-instance>${coord:" + dataInType + "(-3)}</start-instance>";
            appXml += "<end-instance>${coord:" + dataInType + "(1)}</end-instance>";
        }
        appXml += "</data-in>";
        appXml += "</input-events>";
        appXml += "<output-events>";
        appXml += "<data-out name='LOCAL_A' dataset='local_a'>";
        appXml += "<dataset name='local_a' frequency='7' initial-instance='2009-01-01T01:00" + TZ + "' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        appXml += "<uri-template>" + getTestCaseFileUri("${YEAR}/${MONTH}/${DAY}" )+ "</uri-template>";
        appXml += "</dataset>";
        appXml += "<start-instance>${coord:current(-3)}</start-instance>";
        appXml += "<instance>${coord:current(0)}</instance>";
        appXml += "</data-out>";
        appXml += "</output-events>";
        appXml += "<action>";
        appXml += "<workflow>";
        appXml += "<app-path>hdfs:///tmp/workflows/</app-path>";
        appXml += "<configuration>";
        appXml += "<property>";
        appXml += "<name>inputA</name>";
        appXml += "<value>${coord:dataIn('A')}</value>";
        appXml += "</property>";
        appXml += "<property>";
        appXml += "<name>inputB</name>";
        appXml += "<value>${coord:dataOut('LOCAL_A')}</value>";
        appXml += "</property>";
        appXml += "</configuration>";
        appXml += "</workflow>";
        appXml += "</action>";
        appXml += "</coordinator-app>";
        coordJob.setJobXml(appXml);
        coordJob.setLastActionNumber(0);
        coordJob.setFrequency("1");
        coordJob.setExecutionOrder(Execution.FIFO);
        coordJob.setConcurrency(1);

        JPAService jpaService = Services.get().get(JPAService.class);
        if (jpaService != null) {
            try {
                jpaService.execute(new CoordJobInsertJPAExecutor(coordJob));
            }
            catch (JPAExecutorException e) {
                throw new CommandException(e);
            }
        }
        else {
            fail("Unable to insert the test job record to table");
        }
        return coordJob;
    }

    private CoordinatorActionBean checkCoordAction(String actionId, String expDeps, CoordinatorAction.Status stat)
            throws Exception {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            CoordinatorActionBean action = jpaService.execute(new CoordActionGetJPAExecutor(actionId));
            String missDeps = action.getMissingDependencies();
            assertEquals(expDeps, missDeps);
            assertEquals(stat, action.getStatus());

            return action;
        }
        catch (JPAExecutorException se) {
            throw new Exception("Action ID " + actionId + " was not stored properly in db");
        }
    }

    private CoordinatorActionBean checkCoordActionStatus(final String actionId, final CoordinatorAction.Status stat)
            throws Exception {
        final JPAService jpaService = Services.get().get(JPAService.class);
        waitFor(5 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                try {
                    CoordinatorActionBean action = jpaService.execute(new CoordActionGetJPAExecutor(actionId));
                    return stat.equals(action.getStatus());
                }
                catch (JPAExecutorException se) {
                    throw new Exception("Action ID " + actionId + " was not stored properly in db");
                }
            }
        });
        CoordinatorActionBean action = jpaService.execute(new CoordActionGetJPAExecutor(actionId));
        assertEquals(stat, action.getStatus());
        return action;
    }
}
