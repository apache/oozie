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

import java.io.ByteArrayOutputStream;
import java.util.List;

import org.apache.log4j.Appender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.WriterAppender;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.dependency.DependencyChecker;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.CallableQueueService;
import org.apache.oozie.service.HCatAccessorService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.PartitionDependencyManagerService;
import org.apache.oozie.service.RecoveryService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.HCatURI;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestCoordPushDependencyCheckXCommand extends XDataTestCase {
    private String server;
    private Services services = null;

    @Before
    protected void setUp() throws Exception {
        super.setUp();
        services = super.setupServicesForHCatalog();
        services.init();
        server = getMetastoreAuthority();
    }

    @After
    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    @Test
    public void testUpdateCoordTableSingleDep() throws Exception {
        // Test for single dependency which is already in the hcat server
        String db = "default";
        String table = "tablename";
        String newHCatDependency = "hcat://" + server + "/" + db + "/" + table + "/dt=20120430;country=usa";

        populateTable(db, table);

        String actionId = addInitRecords(newHCatDependency);
        checkCoordAction(actionId, newHCatDependency, CoordinatorAction.Status.WAITING);

        new CoordPushDependencyCheckXCommand(actionId).call();

        checkCoordAction(actionId, "", CoordinatorAction.Status.READY);

    }

    @Test
    public void testUpdateCoordTableMultipleDepsV1() throws Exception {
        // Test for two dependencies which are already in the hcat server
        String db = "default";
        String table = "tablename";
        String newHCatDependency1 = "hcat://" + server + "/" + db + "/" + table + "/dt=20120412;country=brazil";
        String newHCatDependency2 = "hcat://" + server + "/" + db + "/" + table + "/dt=20120430;country=usa";
        String newHCatDependency = newHCatDependency1 + CoordELFunctions.INSTANCE_SEPARATOR + newHCatDependency2;
        populateTable(db, table);

        String actionId = addInitRecords(newHCatDependency);
        checkCoordAction(actionId, newHCatDependency, CoordinatorAction.Status.WAITING);

        new CoordPushDependencyCheckXCommand(actionId).call();

        checkCoordAction(actionId, "", CoordinatorAction.Status.READY);

    }

    @Test
    public void testUpdateCoordTableMultipleDepsV2() throws Exception {
        // Test for two dependencies : one of them is already existing in the
        // hcat server. Other one is not.
        // Expected to see both action in WAITING as first one is not available and we only check for first.
        // Later make the other partition also available. action is expected to
        // be READY
        String db = "default";
        String table = "tablename";
        String newHCatDependency1 = "hcat://" + server + "/" + db + "/" + table + "/dt=20120430;country=brazil";
        String newHCatDependency2 = "hcat://" + server + "/" + db + "/" + table + "/dt=20120430;country=usa";
        String newHCatDependency = newHCatDependency1 + CoordELFunctions.INSTANCE_SEPARATOR + newHCatDependency2;
        populateTable(db, table);

        String actionId = addInitRecords(newHCatDependency);
        checkCoordAction(actionId, newHCatDependency, CoordinatorAction.Status.WAITING);

        // Checks only for first missing dependency
        new CoordPushDependencyCheckXCommand(actionId).call();

        // Checks dependencies in order. So list does not change if first one is not available
        checkCoordAction(actionId, newHCatDependency, CoordinatorAction.Status.WAITING);

        // Make first dependency available
        dropPartition(db, table, "dt=20120430;country=usa");
        addPartition(db, table, "dt=20120430;country=brazil");
        new CoordPushDependencyCheckXCommand(actionId).call();
        checkCoordAction(actionId, newHCatDependency2, CoordinatorAction.Status.WAITING);

        addPartition(db, table, "dt=20120430;country=usa");
        new CoordPushDependencyCheckXCommand(actionId).call();
        checkCoordAction(actionId, "", CoordinatorAction.Status.READY);
    }

    @Test
    public void testUpdateCoordTableMultipleDepsV3() throws Exception {
        // Test for two dependencies : one of them is already existing in the
        // hcat server. Other one is not.
        // Expected to see only first action in WAITING as we check for all dependencies.
        // Later make the other partition also available. action is expected to
        // be READY
        String db = "default";
        String table = "tablename";
        String newHCatDependency1 = "hcat://" + server + "/" + db + "/" + table + "/dt=20120430;country=brazil";
        String newHCatDependency2 = "hcat://" + server + "/" + db + "/" + table + "/dt=20120430;country=usa";
        String newHCatDependency = newHCatDependency1 + CoordELFunctions.INSTANCE_SEPARATOR + newHCatDependency2;
        populateTable(db, table);

        String actionId = addInitRecords(newHCatDependency);
        checkCoordAction(actionId, newHCatDependency, CoordinatorAction.Status.WAITING);

        // Checks for all missing dependencies
        new CoordPushDependencyCheckXCommand(actionId, true).call();
        checkCoordAction(actionId, newHCatDependency1, CoordinatorAction.Status.WAITING);
        PartitionDependencyManagerService pdms = Services.get().get(PartitionDependencyManagerService.class);
        HCatAccessorService hcatService = Services.get().get(HCatAccessorService.class);
        assertTrue(pdms.getWaitingActions(new HCatURI(newHCatDependency1)).contains(actionId));
        assertTrue(hcatService.isRegisteredForNotification(new HCatURI(newHCatDependency1)));
        assertNull(pdms.getWaitingActions(new HCatURI(newHCatDependency2)));

        // Make first dependency available
        addPartition(db, table, "dt=20120430;country=brazil");
        new CoordPushDependencyCheckXCommand(actionId).call();
        checkCoordAction(actionId, "", CoordinatorAction.Status.READY);
        assertNull(pdms.getWaitingActions(new HCatURI(newHCatDependency1)));
        assertFalse(hcatService.isRegisteredForNotification(new HCatURI(newHCatDependency1)));
    }

    @Test
    public void testResolveCoordConfiguration() throws Exception {
        String db = "default";
        String table = "tablename";
        String newHCatDependency1 = "hcat://" + server + "/" + db + "/" + table + "/dt=20120412;country=brazil";
        String newHCatDependency2 = "hcat://" + server + "/" + db + "/" + table + "/dt=20120430;country=usa";
        String newHCatDependency = newHCatDependency1 + CoordELFunctions.INSTANCE_SEPARATOR + newHCatDependency2;
        populateTable(db, table);

        CoordinatorJobBean job = addRecordToCoordJobTableForWaiting("coord-job-for-action-input-check.xml",
                CoordinatorJob.Status.RUNNING, false, true);

        CoordinatorActionBean action = addRecordToCoordActionTableForWaiting(job.getId(), 1,
                CoordinatorAction.Status.WAITING, "coord-action-for-action-push-check.xml", null, newHCatDependency,
                "Z");

        String actionId = action.getId();
        checkCoordAction(actionId, newHCatDependency, CoordinatorAction.Status.WAITING);

        new CoordPushDependencyCheckXCommand(actionId).call();

        CoordinatorActionBean caBean = checkCoordAction(actionId, "", CoordinatorAction.Status.READY);
        Element eAction = XmlUtils.parseXml(caBean.getActionXml());
        Element configElem = eAction.getChild("action", eAction.getNamespace())
                .getChild("workflow", eAction.getNamespace()).getChild("configuration", eAction.getNamespace());
        List<?> elementList = configElem.getChildren("property", configElem.getNamespace());
        Element e1 = (Element) elementList.get(0);
        Element e2 = (Element) elementList.get(1);
        assertEquals(
                "hcat://dummyhcat:1000/db1/table1/ds=/2009-29,hcat://dummyhcat:1000/db1/table1/ds=/2009-29," +
                "hcat://dummyhcat:1000/db1/table1/ds=/2009-29",
                e1.getChild("value", e1.getNamespace()).getValue());
        assertEquals("hcat://dummyhcat:1000/db1/table1/ds=/2009-29", e2.getChild("value", e1.getNamespace()).getValue());

    }

    @Test
    public void testTimeOut() throws Exception {
        String db = "default";
        String table = "tablename";
        String newHCatDependency1 = "hcat://" + server + "/" + db + "/" + table + "/dt=20120430;country=brazil";
        String newHCatDependency2 = "hcat://" + server + "/" + db + "/" + table + "/dt=20120430;country=usa";
        String newHCatDependency = newHCatDependency1 + CoordELFunctions.INSTANCE_SEPARATOR + newHCatDependency2;
        populateTable(db, table);

        String actionId = addInitRecords(newHCatDependency);
        checkCoordAction(actionId, newHCatDependency, CoordinatorAction.Status.WAITING);
        new CoordPushDependencyCheckXCommand(actionId, true).call();
        checkCoordAction(actionId, newHCatDependency1, CoordinatorAction.Status.WAITING);
        PartitionDependencyManagerService pdms = Services.get().get(PartitionDependencyManagerService.class);
        HCatAccessorService hcatService = Services.get().get(HCatAccessorService.class);
        assertTrue(pdms.getWaitingActions(new HCatURI(newHCatDependency1)).contains(actionId));
        assertTrue(hcatService.isRegisteredForNotification(new HCatURI(newHCatDependency1)));

        // Timeout is 10 mins. Change action created time to before 12 min to make the action
        // timeout.
        long timeOutCreationTime = System.currentTimeMillis() - (12 * 60 * 1000);
        setCoordActionCreationTime(actionId, timeOutCreationTime);
        new CoordPushDependencyCheckXCommand(actionId).call();
        Thread.sleep(100);
        // Check for timeout status and unregistered missing dependencies
        checkCoordAction(actionId, newHCatDependency1, CoordinatorAction.Status.TIMEDOUT);
        assertNull(pdms.getWaitingActions(new HCatURI(newHCatDependency1)));
        assertFalse(hcatService.isRegisteredForNotification(new HCatURI(newHCatDependency1)));

    }

    @Test
    public void testTimeOutWithUnresolvedMissingDependencies() throws Exception {
        String db = "default";
        String table = "tablename";
        String newHCatDependency1 = "hcat://" + server + "/" + db + "/" + table + "/dt=20120430;country=brazil";
        String newHCatDependency2 = "hcat://" + server + "/" + db + "/" + table + "/dt=20120430;country=usa";
        String newHCatDependency3 = "hcat://" + server + "/" + db + "/" + table + "/dt=20120430;country=uk";
        String newHCatDependency = newHCatDependency1 + CoordELFunctions.INSTANCE_SEPARATOR + newHCatDependency2;
        populateTable(db, table);

        String actionId = addInitRecords(newHCatDependency);
        checkCoordAction(actionId, newHCatDependency, CoordinatorAction.Status.WAITING);
        new CoordPushDependencyCheckXCommand(actionId, true).call();
        checkCoordAction(actionId, newHCatDependency1, CoordinatorAction.Status.WAITING);
        PartitionDependencyManagerService pdms = Services.get().get(PartitionDependencyManagerService.class);
        HCatAccessorService hcatService = Services.get().get(HCatAccessorService.class);
        assertTrue(pdms.getWaitingActions(new HCatURI(newHCatDependency1)).contains(actionId));
        assertTrue(hcatService.isRegisteredForNotification(new HCatURI(newHCatDependency1)));

        // Timeout is 10 mins. Change action created time to before 12 min to make the action
        // timeout.
        long timeOutCreationTime = System.currentTimeMillis() - (12 * 60 * 1000);
        setCoordActionCreationTime(actionId, timeOutCreationTime);
        // Set some missing dependency. Instead of latest or future just setting a current one for testing as
        // we are only interested in ensuring CoordActionInputCheckXCommand is run
        setMissingDependencies(actionId, newHCatDependency + CoordELFunctions.INSTANCE_SEPARATOR + newHCatDependency3);
        addPartition(db, table, "dt=20120430;country=brazil");
        checkDependencies(actionId, newHCatDependency + CoordELFunctions.INSTANCE_SEPARATOR + newHCatDependency3,
                newHCatDependency1);
        new CoordPushDependencyCheckXCommand(actionId).call();
        // Somehow with hive 0.10 it takes 1 second more.
        Thread.sleep(1300);

        checkDependencies(actionId, newHCatDependency3, "");
        assertNull(pdms.getWaitingActions(new HCatURI(newHCatDependency1)));
        assertFalse(hcatService.isRegisteredForNotification(new HCatURI(newHCatDependency1)));
    }

    @Test
    public void testTimeOutWithException1() throws Exception {
        // Test timeout when missing dependencies are from a non existing table
        String newHCatDependency1 = "hcat://" + server + "/nodb/notable/dt=20120430;country=brazil";
        String newHCatDependency2 = "hcat://" + server + "/nodb/notable/dt=20120430;country=usa";
        String newHCatDependency = newHCatDependency1 + CoordELFunctions.INSTANCE_SEPARATOR + newHCatDependency2;

        String actionId = addInitRecords(newHCatDependency);
        checkCoordAction(actionId, newHCatDependency, CoordinatorAction.Status.WAITING);
        try {
            new CoordPushDependencyCheckXCommand(actionId, true).call();
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("NoSuchObjectException"));
        }
        checkCoordAction(actionId, newHCatDependency, CoordinatorAction.Status.WAITING);
        PartitionDependencyManagerService pdms = Services.get().get(PartitionDependencyManagerService.class);
        HCatAccessorService hcatService = Services.get().get(HCatAccessorService.class);
        assertNull(pdms.getWaitingActions(new HCatURI(newHCatDependency1)));
        assertFalse(hcatService.isRegisteredForNotification(new HCatURI(newHCatDependency1)));

        // Timeout is 10 mins. Change action created time to before 12 min to make the action
        // timeout.
        long timeOutCreationTime = System.currentTimeMillis() - (12 * 60 * 1000);
        setCoordActionCreationTime(actionId, timeOutCreationTime);
        try {
            new CoordPushDependencyCheckXCommand(actionId).call();
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("NoSuchObjectException"));
        }
        Thread.sleep(100);
        // Check for timeout status and unregistered missing dependencies
        checkCoordAction(actionId, newHCatDependency, CoordinatorAction.Status.TIMEDOUT);
    }

    @Test
    public void testTimeOutWithException2() throws Exception {
        // Test timeout when table containing missing dependencies is dropped in between
        String db = "default";
        String table = "tablename";
        String newHCatDependency1 = "hcat://" + server + "/" + db + "/" + table + "/dt=20120430;country=brazil";
        String newHCatDependency2 = "hcat://" + server + "/" + db + "/" + table + "/dt=20120430;country=usa";
        String newHCatDependency = newHCatDependency1 + CoordELFunctions.INSTANCE_SEPARATOR + newHCatDependency2;
        populateTable(db, table);

        String actionId = addInitRecords(newHCatDependency);
        checkCoordAction(actionId, newHCatDependency, CoordinatorAction.Status.WAITING);
        new CoordPushDependencyCheckXCommand(actionId, true).call();
        checkCoordAction(actionId, newHCatDependency1, CoordinatorAction.Status.WAITING);
        PartitionDependencyManagerService pdms = Services.get().get(PartitionDependencyManagerService.class);
        HCatAccessorService hcatService = Services.get().get(HCatAccessorService.class);
        assertTrue(pdms.getWaitingActions(new HCatURI(newHCatDependency1)).contains(actionId));
        assertTrue(hcatService.isRegisteredForNotification(new HCatURI(newHCatDependency1)));

        // Timeout is 10 mins. Change action created time to before 12 min to make the action
        // timeout.
        long timeOutCreationTime = System.currentTimeMillis() - (12 * 60 * 1000);
        setCoordActionCreationTime(actionId, timeOutCreationTime);
        dropTable(db, table, true);
        try {
            new CoordPushDependencyCheckXCommand(actionId).call();
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("NoSuchObjectException"));
        }
        Thread.sleep(100);
        // Check for timeout status and unregistered missing dependencies
        checkCoordAction(actionId, newHCatDependency1, CoordinatorAction.Status.TIMEDOUT);
        assertNull(pdms.getWaitingActions(new HCatURI(newHCatDependency1)));
        assertFalse(hcatService.isRegisteredForNotification(new HCatURI(newHCatDependency1)));
    }

    @Test
    public void testRequeueOnException() throws Exception {
        Services.get().getConf().setInt(RecoveryService.CONF_SERVICE_INTERVAL, 6000);
        // Test timeout when table containing missing dependencies is dropped in between
        String newHCatDependency1 = "hcat://" + server + "/nodb/notable/dt=20120430;country=brazil";
        String newHCatDependency2 = "hcat://" + server + "/nodb/notable/dt=20120430;country=usa";
        String newHCatDependency = newHCatDependency1 + CoordELFunctions.INSTANCE_SEPARATOR + newHCatDependency2;


        CoordinatorJobBean job = addRecordToCoordJobTableForWaiting("coord-job-for-action-input-check.xml",
                CoordinatorJob.Status.RUNNING, false, true);

        CoordinatorActionBean action = addRecordToCoordActionTableForWaiting(job.getId(), 1,
                CoordinatorAction.Status.WAITING, "coord-action-for-action-input-check.xml", null,
                newHCatDependency, "Z");
        String actionId = action.getId();
        checkCoordAction(actionId, newHCatDependency, CoordinatorAction.Status.WAITING);
        try {
            new CoordPushDependencyCheckXCommand(actionId, true).call();
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("NoSuchObjectException"));
        }
        // Nothing should be queued as there are no pull dependencies
        CallableQueueService callableQueueService = Services.get().get(CallableQueueService.class);
        assertEquals(0, callableQueueService.getQueueDump().size());

        // Nothing should be queued as there are no pull missing dependencies
        // but only push missing deps are there
        new CoordActionInputCheckXCommand(actionId, job.getId()).call();
        callableQueueService = Services.get().get(CallableQueueService.class);
        assertEquals(0, callableQueueService.getQueueDump().size());

        setMissingDependencies(actionId, newHCatDependency1);
        try {
            new CoordPushDependencyCheckXCommand(actionId, true).call();
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("NoSuchObjectException"));
        }
        // Should be requeued at the recovery service interval
        final List<String> queueDump = callableQueueService.getQueueDump();
        assertEquals(1, callableQueueService.getQueueDump().size());
        assertTrue(queueDump.get(0).contains("coord_push_dep_check"));
        log.info("Queue dump is " + queueDump.toString());
        // Delay should be something like delay=599999. Ignore last three digits
        assertTrue(queueDump.get(0).matches(".* delay=599[0-9]{3}"));
    }

    @Test
    public void testLogMessagePrefix() throws Exception {
        Logger logger = Logger.getLogger(DependencyChecker.class);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Layout layout = new SimpleLayout();
        Appender appender = new WriterAppender(layout, out);
        logger.addAppender(appender);

        String db = "default";
        String table = "tablename";
        populateTable(db, table);

        String newHCatDependency = "hcat://" + server + "/" + db + "/" + table + "/dt=20120430;country=brazil";
        String actionId1 = addInitRecords(newHCatDependency);
        new CoordPushDependencyCheckXCommand(actionId1).call();
        assertTrue(out.toString().contains("ACTION[" + actionId1 + "]"));
        out.reset();
        String actionId2 = addInitRecords(newHCatDependency);
        new CoordPushDependencyCheckXCommand(actionId2).call();
        assertFalse(out.toString().contains("ACTION[" + actionId1 + "]"));
        assertTrue(out.toString().contains("ACTION[" + actionId2 + "]"));
    }

    private void populateTable(String db, String table) throws Exception {
        dropTable(db, table, true);
        dropDatabase(db, true);
        createDatabase(db);
        createTable(db, table, "dt,country");
        addPartition(db, table, "dt=20120430;country=usa");
        addPartition(db, table, "dt=20120412;country=brazil");
        addPartition(db, table, "dt=20120413;country=brazil");
    }

    private CoordinatorActionBean checkCoordAction(String actionId, String expDeps, CoordinatorAction.Status stat)
            throws Exception {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            CoordinatorActionBean action = jpaService.execute(new CoordActionGetJPAExecutor(actionId));
            String missDeps = action.getPushMissingDependencies();
            assertEquals(expDeps, missDeps);
            assertEquals(stat, action.getStatus());

            return action;
        }
        catch (JPAExecutorException se) {
            throw new Exception("Action ID " + actionId + " was not stored properly in db");
        }
    }

    private CoordinatorActionBean checkDependencies(String actionId, String expDeps, String expPushDeps)
            throws Exception {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            CoordinatorActionBean action = jpaService.execute(new CoordActionGetJPAExecutor(actionId));
            assertEquals(expDeps, action.getMissingDependencies());
            assertEquals(expPushDeps, action.getPushMissingDependencies());
            return action;
        }
        catch (JPAExecutorException se) {
            throw new Exception("Action ID " + actionId + " was not stored properly in db");
        }
    }
}
