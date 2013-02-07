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
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.HCatAccessorService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.PartitionDependencyManagerService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.HCatURI;
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
        // Expected to see both action in WAITING as first one is not available.
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
        // Expected to see the action in WAITING
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

        new CoordPushDependencyCheckXCommand(actionId, true).call();
        checkCoordAction(actionId, newHCatDependency1, CoordinatorAction.Status.WAITING);
        PartitionDependencyManagerService pdms = Services.get().get(PartitionDependencyManagerService.class);
        assertTrue(pdms.getWaitingActions(new HCatURI(newHCatDependency1)).contains(actionId));
        HCatAccessorService hcatService = Services.get().get(HCatAccessorService.class);
        assertTrue(hcatService.isRegisteredForNotification(new HCatURI(newHCatDependency1)));

        // Make first dependency available
        addPartition(db, table, "dt=20120430;country=brazil");
        new CoordPushDependencyCheckXCommand(actionId).call();
        assertNull(pdms.getWaitingActions(new HCatURI(newHCatDependency1)));
        checkCoordAction(actionId, "", CoordinatorAction.Status.READY);
        assertFalse(hcatService.isRegisteredForNotification(new HCatURI(newHCatDependency1)));
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
            assertEquals(action.getStatus(), stat);

            return action;
        }
        catch (JPAExecutorException se) {
            throw new Exception("Action ID " + actionId + " was not stored properly in db");
        }
    }

}
