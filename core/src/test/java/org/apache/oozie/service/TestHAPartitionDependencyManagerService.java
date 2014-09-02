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

package org.apache.oozie.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.util.Shell;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.client.CoordinatorAction.Status;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.dependency.hcat.HCatMessageHandler;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.service.RecoveryService.RecoveryRunnable;
import org.apache.oozie.test.ZKXTestCase;
import org.apache.oozie.util.HCatURI;

public class TestHAPartitionDependencyManagerService extends ZKXTestCase {

    protected Services services;
    protected String server;
    protected String db;
    protected String table1;
    protected String table2;
    protected String part1;
    protected String part2;
    protected String part3;

    protected void setUp() throws Exception {
        super.setUp();
        services = super.setupServicesForHCatalog(Services.get());
        // disable recovery service
        services.getConf().setInt(RecoveryService.CONF_SERVICE_INTERVAL, 1000000);
        // disable regular cache purge
        services.getConf().setInt(PartitionDependencyManagerService.CACHE_PURGE_INTERVAL, 1000000);
        server = super.getHCatalogServer().getMetastoreAuthority();
        services.init();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    private void populateTable() throws Exception {
        dropTable(db, table1, true);
        dropTable(db, table2, true);
        dropDatabase(db, true);
        createDatabase(db);
        createTable(db, table1, "dt,country");
        createTable(db, table2, "dt,country");
    }

    protected String getSanitizedTestCaseDir() {
        // On Windows, the working directory will have a colon from to the drive letter. Because colons
        // are not allowed in DFS paths, we remove it. Also, prepend a backslash to simulate an absolute path.
        if(Shell.WINDOWS) {
            return "\\" + getTestCaseDir().replaceAll(":", "");
        }
        else {
            return getTestCaseDir();
        }
    }

    public void testDependencyCacheWithHA() throws Exception {

        db = "default";
        table1 = "mytbl";
        table2 = "mytb2";
        part1 = "dt=20120101;country=us";
        part2 = "dt=20120102;country=us";
        part3 = "dt=20120103;country=us";
        String newHCatDependency1 = "hcat://" + server + "/" + db + "/" + table1 + "/" + part1;
        String newHCatDependency2 = "hcat://" + server + "/" + db + "/" + table1 + "/" + part2;
        String newHCatDependency3 = "hcat://" + server + "/" + db + "/" + table2 + "/" + part3;
        HCatURI dep1 = new HCatURI(newHCatDependency1);
        HCatURI dep2 = new HCatURI(newHCatDependency2);
        HCatURI dep3 = new HCatURI(newHCatDependency3);
        // create db, table and partitions
        populateTable();

        String actionId1 = addInitRecords(newHCatDependency1);
        String actionId2 = addInitRecords(newHCatDependency2);
        String actionId3 = addInitRecords(newHCatDependency3);

        // Assume dependency cache on dummy server with missing push dependencies registered
        PartitionDependencyManagerService dummyPdms = new PartitionDependencyManagerService();
        PartitionDependencyManagerService pdms = Services.get().get(PartitionDependencyManagerService.class);
        dummyPdms.init(Services.get());
        dummyPdms.addMissingDependency(dep1, actionId1);
        dummyPdms.addMissingDependency(dep2, actionId2);
        dummyPdms.addMissingDependency(dep3, actionId3);

        Collection<String> waitingActions = (Collection<String>)dummyPdms.getWaitingActions(dep1);
        assertEquals(1, waitingActions.size());
        waitingActions = (Collection<String>)dummyPdms.getWaitingActions(dep2);
        assertEquals(1, waitingActions.size());
        waitingActions = (Collection<String>)dummyPdms.getWaitingActions(dep3);
        assertEquals(1, waitingActions.size());

        //Dependency cache on living server doesn't have these partitions registered at this point
        waitingActions = (Collection<String>)pdms.getWaitingActions(dep1);
        assertNull(waitingActions);
        waitingActions = (Collection<String>)pdms.getWaitingActions(dep2);
        assertNull(waitingActions);
        waitingActions = (Collection<String>)pdms.getWaitingActions(dep3);
        assertNull(waitingActions);

        //Assume dummy server is down, and recovery service on living server pick up these jobs
        dummyPdms.destroy();
        Runnable recoveryRunnable = new RecoveryRunnable(60, 0, 60);
        recoveryRunnable.run();
        waitFor(30 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                Collection<String> waitingActions;
                PartitionDependencyManagerService pdms = Services.get().get(PartitionDependencyManagerService.class);
                HCatURI dep1 = new HCatURI("hcat://"+ server + "/" + db + "/" + table1 + "/" + part1);
                HCatURI dep2 = new HCatURI("hcat://"+ server + "/" + db + "/" + table1 + "/" + part2);
                HCatURI dep3 = new HCatURI("hcat://"+ server + "/" + db + "/" + table2 + "/" + part3);
                waitingActions = pdms.getWaitingActions(dep1);
                if(waitingActions == null) {
                    return false;
                }
                waitingActions = pdms.getWaitingActions(dep2);
                if(waitingActions == null) {
                    return false;
                }
                waitingActions = pdms.getWaitingActions(dep3);
                if(waitingActions == null) {
                    return false;
                }
                return true;
            }
        });
        //Dependency cache on living server has missing partitions added
        waitingActions = (Collection<String>)pdms.getWaitingActions(dep1);
        assertEquals(1, waitingActions.size());
        assertTrue(waitingActions.contains(actionId1));
        waitingActions = (Collection<String>)pdms.getWaitingActions(dep2);
        assertEquals(1, waitingActions.size());
        assertTrue(waitingActions.contains(actionId2));
        waitingActions = (Collection<String>)pdms.getWaitingActions(dep3);
        assertEquals(1, waitingActions.size());
        assertTrue(waitingActions.contains(actionId3));

        HCatAccessorService hcatService = Services.get().get(HCatAccessorService.class);
        // mytbl and mytb2 registered to topic map to receive notification
        assertTrue(hcatService.isRegisteredForNotification(dep1));
        assertTrue(hcatService.isRegisteredForNotification(dep2));
        assertTrue(hcatService.isRegisteredForNotification(dep3));
    }

    protected void addMissingDependencyAndRegister(HCatURI hcatURI, String actionId, PartitionDependencyManagerService pdms) {
        pdms.addMissingDependency(hcatURI, actionId);
        HCatAccessorService hcatService = Services.get().get(HCatAccessorService.class);
        if (!hcatService.isRegisteredForNotification(hcatURI)) {
            hcatService.registerForNotification(hcatURI, hcatURI.getDb() + "." + hcatURI.getTable(),
                    new HCatMessageHandler(hcatURI.getServer()));
        }
    }

    public void testPurgeMissingDependencies() throws Exception{
        services.setService(ZKJobsConcurrencyService.class);
        PartitionDependencyManagerService pdms = services.get(PartitionDependencyManagerService.class);
        pdms.init(services);
        testPurgeMissingDependenciesForCache(pdms);
    }

    protected void testPurgeMissingDependenciesForCache(PartitionDependencyManagerService pdms) throws Exception{

        String actionId1 = "1234465451";
        String actionId2 = "1234465452";
        String actionId3 = "1234465453";

        // add partitions as missing
        HCatURI dep1 = new HCatURI("hcat://hcat-server1.domain.com:5080/mydb/mytbl1/dt=20120101;country=us");
        HCatURI dep2 = new HCatURI("hcat://hcat-server1.domain.com:5080/mydb/mytbl1/country=us;dt=20120101");
        HCatURI dep3 = new HCatURI("hcat://hcat-server2.domain.com:5080/mydb/mytbl2/dt=20120102;country=us");

        // actionId1-->(dep1,2), actionId2-->(dep2), actionId3-->(dep2,3)
        addMissingDependencyAndRegister(dep1, actionId1, pdms);
        addMissingDependencyAndRegister(dep2, actionId1, pdms);
        addMissingDependencyAndRegister(dep2, actionId2, pdms);
        addMissingDependencyAndRegister(dep2, actionId3, pdms);
        addMissingDependencyAndRegister(dep3, actionId3, pdms);

        List<String> waitingDep1 = (ArrayList<String>) pdms.getWaitingActions(dep1);
        assertEquals(waitingDep1.size(), 1);
        assertEquals(waitingDep1.get(0), actionId1);

        List<String> waitingDep2 = (ArrayList<String>) pdms.getWaitingActions(dep2);
        assertEquals(waitingDep2.size(), 3);
        for (String id : waitingDep2) {
            assertTrue(id.equals(actionId1) || id.equals(actionId2) || id.equals(actionId3));
        }
        List<String> waitingDep3 = (ArrayList<String>) pdms.getWaitingActions(dep3);
        assertEquals(waitingDep3.size(), 1);
        assertTrue(waitingDep3.get(0).equals(actionId3));

        // make only coordAction 1 to WAITING, the rest to RUNNING (only WAITING
        // remain dependency cache)
        ArrayList<JsonBean> insertList = new ArrayList<JsonBean>();
        CoordinatorActionBean coordAction1 = new CoordinatorActionBean();
        coordAction1.setId(actionId1);
        coordAction1.setStatus(Status.WAITING);
        insertList.add(coordAction1);
        CoordinatorActionBean coordAction2 = new CoordinatorActionBean();
        coordAction2.setId(actionId2);
        coordAction2.setStatus(Status.RUNNING);
        insertList.add(coordAction2);
        CoordinatorActionBean coordAction3 = new CoordinatorActionBean();
        coordAction3.setId(actionId3);
        coordAction3.setStatus(Status.RUNNING);
        insertList.add(coordAction3);
        BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(insertList, null, null);

        // run cache purge
        Services.get().getConf().setInt(PartitionDependencyManagerService.CACHE_PURGE_TTL, 0);
        pdms.runCachePurgeWorker();

        // only coord Action 1 still in dependency cache
        waitingDep1 = (ArrayList<String>) pdms.getWaitingActions(dep1);
        assertEquals(waitingDep1.size(), 1);
        assertTrue(waitingDep1.get(0).equals(actionId1));

        // only coord Action 1 still in dependency cache
        waitingDep2 = (ArrayList<String>) pdms.getWaitingActions(dep2);
        assertEquals(waitingDep2.size(), 1);
        assertTrue(waitingDep2.get(0).equals(actionId1));

        waitingDep3 = (ArrayList<String>) pdms.getWaitingActions(dep3);
        assertNull(waitingDep3);

        HCatAccessorService hcatService = Services.get().get(HCatAccessorService.class);
        // mytbl1 should be still in topic map
        assertTrue(hcatService.isRegisteredForNotification(dep1));
        // mytbl1 should be still in topic map
        assertTrue(hcatService.isRegisteredForNotification(dep2));
        // mytbl2 should NOT be in topic map
        assertFalse(hcatService.isRegisteredForNotification(dep3));
    }
}
