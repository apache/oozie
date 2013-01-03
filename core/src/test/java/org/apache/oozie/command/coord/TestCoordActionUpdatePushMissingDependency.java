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

import java.util.List;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JMSAccessorService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.PartitionDependencyManagerService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.HCatURI;
import org.apache.oozie.util.PartitionWrapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestCoordActionUpdatePushMissingDependency extends XDataTestCase {
    private Services services;

    @Before
    protected void setUp() throws Exception {
        super.setUp();
        setSystemProperty(PartitionDependencyManagerService.HCAT_DEFAULT_SERVER_NAME, "myhcatserver");
        setSystemProperty(PartitionDependencyManagerService.HCAT_DEFAULT_DB_NAME, "myhcatdb");
        setSystemProperty(PartitionDependencyManagerService.MAP_MAX_WEIGHTED_CAPACITY, "100");
        services = super.setupServicesForHCatalog();
        services.init();
    }

    @After
    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    @Test
    public void testUpdateCoordTableBasic() throws Exception {
        String newHCatDependency = "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12;region=us";

        String actionId = addInitRecords(newHCatDependency);
        checkCoordAction(actionId, newHCatDependency, CoordinatorAction.Status.WAITING, 0);

        PartitionDependencyManagerService pdms = Services.get().get(PartitionDependencyManagerService.class);

        JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        jmsService.getOrCreateConnection("hcat://hcat.server.com:5080");

        pdms.addMissingPartition(newHCatDependency, actionId);

        pdms.partitionAvailable(newHCatDependency);
        HCatURI hcatUri = new HCatURI(newHCatDependency);

        List<PartitionWrapper> availParts = pdms.getAvailablePartitions(actionId);
        assertNotNull(availParts);
        assertEquals(availParts.get(0), new PartitionWrapper(hcatUri));

        new CoordActionUpdatePushMissingDependency(actionId).call();

        checkCoordAction(actionId, "", CoordinatorAction.Status.READY, 0);

    }

    @Test
    public void testUpdateCoordTableAdvanced() throws Exception {
        String newHCatDependency1 = "hcat://hcat.server.com:5080/mydb/clicks/datastamp=11;region=us";
        String newHCatDependency2 = "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12;region=us";

        String fullDeps = newHCatDependency1 + CoordELFunctions.INSTANCE_SEPARATOR + newHCatDependency2;
        String actionId = addInitRecords(fullDeps);
        checkCoordAction(actionId, fullDeps, CoordinatorAction.Status.WAITING, 0);

        PartitionDependencyManagerService pdms = Services.get().get(PartitionDependencyManagerService.class);

        JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        jmsService.getOrCreateConnection("hcat://hcat.server.com:5080");

        pdms.addMissingPartition(newHCatDependency1, actionId);
        pdms.addMissingPartition(newHCatDependency2, actionId);

        pdms.partitionAvailable(newHCatDependency2);
        HCatURI hcatUri2 = new HCatURI(newHCatDependency2);

        List<PartitionWrapper> availParts = pdms.getAvailablePartitions(actionId);
        assertNotNull(availParts);
        assertEquals(availParts.get(0), new PartitionWrapper(hcatUri2));

        new CoordActionUpdatePushMissingDependency(actionId).call();

        checkCoordAction(actionId, newHCatDependency1, CoordinatorAction.Status.WAITING, 1);

        // second partition available

        pdms.partitionAvailable(newHCatDependency1);
        HCatURI hcatUri1 = new HCatURI(newHCatDependency1);

        availParts = pdms.getAvailablePartitions(actionId);
        assertNotNull(availParts);
        assertEquals(availParts.get(0), new PartitionWrapper(hcatUri1));

        new CoordActionUpdatePushMissingDependency(actionId).call();

        checkCoordAction(actionId, "", CoordinatorAction.Status.READY, 0);

    }

    private CoordinatorActionBean checkCoordAction(String actionId, String expDeps, CoordinatorAction.Status stat,
            int type) throws Exception {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            CoordinatorActionBean action = jpaService.execute(new CoordActionGetJPAExecutor(actionId));
            String missDeps = action.getPushMissingDependencies();
            if (type != 0) {
                assertEquals(new PartitionWrapper(missDeps), new PartitionWrapper(expDeps));
            }
            else {
                assertEquals(missDeps, expDeps);
            }
            assertEquals(action.getStatus(), stat);

            return action;
        }
        catch (JPAExecutorException se) {
            throw new Exception("Action ID " + actionId + " was not stored properly in db");
        }
    }

}
