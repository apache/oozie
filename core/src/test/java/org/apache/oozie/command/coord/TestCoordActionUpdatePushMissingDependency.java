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

import java.util.Collection;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.PartitionDependencyManagerService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.HCatURI;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestCoordActionUpdatePushMissingDependency extends XDataTestCase {
    private Services services;

    @Before
    protected void setUp() throws Exception {
        super.setUp();
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
        HCatURI hcatUri = new HCatURI(newHCatDependency);

        String actionId = addInitRecords(newHCatDependency);
        checkCoordAction(actionId, newHCatDependency, CoordinatorAction.Status.WAITING);

        PartitionDependencyManagerService pdms = Services.get().get(PartitionDependencyManagerService.class);
        pdms.addMissingDependency(hcatUri, actionId);
        pdms.partitionAvailable("hcat.server.com:5080", "mydb", "clicks",
                getPartitionMap("src=search;datastamp=12;region=us"));
        Collection<String> availableURIs = pdms.getAvailableDependencyURIs(actionId);
        assertEquals(availableURIs.size(), 1);
        assertTrue(availableURIs.contains(newHCatDependency));

        new CoordActionUpdatePushMissingDependency(actionId).call();

        checkCoordAction(actionId, "", CoordinatorAction.Status.READY);
        assertNull(pdms.getAvailableDependencyURIs(actionId));
    }

    @Test
    public void testUpdateCoordTableAdvanced() throws Exception {
        String newHCatDependency1 = "hcat://hcat.server.com:5080/mydb/clicks/datastamp=11;region=us";
        String newHCatDependency2 = "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12;region=us";
        HCatURI hcatUri1 = new HCatURI(newHCatDependency1);
        HCatURI hcatUri2 = new HCatURI(newHCatDependency2);

        String fullDeps = newHCatDependency1 + CoordELFunctions.INSTANCE_SEPARATOR + newHCatDependency2;
        String actionId = addInitRecords(fullDeps);
        checkCoordAction(actionId, fullDeps, CoordinatorAction.Status.WAITING);

        PartitionDependencyManagerService pdms = Services.get().get(PartitionDependencyManagerService.class);
        pdms.addMissingDependency(hcatUri1, actionId);
        pdms.addMissingDependency(hcatUri2, actionId);

        pdms.partitionAvailable("hcat.server.com:5080", "mydb", "clicks",
                getPartitionMap("src=search;datastamp=12;region=us"));
        Collection<String> availableURIs = pdms.getAvailableDependencyURIs(actionId);
        assertEquals(1, availableURIs.size());
        assertTrue(availableURIs.contains(newHCatDependency2));

        new CoordActionUpdatePushMissingDependency(actionId).call();

        checkCoordAction(actionId, newHCatDependency1, CoordinatorAction.Status.WAITING);

        // second partition available

        pdms.partitionAvailable("hcat.server.com:5080", "mydb", "clicks",
                getPartitionMap("src=search;datastamp=11;region=us"));
        availableURIs = pdms.getAvailableDependencyURIs(actionId);
        assertEquals(1, availableURIs.size());
        assertTrue(availableURIs.contains(newHCatDependency1));

        new CoordActionUpdatePushMissingDependency(actionId).call();

        checkCoordAction(actionId, "", CoordinatorAction.Status.READY);
        assertNull(pdms.getAvailableDependencyURIs(actionId));

    }

    private CoordinatorActionBean checkCoordAction(String actionId, String expDeps, CoordinatorAction.Status stat)
            throws Exception {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            CoordinatorActionBean action = jpaService.execute(new CoordActionGetJPAExecutor(actionId));
            String missDeps = action.getPushMissingDependencies();
            assertEquals(missDeps, expDeps);
            assertEquals(action.getStatus(), stat);

            return action;
        }
        catch (JPAExecutorException se) {
            throw new Exception("Action ID " + actionId + " was not stored properly in db");
        }
    }

}
