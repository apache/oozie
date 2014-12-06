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

package org.apache.oozie.executor.jpa;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestCoordJobsGetFromParentIdJPAExecutor extends XDataTestCase {
    Services services;
    private String[] excludedServices = { "org.apache.oozie.service.StatusTransitService",
            "org.apache.oozie.service.PauseTransitService", "org.apache.oozie.service.PurgeService",
            "org.apache.oozie.service.CoordMaterializeTriggerService", "org.apache.oozie.service.RecoveryService" };

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        setClassesToBeExcluded(services.getConf(), excludedServices);
        services.init();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testGetBundleParent() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        BundleJobBean bundleJobA = addRecordToBundleJobTable(Job.Status.SUCCEEDED, false);
        BundleJobBean bundleJobB = addRecordToBundleJobTable(Job.Status.SUCCEEDED, false);
        CoordinatorJobBean coordJobA1 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        CoordinatorJobBean coordJobA2 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        coordJobA2.setAppName("something_different");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJobA2);
        CoordinatorJobBean coordJobB = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        CoordinatorActionBean coordActionA1 = addRecordToCoordActionTable(coordJobA1.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);
        CoordinatorActionBean coordActionA2 = addRecordToCoordActionTable(coordJobA2.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);
        CoordinatorActionBean coordActionB = addRecordToCoordActionTable(coordJobB.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);
        BundleActionBean bundleActionA1 = addRecordToBundleActionTable(bundleJobA.getId(), coordJobA1.getId(),
                coordJobA1.getAppName(), 0, Job.Status.SUCCEEDED);
        BundleActionBean bundleActionA2 = addRecordToBundleActionTable(bundleJobA.getId(), coordJobA2.getId(),
                coordJobA2.getAppName(), 0, Job.Status.SUCCEEDED);
        BundleActionBean bundleActionB = addRecordToBundleActionTable(bundleJobB.getId(), coordJobB.getId(),
                coordJobB.getAppName(), 0, Job.Status.SUCCEEDED);

        List<String> children = new ArrayList<String>();
        children.addAll(jpaService.execute(new CoordJobsGetFromParentIdJPAExecutor(bundleJobA.getId(), 10)));
        checkChildren(children, coordJobA1.getId(), coordJobA2.getId());

        children = new ArrayList<String>();
        children.addAll(jpaService.execute(new CoordJobsGetFromParentIdJPAExecutor(bundleJobB.getId(), 10)));
        checkChildren(children, coordJobB.getId());
    }

    public void testGetBundleParentTooMany() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        BundleJobBean bundleJob = addRecordToBundleJobTable(Job.Status.SUCCEEDED, false);
        CoordinatorJobBean coordJob1 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        coordJob1.setAppName("coordJob1");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob1);
        CoordinatorJobBean coordJob2 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        coordJob2.setAppName("coordJob2");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob2);
        CoordinatorJobBean coordJob3 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        coordJob3.setAppName("coordJob3");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob3);
        CoordinatorJobBean coordJob4 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        coordJob4.setAppName("coordJob4");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob4);
        CoordinatorJobBean coordJob5 = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, false, false);
        coordJob5.setAppName("coordJob5");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coordJob5);
        CoordinatorActionBean coordAction1 = addRecordToCoordActionTable(coordJob1.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);
        CoordinatorActionBean coordAction2 = addRecordToCoordActionTable(coordJob2.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);
        CoordinatorActionBean coordAction3 = addRecordToCoordActionTable(coordJob3.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);
        CoordinatorActionBean coordAction4 = addRecordToCoordActionTable(coordJob4.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);
        CoordinatorActionBean coordAction5 = addRecordToCoordActionTable(coordJob5.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml", 0);
        BundleActionBean bundleAction1 = addRecordToBundleActionTable(bundleJob.getId(), coordJob1.getId(),
                coordJob1.getAppName(), 0, Job.Status.SUCCEEDED);
        BundleActionBean bundleAction2 = addRecordToBundleActionTable(bundleJob.getId(), coordJob2.getId(),
                coordJob2.getAppName(), 0, Job.Status.SUCCEEDED);
        BundleActionBean bundleAction3 = addRecordToBundleActionTable(bundleJob.getId(), coordJob3.getId(),
                coordJob3.getAppName(), 0, Job.Status.SUCCEEDED);
        BundleActionBean bundleAction4 = addRecordToBundleActionTable(bundleJob.getId(), coordJob4.getId(),
                coordJob4.getAppName(), 0, Job.Status.SUCCEEDED);
        BundleActionBean bundleAction5 = addRecordToBundleActionTable(bundleJob.getId(), coordJob5.getId(),
                coordJob5.getAppName(), 0, Job.Status.SUCCEEDED);

        List<String> children = new ArrayList<String>();
        // Get the first 3
        children.addAll(jpaService.execute(new CoordJobsGetFromParentIdJPAExecutor(bundleJob.getId(), 3)));
        assertEquals(3, children.size());
        // Get the next 3 (though there's only 2 more)
        children.addAll(jpaService.execute(new CoordJobsGetFromParentIdJPAExecutor(bundleJob.getId(), 3, 3)));
        assertEquals(5, children.size());
        checkChildren(children, coordJob1.getId(), coordJob2.getId(), coordJob3.getId(), coordJob4.getId(), coordJob5.getId());
    }

    private void checkChildren(List<String> children, String... coordJobIDs) {
        assertEquals(coordJobIDs.length, children.size());
        Arrays.sort(coordJobIDs);
        Collections.sort(children);

        for (int i = 0; i < coordJobIDs.length; i++) {
            assertEquals(coordJobIDs[i], children.get(i));
        }
    }
}
