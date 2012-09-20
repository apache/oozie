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


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.XmlUtils;

public class TestCoordActionGetForTimeoutJPAExecutor extends XDataTestCase {
    Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        cleanUpDBTables();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testCoordActionGet() throws Exception {
        int actionNum = 1;
        String resourceXmlName = "coord-action-get.xml";
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        CoordinatorActionBean action = createCoordAction(job.getId(), actionNum, CoordinatorAction.Status.WAITING,
                resourceXmlName, 1);
        // Insert the action
        insertRecordCoordAction(action);
        Path appPath = new Path(getFsTestCaseDir(), "coord");
        Configuration conf = getCoordConf(appPath);
        // Pass expected values
        _testGetForInputCheckX(action.getId(), job.getId(), CoordinatorAction.Status.WAITING, XmlUtils
                .prettyPrint(conf).toString(), 1);
    }

    private void _testGetForInputCheckX(String actionId, String jobId, CoordinatorAction.Status status, String runConf, int pending)
            throws Exception {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordActionGetForTimeoutJPAExecutor actionGetCmd = new CoordActionGetForTimeoutJPAExecutor(actionId);
            CoordinatorActionBean action = jpaService.execute(actionGetCmd);
            assertNotNull(action);

            // Check for expected column values
            assertEquals(actionId, action.getId());
            assertEquals(jobId, action.getJobId());
            assertEquals(status, action.getStatus());
            assertEquals(runConf, action.getRunConf());
            assertEquals(pending, action.getPending());
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Unable to GET a record for COORD Action By External ID. actionId =" + actionId);
        }
    }
}
