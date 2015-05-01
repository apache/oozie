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

import java.io.File;
import java.util.Date;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.service.Services;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;

public class TestCoordELExtensions extends XDataTestCase {
    private Services services;
    private String[] excludedServices = { "org.apache.oozie.service.StatusTransitService",
            "org.apache.oozie.service.PauseTransitService",
            "org.apache.oozie.service.RecoveryService" };

    @Override
    protected void setUp() throws Exception {
        setSystemProperty("oozie.test.config.file", new File(OOZIE_SRC_DIR,
                "core/src/test/resources/oozie-site-coordel.xml").getAbsolutePath());
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

    public void testCoordELActionMater() throws Exception {
        Date startTime = DateUtils.parseDateUTC("2009-03-06T010:00Z");
        Date endTime = DateUtils.parseDateUTC("2009-03-07T12:00Z");
        CoordinatorJobBean job = createCoordJob("coord-job-for-elext.xml",
                CoordinatorJob.Status.RUNNING, startTime, endTime, false, false, 0);
        addRecordToCoordJobTable(job);

        new CoordMaterializeTransitionXCommand(job.getId(), 3600).call();
        checkCoordAction(job.getId() + "@1");
    }

    protected CoordinatorActionBean checkCoordAction(String actionId) throws StoreException {
        try {
            CoordinatorActionBean action = getCoordinatorAction(actionId);
            assertEquals(
                    "file://#testDir/2009/03/06/00/_SUCCESS#file://#testDir/2009/03/05/23/_SUCCESS",
                    action.getMissingDependencies());
            return action;
        } catch (Exception se) {
            se.printStackTrace();
            fail("Action ID " + actionId + " was not stored properly in db");
        }
        return null;
    }
}
