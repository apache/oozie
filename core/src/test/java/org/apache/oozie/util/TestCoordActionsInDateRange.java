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

package org.apache.oozie.util;

import java.util.Date;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;

public class TestCoordActionsInDateRange extends XDataTestCase {

    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        cleanUpDBTables();
        LocalOozie.start();
    }

    @Override
    protected void tearDown() throws Exception {
        LocalOozie.stop();
        services.destroy();
        super.tearDown();
    }

    /**
     * This is unit test case for the 'getCoordActionsFromDates()' method. The method is supposed to retrieve the list of
     * coordinator actions running between a range of start and end date. The following test case tests its accuracy and fails
     * otherwise.
     */

    public void testCoordActionsInDateRange() {
        try {
            int actionNum = 1;
            CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
            CoordinatorActionBean actionId1 = addRecordToCoordActionTable(job.getId(), actionNum,
                    CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
            Date nominalTime = actionId1.getNominalTime();
            long nominalTimeMilliseconds = nominalTime.getTime();
            long noOfMillisecondsinOneHour = 3600000;

            String date1 = DateUtils.formatDateUTC(new Date(nominalTimeMilliseconds - (noOfMillisecondsinOneHour / 2)));
            String date2 = DateUtils.formatDateUTC(new Date(nominalTimeMilliseconds + noOfMillisecondsinOneHour));

            // Test a bad date format.
            try {
              String badDate = "bad" + date1;
              CoordActionsInDateRange.getCoordActionIdsFromDates(
                  job.getId().toString(),
                  badDate + "::" + date2);
              fail("Accepted badly formatted date: " + badDate);
            } catch (XException e) {
              // Pass
              assertEquals(ErrorCode.E0308, e.getErrorCode());
            }

            // Test a bad scope.
            try {
              String badScope = date1 + "0xbad5c09e" + date2;
              CoordActionsInDateRange.getCoordActionIdsFromDates(
                  job.getId().toString(),
                  badScope);
              fail("Accepted bad range scope: " + badScope);
            } catch (XException e) {
              // Pass
              assertEquals(ErrorCode.E0308, e.getErrorCode());
            }

            // Test inverted start and end dates.
            try {
              CoordActionsInDateRange.getCoordActionIdsFromDates(
                  job.getId().toString(),
                  date2 + "::" + date1);
              fail("Accepted inverted dates: [Start::End] = " + date2 + "::" + date1);
            } catch (XException e) {
              // Pass
              assertEquals(ErrorCode.E0308, e.getErrorCode());
            }

            // Testing for the number of coordinator actions in a date range that spans from half an hour prior to the nominal time to 1 hour after the nominal time
            int noOfActions = CoordActionsInDateRange.getCoordActionIdsFromDates(job.getId().toString(), date1 + "::" + date2).size();
            assertEquals(1, noOfActions);

            // Testing for the number of coordinator actions in a date range that spans from half an hour after the nominal time to 1 hour after the nominal time
            date1 = DateUtils.formatDateUTC(new Date(nominalTimeMilliseconds + (noOfMillisecondsinOneHour / 2)));
            noOfActions = CoordActionsInDateRange.getCoordActionIdsFromDates(job.getId().toString(), date1 + "::" + date2).size();
            assertEquals(0, noOfActions);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }
}
