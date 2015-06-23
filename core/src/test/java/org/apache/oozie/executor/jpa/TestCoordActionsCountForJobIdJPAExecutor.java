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

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorEngine;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.Pair;
import org.junit.Assert;

import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.HashMap;


public class TestCoordActionsCountForJobIdJPAExecutor extends XDataTestCase {
    private static final Pair<String, CoordinatorEngine.FILTER_COMPARATORS> POSITIVE_STATUS_FILTER =
            Pair.of(OozieClient.FILTER_STATUS, CoordinatorEngine.FILTER_COMPARATORS.EQUALS);
    private static final Pair<String, CoordinatorEngine.FILTER_COMPARATORS> NEGATIVE_STATUS_FILTER =
            Pair.of(OozieClient.FILTER_STATUS, CoordinatorEngine.FILTER_COMPARATORS.NOT_EQUALS);
    Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    private Timestamp getSqlTime(String str) throws ParseException {
        return new Timestamp(DateUtils.parseDateUTC(str).getTime());
    }

    private List<Object> getList(Object... objs) {
        List<Object> list = new ArrayList<Object>();
        for (Object o : objs) {
            list.add(o);
        }
        return list;
    }

    public void testGetActionsCount() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        String timeStr[] = {"2009-02-01T00:00Z", "2009-02-01T05:00Z", "2009-02-01T10:00Z", "2009-02-01T15:00Z"};
        Date ntime[] = {getSqlTime(timeStr[0]), getSqlTime(timeStr[1]), getSqlTime(timeStr[2]), getSqlTime(timeStr[3])};

        List<String> actionIds = new ArrayList<String>(timeStr.length);
        int startAction = 5;
        for (Date time : ntime) {
            CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), startAction++,
                    CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0, time);
            actionIds.add(action.getId());
        }
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);


        // Without Filters
        Map<Pair<String, CoordinatorEngine.FILTER_COMPARATORS>, List<Object>> filterMap =
                new HashMap<Pair<String, CoordinatorEngine.FILTER_COMPARATORS>, List<Object>>();
        CoordActionsCountForJobIdJPAExecutor actionGetCountCmd = new CoordActionsCountForJobIdJPAExecutor(
                job.getId(), filterMap);
        int actionCount = jpaService.execute(actionGetCountCmd);
        Assert.assertEquals(4, actionCount);

        // With Filter
        filterMap.put(Pair.of(OozieClient.FILTER_NOMINAL_TIME, CoordinatorEngine.FILTER_COMPARATORS.GREATER_EQUAL),
                getList(ntime[2]));
        actionGetCountCmd = new CoordActionsCountForJobIdJPAExecutor(job.getId(), filterMap);
        actionCount = jpaService.execute(actionGetCountCmd);
        Assert.assertEquals(2, actionCount);
    }
}
