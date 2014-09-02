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

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.oozie.SLAEventBean;
import org.apache.oozie.client.SLAEvent.Status;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestSLAEventsGetForFilterJPAExecutor extends XDataTestCase {

    Services services;
    String coordId1;
    String coordActionId1;
    String appName1;
    String coordId2;
    String coordActionId2;
    String appName2;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        populateDB();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    private void populateDB() throws Exception {
        Date current = new Date();
        long currentTime = current.getTime();
        coordId1 = "0000001-" + currentTime + "-TestSLAEventsGetForFilterJPAExecutor-C";
        coordActionId1 = "0000001-" + currentTime + "-TestSLAEventsGetForFilterJPAExecutor-C@1";
        appName1 = "app-1";
        coordId2 = "0000002-" + currentTime + "-TestSLAEventsGetForFilterJPAExecutor-C";
        coordActionId2 = "0000002-" + currentTime + "-TestSLAEventsGetForFilterJPAExecutor-C@1";
        appName2 = "app-2";

        addRecordToSLAEventTable(coordId1, appName1, Status.CREATED, current);
        addRecordToSLAEventTable(coordActionId1, appName1, Status.CREATED, current);
        addRecordToSLAEventTable(coordActionId1, appName1, Status.STARTED, current);
        addRecordToSLAEventTable(coordActionId1, appName1, Status.SUCCEEDED, current);

        addRecordToSLAEventTable(coordId2, appName2, Status.CREATED, current);
        addRecordToSLAEventTable(coordActionId2, appName2, Status.CREATED, current);
    }

    private Map<String, List<String>> createFilterList(String name, String... vals) {
        Map<String, List<String>> filterList = new HashMap<String, List<String>>();
        List<String> valList = Arrays.asList(vals);
        filterList.put(name, valList);
        return filterList;

    }

    public void testGetSLAEventsForCoordJobId() throws Exception {

        Map<String, List<String>> filterListJob1 = createFilterList("jobid", coordId1);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        SLAEventsGetForFilterJPAExecutor slaEventsGetCmd = new SLAEventsGetForFilterJPAExecutor(-1, 100,
                filterListJob1, new long[1]);
        List<SLAEventBean> list = jpaService.execute(slaEventsGetCmd);
        assertNotNull(list);
        assertEquals(1, list.size());
    }

    public void testGetSLAEventsForCoordActionId() throws Exception {
        Map<String, List<String>> filterListAction1 = createFilterList("jobid", coordActionId1);
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        SLAEventsGetForFilterJPAExecutor slaEventsGetCmd = new SLAEventsGetForFilterJPAExecutor(0, 100,
                filterListAction1, new long[1]);
        List<SLAEventBean> list = jpaService.execute(slaEventsGetCmd);
        assertNotNull(list);
        assertEquals(3, list.size());
    }

    public void testGetSLAEventsForAppName() throws Exception {
        Map<String, List<String>> filterListApp2 = createFilterList("appname", appName2);
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        SLAEventsGetForFilterJPAExecutor slaEventsGetCmd = new SLAEventsGetForFilterJPAExecutor(0, 100, filterListApp2,
                new long[1]);
        List<SLAEventBean> list = jpaService.execute(slaEventsGetCmd);
        assertNotNull(list);
        assertEquals(2, list.size());
    }

    public void testGetSLAEventsForOR() throws Exception {
        Map<String, List<String>> filterList = createFilterList("jobid", coordId1, coordActionId1);
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        SLAEventsGetForFilterJPAExecutor slaEventsGetCmd = new SLAEventsGetForFilterJPAExecutor(0, 100, filterList,
                new long[1]);
        List<SLAEventBean> list = jpaService.execute(slaEventsGetCmd);
        assertNotNull(list);
        assertEquals(4, list.size());
    }

    public void testGetSLAEventsForCombined() throws Exception {
        Map<String, List<String>> filterList = createFilterList("jobid", coordId1, coordActionId1);
        filterList.put("appname", Arrays.asList(new String[] { appName1 }));
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        long[] lastSeqId = new long[1];
        lastSeqId[0] = -1;
        SLAEventsGetForFilterJPAExecutor slaEventsGetCmd = new SLAEventsGetForFilterJPAExecutor(0, 100, filterList,
                lastSeqId);
        List<SLAEventBean> list = jpaService.execute(slaEventsGetCmd);
        assertNotNull(list);
        assertEquals(4, list.size());
    }

    public void testGetSLAEventsWithRange() throws Exception {
        Map<String, List<String>> filterList = new HashMap();
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        long[] lastSeqId = new long[1];
        lastSeqId[0] = -1;
        SLAEventsGetForFilterJPAExecutor slaEventsGetCmd = new SLAEventsGetForFilterJPAExecutor(1, 3, filterList,
                lastSeqId);
        List<SLAEventBean> list = jpaService.execute(slaEventsGetCmd);
        assertNotNull(list);
        assertEquals(3, list.size());
    }

    public void testGetSLAEventsForCombinedWithRange() throws Exception {
        Map<String, List<String>> filterList = createFilterList("jobid", coordId1, coordActionId1, coordId2,
                coordActionId2);
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        long[] lastSeqId = new long[1];
        lastSeqId[0] = -1;
        SLAEventsGetForFilterJPAExecutor slaEventsGetCmd = new SLAEventsGetForFilterJPAExecutor(1, 3, filterList,
                lastSeqId);
        List<SLAEventBean> list = jpaService.execute(slaEventsGetCmd);
        assertNotNull(list);
        assertEquals(3, list.size());
    }
}
