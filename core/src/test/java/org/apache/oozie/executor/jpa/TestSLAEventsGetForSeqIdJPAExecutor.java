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

import java.util.Date;
import java.util.List;

import org.apache.oozie.SLAEventBean;
import org.apache.oozie.client.SLAEvent.Status;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestSLAEventsGetForSeqIdJPAExecutor extends XDataTestCase {
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

    public void testSLAEventsGetForSeqId() throws Exception {
        final String wfId = "0000000-" + new Date().getTime() + "-TestSLAEventsGetForSeqIdJPAExecutor-W";
        addRecordToSLAEventTable(wfId, Status.CREATED);
        addRecordToSLAEventTable(wfId, Status.STARTED);
        addRecordToSLAEventTable(wfId, Status.SUCCEEDED);
        _testGetSLAEventsForSeqId(wfId);
    }

    private void _testGetSLAEventsForSeqId(String jobId) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        SLAEventsGetJPAExecutor slaEventsGetAllCmd = new SLAEventsGetJPAExecutor();
        List<SLAEventBean> fullList = jpaService.execute(slaEventsGetAllCmd);
        assertNotNull(fullList);
        long lastId = fullList.get(0).getEvent_id();

        SLAEventsGetForSeqIdJPAExecutor slaEventsGetCmd = new SLAEventsGetForSeqIdJPAExecutor(lastId, 10, new long[1]);
        List<SLAEventBean> list = jpaService.execute(slaEventsGetCmd);
        assertNotNull(list);
        assertEquals(2, list.size());
    }

}
