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

public class TestSLAEventsGetJPAExecutor extends XDataTestCase {
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
        Date current = new Date();
        final String wfId = "0000000-" + current.getTime() + "-TestSLAEventsGetJPAExecutor-W";
        addRecordToSLAEventTable(wfId, Status.CREATED, current);
        addRecordToSLAEventTable(wfId, Status.STARTED, current);
        addRecordToSLAEventTable(wfId, Status.SUCCEEDED, current);
        _testGetSLAEventsForSeqId(wfId);
    }

    private void _testGetSLAEventsForSeqId(String jobId) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        SLAEventsGetJPAExecutor slaEventsGetAllCmd = new SLAEventsGetJPAExecutor();
        List<SLAEventBean> list = jpaService.execute(slaEventsGetAllCmd);
        assertNotNull(list);
        assertEquals(3, list.size());
    }

}
