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

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.client.Job;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestBundleActionsCountForJobGetJPAExecutor extends XDataTestCase {
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

    public void testBundleActionsForJobCountGet() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.RUNNING, false);
        this.addRecordToBundleActionTable(job.getId(), "action1", 1, Job.Status.RUNNING);

        _testGetForJobCount(job.getId(), 1);

        this.addRecordToBundleActionTable(job.getId(), "action2", 0, Job.Status.RUNNING);
        this.addRecordToBundleActionTable(job.getId(), "action3", 1, Job.Status.RUNNING);

        _testGetForJobCount(job.getId(), 3);
    }

    private void _testGetForJobCount(String jobId, int expected) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleActionsCountForJobGetJPAExecutor actionsForJobCmd = new BundleActionsCountForJobGetJPAExecutor(
                jobId);
        int cnt = jpaService.execute(actionsForJobCmd);
        assertEquals(cnt, expected);
    }

}
