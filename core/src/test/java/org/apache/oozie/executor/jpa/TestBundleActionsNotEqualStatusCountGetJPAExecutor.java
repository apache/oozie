/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.executor.jpa;

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.client.Job;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestBundleActionsNotEqualStatusCountGetJPAExecutor extends XDataTestCase {
    Services services;

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

    public void testBundleActionsNotEqualStatusCountGet() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.RUNNING);
        this.addRecordToBundleActionTable(job.getId(), "action1", 0, Job.Status.SUCCEEDED);

        _testNotEqualFalseStatusCount(job.getId(), 0);

        this.addRecordToBundleActionTable(job.getId(), "action2", 1, Job.Status.RUNNING);
        this.addRecordToBundleActionTable(job.getId(), "action3", 0, Job.Status.SUCCEEDED);

        _testNotEqualFalseStatusCount(job.getId(), 1);
    }

    private void _testNotEqualFalseStatusCount(String jobId, int expected) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleActionsNotEqualStatusCountGetJPAExecutor actionNotSucceededCmd = new BundleActionsNotEqualStatusCountGetJPAExecutor(
                jobId, Job.Status.SUCCEEDED.toString());
        int cnt = jpaService.execute(actionNotSucceededCmd);
        assertEquals(cnt, expected);
    }

}