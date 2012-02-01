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

import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.client.Job;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;

public class TestBundleActionsDeleteForPurgeJPAExecutor extends XDataTestCase {
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

    public void testBundleActionsDeleteForPurgeJPAExecutor() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateUTC("2011-01-01T01:00Z"));
        this.addRecordToBundleActionTable(job.getId(), "action1", 0, Job.Status.SUCCEEDED);
        this.addRecordToBundleActionTable(job.getId(), "action2", 0, Job.Status.SUCCEEDED);
        _testBundleActionsDelete(job.getId());
    }

    private void _testBundleActionsDelete(String jobId) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        BundleActionsGetJPAExecutor bundleActionsGetCmd = new BundleActionsGetJPAExecutor(jobId);
        List<BundleActionBean> ret = jpaService.execute(bundleActionsGetCmd);
        assertEquals(2, ret.size());

        BundleActionsDeleteForPurgeJPAExecutor bundleActionsDelCmd = new BundleActionsDeleteForPurgeJPAExecutor(jobId);
        jpaService.execute(bundleActionsDelCmd);

        ret = jpaService.execute(bundleActionsGetCmd);
        assertEquals(0, ret.size());
    }

    protected BundleJobBean addRecordToBundleJobTable(Job.Status jobStatus, Date lastModifiedTime) throws Exception {
        BundleJobBean bundle = createBundleJob(jobStatus, false);
        bundle.setLastModifiedTime(lastModifiedTime);
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            BundleJobInsertJPAExecutor bundleInsertjpa = new BundleJobInsertJPAExecutor(bundle);
            jpaService.execute(bundleInsertjpa);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test bundle job record to table");
            throw je;
        }
        return bundle;
    }

}
