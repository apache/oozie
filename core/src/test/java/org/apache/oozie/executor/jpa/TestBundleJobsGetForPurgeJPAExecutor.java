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

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.client.Job;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;

public class TestBundleJobsGetForPurgeJPAExecutor extends XDataTestCase {
    Services services;

    /* (non-Javadoc)
     * @see org.apache.oozie.test.XFsTestCase#setUp()
     */
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        cleanUpDBTables();
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.test.XFsTestCase#tearDown()
     */
    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testBundleJobsGetForPurgeJPAExecutor() throws Exception {
        this.addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-01-01T01:00Z"));

        _testBundleJobsForPurge(10, 1);

        this.addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-01-02T01:00Z"));
        _testBundleJobsForPurge(10, 2);
    }

    private void _testBundleJobsForPurge(int olderThan, int expected) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        BundleJobsGetForPurgeJPAExecutor executor = new BundleJobsGetForPurgeJPAExecutor(olderThan, 50);
        List<BundleJobBean> jobList = jpaService.execute(executor);
        assertEquals(expected, jobList.size());
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
        catch (JPAExecutorException ce) {
            ce.printStackTrace();
            fail("Unable to insert the test bundle job record to table");
            throw ce;
        }
        return bundle;
    }

}
