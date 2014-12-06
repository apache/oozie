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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.test.XFsTestCase#tearDown()
     */
    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testBundleJobsGetForPurgeJPAExecutorTooMany() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        BundleJobBean job1 = this.addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-01-01T01:00Z"));
        BundleJobBean job2 = this.addRecordToBundleJobTable(Job.Status.FAILED, DateUtils.parseDateOozieTZ("2011-01-01T01:00Z"));
        BundleJobBean job3 = this.addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-01-01T01:00Z"));
        BundleJobBean job4 = this.addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-01-01T01:00Z"));
        BundleJobBean job5 = this.addRecordToBundleJobTable(Job.Status.SUCCEEDED, DateUtils.parseDateOozieTZ("2011-01-01T01:00Z"));

        List<String> list = new ArrayList<String>();
        // Get the first 3
        list.addAll(jpaService.execute(new BundleJobsGetForPurgeJPAExecutor(1, 3)));
        assertEquals(3, list.size());
        // Get the next 3 (though there's only 2 more)
        list.addAll(jpaService.execute(new BundleJobsGetForPurgeJPAExecutor(1, 3, 3)));
        assertEquals(5, list.size());
        checkBundles(list, job1.getId(), job2.getId(), job3.getId(), job4.getId(), job5.getId());
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

    private void checkBundles(List<String> bundles, String... bundleJobIDs) {
        assertEquals(bundleJobIDs.length, bundles.size());
        Arrays.sort(bundleJobIDs);
        Collections.sort(bundles);

        for (int i = 0; i < bundleJobIDs.length; i++) {
            assertEquals(bundleJobIDs[i], bundles.get(i));
        }
    }
}
