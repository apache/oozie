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
package org.apache.oozie.command.bundle;

import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.command.OperationType;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.BundleActionQueryExecutor;
import org.apache.oozie.executor.jpa.BundleActionQueryExecutor.BundleActionQuery;
import org.apache.oozie.executor.jpa.BundleJobQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestBulkBundleXCommand extends XDataTestCase {
    private Services services;

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

    public void testBulkBundleSuspendResumeKillSuccess() throws Exception {
        BundleJobBean job1 = this.addRecordToBundleJobTable(Job.Status.RUNNING, false);
        BundleJobBean job2 = this.addRecordToBundleJobTable(Job.Status.RUNNING, false);

        Map<String, List<String>> map = new HashMap<String, List<String>>();
        List<String> names = new ArrayList<String>();
        names.add("BUNDLE-TEST");
        map.put("name", names);

        new BulkBundleXCommand(map, 1, 50, OperationType.Suspend).call();
        verifyJobStatus(job1.getId(), BundleJob.Status.SUSPENDED);
        verifyChildrenStatus(job1.getId(), CoordinatorJob.Status.SUSPENDED);

        verifyJobStatus(job2.getId(), BundleJob.Status.SUSPENDED);
        verifyChildrenStatus(job2.getId(), CoordinatorJob.Status.SUSPENDED);

        new BulkBundleXCommand(map, 1, 50, OperationType.Resume).call();
        verifyJobStatus(job1.getId(), BundleJob.Status.RUNNING);
        verifyChildrenStatus(job1.getId(), CoordinatorJob.Status.RUNNING);

        verifyJobStatus(job2.getId(), BundleJob.Status.RUNNING);
        verifyChildrenStatus(job2.getId(), CoordinatorJob.Status.RUNNING);

        new BulkBundleXCommand(map, 1, 50, OperationType.Kill).call();
        verifyJobStatus(job1.getId(), BundleJob.Status.KILLED);
        verifyChildrenStatus(job1.getId(), CoordinatorJob.Status.KILLED);

        verifyJobStatus(job2.getId(), BundleJob.Status.KILLED);
        verifyChildrenStatus(job2.getId(), CoordinatorJob.Status.KILLED);
    }

    public void testBulkBundleKillNoOp() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.KILLED, false);

        Map<String, List<String>> map = new HashMap<String, List<String>>();
        List<String> names = new ArrayList<String>();
        names.add("BUNDLE-TEST");
        map.put("name", names);

        new BulkBundleXCommand(map, 1, 50, OperationType.Kill).call();

        verifyJobStatus(job.getId(), BundleJob.Status.KILLED);
        verifyChildrenStatus(job.getId(), CoordinatorJob.Status.KILLED);
    }

    public void testBulkBundleKillNegative() throws Exception {
        BundleJobBean job1 = this.addRecordToBundleJobTable(Job.Status.RUNNING, false);
        BundleJobBean job2 = this.addRecordToBundleJobTable(Job.Status.RUNNING, false);

        Map<String, List<String>> map = new HashMap<String, List<String>>();
        List<String> names = new ArrayList<String>();
        names.add("BUNDLE");
        map.put("name", names);

        new BulkBundleXCommand(map, 1, 50, OperationType.Kill).call();

        verifyJobStatus(job1.getId(), BundleJob.Status.RUNNING);
        verifyChildrenStatus(job1.getId(), CoordinatorJob.Status.RUNNING);

        verifyJobStatus(job2.getId(), BundleJob.Status.RUNNING);
        verifyChildrenStatus(job2.getId(), CoordinatorJob.Status.RUNNING);

    }

    public void testBulkBundleSuspendNoOp() throws Exception {
        BundleJobBean job1 = this.addRecordToBundleJobTable(Job.Status.RUNNING, false);
        BundleJobBean job2 = this.addRecordToBundleJobTable(Job.Status.KILLED, false);

        Map<String, List<String>> map = new HashMap<String, List<String>>();
        List<String> names = new ArrayList<String>();
        names.add("BUNDLE-TEST");
        map.put("name", names);

        new BulkBundleXCommand(map, 1, 10, OperationType.Suspend).call();

        verifyJobStatus(job1.getId(), BundleJob.Status.SUSPENDED);
        verifyChildrenStatus(job1.getId(), CoordinatorJob.Status.SUSPENDED);

        verifyJobStatus(job2.getId(), BundleJob.Status.KILLED);
        verifyChildrenStatus(job2.getId(), CoordinatorJob.Status.KILLED);
    }

    public void testBulkBundleSuspendNegative() throws Exception {
        BundleJobBean job1 = this.addRecordToBundleJobTable(Job.Status.RUNNING, false);
        BundleJobBean job2 = this.addRecordToBundleJobTable(Job.Status.KILLED, false);

        Map<String, List<String>> map = new HashMap<String, List<String>>();
        List<String> names = new ArrayList<String>();
        names.add("BUNDLE");
        map.put("name", names);

        new BulkBundleXCommand(map, 1, 10, OperationType.Suspend).call();

        verifyJobStatus(job1.getId(), BundleJob.Status.RUNNING);
        verifyChildrenStatus(job1.getId(), CoordinatorJob.Status.RUNNING);

        verifyJobStatus(job2.getId(), BundleJob.Status.KILLED);
        verifyChildrenStatus(job2.getId(), CoordinatorJob.Status.KILLED);
    }

    public void testBulkBundleResumeNegative() throws Exception {
        BundleJobBean job1 = this.addRecordToBundleJobTable(Job.Status.RUNNING, false);
        BundleJobBean job2 = this.addRecordToBundleJobTable(Job.Status.SUSPENDED, false);

        Map<String, List<String>> map = new HashMap<String, List<String>>();
        List<String> names = new ArrayList<String>();
        names.add("BUNDLE");
        map.put("name", names);

        new BulkBundleXCommand(map, 1, 10, OperationType.Resume).call();

        verifyJobStatus(job1.getId(), BundleJob.Status.RUNNING);
        verifyChildrenStatus(job1.getId(), CoordinatorJob.Status.RUNNING);

        verifyJobStatus(job2.getId(), BundleJob.Status.SUSPENDED);
        verifyChildrenStatus(job2.getId(), CoordinatorJob.Status.SUSPENDED);
    }

    public void testBulkBundleResumeNoOp() throws Exception {
        BundleJobBean job1 = this.addRecordToBundleJobTable(Job.Status.RUNNING, false);
        BundleJobBean job2 = this.addRecordToBundleJobTable(Job.Status.SUSPENDED, false);

        Map<String, List<String>> map = new HashMap<String, List<String>>();
        List<String> names = new ArrayList<String>();
        names.add("BUNDLE-TEST");
        map.put("name", names);

        new BulkBundleXCommand(map, 1, 10, OperationType.Resume).call();

        verifyJobStatus(job1.getId(), BundleJob.Status.RUNNING);
        verifyChildrenStatus(job1.getId(), CoordinatorJob.Status.RUNNING);

        verifyJobStatus(job2.getId(), BundleJob.Status.RUNNING);
        verifyChildrenStatus(job2.getId(), CoordinatorJob.Status.RUNNING);
    }

    private void verifyJobStatus(String jobId, BundleJob.Status status) throws Exception {
        BundleJobBean job = BundleJobQueryExecutor.getInstance().get(
                BundleJobQueryExecutor.BundleJobQuery.GET_BUNDLE_JOB, jobId);
        assertEquals(status, job.getStatus());
    }

    private void verifyChildrenStatus(String jobId, CoordinatorJob.Status status) throws Exception {
        List<BundleActionBean> actions = BundleActionQueryExecutor.getInstance().getList(
                BundleActionQuery.GET_BUNDLE_ACTIONS_STATUS_UNIGNORED_FOR_BUNDLE, jobId);

        for (BundleActionBean action : actions) {
            String coordId = action.getCoordId();
            CoordinatorJobBean coordJob = CoordJobQueryExecutor.getInstance().get(
                    CoordJobQueryExecutor.CoordJobQuery.GET_COORD_JOB, coordId);
            assertEquals(status, coordJob.getStatus());
        }
    }
}
