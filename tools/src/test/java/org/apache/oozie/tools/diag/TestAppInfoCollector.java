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

package org.apache.oozie.tools.diag;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.*;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.apache.oozie.tools.diag.TestServerInfoCollector.assertFileContains;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public class TestAppInfoCollector {
    private final Configuration conf= new Configuration(false);
    private File testFolder;
    private AppInfoCollector appInfoCollector;

    @Rule public final TemporaryFolder folder = new TemporaryFolder();
    @Mock private DiagOozieClient mockOozieClient;
    @Mock private WorkflowJob wfJob;
    @Mock private WorkflowAction wfAction;
    @Mock private CoordinatorJob coordJob;
    @Mock private CoordinatorAction coordinatorAction;
    @Mock private BundleJob bundleJob;

    @Before
    public void setup() throws IOException {
        conf.set("yarn.resourcemanager.address", "test:1234");
        testFolder = folder.newFolder();
        appInfoCollector = new AppInfoCollector(conf, mockOozieClient);
    }

    @Test
    public void testStoreLastWorkflows() throws Exception {
        final List<WorkflowJob> wfJobs = Arrays.asList(wfJob);
        doReturn(wfJobs).when(mockOozieClient).getJobsInfo(null, 0, 1);
        doReturn(wfJob).when(mockOozieClient).getJobInfo(anyString());

        final String wfName = "0000000-170926142250283-oozie-test-W";
        doReturn(wfName).when(wfJob).getId();
        doReturn("map-reduce-wf").when(wfJob)
                .getAppName();
        doReturn("hdfs://localhost:9000/user/test/examples/apps/map-reduce/workflow.xml").when(wfJob)
                .getAppPath();
        doReturn("test").when(wfJob).getUser();
        doReturn(null).when(wfJob).getAcl();
        doReturn(WorkflowJob.Status.SUCCEEDED).when(wfJob).getStatus();
        doReturn("").when(wfJob).getConsoleUrl();
        doReturn("http://0.0.0.0:11000/oozie?job=0000000-170926142250283-oozie-asas-W").when(wfJob)
                .getExternalId();

        doReturn(null).when(wfJob).getParentId();

        final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss zzz");
        final Date startDate = formatter.parse("2017-09-26 12:22:57 GMT");
        doReturn(startDate).when(wfJob).getCreatedTime();

        final Date endDate = formatter.parse("2017-09-26 12:32:57 GMT");
        doReturn(endDate).when(wfJob).getEndTime();
        doReturn(endDate).when(wfJob).getLastModifiedTime();
        doReturn(startDate).when(wfJob).getStartTime();
        doReturn(0).when(wfJob).getRun();

        doReturn("0000000-170926142250283-oozie-asas-W@:start:").when(wfAction).getId();
        doReturn(":start:").when(wfAction).getName();
        doReturn(":START:").when(wfAction).getType();
        doReturn(WorkflowAction.Status.OK).when(wfAction).getStatus();
        doReturn("mr-node").when(wfAction).getTransition();
        doReturn(startDate).when(wfAction).getStartTime();
        doReturn(endDate).when(wfAction).getEndTime();
        doReturn(null).when(wfAction).getErrorCode();
        doReturn(null).when(wfAction).getErrorMessage();
        doReturn("").when(wfAction).getConsoleUrl();
        doReturn("").when(wfAction).getTrackerUri();
        doReturn(null).when(wfAction).getExternalChildIDs();
        doReturn("").when(wfAction).getExternalId();
        doReturn("OK").when(wfAction).getExternalStatus();
        doReturn(null).when(wfAction).getData();
        doReturn(null).when(wfAction).getStats();
        doReturn(null).when(wfAction).getCred();
        doReturn(0).when(wfAction).getRetries();
        doReturn(10).when(wfAction).getUserRetryInterval();
        doReturn(0).when(wfAction).getUserRetryCount();
        doReturn(0).when(wfAction).getUserRetryMax();

        final List<WorkflowAction> wfActions = Arrays.asList(wfAction);
        doReturn(wfActions).when(wfJob).getActions();

        appInfoCollector.storeLastWorkflows(testFolder, 1, 1);

        final File infoOut = new File (testFolder,  wfName + Path.SEPARATOR+ "info.txt");
        assertTrue(infoOut.exists());
        assertFileContains(infoOut, wfName);
    }

    @Test
    public void testStoreCoordinators() throws Exception {
        final List<CoordinatorJob> coordJobs = Arrays.asList(coordJob);
        doReturn(coordJobs).when(mockOozieClient).getCoordJobsInfo(null, 0, 1);

        final String coordId = "0000000-170926142250283-oozie-test-C";
        doReturn(coordId).when(coordJob).getId();
        doReturn(Job.Status.RUNNING).when(coordJob).getStatus();
        doReturn(CoordinatorJob.Execution.FIFO).when(coordJob).getExecutionOrder();

        final List<CoordinatorAction> coordinatorActions = Arrays.asList(coordinatorAction);
        doReturn(CoordinatorAction.Status.KILLED).when(coordinatorAction).getStatus();
        doReturn(coordinatorActions).when(coordJob).getActions();
        doReturn(coordJob).when(mockOozieClient).getCoordJobInfo(anyString());
        doReturn(CoordinatorJob.Timeunit.MINUTE).when(coordJob).getTimeUnit();

        appInfoCollector.storeLastCoordinators(testFolder,1,1);

        final File coordInfoOut = new File(testFolder, coordId + Path.SEPARATOR + "info.txt");
        assertTrue(coordInfoOut.exists());
        assertFileContains(coordInfoOut, coordId);
    }

    @Test
    public void testStoreLastBundles() throws Exception {
        final List<BundleJob> bundleJobs = Arrays.asList(bundleJob);
        doReturn(bundleJobs).when(mockOozieClient).getBundleJobsInfo(null, 0, 1);

        final String bundleId = "0000027-110322105610515-oozie-chao-B";
        doReturn(bundleId).when(bundleJob).getId();
        doReturn(Job.Status.RUNNING).when(bundleJob).getStatus();
        doReturn(bundleJob).when(mockOozieClient).getBundleJobInfo(anyString());

        appInfoCollector.storeLastBundles(testFolder,1,1);

        final File bundleInfoOut = new File(testFolder, bundleId + Path.SEPARATOR + "info.txt");
        assertTrue(bundleInfoOut.exists());
        assertFileContains(bundleInfoOut, bundleId);
    }
}