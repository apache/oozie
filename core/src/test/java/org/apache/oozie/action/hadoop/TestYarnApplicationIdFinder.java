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

package org.apache.oozie.action.hadoop;

import com.google.common.collect.Lists;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.service.HadoopAccessorException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class TestYarnApplicationIdFinder {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private MapReduceActionExecutor.HadoopJobIdFinder hadoopJobIdFinder;

    @Mock
    private MapReduceActionExecutor.YarnApplicationReportReader reader;

    @Mock
    private WorkflowActionBean workflowActionBean;

    @Mock
    private ApplicationReport applicationReport;

    @Mock
    private ApplicationId applicationId;

    private MapReduceActionExecutor.YarnApplicationIdFinder yarnApplicationIdFinder;

    @Before
    public void setUp() throws Exception {
        yarnApplicationIdFinder = new MapReduceActionExecutor.YarnApplicationIdFinder(hadoopJobIdFinder,
                reader, workflowActionBean);
    }

    @Test
    public void whenHadoopJobIdAndChildYarnApplicationAreNotPresentActionExternalIdIsFound() throws Exception {
        when(hadoopJobIdFinder.find()).thenReturn(null);
        when(reader.read()).thenReturn(Collections.emptyList());
        when(workflowActionBean.getExternalId()).thenReturn("application_1534164756526_0000");

        assertEquals("no Hadoop Job ID nor YARN applications: WorkflowActionBean.externalId should be found",
                "application_1534164756526_0000",
                yarnApplicationIdFinder.find());

        when(applicationReport.getApplicationType()).thenReturn("Oozie Launcher");
        when(applicationReport.getApplicationId()).thenReturn(applicationId);
        when(applicationId.toString()).thenReturn("application_1534164756526_0001");
        when(reader.read()).thenReturn(Lists.newArrayList(applicationReport));

        assertEquals(
                "no Hadoop Job ID nor YARN applications of MAPREDUCE type: WorkflowActionBean.externalId should be found",
                "application_1534164756526_0000",
                yarnApplicationIdFinder.find());

        when(applicationReport.getApplicationType()).thenReturn("MAPREDUCE");
        when(workflowActionBean.getWfId()).thenReturn("workflowId");

        assertEquals(
                "no Hadoop Job ID nor YARN applications of the same workflow: WorkflowActionBean.externalId should be found",
                "application_1534164756526_0000",
                yarnApplicationIdFinder.find());
    }

    @Test
    public void whenHadoopJobIdIsNotCorrectExceptionIsThrown() throws Exception {
        when(hadoopJobIdFinder.find()).thenReturn("notAHadoopJobId");
        expectedException.expect(IllegalArgumentException.class);

        yarnApplicationIdFinder.find();
    }

    @Test
    public void whenHadoopJobIdIsNotPresentChildYarnApplicationIdIsFound() throws Exception {
        when(hadoopJobIdFinder.find()).thenReturn(null);
        when(applicationReport.getApplicationType()).thenReturn("MAPREDUCE");
        when(workflowActionBean.getWfId()).thenReturn("workflowId");
        when(applicationReport.getYarnApplicationState()).thenReturn(YarnApplicationState.RUNNING);
        when(applicationId.toString()).thenReturn("application_1534164756526_0000");
        when(applicationReport.getApplicationId()).thenReturn(applicationId);
        when(reader.read()).thenReturn(Lists.newArrayList(applicationReport));

        assertEquals("no Hadoop Job ID, but an appropriate YARN application: applicationId should be found",
                "application_1534164756526_0000",
                yarnApplicationIdFinder.find());
    }

    @Test
    public void whenHadoopJobIsNotPresentAsYarnApplicationHadoopJobIdIsUsed() throws Exception {
        setupMocks("job_1534164756526_0002", "application_1534164756526_0000", "application_1534164756526_0001");

        assertEquals("Hadoop Job ID should be found when it is not present as a YARN application ID",
                "application_1534164756526_0002",
                yarnApplicationIdFinder.find());
    }

    private void setupMocks(final String mrJobId, final String wfExternalId, final String yarnApplicationId)
            throws HadoopAccessorException, IOException, URISyntaxException, InterruptedException, YarnException {
        when(hadoopJobIdFinder.find()).thenReturn(mrJobId);
        when(applicationReport.getApplicationType()).thenReturn("MAPREDUCE");
        when(workflowActionBean.getWfId()).thenReturn("workflowId");
        when(workflowActionBean.getExternalId()).thenReturn(wfExternalId);
        when(applicationReport.getYarnApplicationState()).thenReturn(YarnApplicationState.RUNNING);
        when(applicationId.toString()).thenReturn(yarnApplicationId);
        when(applicationReport.getApplicationId()).thenReturn(applicationId);
        when(reader.read()).thenReturn(Lists.newArrayList(applicationReport));
    }

    @Test
    public void whenHadoopJobIsPresentAsYarnApplicationAndDifferentFromItsUsed() throws Exception {
        setupMocks("job_1534164756526_0002", "application_1534164756526_0001", "application_1534164756526_0003");

        assertEquals("Hadoop Job ID should be found when different from the YARN application ID",
                "application_1534164756526_0002",
                yarnApplicationIdFinder.find());
    }

    @Test
    public void whenHadoopJobIsPresentAsYarnApplicationAndContainWorkflowIdNotUsed() throws Exception {
        setupMocks("job_1534164756526_0002", "application_1534164756526_0002", "application_1534164756526_0003");

        assertEquals("YARN application ID should be found when greater than WorkflowActionBean.externalId",
                "application_1534164756526_0003",
                yarnApplicationIdFinder.find());
    }

    @Test
    public void whenOldLauncherAndMRobApplicationsAreFinishedAndNewLauncherPresentNewLauncherIsUsed() throws Exception {
        final ApplicationReport oldLauncher = mock(ApplicationReport.class);
        when(oldLauncher.getApplicationType()).thenReturn("Oozie Launcher");
        when(oldLauncher.getYarnApplicationState()).thenReturn(YarnApplicationState.FINISHED);
        final ApplicationId oldLauncherId = mock(ApplicationId.class);
        when(oldLauncherId.toString()).thenReturn("application_1534164756526_0001");
        when(oldLauncher.getApplicationId()).thenReturn(oldLauncherId);
        final ApplicationReport oldMRJob = mock(ApplicationReport.class);
        when(oldMRJob.getApplicationType()).thenReturn("MAPREDUCE");
        when(oldMRJob.getYarnApplicationState()).thenReturn(YarnApplicationState.FINISHED);
        final ApplicationId oldMRJobId = mock(ApplicationId.class);
        when(oldMRJobId.toString()).thenReturn("application_1534164756526_0002");
        when(oldMRJob.getApplicationId()).thenReturn(oldMRJobId);
        final ApplicationReport newLauncher = mock(ApplicationReport.class);
        when(newLauncher.getApplicationType()).thenReturn("Oozie Launcher");
        when(newLauncher.getYarnApplicationState()).thenReturn(YarnApplicationState.FINISHED);
        final ApplicationId newLauncherId = mock(ApplicationId.class);
        when(newLauncherId.toString()).thenReturn("application_1534164756526_0003");
        when(newLauncher.getApplicationId()).thenReturn(newLauncherId);
        final ApplicationReport newMRJob = mock(ApplicationReport.class);
        when(newMRJob.getApplicationType()).thenReturn("MAPREDUCE");
        when(newMRJob.getYarnApplicationState()).thenReturn(YarnApplicationState.RUNNING);
        final ApplicationId newMRJobId = mock(ApplicationId.class);
        when(newMRJobId.toString()).thenReturn("application_1534164756526_0004");
        when(newMRJob.getApplicationId()).thenReturn(newMRJobId);
        when(reader.read()).thenReturn(Lists.newArrayList(oldLauncher, oldMRJob, newLauncher, newMRJob));

        when(workflowActionBean.getExternalId()).thenReturn("application_1534164756526_0003");
        assertEquals("newLauncher should be found", "application_1534164756526_0004", yarnApplicationIdFinder.find());

        when(workflowActionBean.getExternalId()).thenReturn("application_1534164756526_0004");
        assertEquals("newLauncher should be found", "application_1534164756526_0004", yarnApplicationIdFinder.find());

        when(workflowActionBean.getExternalId()).thenReturn("application_1534164756526_0005");
        assertEquals("workflowActionBean.externalId should be found",
                "application_1534164756526_0005", yarnApplicationIdFinder.find());
    }

    @Test
    public void testGetLastYarnIdOnNullThrows() {
        expectedException.expect(NullPointerException.class);
        yarnApplicationIdFinder.getLastYarnId(null);
    }

    @Test
    public void testGetLastYarnIdOnEmptyListThrows() {
        expectedException.expect(IllegalArgumentException.class);
        yarnApplicationIdFinder.getLastYarnId(Collections.emptyList());
    }

    @Test
    public void testGetLastYarnIdOnOneElementSuccess() {
        when(applicationReport.getApplicationId()).thenReturn(applicationId);
        when(applicationId.toString()).thenReturn("application_1534164756526_0000");

        final String lastYarnId = yarnApplicationIdFinder.getLastYarnId(Collections.singletonList(applicationReport));
        assertEquals("last YARN id should be the only element in the list", "application_1534164756526_0000", lastYarnId);
    }

    @Test
    public void testGetLastYarnIdFromUnorderedListSuccess() {
        final ApplicationReport newLauncher = mock(ApplicationReport.class);
        when(newLauncher.getApplicationType()).thenReturn("Oozie Launcher");
        when(newLauncher.getYarnApplicationState()).thenReturn(YarnApplicationState.FINISHED);
        final ApplicationId newLauncherId = mock(ApplicationId.class);
        when(newLauncherId.toString()).thenReturn("application_1534164756526_0003");
        when(newLauncher.getApplicationId()).thenReturn(newLauncherId);
        final ApplicationReport newMRJob = mock(ApplicationReport.class);
        when(newMRJob.getApplicationType()).thenReturn("MAPREDUCE");
        when(newMRJob.getYarnApplicationState()).thenReturn(YarnApplicationState.RUNNING);
        final ApplicationId newMRJobId = mock(ApplicationId.class);
        when(newMRJobId.toString()).thenReturn("application_1534164756526_0004");
        when(newMRJob.getApplicationId()).thenReturn(newMRJobId);

        final String lastYarnId = yarnApplicationIdFinder.getLastYarnId(Lists.newArrayList(newMRJob, newLauncher));
        assertEquals("last YARN id should be the maximal element in the list", "application_1534164756526_0004", lastYarnId);
    }
}