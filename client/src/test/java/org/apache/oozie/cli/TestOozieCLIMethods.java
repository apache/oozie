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

package org.apache.oozie.cli;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import junit.framework.TestCase;

import org.apache.oozie.client.BulkResponse;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;

public class TestOozieCLIMethods extends TestCase {

    static final String jobIdPattern = "Job ID[\\s|:]+";
    static final String jobNamePattern = "Job Name[\\s|:]+";
    static final String workflowNamePattern = "Workflow Name[\\s|:]+";
    static final String appPathPattern = "App Path[\\s|:]+";
    static final String statusPattern = "Status[\\s|:]+RUNNING";
    static final String actionIdPattern = "ID[\\s|:]+";
    static final String actionJobIdPattern = "Job ID[\\s|:]+";
    static final String actionNamePattern = "Name[\\s|:]+";

    static class DataObject {
        String deamonName;
        String appName;
        String appPath;
    }

    private abstract class OutputReaderTemplate {
        protected String read() throws IOException {
            ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
            PipedOutputStream pipeOut = new PipedOutputStream();
            PipedInputStream pipeIn = new PipedInputStream(pipeOut, 1024 * 50);
            System.setOut(new PrintStream(pipeOut));
            execute();
            pipeOut.close();
            ByteStreams.copy(pipeIn, outBytes);
            pipeIn.close();
            return new String(outBytes.toByteArray());
        }

        abstract void execute() throws IOException;
    }

    /**
     *
     * Create {@code CoordinatorJob} and {@code CoordinatorActions} mocks,
     * call {@code new OozieCLI().printCoordJob() },
     * and validate {@code System.out} output on match with
     * expected pattern.
     * <p>
     * Method do pass only if output matched with predefined pattern
     * </p>
     */
    @Test
    public void testValidatePrintCoordJobMethodOutput() throws IOException {
        final DataObject dtObject = new DataObject() {
            {
                this.deamonName = "testCoordJob";
                this.appName = "testCoordinatorJobApp";
                this.appPath = "testCoordinatorJobAppPath";
            }
        };

        CoordinatorJob coordJob = createCoordinatorJob(dtObject);

        assertPrintCoordJobOutput(readCoordJobOutput(coordJob, true), dtObject);
        assertPrintCoordJobOutput(readCoordJobOutput(coordJob, false), dtObject);
    }

    /**
     *
     * Create {@code CoordinatorAction} mock,
     * call {@code new OozieCLI().printCoordAction() },
     * and validate {@code System.out} output on match with
     * expected pattern
     * <p>
     * Method do pass only if output matched with predefined pattern
     * </p>
     */
    @Test
    public void testValidateReadPrintCoordActionOutput() throws IOException {
        final DataObject dtObject = new DataObject() {
            {
                this.deamonName = "testCoordinatorAction";
                this.appName = "testCoordinatorJobApp";
                this.appPath = "testCoordinatorJobAppPath";
            }
        };

        CoordinatorAction coordinatorAction = createCoordinatorAction(dtObject);
        assertPrintCoordActionOutput(readCoordAction(coordinatorAction), dtObject);
    }

    /**
     *
     * Create {@code WorkflowJob}, {@code WorkflowAction} mocks,
     * call {@code new OozieCLI().printJob() },
     * and validate {@code System.out} on match with expected pattern
     * <p>
     * Method do pass only if output matched with predefined pattern
     * </p>
     */
    @Test
    public void testValidatePrintJobOutput() throws IOException {
        final DataObject dtObject = new DataObject() {
            {
                this.deamonName = "testWorkflowJob";
                this.appName = "testWorkflowJobApp";
                this.appPath = "testWorkflowJobAppPath";
            }
        };

        WorkflowJob workflowJob = createWorkflowJob(dtObject);
        assertPrintWorkflowJobOutput1(readWorkflowJobOutput(workflowJob, true), dtObject);
        assertPrintWorkflowJobOutput1(readWorkflowJobOutput(workflowJob, false), dtObject);
    }

    /**
     *
     * Create {@code WorkflowAction} mock
     * call {@code new OozieCLI().printWorkflowAction() }
     * and validate {@code System.out} on match with expected pattern
     * <p>
     * Method do pass only if output matched with predefined pattern
     * </p>
     */
    @Test
    public void testValidatePrintWorkflowActionOutput() throws IOException {
        final DataObject dtObject = new DataObject() {
            {
                this.deamonName = "testWorkflowAction111";
                this.appName = "testWorkflowActionAppName";
                this.appPath = "unused";
            }
        };

        WorkflowAction workflowAction = createWorkflowAction(dtObject);
        assertPrintWorkflowActionOutput(readWorkflowActionOutput(workflowAction, true), dtObject);
        assertPrintWorkflowActionOutput(readWorkflowActionOutput(workflowAction, false), dtObject);
    }

    /**
     * Create {@code CoordinatorJob} mock,
     * call {@code new OozieCLI().printCoordJobs() }
     * and validate {@code System.out} on match with expected pattern
     * <p>
     * Method do pass only if output matched with predefined pattern
     * </p>
     */
    @Test
    public void testValidatePrintCoordJobsOutput() throws IOException {
        final DataObject dtObject1 = new DataObject() {
            {
                this.deamonName = "testCoordJob1";
                this.appName = "testCoordinatorJobApp1";
                this.appPath = "testCoordinatorJobAppPath1";
            }
        };
        final DataObject dtObject2 = new DataObject() {
            {
                this.deamonName = "testCoordJob2";
                this.appName = "testCoordinatorJobApp2";
                this.appPath = "testCoordinatorJobAppPath2";
            }
        };

        final ImmutableList<CoordinatorJob> coordJobs =
                ImmutableList.of(createCoordinatorJob(dtObject1), createCoordinatorJob(dtObject2));

        Pattern pattern =  Pattern.compile(dtObject1.deamonName + "[\\s]+" +
                    dtObject1.appName + "[\\s]+" + dtObject1.appPath);
        assertPrintCoordJobsOutput(readCoordJobsOutput(coordJobs, true), pattern);

        pattern = Pattern.compile(dtObject1.deamonName + "[\\s]+" + dtObject1.appName);
        assertPrintCoordJobsOutput(readCoordJobsOutput(coordJobs, false), pattern);
    }

    /**
     * Create {@code CoordinatorJob} mock,
     * call {@code new OozieCLI().printJobs() }
     * and validate {@code System.out} on match with expected pattern
     * <p>
     * Method do pass only if output matched with predefined pattern
     * </p>
     */
    @Test
    public void testValidatePrintJobsOutput() throws IOException {
        final DataObject dtObject1 = new DataObject() {
            {
                this.deamonName = "testWorkflowJob1";
                this.appName = "testWorkflowJobApp1";
                this.appPath = "testWorkflowJobAppPath1";
            }
        };
        final DataObject dtObject2 = new DataObject() {
            {
                this.deamonName = "testWorkflowJob2";
                this.appName = "testWorkflowJobApp2";
                this.appPath = "testWorkflowJobAppPath2";
            }
        };

        ImmutableList<WorkflowJob> workflowJobs = ImmutableList.of(createWorkflowJob(dtObject1),
                createWorkflowJob(dtObject2));

        Pattern pattern =  Pattern.compile(dtObject1.deamonName + "[\\s]+" +
                    dtObject1.appName + "[\\s]+" + dtObject1.appPath);
        assertPrintWorkflowJobOutput(readWorkflowJobsOutput(workflowJobs, true), pattern);

        pattern = Pattern.compile(dtObject1.deamonName + "[\\s]+" + dtObject1.appName);
        assertPrintWorkflowJobOutput(readWorkflowJobsOutput(workflowJobs, false), pattern);
    }

    /**
     * Create list of {@code BundleJob} mocks,
     * call {@code new OozieCLI().printBundleJobs()}
     * and validate {@code System.out} on match with expected pattern
     * <p>
     * Method do pass only if output matched with predefined pattern
     * </p>
     */
    @Test
    public void testValidationPrintBundleJobsOutput() throws IOException {
        final DataObject dtObject1 = new DataObject() {
            {
                this.deamonName = "testBundleJob1";
                this.appName = "testBundleJobApp1";
                this.appPath = "testBundleJobAppPath1";
            }
        };

        final DataObject dtObject2 = new DataObject() {
            {
                this.deamonName = "testBundleJob2";
                this.appName = "testBundleJobApp2";
                this.appPath = "testBundleJobAppPath2";
            }
        };

        ImmutableList<BundleJob> bundleJobs = ImmutableList.of(createBundleJob(dtObject1), createBundleJob(dtObject2));

        Pattern pattern =  Pattern.compile(dtObject1.deamonName + "[\\s]+"
                    + dtObject1.appName + "[\\s]+" + dtObject1.appPath);
        assertPrintBundleJobsOutput(readBundleJobsOutput(bundleJobs, true), pattern);

        pattern = Pattern.compile(dtObject1.deamonName + "[\\s]+" + dtObject1.appName);
        assertPrintBundleJobsOutput(readBundleJobsOutput(bundleJobs, false), pattern);
    }

    /**
    * Create {@code BundleJob} mock,
    * call {@code new OozieCLI().printBundleJobs()}
    * and validate {@code System.out} on match with expected pattern
    * <p>
    * Method do pass only if output matched with predefined pattern
    * </p>
    */
    @Test
    public void testValidationPrintBundleJobOutput() throws IOException {
        final DataObject dtObject = new DataObject() {
            {
                this.deamonName = "testBundleJob99";
                this.appName = "testBundleJobApp99";
                this.appPath = "testBundleJobAppPath99";
            }
        };

        assertPrintBundleJobsOutput(readBundleJobOutput(createBundleJob(dtObject), true), dtObject);
        assertPrintBundleJobsOutput(readBundleJobOutput(createBundleJob(dtObject), false), dtObject);
    }

    /**
     * Create list of {@code BulkResponse} mock,
     * call {@code new OozieCLI().printBundleJobs()}
     * and validate {@code System.out} on match with expected pattern
     * <p>
     * Method do pass only if output matched with predefined pattern
     * </p>
     */
    @Test
    public void testValidationPrintBulkJobsOutput() throws IOException {
        final DataObject dtObject = new DataObject() {
            {
                this.deamonName = "";
                this.appName = "testBundleName";
                this.appPath = "";
            }
        };

        Pattern pattern = Pattern.compile("Bundle Name[ |:]+" + dtObject.appName);
        assertPrintBulkResponseOutput(readBulkResponseOutput(ImmutableList.of(createBulkResponse(dtObject)), true), pattern);

        pattern = Pattern.compile(dtObject.appName + "-" +"\\s+" + dtObject.appName + "-");
        assertPrintBulkResponseOutput(readBulkResponseOutput(ImmutableList.of(createBulkResponse(dtObject)), false), pattern);
    }

    private String readBulkResponseOutput(final List<BulkResponse> bulkResponses, final boolean verbose) throws IOException {
        return new OutputReaderTemplate() {
            @Override
            void execute() throws IOException {
                new OozieCLI().printBulkJobs(bulkResponses, null, verbose);
            }
        }.read();
    }

    private String readBundleJobOutput(final BundleJob bundleJob, final boolean verbose) throws IOException {
        return new OutputReaderTemplate() {
            @Override
            void execute() throws IOException {
                new OozieCLI().printBundleJob(bundleJob, null, verbose);
            }
        }.read();
    }

    private String readBundleJobsOutput(final ImmutableList<BundleJob> bundleJobs, final boolean verbose)
            throws IOException {
        return new OutputReaderTemplate() {
            @Override
            void execute() throws IOException {
                new OozieCLI().printBundleJobs(bundleJobs, null, verbose);
            }
        }.read();
    }

    private String readWorkflowJobOutput(final WorkflowJob workflowJob, final boolean verbose)
            throws IOException {
        return new OutputReaderTemplate() {
            @Override
            void execute() throws IOException {
                new OozieCLI().printJob(workflowJob, null, verbose);
            }
        }.read();
    }

    private String readWorkflowJobsOutput(final ImmutableList<WorkflowJob> workflowJobs, final boolean verbose)
            throws IOException {
        return new OutputReaderTemplate() {
            @Override
            void execute() throws IOException {
                new OozieCLI().printJobs(workflowJobs, null, verbose);
            }
        }.read();
    }

    private String readCoordJobOutput(final CoordinatorJob coordJob, final boolean verbose)
            throws IOException {
        return new OutputReaderTemplate() {
            @Override
            void execute() throws IOException {
                new OozieCLI().printCoordJob(coordJob, null, verbose);
            }
        }.read();
    }

    private String readCoordJobsOutput(final ImmutableList<CoordinatorJob> coordJobs, final boolean verbose)
            throws IOException {
        return new OutputReaderTemplate() {
            @Override
            void execute() throws IOException {
                new OozieCLI().printCoordJobs(coordJobs, null, verbose);
            }
        }.read();
    }

    private String readWorkflowActionOutput(final WorkflowAction workflowAction, final boolean verbose)
            throws IOException {
        return new OutputReaderTemplate() {
            @Override
            void execute() throws IOException {
                new OozieCLI().printWorkflowAction(workflowAction, null, verbose);
            }
        }.read();
    }

    private String readCoordAction(final CoordinatorAction coordinatorAction)
            throws IOException {
        return new OutputReaderTemplate() {
            @Override
            void execute() throws IOException {
                new OozieCLI().printCoordAction(coordinatorAction, null);
            }
        }.read();
    }

    private void assertPrintBulkResponseOutput(String bulkResponseOutput, Pattern pattern) {
        assertTrue("assertPrintBulkResponseOutput error", pattern.matcher(bulkResponseOutput).find());
    }

    private void assertPrintBundleJobsOutput(String bundleJobOutput, DataObject dtObject) {
        assertTrue("assertPrintBundleJobsOutput Job ID error", Pattern.compile(jobIdPattern + dtObject.deamonName)
                .matcher(bundleJobOutput).find());
        assertTrue("assertPrintBundleJobsOutput Job Name error", Pattern.compile(jobNamePattern + dtObject.appName)
                .matcher(bundleJobOutput).find());
        assertTrue("assertPrintBundleJobsOutput App Path error", Pattern.compile(appPathPattern + dtObject.appPath)
                .matcher(bundleJobOutput).find());
    }

    private void assertPrintBundleJobsOutput(String bundleJobsOutput, Pattern pattern) {
        assertTrue("assertPrintWorkflowJobOutput error", pattern.matcher(bundleJobsOutput).find());
    }

    private void assertPrintWorkflowJobOutput(String workflowJobOutput, Pattern pattern) {
        assertTrue("assertPrintWorkflowJobOutput error", pattern.matcher(workflowJobOutput).find());
    }

    private void assertPrintCoordJobsOutput(String coordJobsOutput, Pattern pattern) {
        assertTrue("assertPrintCoordJobsOutput error", pattern.matcher(coordJobsOutput).find());
    }

    private void assertPrintWorkflowActionOutput(String workflowActionOutput, DataObject dtObject) {
        assertTrue("assertPrintWorkflowActionOutput ID error", Pattern.compile(actionIdPattern + dtObject.deamonName)
                .matcher(workflowActionOutput).find());
        assertTrue("assertPrintWorkflowActionOutput Name error", Pattern.compile(actionNamePattern + dtObject.appName)
                .matcher(workflowActionOutput).find());
    }

    private void assertPrintWorkflowJobOutput1(String workflowJobOutput, DataObject dtObject) {
        assertTrue("assertPrintWorkflowJobOutput Job ID error", Pattern.compile(jobIdPattern + dtObject.deamonName)
                        .matcher(workflowJobOutput).find());
        assertTrue("assertPrintWorkflowJobOutput Job Name error", Pattern.compile(workflowNamePattern + dtObject.appName)
                        .matcher(workflowJobOutput).find());
        assertTrue("assertPrintWorkflowJobOutput App Path error", Pattern.compile(appPathPattern + dtObject.appPath)
                        .matcher(workflowJobOutput).find());
    }

    private void assertPrintCoordActionOutput(String output, DataObject dtObject) {
        assertTrue("assertPrintCoordActionOutput Job ID error", Pattern.compile(actionIdPattern + dtObject.deamonName)
                .matcher(output).find());
        assertTrue("assertPrintCoordActionOutput ID error ", Pattern.compile(actionJobIdPattern + dtObject.appName)
                .matcher(output).find());
    }

    private void assertPrintCoordJobOutput(String line, DataObject dtObject) {
        assertTrue("assertPrintCoordJobOutput Job ID error", Pattern.compile(jobIdPattern + dtObject.deamonName)
                .matcher(line).find());
        assertTrue("assertPrintCoordJobOutput Job Name error", Pattern.compile(jobNamePattern + dtObject.appName)
                .matcher(line).find());
        assertTrue("assertPrintCoordJobOutput App Path error", Pattern.compile(appPathPattern + dtObject.appPath)
                .matcher(line).find());
        assertTrue("assertPrintCoordJobOutput Status error", Pattern.compile(statusPattern).matcher(line).find());
    }

    private static BundleJob createBundleJob(DataObject dtObject) {
        BundleJob bundleJobMock = mock(BundleJob.class);
        when(bundleJobMock.getId()).thenReturn(dtObject.deamonName);
        when(bundleJobMock.getAppName()).thenReturn(dtObject.appName);
        when(bundleJobMock.getAppPath()).thenReturn(dtObject.appPath);
        when(bundleJobMock.getStatus()).thenReturn(org.apache.oozie.client.Job.Status.RUNNING);

        CoordinatorJob coordinatorJobMock = createCoordinatorJob(dtObject);
        when(bundleJobMock.getCoordinators()).thenReturn(ImmutableList.of(coordinatorJobMock));
        return bundleJobMock;
    }

    private static CoordinatorJob createCoordinatorJob(DataObject dtObject) {
        CoordinatorJob coordinatorJobMock = mock(CoordinatorJob.class);
        when(coordinatorJobMock.getId()).thenReturn(dtObject.deamonName);
        when(coordinatorJobMock.getAppName()).thenReturn(dtObject.appName);
        when(coordinatorJobMock.getAppPath()).thenReturn(dtObject.appPath);
        when(coordinatorJobMock.getConcurrency()).thenReturn(15);
        when(coordinatorJobMock.getStatus()).thenReturn(CoordinatorJob.Status.RUNNING);
        when(coordinatorJobMock.getUser()).thenReturn("test");
        when(coordinatorJobMock.getGroup()).thenReturn("test-group");

        ImmutableList.Builder<CoordinatorAction> builder = ImmutableList.builder();

        for (final String id : Arrays.asList("1", "2"))
            builder.add(createCoordinatorAction(new DataObject() {
                {
                    this.deamonName = id;
                    this.appName = "testCoordinatorAction";
                }
            }));

        when(coordinatorJobMock.getActions()).thenReturn(builder.build());
        return coordinatorJobMock;
    }

    private WorkflowJob createWorkflowJob(DataObject dtObject) {
        WorkflowJob workflowJobMock = mock(WorkflowJob.class);
        when(workflowJobMock.getId()).thenReturn(dtObject.deamonName);
        when(workflowJobMock.getAppName()).thenReturn(dtObject.appName);
        when(workflowJobMock.getAppPath()).thenReturn(dtObject.appPath);
        when(workflowJobMock.getStatus()).thenReturn(WorkflowJob.Status.RUNNING);
        WorkflowAction ac = createWorkflowAction(dtObject);
        WorkflowAction ac0 = createWorkflowAction(dtObject);
        when(workflowJobMock.getActions()).thenReturn(Arrays.asList(ac, ac0));
        return workflowJobMock;
    }

    private static CoordinatorAction createCoordinatorAction(DataObject dtObject) {
        CoordinatorAction crdActionMock = mock(CoordinatorAction.class);
        when(crdActionMock.getId()).thenReturn(dtObject.deamonName);
        when(crdActionMock.getJobId()).thenReturn(dtObject.appName);
        when(crdActionMock.getActionNumber()).thenReturn(11);
        when(crdActionMock.getStatus()).thenReturn(CoordinatorAction.Status.SUBMITTED);
        return crdActionMock;
    }

    private static WorkflowAction createWorkflowAction(DataObject dtObject) {
        WorkflowAction workflowActionMock = mock(WorkflowAction.class);
        when(workflowActionMock.getId()).thenReturn(dtObject.deamonName);
        when(workflowActionMock.getName()).thenReturn(dtObject.appName);
        return workflowActionMock;
    }

    private static BulkResponse createBulkResponse(DataObject dtObject) {
        BulkResponse bulkResponse = mock(BulkResponse.class);

        BundleJob bundleJob = createBundleJob(dtObject);
        when(bulkResponse.getBundle()).thenReturn(bundleJob);

        CoordinatorAction coordinatorAction = createCoordinatorAction(dtObject);
        when(bulkResponse.getAction()).thenReturn(coordinatorAction);

        CoordinatorJob coordinatorJob = createCoordinatorJob(dtObject);
        when(bulkResponse.getCoordinator()).thenReturn(coordinatorJob);
        return bulkResponse;
    }
}
