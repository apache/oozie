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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.util.XConfiguration;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "Output directory is specified by user")
class AppInfoCollector {
    private final DiagOozieClient client;
    private final OozieLauncherLogFetcher oozieLauncherLogFetcher;

    AppInfoCollector(final Configuration hadoopConfig, final DiagOozieClient client) {
        this.client = client;
        oozieLauncherLogFetcher = new OozieLauncherLogFetcher(hadoopConfig);
    }

    private void storeWorkflowJobDetails(final File outputDir, final String jobId, int maxChildActions) {
        if (jobId == null || !isWorkflow(jobId)) {
            return;
        }

        try {
            System.out.print("Getting Details for " + jobId + "...");
            final File workflowOutputDir = new File(outputDir, jobId);
            if (!createOutputDirectory(workflowOutputDir)) {
                return;
            }

            final File resolvedActionsDir = new File(workflowOutputDir, "resolved-actions");
            if (!createOutputDirectory(resolvedActionsDir)) {
                System.out.println("Workflow details already stored.");
                return;
            }

            final WorkflowJob job = client.getJobInfo(jobId);

            try (DiagBundleEntryWriter diagBundleEntryWriter = new DiagBundleEntryWriter(workflowOutputDir,"info.txt")) {
                persistWorkflowJobInfo(maxChildActions, resolvedActionsDir, job, diagBundleEntryWriter);
            }

            storeCommonDetails(workflowOutputDir, jobId, "workflow", job.getConf());
            System.out.println("Done");
        } catch (IOException | OozieClientException e) {
            System.err.printf("Exception occurred during the retrieval of workflow information: %s%n", e.getMessage());
        }
    }

    private void persistWorkflowJobInfo(int maxChildActions, final File resolvedActionsDir, final WorkflowJob job,
                                        final DiagBundleEntryWriter bundleEntryWriter) throws IOException {
        bundleEntryWriter.writeString("WORKFLOW\n")
                         .writeString("--------\n")
                         .writeStringValue("Workflow Id        : ", job.getId())
                         .writeStringValue("Name               : ", job.getAppName())
                         .writeStringValue("App Path           : ", job.getAppPath())
                         .writeStringValue("User               : ", job.getUser())
                         .writeStringValue("ACL                : ", job.getAcl())
                         .writeStringValue("Status             : ", job.getStatus().toString())
                         .writeStringValue("Console URL        : ", job.getConsoleUrl())
                         .writeStringValue("External Id        : ", job.getExternalId())
                         .writeStringValue("Parent Id          : ", job.getParentId())
                         .writeDateValue("Created Time       : ", job.getCreatedTime())
                         .writeDateValue("End Time           : ", job.getEndTime())
                         .writeDateValue("Last Modified Time : ", job.getLastModifiedTime())
                         .writeDateValue("Start Time         : ", job.getStartTime())
                         .writeIntValue("Run                : ", job.getRun())
                         .writeIntValue("Action Count       : ", job.getActions().size())
                         .writeNewLine()
                         .writeString("ACTIONS\n")
                         .writeString("------\n")
                         .flush();

        final List<WorkflowAction> workflowActions = job.getActions();
        for (int actionCount = 0; actionCount != workflowActions.size() && actionCount < maxChildActions; ++actionCount) {
            final WorkflowAction action = workflowActions.get(actionCount);
            bundleEntryWriter.writeStringValue("Action Id          : ", action.getId())
                             .writeStringValue("Name               : ", action.getName())
                             .writeStringValue("Type               : ", action.getType())
                             .writeStringValue("Status             : ", action.getStatus().toString())
                             .writeStringValue("Transition         : ", action.getTransition())
                             .writeDateValue("Start Time         : ", action.getStartTime())
                             .writeDateValue("End Time           : ", action.getEndTime())
                             .writeStringValue("Error Code         : ", action.getErrorCode())
                             .writeStringValue("Error Message      : ", action.getErrorMessage())
                             .writeStringValue("Console URL        : ", action.getConsoleUrl())
                             .writeStringValue("Tracker URI        : ", action.getTrackerUri())
                             .writeStringValue("External Child Ids : ", action.getExternalChildIDs())
                             .writeStringValue("External Id        : ", action.getExternalId())
                             .writeStringValue("External Status    : ", action.getExternalStatus())
                             .writeStringValue("Data               : ", action.getData())
                             .writeStringValue("Stats              : ", action.getStats())
                             .writeStringValue("Credentials        : ", action.getCred())
                             .writeIntValue("Retries            : ", action.getRetries())
                             .writeIntValue("User Retry Int     : ", action.getUserRetryInterval())
                             .writeIntValue("User Retry Count   : ", action.getUserRetryCount())
                             .writeIntValue("User Retry Max     : ", action.getUserRetryMax())
                             .writeNewLine()
                             .flush();

            final String actionType = action.getType();
            persistResolvedActionDefinition(action, resolvedActionsDir);

            if (!isControlNode(actionType)) { // skip control nodes
                storeOozieLauncherLog(resolvedActionsDir, action, job.getUser());
            }
        }
    }

    private boolean isControlNode(final String actionType) {
        return isNonDecisionControlNode(actionType) || isDecisionNode(actionType);
    }

    private boolean isDecisionNode(final String actionType) {
        return actionType.contains("switch");
    }

    private boolean isNonDecisionControlNode(final String actionType) {
        return actionType.contains(":");
    }

    private void persistResolvedActionDefinition(final WorkflowAction action, final File resolvedActionsDir) throws IOException {
        persistWorkflowDefinition(resolvedActionsDir, action.getName(), action.getConf());
    }


    private void storeOozieLauncherLog(final File outputDir, final WorkflowAction action, final String user) {
        try (PrintStream fw = new PrintStream(new File(outputDir, "launcher_" + action.getName() + ".log"),
                StandardCharsets.UTF_8.toString())) {

            final ApplicationId appId = ConverterUtils.toApplicationId(action.getExternalId());
            oozieLauncherLogFetcher.dumpAllContainersLogs(appId, user, fw);
        } catch (IOException e) {
            System.err.printf("Exception occurred during the retrieval of Oozie launcher logs for workflow(s): %s%n",
                    e.getMessage());
        }
    }

    private void getCoordJob(final File outputDir, final String jobId, int maxChildActions) {
        if (jobId == null || !isCoordinator(jobId)) {
            return;
        }

        try {
            System.out.print("Getting Details for " + jobId + "...");
            final File coordOutputDir = new File(outputDir, jobId);

            if (!createOutputDirectory(coordOutputDir)) {
                return;
            }

            final CoordinatorJob job = client.getCoordJobInfo(jobId);

            try (DiagBundleEntryWriter bundleEntryWriter = new DiagBundleEntryWriter(coordOutputDir, "info.txt")) {
                persistCoordinatorJobInfo(maxChildActions, job, bundleEntryWriter);
            }

            storeCommonDetails(coordOutputDir, jobId, "coordinator", job.getConf());
            System.out.println("Done");

            final List<CoordinatorAction> coordinatorActions = job.getActions();
            for (int i = 0; i != coordinatorActions.size() && i < maxChildActions; ++i) {
                storeWorkflowJobDetails(outputDir, coordinatorActions.get(i).getExternalId(), maxChildActions);
            }
        } catch (IOException | OozieClientException e) {
            System.err.printf(String.format("Exception occurred during the retrieval of coordinator information:%s%n",
                    e.getMessage()));
        }
    }

    private void persistCoordinatorJobInfo(int maxChildActions, final CoordinatorJob job,
                                           final DiagBundleEntryWriter bundleEntryWriter)
            throws IOException {
        bundleEntryWriter.writeString("COORDINATOR\n")
                         .writeString("-----------\n")
                         .writeStringValue("Coordinator Id           : ", job.getId())
                         .writeStringValue("Name                     : ", job.getAppName())
                         .writeStringValue("App Path                 : ", job.getAppPath())
                         .writeStringValue("User                     : ", job.getUser())
                         .writeStringValue("ACL                      : ", job.getAcl())
                         .writeStringValue("Status                   : ", job.getStatus().toString())
                         .writeStringValue("Console URL              : ", job.getConsoleUrl())
                         .writeStringValue("External Id              : ", job.getExternalId())
                         .writeStringValue("Bundle Id                : ", job.getBundleId())
                         .writeStringValue("Frequency                : ", job.getFrequency())
                         .writeStringValue("Time Unit                : ", job.getTimeUnit().toString())
                         .writeDateValue("Start Time               : ", job.getStartTime())
                         .writeDateValue("End Time                 : ", job.getEndTime())
                         .writeDateValue("Last Action Time         : ", job.getLastActionTime())
                         .writeDateValue("Next Materialized Time   : ", job.getNextMaterializedTime())
                         .writeDateValue("Pause Time               : ", job.getPauseTime())
                         .writeStringValue("Timezone                 : ", job.getTimeZone())
                         .writeIntValue("Concurrency              : ", job.getConcurrency())
                         .writeIntValue("Timeout                  : ", job.getTimeout())
                         .writeStringValue("Execution Order          : ", job.getExecutionOrder().toString())
                         .writeIntValue("Action Count             : ", job.getActions().size())
                         .writeNewLine()
                         .writeString("ACTIONS\n")
                         .writeString("------\n")
                         .flush();

        final List<CoordinatorAction> coordinatorActions = job.getActions();
        for (int i = 0; i < maxChildActions && i != coordinatorActions.size(); ++i) {
            final CoordinatorAction action = coordinatorActions.get(i);
            bundleEntryWriter.writeStringValue("Action Id                 : ", action.getId())
                             .writeIntValue("Action Number             : ", action.getActionNumber())
                             .writeStringValue("Job Id                    : ", action.getJobId())
                             .writeStringValue("Status                    : ", action.getStatus().toString())
                             .writeStringValue("External Id               : ", action.getExternalId())
                             .writeStringValue("External Status           : ", action.getExternalStatus())
                             .writeStringValue("Console URL               : ", action.getConsoleUrl())
                             .writeStringValue("Tracker URI               : ", action.getTrackerUri())
                             .writeDateValue("Created Time              : ", action.getCreatedTime())
                             .writeDateValue("Nominal Time              : ", action.getNominalTime())
                             .writeDateValue("Last Modified Time        : ", action.getLastModifiedTime())
                             .writeStringValue("Error Code                : ", action.getErrorCode())
                             .writeStringValue("Error Message             : ", action.getErrorMessage())
                             .writeStringValue("Missing Dependencies      : ", action.getMissingDependencies())
                             .writeStringValue("Push Missing Dependencies : ", action.getPushMissingDependencies())
                             .writeNewLine()
                             .flush();
        }
    }

    private void getBundleJob(final File outputDir, final String jobId, int maxChildActions) {
        if (jobId == null || !isBundle(jobId)) {
            return;
        }

        try {
            System.out.print("Getting Details for " + jobId + "...");
            final File bundleOutputDir = new File(outputDir, jobId);

            if (!createOutputDirectory(bundleOutputDir)) {
                return;
            }

            final BundleJob job = client.getBundleJobInfo(jobId);

            try (DiagBundleEntryWriter bundleEntryWriter = new DiagBundleEntryWriter(bundleOutputDir, "info.txt")) {
                persistBundleJobInfo(job, bundleEntryWriter);
            }

            storeCommonDetails(bundleOutputDir, jobId, "bundle", job.getConf());
            System.out.println("Done");
            for (CoordinatorJob coordJob : job.getCoordinators()) {
                getCoordJob(outputDir, coordJob.getId(), maxChildActions);
            }


        } catch (IOException | OozieClientException e) {
            System.err.printf(String.format("Exception occurred during the retrieval of bundle information: %s%n",
                    e.getMessage()));
        }
    }

    private boolean createOutputDirectory(final File outputDir) throws IOException {
        if (outputDir.isDirectory()) {
            System.out.println("(Already) Done");
            return false;
        }
        if (!outputDir.mkdirs()) {
            throw new IOException("Could not create output directory: " + outputDir.getAbsolutePath());
        }
        return true;
    }

    private void persistBundleJobInfo(final BundleJob job, final DiagBundleEntryWriter bundleEntryWriter) throws IOException {
        bundleEntryWriter.writeString("BUNDLE\n")
                         .writeString("-----------\n")
                         .writeStringValue("Bundle Id    : ", job.getId())
                         .writeStringValue("Name         : ", job.getAppName())
                         .writeStringValue("App Path     : ", job.getAppPath())
                         .writeStringValue("User         : ", job.getUser())
                         .writeStringValue("Status       : ", job.getStatus().toString())
                         .writeDateValue("Created Time : ", job.getCreatedTime())
                         .writeDateValue("Start Time   : ", job.getStartTime())
                         .writeDateValue("End Time     : ", job.getEndTime())
                         .writeDateValue("KickoffTime  : ", job.getKickoffTime())
                         .writeDateValue("Pause Time   : ", job.getPauseTime())
                         .writeIntValue("Timeout      : ", job.getTimeout())
                         .writeStringValue("Console URL  : ", job.getConsoleUrl())
                         .writeStringValue( "ACL          : ", job.getAcl())
                         .flush();
    }

    private void storeCommonDetails(final File outputDir, final String jobId, final String definitionName,
                                    final String jobPropsConfStr) {
        try {
            final String definition = client.getJobDefinition(jobId);

            if (definition != null) {
                persistWorkflowDefinition(outputDir, definitionName, definition);
            }

            if (jobPropsConfStr != null) {
                persistJobProperties(outputDir, jobPropsConfStr);
            }

            persistJobLog(outputDir, jobId);
        } catch (OozieClientException | IOException e) {
            System.err.printf(String.format("Exception occurred during the retrieval of common job details: %s%n",
                    e.getMessage()));
        }
    }

    private void persistJobLog(final File outputDir, final String jobId) throws FileNotFoundException,
            UnsupportedEncodingException, OozieClientException {
        try (PrintStream ps = new PrintStream(new File(outputDir, "log.txt"), StandardCharsets.UTF_8.toString())) {
            client.getJobLog(jobId, null, null, null, ps);
        }
    }

    private void persistJobProperties(final File outputDir, final String jobPropsConfStr) throws IOException {
        final StringReader sr = new StringReader(jobPropsConfStr);
        final XConfiguration jobPropsConf = new XConfiguration(sr);
        final Properties jobProps = jobPropsConf.toProperties();

        try (OutputStream  outputStream = new FileOutputStream(new File(outputDir, "job.properties"))) {
            jobProps.store(outputStream, "");
        }
    }

    private void persistWorkflowDefinition(final File outputDir, final String definitionName, String definition)
            throws IOException {
        try (DiagBundleEntryWriter bundleEntryWriter = new DiagBundleEntryWriter(outputDir,
                definitionName + ".xml")) {
            bundleEntryWriter.writeString(definition);
        }
    }

    void storeLastWorkflows(final File outputDir, int numWorkflows, int maxChildActions) {
        if (numWorkflows == 0) {
            return;
        }

        try {
            final List<WorkflowJob> jobs = client.getJobsInfo(null, 0, numWorkflows);
            for (WorkflowJob job : jobs) {
                storeWorkflowJobDetails(outputDir, job.getId(), maxChildActions);
            }
        } catch (OozieClientException e) {
            System.err.printf("Exception occurred during the retrieval of information on the last %d workflow(s): %s.%n",
                    numWorkflows, e.getMessage());
        }
    }

    void storeLastCoordinators(final File outputDir, int numCoordinators, int maxChildActions) {
        if (numCoordinators == 0) {
            return;
        }

        try {
            final List<CoordinatorJob> jobs = client.getCoordJobsInfo(null, 0, numCoordinators);
            for (CoordinatorJob job : jobs) {
                getCoordJob(outputDir, job.getId(), maxChildActions);
            }
        } catch (OozieClientException e) {
            System.err.printf("Exception occurred during the retrieval of information on the last %d coordinator(s): %s.%n",
                    numCoordinators, e.getMessage());
        }
    }

    void storeLastBundles(final File outputDir, int numBundles, int maxChildActions) {
        if (numBundles == 0) {
            return;
        }

        try {
            final List<BundleJob> jobs = client.getBundleJobsInfo(null, 0, numBundles);
            for (BundleJob job : jobs) {
                getBundleJob(outputDir, job.getId(), maxChildActions);
            }
        } catch (OozieClientException e) {
            System.err.printf("Exception occurred during the retrieval of information on the last %d bundle(s): %s.%n",
                    numBundles, e.getMessage());
        }
    }

    void getSpecificJobs(final File outputDir, final String[] jobIds, int maxChildActions) {
        if (jobIds == null) {
            return;
        }

        for (String jobId : jobIds) {
            if (isWorkflow(jobId)) {
                storeWorkflowJobDetails(outputDir, jobId, maxChildActions);
            } else if (isCoordinator(jobId)) {
                getCoordJob(outputDir, jobId, maxChildActions);
            } else if (isBundle(jobId)) {
                getBundleJob(outputDir, jobId, maxChildActions);
            }
        }
    }

    private boolean isBundle(final String jobId) {
        return jobId.endsWith("-B");
    }

    private boolean isCoordinator(final String jobId) {
        return jobId.endsWith("-C");
    }

    private boolean isWorkflow(final String jobId) {
        return jobId.endsWith("-W");
    }
}
