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

package org.apache.oozie.test;

import com.google.common.base.Strings;
import org.apache.oozie.action.hadoop.JavaActionExecutor;
import org.apache.oozie.client.WorkflowJob;

import java.util.Date;
import java.util.Properties;

import static org.junit.Assume.assumeFalse;

/**
 * {@code MiniOozie} integration test for different workflow kinds.
 */
public class TestWorkflow extends WorkflowTestCase {

    public void testWorkflowWithStartAndEndCompletesSuccessfully() throws Exception {
        final String workflowXml = "<workflow-app xmlns='uri:oozie:workflow:0.1' name='test-wf'>" + "    <start to='end'/>"
                + "    <end name='end'/>" + "</workflow-app>";

        submitAndAssert(workflowXml, WorkflowJob.Status.SUCCEEDED);
    }

    public void testFsDecisionWorkflowCompletesSuccessfully() throws Exception {
        final String workflowFileName = "fs-decision.xml";
        final Properties additionalWorkflowProperties = new Properties();

        runWorkflowFromFile(workflowFileName, additionalWorkflowProperties);
    }

    public void testParallelFsAndShellWorkflowCompletesSuccessfully() throws Exception {
        final String workflowFileName = "parallel-fs-and-shell.xml";
        final Properties additionalWorkflowProperties = new Properties();
        final boolean isEvenSecond = new Date().getTime() % 2 == 0;
        additionalWorkflowProperties.setProperty("choosePath1", Boolean.toString(isEvenSecond));

        final String envJavaHome = System.getenv("JAVA_HOME");
        assumeFalse("Environment variable JAVA_HOME has to be set", Strings.isNullOrEmpty(envJavaHome));

        additionalWorkflowProperties.setProperty("oozie.launcher." + JavaActionExecutor.YARN_AM_ENV, envJavaHome);

        runWorkflowFromFile(workflowFileName, additionalWorkflowProperties);
    }
}
