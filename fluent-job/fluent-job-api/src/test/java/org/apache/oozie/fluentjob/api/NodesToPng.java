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

package org.apache.oozie.fluentjob.api;

import org.apache.oozie.fluentjob.api.dag.Graph;
import org.apache.oozie.fluentjob.api.workflow.Workflow;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Used in integration tests, eases the generation of visualized {@link Workflow} or {@link Graph} instances
 * in {@code .png} format using a predefined datetime pattern name postfix.
 */
public class NodesToPng extends ExternalResource {
    private final DateFormat df = new SimpleDateFormat("yyyyMMddhhmmss");
    private Date testRunningOn;

    @Override
    protected void before() throws Throwable {
        testRunningOn = new Date();
    }

    public void withWorkflow(final Workflow workflow) throws IOException {
        final String fileName = String.format("%s-%s-workflow.png", df.format(testRunningOn), workflow.getName());
        GraphVisualization.workflowToPng(workflow, fileName);
    }

    public void withGraph(final Graph graph) throws IOException {
        final String fileName = String.format("%s-%s-graph.png", df.format(testRunningOn), graph.getName());
        GraphVisualization.graphToPng(graph, fileName);
    }
}