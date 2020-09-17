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

package org.apache.oozie.jobs.client.minitest;

import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.fluentjob.api.GraphVisualization;
import org.apache.oozie.fluentjob.api.action.*;
import org.apache.oozie.fluentjob.api.dag.Graph;
import org.apache.oozie.fluentjob.api.serialization.WorkflowMarshaller;
import org.apache.oozie.fluentjob.api.workflow.Workflow;
import org.apache.oozie.fluentjob.api.workflow.WorkflowBuilder;
import org.apache.oozie.test.WorkflowTestCase;

import javax.xml.bind.JAXBException;
import java.io.IOException;

public class TestGitAction extends WorkflowTestCase {
    public void testGitAction() throws IOException, JAXBException, OozieClientException {
        final GitAction parent = GitActionBuilder.create()
                .withResourceManager(getJobTrackerUri())
                .withNameNode(getNameNodeUri())
                .withDestinationUri("/user/${wf:user()}/${examplesRoot}/output-data/git/oozie")
                .withGitUri("https://github.com/apache/oozie")
                .build();

        final Workflow workflow = new WorkflowBuilder()
                .withName("simple-git-example")
                .withDagContainingNode(parent).build();

        final String xml = WorkflowMarshaller.marshal(workflow);

        System.out.println(xml);

        GraphVisualization.workflowToPng(workflow, "simple-git-example-workflow.png");

        final Graph intermediateGraph = new Graph(workflow);

        GraphVisualization.graphToPng(intermediateGraph, "simple-git-example-graph.png");

        log.debug("Workflow XML is:\n{0}", xml);

        validate(xml);
    }
}
