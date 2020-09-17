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
import org.apache.oozie.fluentjob.api.action.SshAction;
import org.apache.oozie.fluentjob.api.action.SshActionBuilder;
import org.apache.oozie.fluentjob.api.dag.Graph;
import org.apache.oozie.fluentjob.api.serialization.WorkflowMarshaller;
import org.apache.oozie.fluentjob.api.workflow.Workflow;
import org.apache.oozie.fluentjob.api.workflow.WorkflowBuilder;
import org.apache.oozie.test.WorkflowTestCase;

import javax.xml.bind.JAXBException;
import java.io.IOException;

public class TestSshAction extends WorkflowTestCase {
    public void testForkedSshActions() throws IOException, JAXBException, OozieClientException {
        final SshAction parent = SshActionBuilder.create()
                .withArg("\"Hello Oozie!\"")
                .withHost("localhost")
                .withCommand("echo")
                .withCaptureOutput(true)
                .build();

        //  We are reusing the definition of parent and only modifying and adding what is different.
        final SshAction leftChild = SshActionBuilder.createFromExistingAction(parent)
                .withParent(parent)
                .withoutArg("\"Hello Oozie!\"")
                .withArg("\"Hello Oozie!!\"")
                .withCaptureOutput(false)
                .build();

        SshActionBuilder.createFromExistingAction(leftChild)
                .withoutArg("\"Hello Oozie!!\"")
                .withArg("\"Hello Oozie!!!\"")
                .build();

        final Workflow workflow = new WorkflowBuilder()
                .withName("simple-ssh-example")
                .withDagContainingNode(parent).build();

        final String xml = WorkflowMarshaller.marshal(workflow);

        System.out.println(xml);

        GraphVisualization.workflowToPng(workflow, "simple-ssh-example-workflow.png");

        final Graph intermediateGraph = new Graph(workflow);

        GraphVisualization.graphToPng(intermediateGraph, "simple-ssh-example-graph.png");

        log.debug("Workflow XML is:\n{0}", xml);

        validate(xml);
    }
}
