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
import org.apache.oozie.fluentjob.api.action.DistcpAction;
import org.apache.oozie.fluentjob.api.action.DistcpActionBuilder;
import org.apache.oozie.fluentjob.api.action.Prepare;
import org.apache.oozie.fluentjob.api.action.PrepareBuilder;
import org.apache.oozie.fluentjob.api.action.SshActionBuilder;
import org.apache.oozie.fluentjob.api.dag.Graph;
import org.apache.oozie.fluentjob.api.serialization.WorkflowMarshaller;
import org.apache.oozie.fluentjob.api.workflow.Workflow;
import org.apache.oozie.fluentjob.api.workflow.WorkflowBuilder;
import org.apache.oozie.test.WorkflowTestCase;

import javax.xml.bind.JAXBException;
import java.io.IOException;

public class TestDistcpAction extends WorkflowTestCase {
    public void testForkedDistcpActions() throws IOException, JAXBException, OozieClientException {
        final Prepare prepare = new PrepareBuilder()
                .withDelete("hdfs://localhost:8020/user/${wf:user()}/examples/output")
                .build();

        final DistcpAction parent = DistcpActionBuilder.create()
                .withResourceManager(getJobTrackerUri())
                .withNameNode(getNameNodeUri())
                .withPrepare(prepare)
                .withConfigProperty("mapred.job.queue.name", "default")
                .withJavaOpts("-Dopt1 -Dopt2")
                .withArg("arg1")
                .build();

        //  We are reusing the definition of parent and only modifying and adding what is different.
        final DistcpAction leftChild = DistcpActionBuilder.createFromExistingAction(parent)
                .withParent(parent)
                .withoutArg("arg1")
                .withArg("arg2")
                .build();

        final DistcpAction rightChild = DistcpActionBuilder.createFromExistingAction(leftChild)
                .withoutArg("arg2")
                .withArg("arg3")
                .build();

        final Workflow workflow = new WorkflowBuilder()
                .withName("simple-distcp-example")
                .withDagContainingNode(parent).build();

        SshActionBuilder.create()
                .withParent(leftChild)
                .withParent(rightChild)
                .withHost("localhost")
                .withCommand("pwd")
                .build();

        final String xml = WorkflowMarshaller.marshal(workflow);

        System.out.println(xml);

        GraphVisualization.workflowToPng(workflow, "simple-distcp-example-workflow.png");

        final Graph intermediateGraph = new Graph(workflow);

        GraphVisualization.graphToPng(intermediateGraph, "simple-distcp-example-graph.png");

        log.debug("Workflow XML is:\n{0}", xml);

        validate(xml);
    }
}
