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
import org.apache.oozie.fluentjob.api.action.Prepare;
import org.apache.oozie.fluentjob.api.action.PrepareBuilder;
import org.apache.oozie.fluentjob.api.action.SparkAction;
import org.apache.oozie.fluentjob.api.action.SparkActionBuilder;
import org.apache.oozie.fluentjob.api.dag.Graph;
import org.apache.oozie.fluentjob.api.serialization.WorkflowMarshaller;
import org.apache.oozie.fluentjob.api.workflow.Workflow;
import org.apache.oozie.fluentjob.api.workflow.WorkflowBuilder;
import org.apache.oozie.test.WorkflowTestCase;

import javax.xml.bind.JAXBException;
import java.io.IOException;

public class TestSparkAction extends WorkflowTestCase {
    public void testForkedSparkActions() throws IOException, JAXBException, OozieClientException {
        final Prepare prepare = new PrepareBuilder()
                .withDelete("hdfs://localhost:8020/user/${wf:user()}/examples/output")
                .build();

        final SparkAction parent = SparkActionBuilder.create()
                .withResourceManager(getJobTrackerUri())
                .withNameNode(getNameNodeUri())
                .withPrepare(prepare)
                .withConfigProperty("mapred.job.queue.name", "default")
                .withArg("inputpath=hdfs://localhost/input/file.txt")
                .withArg("value=1")
                .withMaster("yarn")
                .withMode("cluster")
                .withActionName("Spark Example")
                .withActionClass("org.apache.spark.examples.mllib.JavaALS")
                .withJar("/lib/spark-examples_2.10-1.1.0.jar")
                .withSparkOpts("--executor-memory 20G --num-executors 50")
                .build();

        //  We are reusing the definition of parent and only modifying and adding what is different.
        final SparkAction leftChild = SparkActionBuilder.createFromExistingAction(parent)
                .withParent(parent)
                .withoutArg("value=1")
                .withArg("value=3")
                .build();

        SparkActionBuilder.createFromExistingAction(leftChild)
                .withoutArg("value=2")
                .withArg("value=3")
                .build();

        final Workflow workflow = new WorkflowBuilder()
                .withName("simple-spark-example")
                .withDagContainingNode(parent).build();

        final String xml = WorkflowMarshaller.marshal(workflow);

        System.out.println(xml);

        GraphVisualization.workflowToPng(workflow, "simple-spark-example-workflow.png");

        final Graph intermediateGraph = new Graph(workflow);

        GraphVisualization.graphToPng(intermediateGraph, "simple-spark-example-graph.png");

        log.debug("Workflow XML is:\n{0}", xml);

        validate(xml);
    }
}
