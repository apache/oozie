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

package org.apache.oozie.example.fluentjob;

import org.apache.oozie.fluentjob.api.action.JavaAction;
import org.apache.oozie.fluentjob.api.action.JavaActionBuilder;

import org.apache.oozie.fluentjob.api.factory.WorkflowFactory;
import org.apache.oozie.fluentjob.api.workflow.GlobalBuilder;
import org.apache.oozie.fluentjob.api.workflow.Workflow;
import org.apache.oozie.fluentjob.api.workflow.WorkflowBuilder;

/**
 * This {@link WorkflowFactory} generates a workflow definition that has global section.
 */
public class Global implements WorkflowFactory {
    @Override
    public Workflow create() {
        final JavaAction parent = JavaActionBuilder.create()
                .withName("java-main-1")
                .withMainClass("org.apache.oozie.example.DemoJavaMain")
                .withArchive(
                        "${nameNode}/user/${wf:user()}/${examplesRoot}/apps/java-main/lib/oozie-examples-${projectVersion}.jar")
                .withArg("Hello")
                .withArg("Oozie!")
                .build();
        JavaActionBuilder.createFromExistingAction(parent)
                .withName("java-main-2")
                .withoutArg("Oozie!")
                .withArg("Oozie2!")
                .withParent(parent)
                .build();

        final Workflow workflow = new WorkflowBuilder()
                .withName("workflow-with-global")
                .withGlobal(GlobalBuilder.create()
                        .withResourceManager("${resourceManager}")
                        .withNameNode("${nameNode}")
                        .withConfigProperty("mapred.job.queue.name", "${queueName}")
                        .build())
                .withDagContainingNode(parent).build();

        return workflow;
    }
}
