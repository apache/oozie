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

import org.apache.oozie.fluentjob.api.action.EmailActionBuilder;
import org.apache.oozie.fluentjob.api.action.ErrorHandler;
import org.apache.oozie.fluentjob.api.action.JavaAction;
import org.apache.oozie.fluentjob.api.action.JavaActionBuilder;
import org.apache.oozie.fluentjob.api.factory.WorkflowFactory;
import org.apache.oozie.fluentjob.api.workflow.Workflow;
import org.apache.oozie.fluentjob.api.workflow.WorkflowBuilder;

/**
 * This {@link WorkflowFactory} generates the exact same workflow definition as {@code apps/java-main/workflow.xml}.
 */
public class JavaMain implements WorkflowFactory {
    @Override
    public Workflow create() {
        final JavaAction parent = JavaActionBuilder.create()
                .withName("java-main")
                .withResourceManager("${resourceManager}")
                .withNameNode("${nameNode}")
                .withConfigProperty("mapred.job.queue.name", "${queueName}")
                .withMainClass("org.apache.oozie.example.DemoJavaMain")
                .withArchive(
                        "${nameNode}/user/${wf:user()}/${examplesRoot}/apps/java-main/lib/oozie-examples-${projectVersion}.jar")
                .withArg("Hello")
                .withArg("Oozie!")
                .withErrorHandler(ErrorHandler.buildAsErrorHandler(EmailActionBuilder.create()
                        .withName("email-on-error")
                        .withRecipient("somebody@apache.org")
                        .withSubject("Workflow error")
                        .withBody("Shell action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]")))
                .build();

        final Workflow workflow = new WorkflowBuilder()
                .withName("java-main-example")
                .withDagContainingNode(parent).build();

        return workflow;
    }
}
