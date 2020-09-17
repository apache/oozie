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

package org.apache.oozie.fluentjob.api.factory;

import org.apache.oozie.fluentjob.api.workflow.Workflow;
import org.apache.oozie.fluentjob.api.workflow.WorkflowBuilder;
import org.apache.oozie.fluentjob.api.action.ShellAction;
import org.apache.oozie.fluentjob.api.action.ShellActionBuilder;

/**
 * An easily understandable {@link WorkflowFactory} that creates a {@link Workflow} instance consisting of
 * three {@link ShellAction}s, the two latter depending conditionally on the output of the former.
 * <p>
 * It demonstrates how the Jobs API can be used to create dynamic {@code Workflow} artifacts, as well as
 * serves as an input for {@code TestOozieCLI} methods that check, submit or run Jobs API {@code .jar} files.
 */
public class SimpleWorkflowFactory implements WorkflowFactory {

    @Override
    public Workflow create() {
        final ShellAction parent = ShellActionBuilder.create()
                .withName("parent")
                .withResourceManager("${resourceManager}")
                .withNameNode("${nameNode}")
                .withConfigProperty("mapred.job.queue.name", "${queueName}")
                .withArgument("my_output=Hello Oozie")
                .withExecutable("echo")
                .withCaptureOutput(true)
                .build();

        ShellActionBuilder.createFromExistingAction(parent)
                .withName("happy-path")
                .withParentWithCondition(parent, "${wf:actionData('parent')['my_output'] eq 'Hello Oozie'}")
                .withoutArgument("my_output=Hello Oozie")
                .withArgument("Happy path")
                .withCaptureOutput(null)
                .build();

        ShellActionBuilder.createFromExistingAction(parent)
                .withName("sad-path")
                .withParentDefaultConditional(parent)
                .withArgument("Sad path")
                .build();

        return new WorkflowBuilder()
                .withName("shell-example")
                .withDagContainingNode(parent).build();
    }
}
