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

import org.apache.oozie.fluentjob.api.action.GitAction;
import org.apache.oozie.fluentjob.api.action.GitActionBuilder;
import org.apache.oozie.fluentjob.api.action.Prepare;
import org.apache.oozie.fluentjob.api.action.PrepareBuilder;
import org.apache.oozie.fluentjob.api.factory.WorkflowFactory;
import org.apache.oozie.fluentjob.api.workflow.Workflow;
import org.apache.oozie.fluentjob.api.workflow.WorkflowBuilder;

/**
 * This {@link WorkflowFactory} generates a similar workflow definition to {@code apps/git/workflow.xml}.
 */
public class Git implements WorkflowFactory {
    @Override
    public Workflow create() {
        final Prepare prepare = new PrepareBuilder()
                .withDelete("${nameNode}/user/${wf:user()}/${examplesRoot}/output-data/git/oozie")
                .build();

        final GitAction parent = GitActionBuilder.create()
                .withResourceManager("${resourceManager}")
                .withNameNode("${nameNode}")
                .withPrepare(prepare)
                .withDestinationUri("${nameNode}/user/${wf:user()}/${examplesRoot}/output-data/git/oozie")
                .withGitUri("https://github.com/apache/oozie")
                .build();

        final Workflow workflow = new WorkflowBuilder()
                .withName("git-example")
                .withDagContainingNode(parent).build();

        return workflow;
    }
}
