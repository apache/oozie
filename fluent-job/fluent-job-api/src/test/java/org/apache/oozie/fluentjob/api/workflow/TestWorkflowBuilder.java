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

package org.apache.oozie.fluentjob.api.workflow;

import com.google.common.collect.Lists;
import org.apache.oozie.fluentjob.api.action.MapReduceActionBuilder;
import org.apache.oozie.fluentjob.api.action.Node;
import org.apache.oozie.fluentjob.api.action.ShellAction;
import org.apache.oozie.fluentjob.api.action.ShellActionBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestWorkflowBuilder {
    private static final String NAME = "workflow-name";

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testAddName() {
        final WorkflowBuilder builder = new WorkflowBuilder();
        builder.withName(NAME);

        final Workflow workflow = builder.build();

        assertEquals(NAME, workflow.getName());
    }

    @Test
    public void testAddDagTrivial() {
        final Node mrAction1 = MapReduceActionBuilder.create()
                .withName("mr1")
                .withNameNode("${nameNode}")
                .withResourceManager("${resourceManager}")
                .withConfigProperty("mapred.output.dir", "${outputDir}")
                .build();

        final Node mrAction2 = MapReduceActionBuilder.create()
                .withName("mr2")
                .withNameNode("${nameNode}")
                .withResourceManager("${resourceManager}")
                .withConfigProperty("mapred.output.dir", "${outputDir}")
                .build();

        final WorkflowBuilder builder = new WorkflowBuilder();

        builder.withDagContainingNode(mrAction1)
                .withDagContainingNode(mrAction2);

        final Workflow workflow = builder.build();

        final Set<Node> expectedRoots = new HashSet<>(Arrays.asList(mrAction1, mrAction2));
        assertEquals(expectedRoots, workflow.getRoots());

        final Set<Node> expectedNodes = new HashSet<>(Arrays.asList(mrAction1, mrAction2));
        assertEquals(expectedNodes, workflow.getNodes());
    }

    @Test
    public void testAddDagFindRoots() {
        final Node mrAction1 = MapReduceActionBuilder.create()
                .withName("mr1")
                .build();

        final Node mrAction2 = MapReduceActionBuilder.create()
                .withName("mr2")
                .build();

        final Node mrAction3 = MapReduceActionBuilder.create()
                .withName("mr3")
                .withParent(mrAction1)
                .withParent(mrAction2)
                .build();

        final WorkflowBuilder builder = new WorkflowBuilder();

        builder.withDagContainingNode(mrAction3);

        final Workflow workflow = builder.build();

        final Set<Node> expectedRoots = new HashSet<>(Arrays.asList(mrAction1, mrAction2));
        assertEquals(expectedRoots, workflow.getRoots());

        final Set<Node> expectedNodes = new HashSet<>(Arrays.asList(mrAction1, mrAction2, mrAction3));
        assertEquals(expectedNodes, workflow.getNodes());
    }

    @Test
    public void testAddDagThrowOnDuplicateNodeNames() {
        final Node mrAction = MapReduceActionBuilder.create()
                .withName("mr-action")
                .build();

        final Node mrActionWithTheSameName = MapReduceActionBuilder.create()
                .withName("mr-action")
                .build();

        final WorkflowBuilder builder = new WorkflowBuilder();
        builder.withName(NAME)
                .withDagContainingNode(mrAction)
                .withDagContainingNode(mrActionWithTheSameName);

        expectedException.expect(IllegalArgumentException.class);
        builder.build();
    }

    @Test
    public void testAddDagWithConditionalChildrenAndConditionalParents() {
        final String condition = "condition";

        final Node mrAction1 = MapReduceActionBuilder.create()
                .withName("mr1")
                .build();

        final Node mrAction2 = MapReduceActionBuilder.create()
                .withName("mr2")
                .build();

        final Node mrAction3 = MapReduceActionBuilder.create()
                .withName("mr3")
                .withParentWithCondition(mrAction1, condition)
                .withParent(mrAction2)
                .build();
        final Node mrAction4 = MapReduceActionBuilder.create()
                .withName("mr4")
                .withParentWithCondition(mrAction3, condition)
                .build();
        final Node mrAction5 = MapReduceActionBuilder.create()
                .withName("mr5")
                .withParentWithCondition(mrAction3, condition)
                .build();

        final WorkflowBuilder builder = new WorkflowBuilder();

        builder.withDagContainingNode(mrAction3);

        final Workflow workflow = builder.build();

        final Set<Node> expectedRoots = new HashSet<>(Arrays.asList(mrAction1, mrAction2));
        assertEquals(expectedRoots, workflow.getRoots());

        final Set<Node> expectedNodes = new HashSet<>(Arrays.asList(mrAction1, mrAction2, mrAction3, mrAction4, mrAction5));
        assertEquals(expectedNodes, workflow.getNodes());
    }

    @Test
    public void testAddMixedParameters() {
        final Workflow workflow = new WorkflowBuilder()
                .withParameter("name1", "value1")
                .withParameter("name2", "value2", "description2")
                .build();

        assertEquals("name1", workflow.getParameters().getParameters().get(0).getName());
        assertEquals("value1", workflow.getParameters().getParameters().get(0).getValue());
        assertNull(workflow.getParameters().getParameters().get(0).getDescription());
        assertEquals("name2", workflow.getParameters().getParameters().get(1).getName());
        assertEquals("value2", workflow.getParameters().getParameters().get(1).getValue());
        assertEquals("description2", workflow.getParameters().getParameters().get(1).getDescription());
    }

    @Test
    public void testAddGlobal() {
        final Workflow workflow = new WorkflowBuilder()
                .withGlobal(GlobalBuilder.create()
                        .withConfigProperty("key1", "value1")
                        .build())
                .build();

        assertEquals("value1", workflow.getGlobal().getConfigProperty("key1"));
    }

    @Test
    public void testAddCredentials() {
        final Workflow workflow = new WorkflowBuilder()
                .withCredentials(CredentialsBuilder.create()
                        .withCredential("hbase", "hbase")
                        .build())
                .build();

        assertEquals("hbase", workflow.getCredentials().getCredentials().get(0).getName());
        assertEquals("hbase", workflow.getCredentials().getCredentials().get(0).getType());
    }

    @Test
    public void testOmittedCredentialsAreAutogenerated() {
        final Credential hbaseCredential = CredentialBuilder.create()
                .withName("hbase")
                .withType("hbase")
                .build();

        final ShellAction hbaseAction = ShellActionBuilder.create()
                .withName("hbase")
                .withCredential(hbaseCredential)
                .build();

        final Workflow workflow = new WorkflowBuilder()
                .withDagContainingNode(hbaseAction)
                .build();

        assertEquals(1, workflow.getCredentials().getCredentials().size());
        assertEquals("hbase", workflow.getCredentials().getCredentials().get(0).getName());
        assertEquals("hbase", workflow.getCredentials().getCredentials().get(0).getType());
    }

    @Test
    public void testOverrideCredentialsPreviouslyAddedInActionsOnWorkflowLevel() {
        final Credential hbaseCredential = CredentialBuilder.create()
                .withName("hbase")
                .withType("hbase")
                .build();

        final ShellAction hbaseAction = ShellActionBuilder.create()
                .withName("hbase")
                .withCredential(hbaseCredential)
                .build();

        final Workflow workflow = new WorkflowBuilder()
                .withDagContainingNode(hbaseAction)
                .withCredentials(CredentialsBuilder.create()
                        .withCredential("hive2", "hive2", Lists.newArrayList(
                                new ConfigurationEntry("jdbcUrl", "jdbc://localhost/hive")))
                        .build())
                .build();

        assertEquals(1, workflow.getCredentials().getCredentials().size());
        assertEquals("hive2", workflow.getCredentials().getCredentials().get(0).getName());
        assertEquals("hive2", workflow.getCredentials().getCredentials().get(0).getType());
        assertEquals(1, workflow.getCredentials().getCredentials().get(0).getConfigurationEntries().size());
        assertEquals("jdbcUrl",
                workflow.getCredentials().getCredentials().get(0).getConfigurationEntries().get(0).getName());
        assertEquals("jdbc://localhost/hive",
                workflow.getCredentials().getCredentials().get(0).getConfigurationEntries().get(0).getValue());
    }
}
