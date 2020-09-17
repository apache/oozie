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

package org.apache.oozie.fluentjob.api.action;

import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestDistcpActionBuilder extends TestNodeBuilderBaseImpl<DistcpAction, DistcpActionBuilder> {
    private static final String NAME = "distcp-name";
    private static final String RESOURCE_MANAGER = "${resourceManager}";
    private static final String NAME_NODE = "${nameNode}";
    private static final String EXAMPLE_DIR = "/path/to/directory";
    private static final String[] ARGS = {"arg1", "arg2", "arg3"};

    private static final String MAPRED_JOB_QUEUE_NAME = "mapred.job.queue.name";
    private static final String DEFAULT = "default";

    @Override
    protected DistcpActionBuilder getBuilderInstance() {
        return DistcpActionBuilder.create();
    }

    @Override
    protected DistcpActionBuilder getBuilderInstance(final DistcpAction action) {
        return DistcpActionBuilder.createFromExistingAction(action);
    }

    @Test
    public void testResourceManagerAdded() {
        final DistcpActionBuilder builder = getBuilderInstance();
        builder.withResourceManager(RESOURCE_MANAGER);

        final DistcpAction action = builder.build();
        assertEquals(RESOURCE_MANAGER, action.getResourceManager());
    }

    @Test
    public void testNameNodeAdded() {
        final DistcpActionBuilder builder = getBuilderInstance();
        builder.withNameNode(NAME_NODE);

        final DistcpAction action = builder.build();
        assertEquals(NAME_NODE, action.getNameNode());
    }

    @Test
    public void testPrepareAdded() {
        final DistcpActionBuilder builder = getBuilderInstance();
        builder.withPrepare(new PrepareBuilder().withDelete(EXAMPLE_DIR).build());

        final DistcpAction action = builder.build();
        assertEquals(EXAMPLE_DIR, action.getPrepare().getDeletes().get(0).getPath());
    }

    @Test
    public void testSameConfigPropertyAddedTwiceThrows() {
        final DistcpActionBuilder builder = getBuilderInstance();
        builder.withConfigProperty(MAPRED_JOB_QUEUE_NAME, DEFAULT);

        expectedException.expect(IllegalStateException.class);
        builder.withConfigProperty(MAPRED_JOB_QUEUE_NAME, DEFAULT);
    }

    @Test
    public void testJavaOptsAdded() {
        final DistcpActionBuilder builder = getBuilderInstance();
        builder.withJavaOpts("-Dopt1 -Dopt2");

        final DistcpAction action = builder.build();
        assertEquals("-Dopt1 -Dopt2", action.getJavaOpts());
    }

    @Test
    public void testSeveralArgsAdded() {
        final DistcpActionBuilder builder = getBuilderInstance();

        for (final String arg : ARGS) {
            builder.withArg(arg);
        }

        final DistcpAction action = builder.build();

        final List<String> argList = action.getArgs();
        assertEquals(ARGS.length, argList.size());

        for (int i = 0; i < ARGS.length; ++i) {
            assertEquals(ARGS[i], argList.get(i));
        }
    }

    @Test
    public void testRemoveArgs() {
        final DistcpActionBuilder builder = getBuilderInstance();

        for (final String file : ARGS) {
            builder.withArg(file);
        }

        builder.withoutArg(ARGS[0]);

        final DistcpAction action = builder.build();

        final List<String> argList = action.getArgs();
        final String[] remainingArgs = Arrays.copyOfRange(ARGS, 1, ARGS.length);
        assertEquals(remainingArgs.length, argList.size());

        for (int i = 0; i < remainingArgs.length; ++i) {
            assertEquals(remainingArgs[i], argList.get(i));
        }
    }

    @Test
    public void testClearArgs() {
        final DistcpActionBuilder builder = getBuilderInstance();

        for (final String file : ARGS) {
            builder.withArg(file);
        }

        builder.clearArgs();

        final DistcpAction mrAction = builder.build();

        final List<String> argList = mrAction.getArgs();
        assertEquals(0, argList.size());
    }
    @Test
    public void testFromExistingActionDistcpSpecific() {
        final DistcpActionBuilder builder = getBuilderInstance();

        builder.withName(NAME)
                .withNameNode(NAME_NODE)
                .withConfigProperty(MAPRED_JOB_QUEUE_NAME, DEFAULT)
                .withArg(ARGS[0])
                .withArg(ARGS[1]);

        final DistcpAction action = builder.build();

        final DistcpActionBuilder fromExistingBuilder = getBuilderInstance(action);

        final String newName = "fromExisting_" + NAME;
        fromExistingBuilder.withName(newName)
                .withoutArg(ARGS[1])
                .withArg(ARGS[2]);

        final DistcpAction modifiedAction = fromExistingBuilder.build();

        assertEquals(newName, modifiedAction.getName());
        assertEquals(action.getNameNode(), modifiedAction.getNameNode());

        final Map<String, String> expectedConfiguration = new LinkedHashMap<>();
        expectedConfiguration.put(MAPRED_JOB_QUEUE_NAME, DEFAULT);
        assertEquals(expectedConfiguration, modifiedAction.getConfiguration());

        assertEquals(Arrays.asList(ARGS[0], ARGS[2]), modifiedAction.getArgs());
    }

    @Test
    public void testFromOtherAction() {
        final ShellAction parent = ShellActionBuilder.create()
                .withName("parent")
                .withExecutable("echo")
                .build();

        final ShellAction otherAction = ShellActionBuilder.createFromExistingAction(parent)
                .withName("shell")
                .withParent(parent)
                .withNameNode(NAME_NODE)
                .withConfigProperty(MAPRED_JOB_QUEUE_NAME, DEFAULT)
                .withArgument(ARGS[0])
                .withArgument(ARGS[1])
                .build();

        final String newName = "fromExisting_" + NAME;
        final DistcpAction fromOtherAction = DistcpActionBuilder.createFromExistingAction(otherAction)
                .withName(newName)
                .withoutArg(ARGS[1])
                .withArg(ARGS[2])
                .build();

        assertEquals(newName, fromOtherAction.getName());
        assertEquals(otherAction.getNameNode(), fromOtherAction.getNameNode());
        assertEquals(parent, fromOtherAction.getParentsWithoutConditions().get(0));

        final Map<String, String> expectedConfiguration = new LinkedHashMap<>();
        expectedConfiguration.put(MAPRED_JOB_QUEUE_NAME, DEFAULT);
        assertEquals(expectedConfiguration, fromOtherAction.getConfiguration());

        assertEquals(Arrays.asList(ARGS[0], ARGS[2]), fromOtherAction.getArgs());
    }
}
