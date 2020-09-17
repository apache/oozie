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

public class TestShellActionBuilder extends TestNodeBuilderBaseImpl<ShellAction, ShellActionBuilder> {
    private static final String NAME = "shell-name";
    private static final String NAME_NODE = "${nameNode}";
    private static final String EXAMPLE_DIR = "/path/to/directory";
    private static final String[] ARGS = {"arg1", "arg2", "arg3"};
    private static final String MAPRED_JOB_QUEUE_NAME = "mapred.job.queue.name";
    private static final String DEFAULT = "default";
    private static final String RESOURCE_MANAGER = "${resourceManager}";
    private static final String PATH_TO_DELETE = "/path/to/delete";
    private static final String PATH_TO_MKDIR = "/path/to/mkdir";

    @Override
    protected ShellActionBuilder getBuilderInstance() {
        return ShellActionBuilder.create();
    }

    @Override
    protected ShellActionBuilder getBuilderInstance(final ShellAction action) {
        return ShellActionBuilder.createFromExistingAction(action);
    }

    @Test
    public void testResourceManagerAdded() {
        final ShellActionBuilder builder = getBuilderInstance();
        builder.withResourceManager(RESOURCE_MANAGER);

        final ShellAction action = builder.build();
        assertEquals(RESOURCE_MANAGER, action.getResourceManager());
    }

    @Test
    public void testNameNodeAdded() {
        final ShellActionBuilder builder = getBuilderInstance();
        builder.withNameNode(NAME_NODE);

        final ShellAction action = builder.build();
        assertEquals(NAME_NODE, action.getNameNode());
    }

    @Test
    public void testPrepareAdded() {
        final ShellActionBuilder builder = getBuilderInstance();
        builder.withPrepare(new PrepareBuilder().withDelete(EXAMPLE_DIR).build());

        final ShellAction action = builder.build();
        assertEquals(EXAMPLE_DIR, action.getPrepare().getDeletes().get(0).getPath());
    }

    @Test
    public void testSameConfigPropertyAddedTwiceThrows() {
        final ShellActionBuilder builder = getBuilderInstance();
        builder.withConfigProperty(MAPRED_JOB_QUEUE_NAME, DEFAULT);

        expectedException.expect(IllegalStateException.class);
        builder.withConfigProperty(MAPRED_JOB_QUEUE_NAME, DEFAULT);
    }

    @Test
    public void testSeveralArgumentsAdded() {
        final ShellActionBuilder builder = getBuilderInstance();

        for (final String arg : ARGS) {
            builder.withArgument(arg);
        }

        final ShellAction action = builder.build();

        final List<String> argList = action.getArguments();
        assertEquals(ARGS.length, argList.size());

        for (int i = 0; i < ARGS.length; ++i) {
            assertEquals(ARGS[i], argList.get(i));
        }
    }

    @Test
    public void testRemoveArguments() {
        final ShellActionBuilder builder = getBuilderInstance();

        for (final String file : ARGS) {
            builder.withArgument(file);
        }

        builder.withoutArgument(ARGS[0]);

        final ShellAction action = builder.build();

        final List<String> argList = action.getArguments();
        final String[] remainingArgs = Arrays.copyOfRange(ARGS, 1, ARGS.length);
        assertEquals(remainingArgs.length, argList.size());

        for (int i = 0; i < remainingArgs.length; ++i) {
            assertEquals(remainingArgs[i], argList.get(i));
        }
    }

    @Test
    public void testClearArguments() {
        final ShellActionBuilder builder = getBuilderInstance();

        for (final String file : ARGS) {
            builder.withArgument(file);
        }

        builder.clearArguments();

        final ShellAction action = builder.build();

        final List<String> argList = action.getArguments();
        assertEquals(0, argList.size());
    }

    @Test
    public void testFromExistingShellAction() {
        final ShellActionBuilder builder = getBuilderInstance();

        builder.withName(NAME)
                .withResourceManager(RESOURCE_MANAGER)
                .withNameNode(NAME_NODE)
                .withConfigProperty(MAPRED_JOB_QUEUE_NAME, DEFAULT)
                .withPrepare(new PrepareBuilder()
                        .withDelete(PATH_TO_DELETE)
                        .withMkdir(PATH_TO_MKDIR)
                        .build())
                .withLauncher(new LauncherBuilder()
                        .withMemoryMb(1024L)
                        .withVCores(2L)
                        .withQueue(DEFAULT)
                        .withSharelib(DEFAULT)
                        .withViewAcl(DEFAULT)
                        .withModifyAcl(DEFAULT)
                        .build())
                .withExecutable(DEFAULT)
                .withEnvironmentVariable(DEFAULT)
                .withArgument(ARGS[0])
                .withArgument(ARGS[1])
                .withArchive(DEFAULT)
                .withFile(DEFAULT)
                .withCaptureOutput(true);

        final ShellAction action = builder.build();

        final ShellActionBuilder fromExistingBuilder = getBuilderInstance(action);

        final String newName = "fromExisting_" + NAME;
        fromExistingBuilder.withName(newName)
                .withoutArgument(ARGS[1])
                .withArgument(ARGS[2]);

        final ShellAction modifiedAction = fromExistingBuilder.build();

        assertEquals(newName, modifiedAction.getName());
        assertEquals(action.getNameNode(), modifiedAction.getNameNode());

        final Map<String, String> expectedConfiguration = new LinkedHashMap<>();
        expectedConfiguration.put(MAPRED_JOB_QUEUE_NAME, DEFAULT);
        assertEquals(expectedConfiguration, modifiedAction.getConfiguration());

        assertEquals(Arrays.asList(ARGS[0], ARGS[2]), modifiedAction.getArguments());

        assertEquals(PATH_TO_DELETE, modifiedAction.getPrepare().getDeletes().get(0).getPath());
        assertEquals(PATH_TO_MKDIR, modifiedAction.getPrepare().getMkdirs().get(0).getPath());

        assertEquals(1024L, modifiedAction.getLauncher().getMemoryMb());
        assertEquals(2L, modifiedAction.getLauncher().getVCores());
        assertEquals(DEFAULT, modifiedAction.getLauncher().getQueue());
        assertEquals(DEFAULT, modifiedAction.getLauncher().getSharelib());
        assertEquals(DEFAULT, modifiedAction.getLauncher().getViewAcl());
        assertEquals(DEFAULT, modifiedAction.getLauncher().getModifyAcl());

        assertEquals(action.getExecutable(), modifiedAction.getExecutable());
        assertEquals(action.getEnvironmentVariables().get(0), modifiedAction.getEnvironmentVariables().get(0));
        assertEquals(action.isCaptureOutput(), modifiedAction.isCaptureOutput());
    }

    @Test
    public void testFromOtherAction() {
        final PigAction parent = PigActionBuilder.create()
                .withName("parent")
                .build();

        final PigAction otherAction = PigActionBuilder.createFromExistingAction(parent)
                .withName("pig")
                .withParent(parent)
                .build();

        final ShellAction fromOtherAction = ShellActionBuilder.createFromExistingAction(otherAction)
                .withName("shell")
                .build();

        assertEquals("shell", fromOtherAction.getName());
        assertEquals(parent, fromOtherAction.getParentsWithoutConditions().get(0));
    }
}