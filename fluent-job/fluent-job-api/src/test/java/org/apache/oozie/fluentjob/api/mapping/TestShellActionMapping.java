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

package org.apache.oozie.fluentjob.api.mapping;

import org.apache.oozie.fluentjob.api.action.LauncherBuilder;
import org.apache.oozie.fluentjob.api.action.PrepareBuilder;
import org.apache.oozie.fluentjob.api.action.ShellAction;
import org.apache.oozie.fluentjob.api.action.ShellActionBuilder;
import org.apache.oozie.fluentjob.api.generated.action.shell.ACTION;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestShellActionMapping {

    public static final String DEFAULT = "default";

    @Test
    public void testMappingShellAction() {
        final String resourceManager = "${resourceManager}";
        final String nameNode = "${nameNode}";
        final List<String> args = Arrays.asList("arg1", "arg2");

        final ShellActionBuilder builder = ShellActionBuilder.create();

        builder.withResourceManager(resourceManager)
                .withNameNode(nameNode)
                .withPrepare(new PrepareBuilder()
                        .withDelete("/path/to/delete")
                        .withMkdir("/path/to/mkdir")
                        .build())
                .withLauncher(new LauncherBuilder()
                        .withMemoryMb(1024)
                        .withVCores(2)
                        .withQueue(DEFAULT)
                        .withSharelib(DEFAULT)
                        .withViewAcl(DEFAULT)
                        .withModifyAcl(DEFAULT)
                        .build())
                .withExecutable(DEFAULT)
                .withEnvironmentVariable(DEFAULT)
                .withCaptureOutput(true);

        for (final String arg : args) {
            builder.withArgument(arg);
        }

        builder.withConfigProperty("propertyName1", "propertyValue1")
                .withConfigProperty("propertyName2", "propertyValue2");

        final ShellAction action = builder.build();

        final ACTION shell = DozerBeanMapperSingleton.instance().map(action, ACTION.class);

        assertEquals(resourceManager, shell.getResourceManager());
        assertEquals(nameNode, shell.getNameNode());
        assertNotNull(shell.getPrepare());
        assertEquals("/path/to/delete", shell.getPrepare().getDelete().get(0).getPath());
        assertEquals("/path/to/mkdir", shell.getPrepare().getMkdir().get(0).getPath());
        assertNotNull(shell.getConfiguration());
        assertEquals(args, shell.getArgument());
        assertEquals(1024L, shell.getLauncher().getMemoryMbOrVcoresOrJavaOpts().get(0).getValue());
        assertEquals(2L, shell.getLauncher().getMemoryMbOrVcoresOrJavaOpts().get(1).getValue());
        assertEquals(DEFAULT, shell.getLauncher().getMemoryMbOrVcoresOrJavaOpts().get(2).getValue());
        assertEquals(DEFAULT, shell.getLauncher().getMemoryMbOrVcoresOrJavaOpts().get(3).getValue());
        assertEquals(DEFAULT, shell.getLauncher().getMemoryMbOrVcoresOrJavaOpts().get(4).getValue());
        assertEquals(DEFAULT, shell.getLauncher().getMemoryMbOrVcoresOrJavaOpts().get(5).getValue());
        assertEquals(DEFAULT, shell.getExec());
        assertEquals(DEFAULT, shell.getEnvVar().get(0));
        assertNotNull(shell.getCaptureOutput());
    }
}
