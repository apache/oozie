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

import org.apache.oozie.fluentjob.api.generated.action.sqoop.ACTION;
import org.apache.oozie.fluentjob.api.action.LauncherBuilder;
import org.apache.oozie.fluentjob.api.action.PrepareBuilder;
import org.apache.oozie.fluentjob.api.action.SqoopAction;
import org.apache.oozie.fluentjob.api.action.SqoopActionBuilder;
import org.apache.oozie.fluentjob.api.generated.action.sqoop.ACTION;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestSqoopActionMapping {

    public static final String DEFAULT = "default";

    @Test
    public void testMappingSqoopAction() {
        final String resourceManager = "${resourceManager}";
        final String nameNode = "${nameNode}";
        final List<String> args = Arrays.asList("arg1", "arg2");

        final SqoopActionBuilder builder = SqoopActionBuilder.create();

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
                .withCommand(DEFAULT);

        for (final String arg : args) {
            builder.withArgument(arg);
        }

        builder.withConfigProperty("propertyName1", "propertyValue1")
                .withConfigProperty("propertyName2", "propertyValue2");

        final SqoopAction action = builder.build();

        final ACTION sqoop = DozerBeanMapperSingleton.instance().map(action, ACTION.class);

        assertEquals(resourceManager, sqoop.getResourceManager());
        assertEquals(nameNode, sqoop.getNameNode());
        assertNotNull(sqoop.getPrepare());
        assertEquals("/path/to/delete", sqoop.getPrepare().getDelete().get(0).getPath());
        assertEquals("/path/to/mkdir", sqoop.getPrepare().getMkdir().get(0).getPath());
        assertNotNull(sqoop.getConfiguration());
        assertEquals(args, sqoop.getArg());
        assertEquals(1024L, sqoop.getLauncher().getMemoryMbOrVcoresOrJavaOpts().get(0).getValue());
        assertEquals(2L, sqoop.getLauncher().getMemoryMbOrVcoresOrJavaOpts().get(1).getValue());
        assertEquals(DEFAULT, sqoop.getLauncher().getMemoryMbOrVcoresOrJavaOpts().get(2).getValue());
        assertEquals(DEFAULT, sqoop.getLauncher().getMemoryMbOrVcoresOrJavaOpts().get(3).getValue());
        assertEquals(DEFAULT, sqoop.getLauncher().getMemoryMbOrVcoresOrJavaOpts().get(4).getValue());
        assertEquals(DEFAULT, sqoop.getLauncher().getMemoryMbOrVcoresOrJavaOpts().get(5).getValue());
        assertEquals(DEFAULT, sqoop.getCommand());
    }
}
