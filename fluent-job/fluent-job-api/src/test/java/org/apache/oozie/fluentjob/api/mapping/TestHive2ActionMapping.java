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

import org.apache.oozie.fluentjob.api.action.Hive2Action;
import org.apache.oozie.fluentjob.api.action.Hive2ActionBuilder;
import org.apache.oozie.fluentjob.api.action.LauncherBuilder;
import org.apache.oozie.fluentjob.api.action.PrepareBuilder;
import org.apache.oozie.fluentjob.api.generated.action.hive2.ACTION;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestHive2ActionMapping {

    @Test
    public void testMappingHive2Action() {
        final String resourceManager = "${resourceManager}";
        final String nameNode = "${nameNode}";
        final List<String> args = Arrays.asList("arg1", "arg2");

        final Hive2ActionBuilder builder = Hive2ActionBuilder.create();

        builder.withResourceManager(resourceManager)
                .withNameNode(nameNode)
                .withPrepare(new PrepareBuilder()
                        .withDelete("/path/to/delete")
                        .withMkdir("/path/to/mkdir")
                        .build())
                .withLauncher(new LauncherBuilder()
                        .withMemoryMb(1024)
                        .withVCores(2)
                        .withQueue("default")
                        .withSharelib("default")
                        .withViewAcl("default")
                        .withModifyAcl("default")
                        .build());

        for (final String arg : args) {
            builder.withArg(arg);
        }

        builder.withConfigProperty("propertyName1", "propertyValue1")
                .withConfigProperty("propertyName2", "propertyValue2");

        final Hive2Action action = builder.build();

        final ACTION hive2 = DozerBeanMapperSingleton.instance().map(action, ACTION.class);

        assertEquals(resourceManager, hive2.getResourceManager());
        assertEquals(nameNode, hive2.getNameNode());
        assertNotNull(hive2.getPrepare());
        assertEquals("/path/to/delete", hive2.getPrepare().getDelete().get(0).getPath());
        assertEquals("/path/to/mkdir", hive2.getPrepare().getMkdir().get(0).getPath());
        assertNotNull(hive2.getConfiguration());
        assertEquals(args, hive2.getArgument());
        assertEquals(1024L, hive2.getLauncher().getMemoryMbOrVcoresOrJavaOpts().get(0).getValue());
        assertEquals(2L, hive2.getLauncher().getMemoryMbOrVcoresOrJavaOpts().get(1).getValue());
        assertEquals("default", hive2.getLauncher().getMemoryMbOrVcoresOrJavaOpts().get(2).getValue());
        assertEquals("default", hive2.getLauncher().getMemoryMbOrVcoresOrJavaOpts().get(3).getValue());
        assertEquals("default", hive2.getLauncher().getMemoryMbOrVcoresOrJavaOpts().get(4).getValue());
        assertEquals("default", hive2.getLauncher().getMemoryMbOrVcoresOrJavaOpts().get(5).getValue());
    }
}
