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

import org.apache.oozie.fluentjob.api.generated.workflow.GLOBAL;
import org.apache.oozie.fluentjob.api.action.LauncherBuilder;
import org.apache.oozie.fluentjob.api.generated.workflow.GLOBAL;
import org.apache.oozie.fluentjob.api.workflow.Global;
import org.apache.oozie.fluentjob.api.workflow.GlobalBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestGlobalMapping {
    private static final String DEFAULT = "default";

    @Test
    public void testMappingGlobal() {
        final Global source = GlobalBuilder.create()
                .withResourceManager(DEFAULT)
                .withNameNode(DEFAULT)
                .withJobXml(DEFAULT)
                .withConfigProperty("key1", "value1")
                .withLauncher(new LauncherBuilder()
                        .withMemoryMb(1024L)
                        .withVCores(1L)
                        .build())
                .build();

        final GLOBAL destination = DozerBeanMapperSingleton.instance().map(source, GLOBAL.class);

        assertEquals(DEFAULT, destination.getResourceManager());
        assertEquals(DEFAULT, destination.getNameNode());
        assertEquals(DEFAULT, destination.getJobXml().get(0));
        assertEquals("key1", destination.getConfiguration().getProperty().get(0).getName());
        assertEquals("value1", destination.getConfiguration().getProperty().get(0).getValue());
        assertEquals(1024L, destination.getLauncher().getMemoryMbOrVcoresOrJavaOpts().get(0).getValue());
        assertEquals(1L, destination.getLauncher().getMemoryMbOrVcoresOrJavaOpts().get(1).getValue());
    }
}
