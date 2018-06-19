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

import org.apache.oozie.fluentjob.api.action.LauncherBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestGlobalBuilder {

    public static final String DEFAULT = "default";

    @Test
    public void testAfterCopyFieldsAreSetCorrectly() {
        final Global original = GlobalBuilder.create()
                .withResourceManager(DEFAULT)
                .withNameNode(DEFAULT)
                .withJobXml(DEFAULT)
                .withConfigProperty("key1", "value1")
                .withLauncher(new LauncherBuilder()
                        .withMemoryMb(1024L)
                        .withVCores(1L)
                        .build())
                .build();

        assertEquals(DEFAULT, original.getResourceManager());
        assertEquals(DEFAULT, original.getNameNode());
        assertEquals(DEFAULT, original.getJobXmls().get(0));
        assertEquals("value1", original.getConfigProperty("key1"));
        assertEquals(1024L, original.getLauncher().getMemoryMb());
        assertEquals(1L, original.getLauncher().getVCores());

        final Global copied = GlobalBuilder.createFromExisting(original)
                .withoutJobXml(DEFAULT)
                .withConfigProperty("key1", null)
                .build();

        assertEquals(DEFAULT, copied.getResourceManager());
        assertEquals(DEFAULT, copied.getNameNode());
        assertEquals(0, copied.getJobXmls().size());
        assertNull(copied.getConfigProperty("key1"));
        assertEquals(1024L, copied.getLauncher().getMemoryMb());
        assertEquals(1L, copied.getLauncher().getVCores());
    }
}