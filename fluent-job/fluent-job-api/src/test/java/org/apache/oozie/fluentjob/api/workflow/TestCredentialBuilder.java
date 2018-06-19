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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestCredentialBuilder {

    @Test
    public void testCreate() {
        final Credential credential = CredentialBuilder.create()
                .withName("hive2")
                .withType("hive")
                .withConfigurationEntry("jdbcUrl", "jdbc://localhost/hive")
                .build();

        assertEquals("hive2", credential.getName());
        assertEquals("hive", credential.getType());
        assertEquals("jdbcUrl", credential.getConfigurationEntries().get(0).getName());
        assertEquals("jdbc://localhost/hive", credential.getConfigurationEntries().get(0).getValue());
    }

    @Test
    public void testCreateFromExisting() {
        final Credential credential = CredentialBuilder.create()
                .withName("hive2")
                .withType("hive")
                .withConfigurationEntry("jdbcUrl", "jdbc://localhost/hive")
                .build();

        final Credential fromExisting = CredentialBuilder.createFromExisting(credential).build();

        assertEquals("hive2", fromExisting.getName());
        assertEquals("hive", fromExisting.getType());
        assertEquals("jdbcUrl", fromExisting.getConfigurationEntries().get(0).getName());
        assertEquals("jdbc://localhost/hive", fromExisting.getConfigurationEntries().get(0).getValue());
    }
}