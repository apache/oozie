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
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestCredentialsBuilder {

    @Test
    public void testCreate() {
        final Credentials credentials = CredentialsBuilder.create()
                .withCredential("hive2",
                        "hive",
                        Lists.newArrayList(
                                new ConfigurationEntry("jdbcUrl", "jdbc://localhost/hive")))
                .build();

        assertEquals("hive2", credentials.getCredentials().get(0).getName());
        assertEquals("hive", credentials.getCredentials().get(0).getType());
        assertEquals("jdbcUrl", credentials.getCredentials().get(0).getConfigurationEntries().get(0).getName());
        assertEquals("jdbc://localhost/hive", credentials.getCredentials().get(0).getConfigurationEntries().get(0).getValue());
    }

    @Test
    public void testCreateFromExisting() {
        final Credentials credentials = CredentialsBuilder.create()
                .withCredential("hive2",
                        "hive",
                        Lists.newArrayList(
                                new ConfigurationEntry("jdbcUrl", "jdbc://localhost/hive")))
                .build();

        final Credentials fromExisting = CredentialsBuilder.createFromExisting(credentials)
                .withCredential("hbase",
                        "hbase")
                .build();

        assertEquals("hive2", fromExisting.getCredentials().get(0).getName());
        assertEquals("hive", fromExisting.getCredentials().get(0).getType());
        assertEquals("jdbcUrl", fromExisting.getCredentials().get(0).getConfigurationEntries().get(0).getName());
        assertEquals("jdbc://localhost/hive", fromExisting.getCredentials().get(0).getConfigurationEntries().get(0).getValue());

        assertEquals("hbase", fromExisting.getCredentials().get(1).getName());
        assertEquals("hbase", fromExisting.getCredentials().get(1).getType());
        assertEquals(0, fromExisting.getCredentials().get(1).getConfigurationEntries().size());
    }
}