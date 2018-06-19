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

import com.google.common.collect.Lists;
import org.apache.oozie.fluentjob.api.generated.workflow.CREDENTIALS;
import org.apache.oozie.fluentjob.api.workflow.ConfigurationEntry;
import org.apache.oozie.fluentjob.api.workflow.Credentials;
import org.apache.oozie.fluentjob.api.workflow.CredentialsBuilder;

import static org.junit.Assert.assertEquals;

class SourceDataFactory {

    Credentials createCredentials() {
        return CredentialsBuilder.create()
                .withCredential("hbase", "hbase")
                .withCredential("hive2", "hive2",
                        Lists.newArrayList(new ConfigurationEntry("jdbcUrl", "jdbc://localhost/hive2")))
                .build();
    }

    void assertCredentials(final CREDENTIALS destination) {
        assertEquals("hbase", destination.getCredential().get(0).getName());
        assertEquals("hbase", destination.getCredential().get(0).getType());
        assertEquals(0, destination.getCredential().get(0).getProperty().size());
        assertEquals("hive2", destination.getCredential().get(1).getName());
        assertEquals("hive2", destination.getCredential().get(1).getType());
        assertEquals("jdbcUrl", destination.getCredential().get(1).getProperty().get(0).getName());
        assertEquals("jdbc://localhost/hive2", destination.getCredential().get(1).getProperty().get(0).getValue());
    }
}
