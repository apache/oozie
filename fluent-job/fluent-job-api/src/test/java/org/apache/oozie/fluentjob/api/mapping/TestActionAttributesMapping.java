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

import com.google.common.base.Strings;
import org.apache.oozie.fluentjob.api.action.MapReduceAction;
import org.apache.oozie.fluentjob.api.action.MapReduceActionBuilder;
import org.apache.oozie.fluentjob.api.dag.ExplicitNode;
import org.apache.oozie.fluentjob.api.generated.workflow.ACTION;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestActionAttributesMapping {

    private final SourceDataFactory factory = new SourceDataFactory();

    @Test
    public void testMappingNoCredentialsToAction() {
        final MapReduceAction source = MapReduceActionBuilder
                .create()
                .build();

        final ACTION target = DozerBeanMapperSingleton.instance().map(new ExplicitNode("explicitNode", source), ACTION.class);

        assertTrue(Strings.isNullOrEmpty(target.getCred()));
    }

    @Test
    public void testMappingOneCredentialToAction() {
        final MapReduceAction source = MapReduceActionBuilder
                .create()
                .withCredential(factory.createCredentials().getCredentials().get(0))
                .build();

        final ACTION target = DozerBeanMapperSingleton.instance().map(new ExplicitNode("explicitNode", source), ACTION.class);

        assertEquals("hbase", target.getCred());
    }

    @Test
    public void testMappingTwoCredentialsToSameAction() {
        final MapReduceAction source = MapReduceActionBuilder
                .create()
                .withCredential(factory.createCredentials().getCredentials().get(0))
                .withCredential(factory.createCredentials().getCredentials().get(1))
                .build();

        final ACTION target = DozerBeanMapperSingleton.instance().map(new ExplicitNode("explicitNode", source), ACTION.class);

        assertEquals("hbase,hive2", target.getCred());
    }

    @Test
    public void testMappingNoRetryAttributesToAction() {
        final MapReduceAction source = MapReduceActionBuilder
                .create()
                .build();

        final ACTION target = DozerBeanMapperSingleton.instance().map(new ExplicitNode("explicitNode", source), ACTION.class);

        assertNull(target.getRetryInterval());
        assertNull(target.getRetryMax());
        assertNull(target.getRetryPolicy());
    }

    @Test
    public void testMappingRetryAttributesToAction() {
        final MapReduceAction source = MapReduceActionBuilder
                .create()
                .withRetryInterval(1)
                .withRetryMax(3)
                .withRetryPolicy("retry-policy")
                .build();

        final ACTION target = DozerBeanMapperSingleton.instance().map(new ExplicitNode("explicitNode", source), ACTION.class);

        assertEquals("1", target.getRetryInterval());
        assertEquals("3", target.getRetryMax());
        assertEquals("retry-policy", target.getRetryPolicy());
    }
}
