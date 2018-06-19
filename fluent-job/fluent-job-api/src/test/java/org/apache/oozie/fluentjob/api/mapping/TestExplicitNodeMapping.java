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

import org.apache.oozie.fluentjob.api.generated.workflow.ACTION;
import org.apache.oozie.fluentjob.api.action.MapReduceAction;
import org.apache.oozie.fluentjob.api.action.MapReduceActionBuilder;
import org.apache.oozie.fluentjob.api.dag.End;
import org.apache.oozie.fluentjob.api.dag.ExplicitNode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestExplicitNodeMapping {
    @Test
    public void testMappingExplicitNode() {
        final MapReduceAction mrAction = MapReduceActionBuilder.create().withName("map-reduce-action").build();
        final ExplicitNode node = new ExplicitNode(mrAction.getName(), mrAction);

        final End end = new End("end");

        end.addParent(node);

        final ACTION action = DozerBeanMapperSingleton.instance().map(node, ACTION.class);

        assertEquals(mrAction.getName(), action.getName());
        assertEquals(end.getName(), action.getOk().getTo());
        assertNotNull(action.getMapReduce());
    }
}
