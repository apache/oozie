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

import org.apache.oozie.fluentjob.api.generated.workflow.START;
import org.apache.oozie.fluentjob.api.dag.Decision;
import org.apache.oozie.fluentjob.api.dag.DecisionJoin;
import org.apache.oozie.fluentjob.api.dag.ExplicitNode;
import org.apache.oozie.fluentjob.api.dag.NodeBase;
import org.apache.oozie.fluentjob.api.dag.Start;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestStartMapping extends TestControlNodeMappingBase {

    @Test
    public void testMappingStart() {
        final String childName = "child";
        final Start start = new Start("start");
        final NodeBase child = new ExplicitNode(childName, null);

        child.addParent(start);

        final START mappedStart = DozerBeanMapperSingleton.instance().map(start, START.class);

        assertEquals(childName, mappedStart.getTo());
    }

    @Test
    public void testMappingStartWithDecisionJoin() {
        final String childName = "child";
        final Start start = new Start("start");

        final NodeBase decisionJoin = new DecisionJoin("decisionJoin", new Decision("decision"));
        decisionJoin.addParent(start);

        final NodeBase child = new ExplicitNode(childName, null);
        child.addParent(decisionJoin);

        final START mappedStart = DozerBeanMapperSingleton.instance().map(start, START.class);

        assertEquals(childName, mappedStart.getTo());
    }
}
