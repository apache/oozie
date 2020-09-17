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

import org.apache.oozie.fluentjob.api.generated.workflow.JOIN;
import org.apache.oozie.fluentjob.api.dag.Decision;
import org.apache.oozie.fluentjob.api.dag.DecisionJoin;
import org.apache.oozie.fluentjob.api.dag.ExplicitNode;
import org.apache.oozie.fluentjob.api.dag.Fork;
import org.apache.oozie.fluentjob.api.dag.Join;
import org.apache.oozie.fluentjob.api.dag.NodeBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestJoinMapping extends TestControlNodeMappingBase {
    @Test
    public void testMappingJoin() {
        final String joinName = "join";
        final String childName = "child";
        final Join join = new Join(joinName, new Fork("fork"));

        final NodeBase child = new ExplicitNode(childName, null);

        child.addParent(join);

        final JOIN mappedJoin = DozerBeanMapperSingleton.instance().map(join, JOIN.class);

        assertEquals(joinName, mappedJoin.getName());
        assertEquals(childName, mappedJoin.getTo());
    }

    @Test
    public void testMappingJoinWithDecisionJoin() {
        final String childName = "child";
        final Join join = new Join("join", new Fork("fork"));

        final NodeBase decisionJoin = new DecisionJoin("decisionJoin", new Decision("decision"));
        decisionJoin.addParent(join);

        final NodeBase child = new ExplicitNode(childName, null);
        child.addParent(decisionJoin);

        final JOIN mappedJoin = DozerBeanMapperSingleton.instance().map(join, JOIN.class);

        assertEquals(childName, mappedJoin.getTo());
    }
}
