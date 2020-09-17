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

import org.apache.oozie.fluentjob.api.generated.workflow.FORK;
import org.apache.oozie.fluentjob.api.generated.workflow.FORKTRANSITION;
import org.apache.oozie.fluentjob.api.dag.Decision;
import org.apache.oozie.fluentjob.api.dag.DecisionJoin;
import org.apache.oozie.fluentjob.api.dag.ExplicitNode;
import org.apache.oozie.fluentjob.api.dag.Fork;
import org.apache.oozie.fluentjob.api.dag.NodeBase;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestForkMapping extends TestControlNodeMappingBase {
    @Test
    public void testMappingFork() {
        final String name = "fork";
        final Fork fork = new Fork(name);

        final NodeBase child1 = new ExplicitNode("child1", null);
        final NodeBase child2 = new ExplicitNode("child2", null);

        child1.addParent(fork);
        child2.addParent(fork);

        final FORK mappedFork = DozerBeanMapperSingleton.instance().map(fork, FORK.class);

        assertEquals(name, mappedFork.getName());

        final List<FORKTRANSITION> transitions = mappedFork.getPath();
        assertEquals(child1.getName(), transitions.get(0).getStart());
        assertEquals(child2.getName(), transitions.get(1).getStart());
    }

    @Test
    public void testMappingForkWithDecisionJoin() {
        final String childName = "child";
        final Fork fork = new Fork("fork");

        final NodeBase decisionJoin = new DecisionJoin("decisionJoin", new Decision("decision"));
        decisionJoin.addParent(fork);

        final NodeBase child = new ExplicitNode(childName, null);
        child.addParent(decisionJoin);

        final FORK mappedFork = DozerBeanMapperSingleton.instance().map(fork, FORK.class);

        assertEquals(childName, mappedFork.getPath().get(0).getStart());
    }
}
