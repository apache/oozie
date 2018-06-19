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

import org.apache.oozie.fluentjob.api.generated.workflow.DECISION;
import org.apache.oozie.fluentjob.api.generated.workflow.DEFAULT;
import org.apache.oozie.fluentjob.api.Condition;
import org.apache.oozie.fluentjob.api.generated.workflow.CASE;
import org.apache.oozie.fluentjob.api.generated.workflow.SWITCH;
import org.apache.oozie.fluentjob.api.dag.Decision;
import org.apache.oozie.fluentjob.api.dag.DecisionJoin;
import org.apache.oozie.fluentjob.api.dag.ExplicitNode;
import org.apache.oozie.fluentjob.api.dag.NodeBase;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestDecisionMapping extends TestControlNodeMappingBase {
    @Test
    public void testMappingDecision() {
        final String name = "decision";
        final Decision decision = new Decision(name);

        final NodeBase child1 = new ExplicitNode("child1", null);
        final NodeBase child2 = new ExplicitNode("child2", null);
        final NodeBase defaultChild = new ExplicitNode("defaultChild", null);

        final String condition1String = "condition1";
        final String condition2String = "condition2";

        child1.addParentWithCondition(decision, Condition.actualCondition(condition1String));
        child2.addParentWithCondition(decision, Condition.actualCondition(condition2String));
        defaultChild.addParentDefaultConditional(decision);

        final DECISION mappedDecision = DozerBeanMapperSingleton.instance().map(decision, DECISION.class);

        assertEquals(name, mappedDecision.getName());

        final SWITCH decisionSwitch = mappedDecision.getSwitch();
        final List<CASE> cases = decisionSwitch.getCase();

        assertEquals(2, cases.size());

        assertEquals(child1.getName(), cases.get(0).getTo());
        assertEquals(condition1String, cases.get(0).getValue());

        assertEquals(child2.getName(), cases.get(1).getTo());
        assertEquals(condition2String, cases.get(1).getValue());

        final DEFAULT decisionDefault = decisionSwitch.getDefault();
        assertEquals(defaultChild.getName(), decisionDefault.getTo());
    }

    @Test
    public void testMappingDecisionWithoutDefaultThrows() {
        final String name = "decision";
        final Decision decision = new Decision(name);

        final NodeBase child1 = new ExplicitNode("child1", null);
        final NodeBase child2 = new ExplicitNode("child2", null);

        final Condition condition1 = Condition.actualCondition("condition1");
        final Condition condition2 = Condition.actualCondition("condition2");

        child1.addParentWithCondition(decision, condition1);
        child2.addParentWithCondition(decision, condition2);

        expectedException.expect(IllegalStateException.class);
        DozerBeanMapperSingleton.instance().map(decision, DECISION.class);
    }

    @Test
    public void testMappingDecisionWithDecisionJoin() {
        final String child1Name = "child1";
        final String child2Name = "child2";
        final Decision decision = new Decision("decision");

        final NodeBase decisionJoin1 = new DecisionJoin("decisionJoin1", new Decision("decision"));
        decisionJoin1.addParentWithCondition(decision, Condition.actualCondition("condition"));

        final NodeBase decisionJoin2 = new DecisionJoin("decisionJoin2", new Decision("decision2"));
        decisionJoin2.addParentDefaultConditional(decision);

        final NodeBase child1 = new ExplicitNode(child1Name, null);
        child1.addParent(decisionJoin1);

        final NodeBase child2 = new ExplicitNode(child2Name, null);
        child2.addParent(decisionJoin2);

        final DECISION mappedDecision = DozerBeanMapperSingleton.instance().map(decision, DECISION.class);

        assertEquals(child1Name, mappedDecision.getSwitch().getCase().get(0).getTo());
        assertEquals(child2Name, mappedDecision.getSwitch().getDefault().getTo());
    }
}
