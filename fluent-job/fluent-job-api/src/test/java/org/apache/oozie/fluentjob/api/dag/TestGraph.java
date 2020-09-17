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

package org.apache.oozie.fluentjob.api.dag;

import org.apache.oozie.fluentjob.api.Condition;
import org.apache.oozie.fluentjob.api.NodesToPng;
import org.apache.oozie.fluentjob.api.action.MapReduceActionBuilder;
import org.apache.oozie.fluentjob.api.action.Node;
import org.apache.oozie.fluentjob.api.workflow.Workflow;
import org.apache.oozie.fluentjob.api.workflow.WorkflowBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestGraph {
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Rule
    public final NodesToPng nodesToPng = new NodesToPng();

    @Test
    public void testNameIsCorrect() {
        final Node a = MapReduceActionBuilder.create().withName("A").build();

        MapReduceActionBuilder.create().withName("B").withParent(a).build();
        MapReduceActionBuilder.create().withName("C").withParent(a).build();

        final String name = "workflow-name";
        final Workflow workflow = new WorkflowBuilder().withName(name).withDagContainingNode(a).build();

        final Graph graph = new Graph(workflow);
        assertEquals(name, graph.getName());

//        nodesToPng.withWorkflow(workflow);
//        nodesToPng.withGraph(graph);
    }

    @Test
    public void testDuplicateNamesThrow() {
        final Node a = MapReduceActionBuilder.create().withName("A").build();
        MapReduceActionBuilder.create().withName("A").withParent(a).build();

        // The exception will be thrown by the Workflow object,
        // but if it breaks there, we want to catch duplicates here, too.
        expectedException.expect(IllegalArgumentException.class);
        final Workflow workflow = new WorkflowBuilder().withDagContainingNode(a).build();

        new Graph(workflow);
    }

    @Test
    public void testWorkflowWithoutJoin() {
        final Node a = MapReduceActionBuilder.create().withName("A").build();

        MapReduceActionBuilder.create().withName("B").withParent(a).build();
        MapReduceActionBuilder.create().withName("C").withParent(a).build();

        final Workflow workflow = new WorkflowBuilder().withName("without-join").withDagContainingNode(a).build();
        final Graph graph = new Graph(workflow);

        checkDependencies(workflow.getNodes(), graph);

//        nodesToPng.withWorkflow(workflow);
//        nodesToPng.withGraph(graph);
    }

    @Test
    public void testWorkflowWithTrivialJoin() {
        final Node a = MapReduceActionBuilder.create().withName("A").build();

        final Node b = MapReduceActionBuilder.create().withName("B").withParent(a).build();
        final Node c = MapReduceActionBuilder.create().withName("C").withParent(a).build();
        MapReduceActionBuilder.create().withName("D").withParent(b).withParent(c).build();

        final Workflow workflow = new WorkflowBuilder().withName("trivial-join").withDagContainingNode(a).build();
        final Graph graph = new Graph(workflow);

        checkDependencies(workflow.getNodes(), graph);

//        nodesToPng.withWorkflow(workflow);
//        nodesToPng.withGraph(graph);
    }

    @Test
    public void testWorkflowNewDependenciesNeeded() {
        final Node a = MapReduceActionBuilder.create().withName("A").build();

        final Node b = MapReduceActionBuilder.create().withName("B").withParent(a).build();
        final Node c = MapReduceActionBuilder.create().withName("C").withParent(a).build();

        final Node d = MapReduceActionBuilder.create().withName("D").withParent(b).withParent(c).build();
        final Node e = MapReduceActionBuilder.create().withName("E").withParent(c).build();

        MapReduceActionBuilder.create().withName("F").withParent(d).withParent(e).build();

        final Workflow workflow = new WorkflowBuilder().withName("new-dependencies-needed").withDagContainingNode(a).build();
        final Graph graph = new Graph(workflow);

        checkDependencies(workflow.getNodes(), graph);

        final NodeBase A = new ExplicitNode("A", null);
        final NodeBase B = new ExplicitNode("B", null);
        final NodeBase C = new ExplicitNode("C", null);
        final NodeBase D = new ExplicitNode("D", null);
        final NodeBase E = new ExplicitNode("E", null);
        final NodeBase F = new ExplicitNode("F", null);

        final Start start = new Start("start");
        final End end = new End("end");
        final Fork fork1 = new Fork("fork1");
        final Fork fork2 = new Fork("fork2");
        final Join join1 = new Join("join1", fork1);
        final Join join2 = new Join("join2", fork2);

        end.addParent(F);
        F.addParent(join2);
        join2.addParent(D);
        join2.addParent(E);
        D.addParent(fork2);
        E.addParent(fork2);
        fork2.addParent(join1);
        join1.addParent(B);
        join1.addParent(C);
        B.addParent(fork1);
        C.addParent(fork1);
        fork1.addParent(A);
        A.addParent(start);

        final List<NodeBase> nodes = Arrays.asList(start, end, fork1, fork2, join1, join2, A, B, C, D, E, F);

        checkEqualStructureByNames(nodes, graph);

//        nodesToPng.withWorkflow(workflow);
//        nodesToPng.withGraph(graph);
    }

    @Test
    public void testCrossingDependencyLines() {
        final Node a = MapReduceActionBuilder.create().withName("A").build();

        final Node b = MapReduceActionBuilder.create().withName("B").build();
        MapReduceActionBuilder.create().withName("C").withParent(a).withParent(b).build();

        MapReduceActionBuilder.create().withName("D").withParent(a).withParent(b).build();

        final Workflow workflow = new WorkflowBuilder().withName("crossing-dependencies").withDagContainingNode(a).build();
        final Graph graph = new Graph(workflow);

        checkDependencies(workflow.getNodes(), graph);

        final NodeBase A = new ExplicitNode("A", null);
        final NodeBase B = new ExplicitNode("B", null);
        final NodeBase C = new ExplicitNode("C", null);
        final NodeBase D = new ExplicitNode("D", null);

        final Start start = new Start("start");
        final End end = new End("end");
        final Fork fork1 = new Fork("fork1");
        final Fork fork2 = new Fork("fork2");
        final Join join1 = new Join("join1", fork1);
        final Join join2 = new Join("join2", fork2);

        end.addParent(join2);
        join2.addParent(C);
        join2.addParent(D);
        C.addParent(fork2);
        D.addParent(fork2);
        fork2.addParent(join1);
        join1.addParent(A);
        join1.addParent(B);
        A.addParent(fork1);
        B.addParent(fork1);
        fork1.addParent(start);

        final List<NodeBase> nodes = Arrays.asList(start, end, fork1, fork2, join1, join2, A, B, C, D);
        checkEqualStructureByNames(nodes, graph);

//        nodesToPng.withWorkflow(workflow);
//        nodesToPng.withGraph(graph);
    }

    @Test
    public void testSplittingJoins() {
        final Node a = MapReduceActionBuilder.create().withName("A").build();

        final Node b = MapReduceActionBuilder.create().withName("B").withParent(a).build();
        final Node c = MapReduceActionBuilder.create().withName("C").withParent(b).build();

        final Node d = MapReduceActionBuilder.create().withName("D").withParent(b).build();
        final Node e = MapReduceActionBuilder.create().withName("E").withParent(a).build();

        MapReduceActionBuilder.create().withName("F").withParent(c).withParent(d).withParent(e).build();

        final Workflow workflow = new WorkflowBuilder().withName("splitting-joins").withDagContainingNode(a).build();
        final Graph graph = new Graph(workflow);

        checkDependencies(workflow.getNodes(), graph);

        final NodeBase A = new ExplicitNode("A", null);
        final NodeBase B = new ExplicitNode("B", null);
        final NodeBase C = new ExplicitNode("C", null);
        final NodeBase D = new ExplicitNode("D", null);
        final NodeBase E = new ExplicitNode("E", null);
        final NodeBase F = new ExplicitNode("F", null);

        final Start start = new Start("start");
        final End end = new End("end");
        final Fork fork1 = new Fork("fork1");
        final Fork fork2 = new Fork("fork2");
        final Join join1 = new Join("join1", fork1);
        final Join join2 = new Join("join2", fork2);

        end.addParent(F);
        F.addParent(join1);
        join1.addParent(join2);
        join1.addParent(E);
        join2.addParent(C);
        join2.addParent(D);
        C.addParent(fork2);
        D.addParent(fork2);
        fork2.addParent(B);
        B.addParent(fork1);
        E.addParent(fork1);
        fork1.addParent(A);
        A.addParent(start);

        final List<NodeBase> nodes = Arrays.asList(start, end, fork1, fork2, join1, join2, A, B, C, D, E, F);

        checkEqualStructureByNames(nodes, graph);

//        nodesToPng.withWorkflow(workflow);
//        nodesToPng.withGraph(graph);
    }

    @Test
    public void testSplittingForks() {
        final Node a = MapReduceActionBuilder.create().withName("A").build();

        final Node b = MapReduceActionBuilder.create().withName("B").withParent(a).build();
        final Node c = MapReduceActionBuilder.create().withName("C").withParent(a).build();

        final Node d = MapReduceActionBuilder.create().withName("D").withParent(a).build();
        final Node e = MapReduceActionBuilder.create().withName("E").withParent(b).withParent(c).build();

        MapReduceActionBuilder.create().withName("F").withParent(e).withParent(d).build();

        final Workflow workflow = new WorkflowBuilder().withName("splitting-forks").withDagContainingNode(a).build();
        final Graph graph = new Graph(workflow);

        checkDependencies(workflow.getNodes(), graph);

        final NodeBase A = new ExplicitNode("A", null);
        final NodeBase B = new ExplicitNode("B", null);
        final NodeBase C = new ExplicitNode("C", null);
        final NodeBase D = new ExplicitNode("D", null);
        final NodeBase E = new ExplicitNode("E", null);
        final NodeBase F = new ExplicitNode("F", null);

        final Start start = new Start("start");
        final End end = new End("end");
        final Fork fork1 = new Fork("fork1");
        final Fork fork2 = new Fork("fork2");
        final Join join1 = new Join("join1", fork1);
        final Join join2 = new Join("join2", fork2);

        end.addParent(F);
        F.addParent(join1);
        join1.addParent(E);
        join1.addParent(D);
        E.addParent(join2);
        join2.addParent(B);
        join2.addParent(C);
        B.addParent(fork2);
        C.addParent(fork2);
        fork2.addParent(fork1);
        D.addParent(fork1);
        fork1.addParent(A);
        A.addParent(start);

        final List<NodeBase> nodes = Arrays.asList(start, end, fork1, fork2, join1, join2, A, B, C, D, E, F);

        checkEqualStructureByNames(nodes, graph);

//        nodesToPng.withWorkflow(workflow);
//        nodesToPng.withGraph(graph);
    }

    @Test
    public void testBranchingUncles() {
        final Node a = MapReduceActionBuilder.create().withName("A").build();

        final Node b = MapReduceActionBuilder.create().withName("B").withParent(a).build();
        final Node c = MapReduceActionBuilder.create().withName("C").withParent(a).build();

        final Node d = MapReduceActionBuilder.create().withName("D").withParent(b).build();
        final Node e = MapReduceActionBuilder.create().withName("E").withParent(c).build();

        final Node f = MapReduceActionBuilder.create().withName("F").withParent(d).withParent(e).build();
        final Node g = MapReduceActionBuilder.create().withName("G").withParent(c).build();
        MapReduceActionBuilder.create().withName("H").withParent(f).withParent(g).build();

        final Workflow workflow = new WorkflowBuilder().withName("branching-uncles").withDagContainingNode(a).build();
        final Graph graph = new Graph(workflow);

        checkDependencies(workflow.getNodes(), graph);

        final NodeBase A = new ExplicitNode("A", null);
        final NodeBase B = new ExplicitNode("B", null);
        final NodeBase C = new ExplicitNode("C", null);
        final NodeBase D = new ExplicitNode("D", null);
        final NodeBase E = new ExplicitNode("E", null);
        final NodeBase F = new ExplicitNode("F", null);
        final NodeBase G = new ExplicitNode("G", null);
        final NodeBase H = new ExplicitNode("H", null);

        final Start start = new Start("start");
        final End end = new End("end");
        final Fork fork1 = new Fork("fork1");
        final Fork fork2 = new Fork("fork3");
        final Join join1 = new Join("join1", fork1);
        final Join join2 = new Join("join3", fork2);

        end.addParent(H);
        H.addParent(join2);
        join2.addParent(F);
        join2.addParent(G);
        F.addParent(fork2);
        G.addParent(fork2);
        fork2.addParent(join1);
        join1.addParent(D);
        join1.addParent(E);
        D.addParent(B);
        E.addParent(C);
        B.addParent(fork1);
        C.addParent(fork1);
        fork1.addParent(A);
        A.addParent(start);

        final List<NodeBase> nodes = Arrays.asList(start, end, fork1, fork2, join1, join2, A, B, C, D, E, F, G, H);

        checkEqualStructureByNames(nodes, graph);

//        nodesToPng.withWorkflow(workflow);
//        nodesToPng.withGraph(graph);
    }

    @Test
    public void testTrivialRedundantEdge() {
        final Node a = MapReduceActionBuilder.create().withName("A").build();

        final Node b = MapReduceActionBuilder.create().withName("B").withParent(a).build();
        MapReduceActionBuilder.create().withName("C").withParent(a).withParent(b).build();

        final Workflow workflow = new WorkflowBuilder().withName("trivial-redundant-edge").withDagContainingNode(a).build();
        final Graph graph = new Graph(workflow);

        checkDependencies(workflow.getNodes(), graph);

        final NodeBase A = new ExplicitNode("A", null);
        final NodeBase B = new ExplicitNode("B", null);
        final NodeBase C = new ExplicitNode("C", null);

        final Start start = new Start("start");
        final End end = new End("end");

        end.addParent(C);
        C.addParent(B);
        B.addParent(A);
        A.addParent(start);

        final List<NodeBase> nodes = Arrays.asList(start, end, A, B, C);

        checkEqualStructureByNames(nodes, graph);
//
//        nodesToPng.withWorkflow(workflow);
//        nodesToPng.withGraph(graph);
    }

    @Test
    public void testRedundantEdge() {
        final Node a = MapReduceActionBuilder.create().withName("A").build();

        final Node b = MapReduceActionBuilder.create().withName("B").withParent(a).build();
        final Node c = MapReduceActionBuilder.create().withName("C").withParent(a).build();

        final Node d = MapReduceActionBuilder.create().withName("D").withParent(b).withParent(c).build();
        final Node e = MapReduceActionBuilder.create().withName("E").withParent(c).build();

        MapReduceActionBuilder.create().withName("F").withParent(d).withParent(e).withParent(a).build();

        final Workflow workflow = new WorkflowBuilder().withName("redundant-edge").withDagContainingNode(a).build();
        final Graph graph = new Graph(workflow);

        checkDependencies(workflow.getNodes(), graph);

        final NodeBase A = new ExplicitNode("A", null);
        final NodeBase B = new ExplicitNode("B", null);
        final NodeBase C = new ExplicitNode("C", null);
        final NodeBase D = new ExplicitNode("D", null);
        final NodeBase E = new ExplicitNode("E", null);
        final NodeBase F = new ExplicitNode("F", null);

        final Start start = new Start("start");
        final End end = new End("end");
        final Fork fork1 = new Fork("fork1");
        final Fork fork2 = new Fork("fork2");
        final Join join1 = new Join("join1", fork1);
        final Join join2 = new Join("join2", fork2);

        end.addParent(F);
        F.addParent(join2);
        join2.addParent(D);
        join2.addParent(E);
        D.addParent(fork2);
        E.addParent(fork2);
        fork2.addParent(join1);
        join1.addParent(B);
        join1.addParent(C);
        B.addParent(fork1);
        C.addParent(fork1);
        fork1.addParent(A);
        A.addParent(start);

        final List<NodeBase> nodes = Arrays.asList(start, end, fork1, fork2, join1, join2, A, B, C, D, E, F);

        checkEqualStructureByNames(nodes, graph);

//        nodesToPng.withWorkflow(workflow);
//        nodesToPng.withGraph(graph);
    }

    @Test
    public void testLateUncle() {
        final Node a = MapReduceActionBuilder.create().withName("A").build();

        final Node b = MapReduceActionBuilder.create().withName("B").withParent(a).build();
        final Node c = MapReduceActionBuilder.create().withName("C").withParent(a).build();

        final Node d = MapReduceActionBuilder.create().withName("D").withParent(b).build();
        final Node e = MapReduceActionBuilder.create().withName("E").withParent(b).build();

        final Node f = MapReduceActionBuilder.create().withName("F").withParent(c).build();

        final Node g = MapReduceActionBuilder.create().withName("G").withParent(e).build();
        final Node h = MapReduceActionBuilder.create().withName("H").withParent(f).build();
        final Node i = MapReduceActionBuilder.create().withName("I").withParent(d).withParent(g).build();
        final Node j = MapReduceActionBuilder.create().withName("J").withParent(e).withParent(h).build();
        MapReduceActionBuilder.create().withName("K").withParent(i).withParent(j).build();

        final Workflow workflow = new WorkflowBuilder().withName("late-uncle").withDagContainingNode(a).build();
        final Graph graph = new Graph(workflow);

        checkDependencies(workflow.getNodes(), graph);

        final NodeBase A = new ExplicitNode("A", null);
        final NodeBase B = new ExplicitNode("B", null);
        final NodeBase C = new ExplicitNode("C", null);
        final NodeBase D = new ExplicitNode("D", null);
        final NodeBase E = new ExplicitNode("E", null);
        final NodeBase F = new ExplicitNode("F", null);
        final NodeBase G = new ExplicitNode("G", null);
        final NodeBase H = new ExplicitNode("H", null);
        final NodeBase I = new ExplicitNode("I", null);
        final NodeBase J = new ExplicitNode("J", null);
        final NodeBase K = new ExplicitNode("K", null);

        final Start start = new Start("start");
        final End end = new End("end");
        final Fork fork1 = new Fork("fork1");
        final Fork fork2 = new Fork("fork2");
        final Fork fork3 = new Fork("fork3");
        final Join join1 = new Join("join1", fork1);
        final Join join2 = new Join("join2", fork2);
        final Join join3 = new Join("join3", fork3);

        end.addParent(K);
        K.addParent(join3);
        join3.addParent(I);
        join3.addParent(J);
        I.addParent(fork3);
        J.addParent(fork3);
        fork3.addParent(join1);
        join1.addParent(join2);
        join1.addParent(H);
        join2.addParent(D);
        join2.addParent(G);
        G.addParent(E);
        D.addParent(fork2);
        E.addParent(fork2);
        fork2.addParent(B);
        B.addParent(fork1);
        H.addParent(F);
        F.addParent(C);
        C.addParent(fork1);
        fork1.addParent(A);
        A.addParent(start);

        final List<NodeBase> nodes = Arrays.asList(start, end, fork1, fork2, fork3, join1, join2, join3,
                                             A, B, C, D, E, F, G, H, I, J, K);

        checkEqualStructureByNames(nodes, graph);

//        nodesToPng.withWorkflow(workflow);
//        nodesToPng.withGraph(graph);
    }

    @Test
    public void testMultipleRoots() {
        final Node a = MapReduceActionBuilder.create().withName("A").build();
        final Node g = MapReduceActionBuilder.create().withName("G").build();

        final Node b = MapReduceActionBuilder.create().withName("B").withParent(a).withParent(g).build();
        final Node c = MapReduceActionBuilder.create().withName("C").withParent(a).build();

        final Node d = MapReduceActionBuilder.create().withName("D").withParent(b).withParent(c).build();
        final Node e = MapReduceActionBuilder.create().withName("E").withParent(c).build();

        MapReduceActionBuilder.create().withName("F").withParent(d).withParent(e).build();

        final Workflow workflow = new WorkflowBuilder().withName("multiple-roots").withDagContainingNode(a).build();
        final Graph graph = new Graph(workflow);

        checkDependencies(workflow.getNodes(), graph);

        final NodeBase A = new ExplicitNode("A", null);
        final NodeBase B = new ExplicitNode("B", null);
        final NodeBase C = new ExplicitNode("C", null);
        final NodeBase D = new ExplicitNode("D", null);
        final NodeBase E = new ExplicitNode("E", null);
        final NodeBase F = new ExplicitNode("F", null);
        final NodeBase G = new ExplicitNode("G", null);

        final Start start = new Start("start");
        final End end = new End("end");
        final Fork fork1 = new Fork("fork1");
        final Fork fork2 = new Fork("fork2");
        final Fork fork3 = new Fork("fork3");
        final Join join1 = new Join("join1", fork1);
        final Join join2 = new Join("join2", fork2);
        final Join join3 = new Join("join3", fork3);

        end.addParent(F);
        F.addParent(join3);
        join3.addParent(D);
        join3.addParent(E);
        D.addParent(fork3);
        E.addParent(fork3);
        fork3.addParent(join2);
        join2.addParent(B);
        join2.addParent(C);
        B.addParent(fork2);
        C.addParent(fork2);
        fork2.addParent(join1);
        join1.addParent(G);
        join1.addParent(A);
        G.addParent(fork1);
        A.addParent(fork1);
        fork1.addParent(start);

        final List<NodeBase> nodes = Arrays.asList(
                start, end, fork1, fork2, fork3, join1, join2, join3, A, B, C, D, E, F, G);

        checkEqualStructureByNames(nodes, graph);

//        nodesToPng.withWorkflow(workflow);
//        nodesToPng.withGraph(graph);
    }

    @Test
    public void testTrivialDecision() {
        final String conditionGotoB = "condition_goto_B";

        final Node a = MapReduceActionBuilder.create().withName("A").build();
        MapReduceActionBuilder.create().withName("B").withParentWithCondition(a, conditionGotoB).build();
        MapReduceActionBuilder.create().withName("C").withParentDefaultConditional(a).build();

        final Workflow workflow = new WorkflowBuilder()
                .withName("Workflow_to_map")
                .withDagContainingNode(a)
                .build();
        final Graph graph = new Graph(workflow);

        final Start start = new Start("start");
        final End end = new End("end");
        final Decision decision = new Decision("decision1");
        final DecisionJoin decisionJoin = new DecisionJoin("decisionJoin1", decision);

        final NodeBase A = new ExplicitNode("A", null);
        final NodeBase B = new ExplicitNode("B", null);
        final NodeBase C = new ExplicitNode("C", null);

        end.addParent(decisionJoin);
        decisionJoin.addParent(B);
        decisionJoin.addParent(C);
        B.addParentWithCondition(decision, Condition.actualCondition(conditionGotoB));
        C.addParentDefaultConditional(decision);
        decision.addParent(A);
        A.addParent(start);

        final List<NodeBase> nodes = Arrays.asList(
                start, end, decision, decisionJoin, A, B, C);

        checkEqualStructureByNames(nodes, graph);
    }

    @Test
    public void testTrivialDiamondDecision() {
        final String conditionGotoB = "condition_goto_B";
        final String conditionGotoC = "condition_goto_C";

        final Node a = MapReduceActionBuilder.create().withName("A").build();
        final Node b = MapReduceActionBuilder.create().withName("B").withParentWithCondition(a, conditionGotoB).build();
        final Node c = MapReduceActionBuilder.create().withName("C").withParentWithCondition(a, conditionGotoC).build();
        MapReduceActionBuilder.create().withName("D").withParent(b).withParent(c).build();

        final Workflow workflow = new WorkflowBuilder().withName("trivial-decision").withDagContainingNode(a).build();
        final Graph graph = new Graph(workflow);

        final NodeBase A = new ExplicitNode("A", null);
        final NodeBase B = new ExplicitNode("B", null);
        final NodeBase C = new ExplicitNode("C", null);
        final NodeBase D = new ExplicitNode("D", null);

        final Start start = new Start("start");
        final End end = new End("end");
        final Decision decision = new Decision("decision1");
        final DecisionJoin decisionJoin = new DecisionJoin("decisionJoin1", decision);

        end.addParent(D);
        D.addParent(decisionJoin);
        decisionJoin.addParent(B);
        decisionJoin.addParent(C);
        B.addParentWithCondition(decision, Condition.actualCondition(conditionGotoB));
        C.addParentWithCondition(decision, Condition.actualCondition(conditionGotoC));
        decision.addParent(A);
        A.addParent(start);

        final List<NodeBase> nodes = Arrays.asList(
                start, end, decision, decisionJoin, A, B, C, D);

        checkEqualStructureByNames(nodes, graph);

//        nodesToPng.withWorkflow(workflow);
//        nodesToPng.withGraph(graph);
    }

    @Test
    public void testDecisionAndJoin() {
        final String conditionGotoD = "condition_goto_D";
        final String conditionGotoE = "condition_goto_E";

        final Node a = MapReduceActionBuilder.create().withName("A").build();
        final Node b = MapReduceActionBuilder.create().withName("B").withParent(a).build();
        final Node c = MapReduceActionBuilder.create().withName("C").withParent(a).build();
        final Node d = MapReduceActionBuilder.create().withName("D").withParent(b)
                .withParentWithCondition(c, conditionGotoD).build();
        final Node e = MapReduceActionBuilder.create().withName("E").withParentWithCondition(c, conditionGotoE).build();
        MapReduceActionBuilder.create().withName("F").withParent(d).build();
        MapReduceActionBuilder.create().withName("G").withParent(e).build();

        final Workflow workflow = new WorkflowBuilder().withName("decision-and-join").withDagContainingNode(a).build();
        final Graph graph = new Graph(workflow);

        final NodeBase A = new ExplicitNode("A", null);
        final NodeBase B = new ExplicitNode("B", null);
        final NodeBase C = new ExplicitNode("C", null);
        final NodeBase D = new ExplicitNode("D", null);
        final NodeBase E = new ExplicitNode("E", null);
        final NodeBase F = new ExplicitNode("F", null);
        final NodeBase G = new ExplicitNode("G", null);

        final Start start = new Start("start");
        final End end = new End("end");
        final Fork fork = new Fork("fork1");
        final Join join = new Join("join1", fork);
        final Decision decision = new Decision("decision1");
        final DecisionJoin decisionJoin = new DecisionJoin("decisionJoin1", decision);

        end.addParent(decisionJoin);
        decisionJoin.addParent(F);
        decisionJoin.addParent(G);
        F.addParent(D);
        D.addParentWithCondition(decision, Condition.actualCondition(conditionGotoD));
        G.addParent(E);
        E.addParentWithCondition(decision, Condition.actualCondition(conditionGotoE));
        decision.addParent(join);
        join.addParent(B);
        join.addParent(C);
        B.addParent(fork);
        C.addParent(fork);
        fork.addParent(A);
        A.addParent(start);

        final List<NodeBase> nodes = Arrays.asList(
                start, end, fork, join, decision, decisionJoin, A, B, C, D, E, F, G);

//        nodesToPng.withWorkflow(workflow);
//        nodesToPng.withGraph(graph);

        checkEqualStructureByNames(nodes, graph);
    }

    @Test
    public void testDecisionAtUncleOfJoin() {
        final String conditionGotoD = "condition_goto_D";
        final String conditionGotoF = "condition_goto_F";

        final Node a = MapReduceActionBuilder.create().withName("A").build();
        final Node b = MapReduceActionBuilder.create().withName("B").withParent(a).build();
        final Node c = MapReduceActionBuilder.create().withName("C").withParent(a).build();
        final Node d = MapReduceActionBuilder.create().withName("D").withParentWithCondition(c, conditionGotoD).build();
        final Node e = MapReduceActionBuilder.create().withName("E").withParent(b).withParent(d).build();
        final Node f = MapReduceActionBuilder.create().withName("F").withParentWithCondition(c, conditionGotoF).build();
        MapReduceActionBuilder.create().withName("G").withParent(e).build();
        MapReduceActionBuilder.create().withName("H").withParent(f).build();

        final Workflow workflow = new WorkflowBuilder().withName("decision-at-uncle-of-join").withDagContainingNode(a).build();
        final Graph graph = new Graph(workflow);

        final NodeBase A = new ExplicitNode("A", null);
        final NodeBase B = new ExplicitNode("B", null);
        final NodeBase C = new ExplicitNode("C", null);
        final NodeBase D = new ExplicitNode("D", null);
        final NodeBase E = new ExplicitNode("E", null);
        final NodeBase F = new ExplicitNode("F", null);
        final NodeBase G = new ExplicitNode("G", null);
        final NodeBase H = new ExplicitNode("H", null);

        final Start start = new Start("start");
        final End end = new End("end");
        final Fork fork = new Fork("fork1");
        final Join join = new Join("join1", fork);
        final Decision decision = new Decision("decision1");
        final DecisionJoin decisionJoin = new DecisionJoin("decisionJoin1", decision);

        end.addParent(decisionJoin);
        decisionJoin.addParent(G);
        decisionJoin.addParent(H);
        G.addParent(E);
        H.addParent(F);
        E.addParent(D);
        D.addParentWithCondition(decision, Condition.actualCondition(conditionGotoD));
        F.addParentWithCondition(decision, Condition.actualCondition(conditionGotoF));
        decision.addParent(join);
        join.addParent(B);
        join.addParent(C);
        B.addParent(fork);
        C.addParent(fork);
        fork.addParent(A);
        A.addParent(start);

        final List<NodeBase> nodes = Arrays.asList(
                start, end, fork, join, decision, decisionJoin, A, B, C, D, E, F, G, H);

//        nodesToPng.withWorkflow(workflow);
//        nodesToPng.withGraph(graph);

        checkEqualStructureByNames(nodes, graph);
    }

    @Test
    public void testAlreadyClosedDecisionBranching() {
        final String conditionGotoD = "condition_goto_D";
        final String conditionGotoE = "condition_goto_E";

        final Node a = MapReduceActionBuilder.create().withName("A").build();
        final Node b = MapReduceActionBuilder.create().withName("B").withParent(a).build();
        final Node c = MapReduceActionBuilder.create().withName("C").withParent(a).build();
        final Node d = MapReduceActionBuilder.create().withName("D").withParentWithCondition(b, conditionGotoD).build();
        final Node e = MapReduceActionBuilder.create().withName("E").withParentWithCondition(b, conditionGotoE).build();

        final Node f = MapReduceActionBuilder.create().withName("F").withParent(d).withParent(e).build();
        MapReduceActionBuilder.create().withName("G").withParent(f).withParent(c).build();

        final Workflow workflow = new WorkflowBuilder()
                .withName("already-closed-decision-branching")
                .withDagContainingNode(a)
                .build();
        final Graph graph = new Graph(workflow);

        final NodeBase A = new ExplicitNode("A", null);
        final NodeBase B = new ExplicitNode("B", null);
        final NodeBase C = new ExplicitNode("C", null);
        final NodeBase D = new ExplicitNode("D", null);
        final NodeBase E = new ExplicitNode("E", null);
        final NodeBase F = new ExplicitNode("F", null);
        final NodeBase G = new ExplicitNode("G", null);

        final Start start = new Start("start");
        final End end = new End("end");
        final Fork fork = new Fork("fork1");
        final Join join = new Join("join1", fork);
        final Decision decision = new Decision("decision1");
        final DecisionJoin decisionJoin = new DecisionJoin("decisionJoin1", decision);

        end.addParent(G);
        G.addParent(join);
        join.addParent(F);
        join.addParent(C);
        F.addParent(decisionJoin);
        decisionJoin.addParent(D);
        decisionJoin.addParent(E);
        D.addParentWithCondition(decision, Condition.actualCondition(conditionGotoD));
        E.addParentWithCondition(decision, Condition.actualCondition(conditionGotoE));
        decision.addParent(B);
        B.addParent(fork);
        C.addParent(fork);
        fork.addParent(A);
        A.addParent(start);

        final List<NodeBase> nodes = Arrays.asList(
                start, end, fork, join, decision, decisionJoin, A, B, C, D, E, F, G);

//        nodesToPng.withWorkflow(workflow);
//        nodesToPng.withGraph(graph);

        checkEqualStructureByNames(nodes, graph);

    }

    @Test
    public void testIncomingConditionalBranchesFromDifferentDecisionsThrows() {
        final Node a = MapReduceActionBuilder.create().withName("A").build();

        final Node b = MapReduceActionBuilder.create().withName("B").withParent(a).build();
        final Node c = MapReduceActionBuilder.create().withName("C").withParent(a).build();
        final Node d = MapReduceActionBuilder.create().withName("D").withParent(a).build();

        MapReduceActionBuilder.create().withName("E").withParentWithCondition(c, "condition_goto_E").build();
        final Node f = MapReduceActionBuilder.create().withName("F").withParentDefaultConditional(c).build();

        final Node g = MapReduceActionBuilder.create().withName("G").withParentWithCondition(d, "condition_goto_G").build();
        final Node h = MapReduceActionBuilder.create().withName("H").withParentDefaultConditional(d).build();

        MapReduceActionBuilder.create().withName("I").withParent(b).withParent(f).withParent(g).build();
        MapReduceActionBuilder.create().withName("J").withParent(h).build();

        final Workflow workflow = new WorkflowBuilder()
                .withName("incoming-conditional-branches-from-different-decisions")
                .withDagContainingNode(a)
                .build();

//        nodesToPng.withWorkflow(workflow);

        // TODO: We might choose to implement it later without an exception.
        expectedException.expect(IllegalStateException.class);
        new Graph(workflow);
    }

    private void checkEqualStructureByNames(final Collection<NodeBase> expectedNodes, final Graph graph2) {
        assertEquals(expectedNodes.size(), graph2.getNodes().size());

        for (final NodeBase expectedNode : expectedNodes) {
            final NodeBase nodeInOtherGraph = graph2.getNodeByName(expectedNode.getName());

            assertNotNull(nodeInOtherGraph);

            final List<NodeBase> expectedChildren = expectedNode.getChildren();
            final List<NodeBase> actualChildren = nodeInOtherGraph.getChildren();

            final List<String> expectedChildrenNames = new ArrayList<>();
            for (final NodeBase child : expectedChildren) {
                expectedChildrenNames.add(child.getName());
            }

            final List<String> actualChildrenNames = new ArrayList<>();
            for (final NodeBase child : actualChildren) {
                actualChildrenNames.add(child.getName());
            }

            if (expectedNode instanceof Fork) {
                // The order of the children of fork nodes is not important.
                Collections.sort(expectedChildrenNames);
                Collections.sort(actualChildrenNames);
            }

            assertEquals(expectedChildrenNames.size(), actualChildrenNames.size());

            for (int i = 0; i < expectedChildren.size(); ++i) {
                final String expectedName = expectedChildrenNames.get(i);
                final String actualName = actualChildrenNames.get(i);

                if (graph2.getNodeByName(actualName) instanceof ExplicitNode) {
                    assertEquals(expectedName, actualName);
                }
            }
        }
    }

    private void checkDependencies(final Set<Node> originalNodes, final Graph graph) {
        for (final Node originalNode : originalNodes) {
            for (final Node originalParent : originalNode.getAllParents()) {
                final NodeBase node = graph.getNodeByName(originalNode.getName());
                final NodeBase parent = graph.getNodeByName(originalParent.getName());

                assertTrue(verifyDependency(parent, node));
            }
        }
    }

    private boolean verifyDependency(final NodeBase dependency, final NodeBase dependent) {
        final List<NodeBase> children = dependency.getChildren();

        for (final NodeBase child : children) {
            if (child == dependent || verifyDependency(child, dependent)) {
                return true;
            }
        }

        return false;
    }
}
