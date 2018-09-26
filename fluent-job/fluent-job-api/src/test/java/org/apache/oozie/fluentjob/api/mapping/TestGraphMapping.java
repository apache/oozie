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

import org.apache.oozie.fluentjob.api.Condition;
import org.apache.oozie.fluentjob.api.action.EmailActionBuilder;
import org.apache.oozie.fluentjob.api.action.ErrorHandler;
import org.apache.oozie.fluentjob.api.action.FSAction;
import org.apache.oozie.fluentjob.api.action.FSActionBuilder;
import org.apache.oozie.fluentjob.api.action.MapReduceAction;
import org.apache.oozie.fluentjob.api.action.MapReduceActionBuilder;
import org.apache.oozie.fluentjob.api.action.Node;
import org.apache.oozie.fluentjob.api.generated.workflow.ACTION;
import org.apache.oozie.fluentjob.api.generated.workflow.ACTIONTRANSITION;
import org.apache.oozie.fluentjob.api.generated.workflow.CASE;
import org.apache.oozie.fluentjob.api.generated.workflow.DECISION;
import org.apache.oozie.fluentjob.api.generated.workflow.DEFAULT;
import org.apache.oozie.fluentjob.api.generated.workflow.END;
import org.apache.oozie.fluentjob.api.generated.workflow.FORK;
import org.apache.oozie.fluentjob.api.generated.workflow.FORKTRANSITION;
import org.apache.oozie.fluentjob.api.generated.workflow.JOIN;
import org.apache.oozie.fluentjob.api.generated.workflow.KILL;
import org.apache.oozie.fluentjob.api.generated.workflow.ObjectFactory;
import org.apache.oozie.fluentjob.api.generated.workflow.START;
import org.apache.oozie.fluentjob.api.generated.workflow.SWITCH;
import org.apache.oozie.fluentjob.api.generated.workflow.WORKFLOWAPP;
import org.apache.oozie.fluentjob.api.dag.Decision;
import org.apache.oozie.fluentjob.api.dag.DecisionJoin;
import org.apache.oozie.fluentjob.api.dag.End;
import org.apache.oozie.fluentjob.api.dag.ExplicitNode;
import org.apache.oozie.fluentjob.api.dag.Fork;
import org.apache.oozie.fluentjob.api.dag.Graph;
import org.apache.oozie.fluentjob.api.dag.Join;
import org.apache.oozie.fluentjob.api.dag.Start;
import org.apache.oozie.fluentjob.api.workflow.Workflow;
import org.apache.oozie.fluentjob.api.workflow.WorkflowBuilder;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestGraphMapping {
    private static final ObjectFactory OBJECT_FACTORY = new ObjectFactory();

    @Test
    public void testMappingGraphFromWorkflow() {
        final String errorHandlerName = "error-handler";

        final EmailActionBuilder emailBuilder = EmailActionBuilder.create().withName(errorHandlerName)
                .withRecipient("somebody@workplace.com")
                .withSubject("Subject")
                .withBody("Email body.");

        final ErrorHandler errorHandler = ErrorHandler.buildAsErrorHandler(emailBuilder);

        final MapReduceAction mrAction = MapReduceActionBuilder.create()
                .withName("map-reduce-action")
                .withErrorHandler(errorHandler)
                .build();
        final FSAction fsAction = FSActionBuilder.create()
                .withName("fs-action")
                .withParent(mrAction)
                .withErrorHandler(errorHandler)
                .build();

        final Workflow workflow = new WorkflowBuilder().withName("workflow-name").withDagContainingNode(mrAction).build();

        final Graph graph = new Graph(workflow);

        final WORKFLOWAPP workflowapp = DozerBeanMapperSingleton.instance().map(graph, WORKFLOWAPP.class);

        final WORKFLOWAPP expectedWorkflowapp = OBJECT_FACTORY.createWORKFLOWAPP();
        expectedWorkflowapp.setName(workflow.getName());

        final START start = OBJECT_FACTORY.createSTART();
        start.setTo(mrAction.getName());
        expectedWorkflowapp.setStart(start);

        final END end = OBJECT_FACTORY.createEND();
        end.setName("end");
        expectedWorkflowapp.setEnd(end);

        final List<Object> actions = expectedWorkflowapp.getDecisionOrForkOrJoin();

        final ACTION actionMr = convertEmailActionByHand((ExplicitNode) graph.getNodeByName(mrAction.getName()));
        final ACTIONTRANSITION mrErrorTransition = actionMr.getError();
        mrErrorTransition.setTo(errorHandlerName);

        final ACTION actionFs = convertEmailActionByHand((ExplicitNode) graph.getNodeByName(fsAction.getName()));
        final ACTIONTRANSITION fsErrorTransition = actionFs.getError();
        fsErrorTransition.setTo(errorHandlerName);

        final Node emailErrorHandlerNode = emailBuilder.build();
        final ExplicitNode emailErrorHandlerExplicitNode
                = new ExplicitNode(emailErrorHandlerNode.getName(), emailErrorHandlerNode);
        final ACTION errorHandlerAction = convertEmailActionByHand(emailErrorHandlerExplicitNode);

        final KILL kill = createKill();

        final ACTIONTRANSITION okAndError = OBJECT_FACTORY.createACTIONTRANSITION();
        okAndError.setTo(kill.getName());

        errorHandlerAction.setOk(okAndError);
        errorHandlerAction.setError(okAndError);

        actions.add(kill);
        actions.add(errorHandlerAction);
        actions.add(actionMr);
        actions.add(actionFs);

        assertEquals("expected and actual WORKFLOWAPP should look the same", expectedWorkflowapp, workflowapp);
    }

    @Test
    public void testMappingGraphFromNodes() {
        final String workflowName = "test-workflow";
        final String condition = "condition";

        final ExplicitNode A = new ExplicitNode("A", EmailActionBuilder.create().build());
        final ExplicitNode B = new ExplicitNode("B", EmailActionBuilder.create().build());
        final ExplicitNode C = new ExplicitNode("C", EmailActionBuilder.create().build());
        final ExplicitNode D = new ExplicitNode("D", EmailActionBuilder.create().build());
        final ExplicitNode E = new ExplicitNode("E", EmailActionBuilder.create().build());

        final Start start = new Start("start");
        final End end = new End("end");
        final Fork fork = new Fork("fork1");
        final Join join = new Join("join1", fork);
        final Decision decision = new Decision("decision");
        final DecisionJoin decisionJoin = new DecisionJoin("decisionJoin", decision);

        // TODO: Unfortunately the order of the elements counts.
        end.addParent(join);
        join.addParent(decisionJoin);
        join.addParent(C);
        decisionJoin.addParent(D);
        decisionJoin.addParent(E);
        D.addParentWithCondition(decision, Condition.actualCondition(condition));
        E.addParentDefaultConditional(decision);
        decision.addParent(B);
        B.addParent(fork);
        C.addParent(fork);
        fork.addParent(A);
        A.addParent(start);

        final GraphNodes graphNodes = new GraphNodes(workflowName,
                null,
                null,
                null,
                start,
                end,
                Arrays.asList(A, B, C, D, E, fork, join, decision, decisionJoin));


        final WORKFLOWAPP expectedWorkflowApp = OBJECT_FACTORY.createWORKFLOWAPP();
        expectedWorkflowApp.setName(workflowName);

        final START startJaxb = OBJECT_FACTORY.createSTART();
        startJaxb.setTo(start.getChild().getName());
        expectedWorkflowApp.setStart(startJaxb);

        final END endJaxb = OBJECT_FACTORY.createEND();
        endJaxb.setName(end.getName());
        expectedWorkflowApp.setEnd(endJaxb);

        final List<Object> nodesInWorkflowApp = expectedWorkflowApp.getDecisionOrForkOrJoin();

        final FORK forkJaxb = OBJECT_FACTORY.createFORK();
        forkJaxb.setName(fork.getName());
        final List<FORKTRANSITION> transitions = forkJaxb.getPath();
        final FORKTRANSITION transitionB = OBJECT_FACTORY.createFORKTRANSITION();
        transitionB.setStart(B.getName());
        final FORKTRANSITION transitionC = OBJECT_FACTORY.createFORKTRANSITION();
        transitionC.setStart(C.getName());
        transitions.add(transitionB);
        transitions.add(transitionC);

        final ACTION actionA = convertEmailActionByHand(A);

        final ACTION actionB = convertEmailActionByHand(B);

        final ACTION actionC = convertEmailActionByHand(C);

        final DECISION decisionJaxb = OBJECT_FACTORY.createDECISION();
        decisionJaxb.setName(decision.getName());
        final SWITCH _switch = OBJECT_FACTORY.createSWITCH();
        final List<CASE> cases = _switch.getCase();
        final CASE _case = OBJECT_FACTORY.createCASE();
        _case.setTo(D.getName());
        _case.setValue(condition);
        cases.add(_case);
        final DEFAULT _default = OBJECT_FACTORY.createDEFAULT();
        _default.setTo(E.getName());
        _switch.setDefault(_default);
        decisionJaxb.setSwitch(_switch);

        final ACTION actionD = convertEmailActionByHand(D);

        final ACTION actionE = convertEmailActionByHand(E);

        final JOIN joinJaxb = OBJECT_FACTORY.createJOIN();
        joinJaxb.setName(join.getName());
        joinJaxb.setTo(end.getName());

        // TODO: Unfortunately the order of the elements counts.
        nodesInWorkflowApp.add(createKill());
        nodesInWorkflowApp.add(actionA);
        nodesInWorkflowApp.add(actionB);
        nodesInWorkflowApp.add(actionC);
        nodesInWorkflowApp.add(actionD);
        nodesInWorkflowApp.add(actionE);
        nodesInWorkflowApp.add(forkJaxb);
        nodesInWorkflowApp.add(joinJaxb);
        nodesInWorkflowApp.add(decisionJaxb);

        final WORKFLOWAPP workflowapp = DozerBeanMapperSingleton.instance().map(graphNodes, WORKFLOWAPP.class);

        assertEquals(expectedWorkflowApp, workflowapp);
    }

    private ACTION convertEmailActionByHand(final ExplicitNode node) {
        final ACTION action = DozerBeanMapperSingleton.instance().map(node, ACTION.class);

        final ACTIONTRANSITION error = OBJECT_FACTORY.createACTIONTRANSITION();
        error.setTo(createKill().getName());
        action.setError(error);

        return action;
    }

    private KILL createKill() {
        final KILL kill = OBJECT_FACTORY.createKILL();

        kill.setName("kill");
        kill.setMessage("Action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]");

        return kill;
    }
}
