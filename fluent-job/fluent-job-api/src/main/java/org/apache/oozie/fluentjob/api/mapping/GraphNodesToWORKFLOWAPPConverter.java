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

import com.google.common.base.Preconditions;
import org.apache.oozie.fluentjob.api.generated.workflow.ACTION;
import org.apache.oozie.fluentjob.api.generated.workflow.ACTIONTRANSITION;
import org.apache.oozie.fluentjob.api.generated.workflow.CREDENTIALS;
import org.apache.oozie.fluentjob.api.generated.workflow.DECISION;
import org.apache.oozie.fluentjob.api.generated.workflow.END;
import org.apache.oozie.fluentjob.api.generated.workflow.FORK;
import org.apache.oozie.fluentjob.api.generated.workflow.GLOBAL;
import org.apache.oozie.fluentjob.api.generated.workflow.JOIN;
import org.apache.oozie.fluentjob.api.generated.workflow.KILL;
import org.apache.oozie.fluentjob.api.generated.workflow.ObjectFactory;
import org.apache.oozie.fluentjob.api.generated.workflow.PARAMETERS;
import org.apache.oozie.fluentjob.api.generated.workflow.START;
import org.apache.oozie.fluentjob.api.generated.workflow.WORKFLOWAPP;
import org.apache.oozie.fluentjob.api.workflow.Credentials;
import org.apache.oozie.fluentjob.api.workflow.Global;
import org.apache.oozie.fluentjob.api.workflow.Parameters;
import org.apache.oozie.fluentjob.api.action.ErrorHandler;
import org.apache.oozie.fluentjob.api.action.Node;
import org.apache.oozie.fluentjob.api.dag.Decision;
import org.apache.oozie.fluentjob.api.dag.ExplicitNode;
import org.apache.oozie.fluentjob.api.dag.Fork;
import org.apache.oozie.fluentjob.api.dag.Join;
import org.apache.oozie.fluentjob.api.dag.NodeBase;
import org.dozer.DozerConverter;
import org.dozer.Mapper;
import org.dozer.MapperAware;

import java.util.HashMap;
import java.util.Map;

/**
 * A {@link DozerConverter} converting from {@link GraphNodes} to JAXB {@link WORKFLOWAPP}.
 * <p>
 * It performs tasks that are normally present when users write workflow XML files,
 * but are hidden when using Jobs API:
 * <ul>
 *     <li>sets attributes</li>
 *     <li>by delegating converts and sets JAXB objects of {@code <parameters>} section</li>
 *     <li>by delegating converts and sets JAXB objects of {@code <global>} section</li>
 *     <li>by delegating converts and sets JAXB objects of {@code <credentials>} section</li>
 *     <li>generates and sets JAXB object of {@code <start>} node</li>
 *     <li>generates and sets JAXB object of {@code <end>} node</li>
 *     <li>generates and sets JAXB object of {@code <kill>} node</li>
 *     <li>iterates through {@code GraphNodes} children, generates and sets child {@code <action>} instances</li>
 * </ul>
 */
public class GraphNodesToWORKFLOWAPPConverter extends DozerConverter<GraphNodes, WORKFLOWAPP> implements MapperAware {
    private static final ObjectFactory OBJECT_FACTORY = new ObjectFactory();

    private Mapper mapper;

    private final static Map<Class<? extends Object>, Class<? extends Object>> SOURCE_TARGET_CLASSES = new HashMap<>();
    static {
        SOURCE_TARGET_CLASSES.put(Decision.class, DECISION.class);
        SOURCE_TARGET_CLASSES.put(Fork.class, FORK.class);
        SOURCE_TARGET_CLASSES.put(Join.class, JOIN.class);
        SOURCE_TARGET_CLASSES.put(ExplicitNode.class, ACTION.class);
        SOURCE_TARGET_CLASSES.put(Parameters.class, PARAMETERS.class);
        SOURCE_TARGET_CLASSES.put(Global.class, GLOBAL.class);
        SOURCE_TARGET_CLASSES.put(Credentials.class, CREDENTIALS.class);
    }

    public GraphNodesToWORKFLOWAPPConverter() {
        super(GraphNodes.class, WORKFLOWAPP.class);
    }

    @Override
    public WORKFLOWAPP convertTo(final GraphNodes graphNodes, WORKFLOWAPP workflowapp) {
        workflowapp = ensureWorkflowApp(workflowapp);

        workflowapp.setName(graphNodes.getName());

        mapParameters(graphNodes, workflowapp);

        mapGlobal(graphNodes, workflowapp);

        mapCredentials(graphNodes, workflowapp);

        mapStart(graphNodes, workflowapp);

        mapEnd(graphNodes, workflowapp);

        final KILL kill = mapKill(workflowapp);

        mapChildren(graphNodes, workflowapp, kill);

        return workflowapp;
    }

    private WORKFLOWAPP ensureWorkflowApp(WORKFLOWAPP workflowapp) {
        if (workflowapp == null) {
            workflowapp = new ObjectFactory().createWORKFLOWAPP();
        }
        return workflowapp;
    }

    private void mapParameters(final GraphNodes graphNodes, final WORKFLOWAPP workflowapp) {
        if (graphNodes.getParameters() == null) {
            return;
        }

        final PARAMETERS mappedParameters = mapper.map(graphNodes.getParameters(), PARAMETERS.class);
        workflowapp.setParameters(mappedParameters);
    }

    private void mapGlobal(final GraphNodes graphNodes, final WORKFLOWAPP workflowapp) {
        if (graphNodes.getGlobal() == null) {
            return;
        }

        final GLOBAL mappedGlobal = mapper.map(graphNodes.getGlobal(), GLOBAL.class);
        workflowapp.setGlobal(mappedGlobal);
    }

    private void mapCredentials(final GraphNodes graphNodes, final WORKFLOWAPP workflowapp) {
        if (graphNodes.getCredentials() == null) {
            return;
        }

        final CREDENTIALS mappedCredentials = mapper.map(graphNodes.getCredentials(), CREDENTIALS.class);
        workflowapp.setCredentials(mappedCredentials);
    }

    private void mapStart(final GraphNodes graphNodes, final WORKFLOWAPP workflowapp) {
        final START start = mapper.map(graphNodes.getStart(), START.class);
        workflowapp.setStart(start);
    }

    private void mapEnd(final GraphNodes graphNodes, final WORKFLOWAPP workflowapp) {
        final END end = mapper.map(graphNodes.getEnd(), END.class);
        workflowapp.setEnd(end);
    }

    private KILL mapKill(final WORKFLOWAPP workflowapp) {
        final KILL kill = createKillNode();
        workflowapp.getDecisionOrForkOrJoin().add(kill);
        return kill;
    }

    private void mapChildren(final GraphNodes graphNodes, final WORKFLOWAPP workflowapp, final KILL kill) {
        for (final NodeBase nodeBase : graphNodes.getNodes()) {
            convertNode(nodeBase, workflowapp, kill);
        }
    }

    @Override
    public GraphNodes convertFrom(final WORKFLOWAPP workflowapp, final GraphNodes graphNodes) {
        throw new UnsupportedOperationException("This mapping is not bidirectional.");
    }

    @Override
    public void setMapper(final Mapper mapper) {
        this.mapper = mapper;
    }

    private void convertNode(final NodeBase nodeBase, final WORKFLOWAPP workflowapp, final KILL kill) {
        Preconditions.checkNotNull(nodeBase, "nodeBase");

        final Class<?> sourceClass = nodeBase.getClass();
        if (SOURCE_TARGET_CLASSES.containsKey(sourceClass)) {
            final Object mappedObject = mapper.map(nodeBase, SOURCE_TARGET_CLASSES.get(sourceClass));

            if (nodeBase instanceof ExplicitNode) {
                final ACTION errorHandlerAction = ensureErrorTransition(workflowapp, (ExplicitNode) nodeBase,
                        (ACTION) mappedObject,
                        kill
                );
                if (errorHandlerAction != null && !workflowapp.getDecisionOrForkOrJoin().contains(errorHandlerAction)) {
                    workflowapp.getDecisionOrForkOrJoin().add(errorHandlerAction);
                }
            }

            workflowapp.getDecisionOrForkOrJoin().add(mappedObject);
        }
    }

    private KILL createKillNode() {
        final KILL kill = OBJECT_FACTORY.createKILL();
        kill.setName("kill");
        kill.setMessage("Action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]");

        return kill;
    }

    private ACTION ensureErrorTransition(final WORKFLOWAPP workflowapp,
                                         final ExplicitNode node,
                                         final ACTION action,
                                         final KILL kill) {
        final ACTIONTRANSITION error = ensureError(action);

        final ErrorHandler errorHandler = node.getRealNode().getErrorHandler();

        if (errorHandler == null) {
            error.setTo(kill.getName());

            return null;
        }
        else {
            final Node handlerNode = errorHandler.getHandlerNode();

            final ACTION handlerAction = ensureErrorHandlerAction(workflowapp, handlerNode, kill);
            error.setTo(handlerAction.getName());

            return handlerAction;
        }
    }

    private ACTION ensureErrorHandlerAction(final WORKFLOWAPP workflowapp, final Node handlerNode, final KILL kill) {
        ACTION handlerAction = null;
        for (final Object alreadyPresentObject : workflowapp.getDecisionOrForkOrJoin()) {
            if (alreadyPresentObject instanceof ACTION) {
                final ACTION alreadyPresentAction = (ACTION) alreadyPresentObject;
                if (alreadyPresentAction.getName().equals(handlerNode.getName())) {
                    handlerAction = alreadyPresentAction;
                    break;
                }
            }
        }

        if (handlerAction == null) {
            handlerAction = createErrorHandlerAction(handlerNode, kill);
        }

        return handlerAction;
    }

    private ACTIONTRANSITION ensureError(final ACTION action) {
        ACTIONTRANSITION error = action.getError();

        if (error == null) {
            error = OBJECT_FACTORY.createACTIONTRANSITION();
            action.setError(error);
        }

        return error;
    }

    private ACTIONTRANSITION ensureOk(final ACTION handlerAction) {
        ACTIONTRANSITION ok = handlerAction.getOk();

        if (ok == null) {
            ok = OBJECT_FACTORY.createACTIONTRANSITION();
            handlerAction.setOk(ok);
        }

        return ok;
    }

    private ACTION createErrorHandlerAction(final Node handlerNode, final KILL kill) {
        final ExplicitNode explicitNode = new ExplicitNode(handlerNode.getName(), handlerNode);
        final ACTION handlerAction = mapper.map(explicitNode, ACTION.class);

        final ACTIONTRANSITION ok = ensureOk(handlerAction);
        ok.setTo(kill.getName());

        final ACTIONTRANSITION error = ensureError(handlerAction);
        error.setTo(kill.getName());

        return handlerAction;
    }
}
