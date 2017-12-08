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

package org.apache.oozie.workflow.lite;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.workflow.WorkflowException;
import org.jdom.Element;
import org.jdom.JDOMException;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Optional;

public class LiteWorkflowValidator {

    public void validateWorkflow(LiteWorkflowApp app, boolean validateForkJoin) throws WorkflowException {
        NodeDef startNode = app.getNode(StartNodeDef.START);
        if (startNode == null) {
            throw new WorkflowException(ErrorCode.E0700, "no start node"); // shouldn't happen, but just in case...
        }

        ForkJoinCount forkJoinCount = new ForkJoinCount();

        performBasicValidation(app, startNode, new ArrayDeque<String>(), new HashSet<NodeDef>(), forkJoinCount);

        if (validateForkJoin) {
            // don't validate fork/join pairs if the number of forks and joins mismatch
            if (forkJoinCount.forks != forkJoinCount.joins) {
                throw new WorkflowException(ErrorCode.E0730);
            }

            validateForkJoin(app,
                    startNode,
                    null,
                    null,
                    true,
                    new ArrayDeque<String>(),
                    new HashMap<String, String>(),
                    new HashMap<String, Optional<String>>());
        }
    }

    /**
     * Basic recursive validation of the workflow:
     * - it is acyclic, no loops
     * - names of the actions follow a specific pattern
     * - all nodes have valid transitions
     * - it only has supported action nodes
     * - there is no node that points to itself
     * - counts fork/join nodes
     *
     * @param app The WorkflowApp
     * @param node Current node we're checking
     * @param path The list of nodes that we've visited so far in this call chain
     * @param checkedNodes The list of nodes that we've already checked. For example, if it's a decision node, then the we
     * don't have to re-walk the entire path because it indicates that it've been done before on a separate path
     * @param forkJoinCount Number of fork and join nodes
     * @throws WorkflowException If there is any of the constraints described above is violated
     */
    private void performBasicValidation(LiteWorkflowApp app, NodeDef node, Deque<String> path, Set<NodeDef> checkedNodes,
            ForkJoinCount forkJoinCount) throws WorkflowException {
        String nodeName = node.getName();

        checkActionName(node);
        if (node instanceof ActionNodeDef) {
            checkActionNode(node);
        } else if (node instanceof ForkNodeDef) {
            forkJoinCount.forks++;
        } else if (node instanceof JoinNodeDef) {
            forkJoinCount.joins++;
        }
        checkCycle(path, nodeName);

        path.addLast(nodeName);

        List<String> transitions = node.getTransitions();
        // Get all transitions and walk the workflow recursively
        if (!transitions.isEmpty()) {
            for (final String t : transitions) {
                NodeDef transitionNode = app.getNode(t);
                if (transitionNode == null) {
                    throw new WorkflowException(ErrorCode.E0708, node.getName(), t);
                }

                if (!checkedNodes.contains(transitionNode)) {
                    performBasicValidation(app, transitionNode, path, checkedNodes, forkJoinCount);
                    checkedNodes.add(transitionNode);
                }
            }
        }

        path.remove(nodeName);
    }

    /**
     * This method recursively validates two things:
     * - fork/join methods are properly paired
     * - there are no multiple "okTo" paths to a given node
     *
     * Important: this method assumes that the workflow is not acyclic - therefore this must run after performBasicValidation()
     *
     * @param app The WorkflowApp
     * @param node Current node we're checking
     * @param currentFork Current fork node (null if we are not under a fork path)
     * @param topDecisionParent The top (eldest) decision node along the path to this node, or null if there isn't one
     * @param okPath false if node (or an ancestor of node) was gotten to via an "error to" transition or via a join node that has
     * already been visited at least once before
     * @param forkJoins Map that contains a mapping of fork-join node pairs.
     * @param nodeAndDecisionParents Map that contains a mapping of nodes and their eldest decision node
     * @throws WorkflowException If there is any of the constraints described above is violated
     */
    private void validateForkJoin(LiteWorkflowApp app, NodeDef node, NodeDef currentFork, String topDecisionParent,
            boolean okPath, Deque<String> path, Map<String, String> forkJoins,
            Map<String, Optional<String>> nodeAndDecisionParents) throws WorkflowException {
        final String nodeName = node.getName();

        path.addLast(nodeName);

        /* If we're walking an "okTo" path and the nodes are not Kill/Join/End, we have to make sure that only a single
         * "okTo" path exists to the current node.
         *
         * The "topDecisionParent" represents the eldest decision in the chain that we've gone through. For example, let's assume
         * that D1, D2, D3 are decision nodes and A is an action node.
         *
         * D1-->D2-->D3---> ... (rest of the WF)
         *  |   |    |
         *  |   |    |
         *  |   |    +----> +---+
         *  |   +---------> | A |
         *  +-------------> +---+
         *
         * In this case, there are three "okTo" paths to "A" but it's still a valid workflow because the eldest decision node
         * is D1 and during every run, there is only one possible execution path that leads to A (D1->A, D1->D2->A or
         * (D1->D2->D3->A). In the code, if we encounter a decision node and we already have one, we don't update it. If it's null
         * then we set it to the current decision node we're under.
         *
         * If the "current" and "top" parents are null, it means that we reached the node from two separate "okTo" paths, which is
         * not acceptable.
         *
         * Also, if we have two distinct top decision parents it means that the node is reachable from two decision paths which
         * are not "chained" (like in the example).
         *
         * It's worth noting that the last two examples can only occur in case of fork-join when we start to execute at least
         * two separate paths in parallel. Without fork-join, multiple parents or two null parents would mean that there is a loop
         * in the workflow but that should not happen since it has been validated.
         */
        if (okPath && !(node instanceof KillNodeDef) && !(node instanceof JoinNodeDef) && !(node instanceof EndNodeDef)) {
            // using Optional here so we can distinguish between "non-visited" and "visited - no parent" state.
            Optional<String> decisionParentOpt = nodeAndDecisionParents.get(nodeName);
            if (decisionParentOpt == null) {
                nodeAndDecisionParents.put(node.getName(), Optional.fromNullable(topDecisionParent));
            } else {
                String decisionParent = decisionParentOpt.isPresent() ? decisionParentOpt.get() : null;

                if ((decisionParent == null && topDecisionParent == null) || !Objects.equal(decisionParent, topDecisionParent)) {
                    throw new WorkflowException(ErrorCode.E0743, nodeName);
                }
            }
        }

        /* Fork-Join validation logic:
         *
         * At each Fork node, we recurse to every possible paths, changing the "currentFork" variable to the Fork node. We stop
         * walking as soon as we encounter a Join node. At the Join node, we update the forkJoin mapping, which maintains
         * the relationship between every fork-join pair (actually it's join->fork mapping). We check whether the join->fork
         * mapping already contains another Fork node, which means that the Join is reachable from at least two distinct
         * Fork nodes, so we terminate the validation.
         *
         * From the Join node, we don't recurse further. Therefore, all recursive calls return back to the point where we called
         * validateForkJoin() from the Fork node in question.
         *
         * At this point, we have to check how many different Join nodes we've found at each different paths. We collect them to
         * a set, then we make sure that we have only a single Join node for all Fork paths. Otherwise the workflow is broken.
         *
         * If we have only a single Join, then we get the transition node from the Join and go on with the recursive validation -
         * this time we use the original "currentFork" variable that we have on the stack. With this approach, nested
         * Fork-Joins are handled correctly.
         */
        if (node instanceof ForkNodeDef) {
            final List<String> transitions = node.getTransitions();

            checkForkTransitions(app, transitions, node);

            for (String t : transitions) {
                NodeDef transition = app.getNode(t);
                validateForkJoin(app, transition, node, topDecisionParent, okPath, path, forkJoins, nodeAndDecisionParents);
            }

            // get the Join node for this ForkNode & validate it (we must have only one)
            Set<String> joins = new HashSet<String>();
            collectJoins(app, forkJoins, nodeName, joins);
            checkJoins(joins, nodeName);

            List<String> joinTransitions = app.getNode(joins.iterator().next()).getTransitions();
            NodeDef next = app.getNode(joinTransitions.get(0));

            validateForkJoin(app, next, currentFork, topDecisionParent, okPath, path, forkJoins, nodeAndDecisionParents);
        } else if (node instanceof JoinNodeDef) {
            if (currentFork == null) {
                throw new WorkflowException(ErrorCode.E0742, node.getName());
            }

            // join --> fork mapping
            String forkNode = forkJoins.get(nodeName);
            if (forkNode == null) {
                forkJoins.put(nodeName, currentFork.getName());
            } else if (!forkNode.equals(currentFork.getName())) {
                throw new WorkflowException(ErrorCode.E0758, node.getName(), forkNode + "," + currentFork);
            }
        } else if (node instanceof DecisionNodeDef) {
            List<String> transitions = node.getTransitions();

            // see explanation above - if we already have a topDecisionParent, we don't update it
            String parentDecisionNode = topDecisionParent;
            if (parentDecisionNode == null) {
                parentDecisionNode = nodeName;
            }

            for (String t : transitions) {
                NodeDef transition = app.getNode(t);
                validateForkJoin(app, transition, currentFork, parentDecisionNode, okPath, path, forkJoins,
                        nodeAndDecisionParents);
            }
        } else if (node instanceof KillNodeDef) {
            // no op
        } else if (node instanceof EndNodeDef) {
            // We can't end the WF if we're on a Fork path. From the "path" deque, we remove the last node (which
            // is the current "End") and look at last node again so we know where we came from
            if (currentFork != null) {
                path.removeLast();
                String previous = path.peekLast();
                throw new WorkflowException(ErrorCode.E0737, previous, node.getName());
            }
        } else if (node instanceof ActionNodeDef) {
            String transition = node.getTransitions().get(0);   // "ok to" transition
            NodeDef okNode = app.getNode(transition);
            validateForkJoin(app, okNode, currentFork, topDecisionParent, okPath, path, forkJoins, nodeAndDecisionParents);

            transition = node.getTransitions().get(1);          // "error to" transition
            NodeDef errorNode = app.getNode(transition);
            validateForkJoin(app, errorNode, currentFork, topDecisionParent, false, path, forkJoins, nodeAndDecisionParents);
        } else if (node instanceof StartNodeDef) {
            String transition = node.getTransitions().get(0);   // start always has only 1 transition
            NodeDef tranNode = app.getNode(transition);
            validateForkJoin(app, tranNode, currentFork, topDecisionParent, okPath, path, forkJoins, nodeAndDecisionParents);
        } else {
            throw new WorkflowException(ErrorCode.E0740, node.getClass());
        }

        path.remove(nodeName);
    }

    private void checkActionName(NodeDef node) throws WorkflowException {
        if (!(node instanceof StartNodeDef)) {
            try {
                ParamChecker.validateActionName(node.getName());
            } catch (IllegalArgumentException ex) {
                throw new WorkflowException(ErrorCode.E0724, ex.getMessage());
            }
        }
    }

    private void checkActionNode(NodeDef node) throws WorkflowException {
        try {
            Element action = XmlUtils.parseXml(node.getConf());
            ActionService actionService = Services.get().get(ActionService.class);
            boolean supportedAction = actionService.hasActionType(action.getName());
            if (!supportedAction) {
                throw new WorkflowException(ErrorCode.E0723, node.getName(), action.getName());
            }
        } catch (JDOMException ex) {
            throw new WorkflowException(ErrorCode.E0700, "JDOMException: " + ex.getMessage());
        }
    }

    private void checkCycle(Deque<String> path, String nodeName) throws WorkflowException {
        if (path.contains(nodeName)) {
            path.addLast(nodeName);
            throw new WorkflowException(ErrorCode.E0707, nodeName, Joiner.on("->").join(path));
        }
    }

    // Check that a fork doesn't go to the same node more than once
    private void checkForkTransitions(LiteWorkflowApp app, List<String> transitionsList, NodeDef node) throws WorkflowException {
        for (final String t : transitionsList) {
            NodeDef aNode = app.getNode(t);
            // Now we have to figure out which node is the problem and what type of node they are (join and kill are ok)
            if (!(aNode instanceof JoinNodeDef) && !(aNode instanceof KillNodeDef)) {
                int count = CollectionUtils.cardinality(t, transitionsList);

                if (count > 1) {
                    throw new WorkflowException(ErrorCode.E0744, node.getName(), t);
                }
            }
        }
    }

    private void collectJoins(LiteWorkflowApp app, Map<String, String> forkJoinPairs, String nodeName, Set<String> joins) {
        for (Entry<String, String> entry : forkJoinPairs.entrySet()) {
            if (entry.getValue().equals(nodeName)) {
                joins.add(app.getNode(entry.getKey()).getName());
            }
        }
    }

    private void checkJoins(Set<String> joinNodes, String forkName) throws WorkflowException {
        if (joinNodes.size() == 0) {
            throw new WorkflowException(ErrorCode.E0733, forkName);
        }

        if (joinNodes.size() > 1) {
            throw new WorkflowException(ErrorCode.E0757, forkName, Joiner.on(",").join(joinNodes));
        }
    }

    // Tiny utility class where we keep track of how many fork and join nodes we have found
    private class ForkJoinCount {
        int forks = 0;
        int joins = 0;
    }
}