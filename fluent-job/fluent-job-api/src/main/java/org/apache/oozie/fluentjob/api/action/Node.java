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

package org.apache.oozie.fluentjob.api.action;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.oozie.fluentjob.api.Condition;
import org.apache.oozie.fluentjob.api.workflow.Credential;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * An abstract base class for API level action nodes. Concrete instances of the actions should be created using their
 * respective builders that inherit from {@link NodeBuilderBaseImpl} and implement the {@link Builder} interface.
 *
 * The public interface of {@link Node} objects is immutable. This way we can ensure at compile time that there are no
 * cycles in the graph: once a node is built, it cannot get new parents. On the other hand, {@link Node} objects still
 * keep track of their children, which are necessarily added after the parent node is built, so  these objects are not
 * really immutable and should not be used in a multithreaded environment without external synchronization.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class Node {
    private final Attributes attributes;
    private final ImmutableList<Node> parentsWithoutConditions;
    private final ImmutableList<Node.NodeWithCondition> parentsWithConditions;
    private final ErrorHandler errorHandler;

    private final List<Node> childrenWithoutConditions; // MUTABLE!
    private final List<NodeWithCondition> childrenWithConditions; // MUTABLE!
    private Node defaultConditionalChild; // MUTABLE!

    Node(final ConstructionData constructionData) {
        this(constructionData.attributes,
             constructionData.parents,
             constructionData.parentsWithConditions,
             constructionData.errorHandler);
    }

    Node(final Attributes attributes,
         final ImmutableList<Node> parentsWithoutConditions,
         final ImmutableList<Node.NodeWithCondition> parentsWithConditions,
         final ErrorHandler errorHandler)
    {
        this.attributes = attributes;
        this.parentsWithoutConditions = parentsWithoutConditions;
        this.parentsWithConditions = parentsWithConditions;
        this.errorHandler = errorHandler;

        this.childrenWithoutConditions = new ArrayList<>();
        this.childrenWithConditions = new ArrayList<>();
        this.defaultConditionalChild = null;
    }

    /**
     * Returns the name of the node.
     * @return The name of the node.
     */
    public String getName() {
        return attributes.name;
    }

    /**
     * Get the {@link Credential} associated with this {@link Node}.
     * @return the {@link Credential} associated with this {@link Node}
     */
    public List<Credential> getCredentials() {
        return attributes.credentials;
    }

    /**
     * Get the {@code retry-max} attribute of this {@link Node}.
     * @return the {@code retry-max}
     */
    public Integer getRetryMax() {
        return attributes.retryMax;
    }

    /**
     * Get the {@code retry-interval} attribute of this {@link Node}.
     * @return the {@code retry-interval}
     */
    public Integer getRetryInterval() {
        return attributes.retryInterval;
    }

    /**
     * Get the {@code retry-policy} attribute of this {@link Node}.
     * @return the {@code retry-policy}
     */
    public String getRetryPolicy() {
        return attributes.retryPolicy;
    }

    /**
     * Returns a list of all the parents of this node, including unconditional and conditional parents.
     * @return A list of all the parents of this node object.
     */
    public List<Node> getAllParents() {
        final List<Node> allParents = new ArrayList<>(parentsWithoutConditions);

        for (final NodeWithCondition parentWithCondition : parentsWithConditions) {
            allParents.add(parentWithCondition.getNode());
        }

        return Collections.unmodifiableList(allParents);
    }

    /**
     * Returns a list of the unconditional parents of this node.
     * @return A list of the unconditional parents of this node.
     */
    public List<Node> getParentsWithoutConditions() {
        return parentsWithoutConditions;
    }

    /**
     * Returns a list of the conditional parents of this node together with their conditions.
     * @return A list of the conditional parents of this node together with their conditions.
     */
    public List<Node.NodeWithCondition> getParentsWithConditions() {
        return parentsWithConditions;
    }

    /**
     * Returns the error handler of this node.
     * @return The error handler of this node.
     */
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    void addChild(final Node child) {
        Preconditions.checkState(childrenWithConditions.isEmpty(),
                "Trying to add a child without condition to a node that already has at least one child with a condition.");

        this.childrenWithoutConditions.add(child);
    }

    void addChildWithCondition(final Node child, final String condition) {
        Preconditions.checkState(childrenWithoutConditions.isEmpty(),
                "Trying to add a child with condition to a node that already has at least one child without a condition.");

        this.childrenWithConditions.add(new NodeWithCondition(child, Condition.actualCondition(condition)));
    }

    void addChildAsDefaultConditional(final Node child) {
        Preconditions.checkState(childrenWithoutConditions.isEmpty(),
                "Trying to add a default conditional child to a node that already has at least one child without a condition.");

        Preconditions.checkState(defaultConditionalChild == null,
                "Trying to add a default conditional child to a node that already has one.");

        this.defaultConditionalChild = child;
    }

    /**
     * Returns an unmodifiable view of list of all the children of this {@link Node}.
     * @return An unmodifiable view of list of all the children of this {@link Node}.
     */
    public List<Node> getAllChildren() {
        final List<Node> allChildren = new ArrayList<>(childrenWithoutConditions);

        for (final NodeWithCondition nodeWithCondition : getChildrenWithConditions()) {
            allChildren.add(nodeWithCondition.getNode());
        }

        return Collections.unmodifiableList(allChildren);
    }

    /**
     * Returns an unmodifiable view of list of the children without condition of this {@link Node}.
     * @return An unmodifiable view of list of the children without condition of this {@link Node}.
     */
    public List<Node> getChildrenWithoutConditions() {
        return Collections.unmodifiableList(childrenWithoutConditions);
    }

    /**
     * Returns an unmodifiable view of list of the children with condition (including the default) of this {@link Node}.
     * @return An unmodifiable view of list of the children with condition (including the default) of this {@link Node}.
     */
    public List<NodeWithCondition> getChildrenWithConditions() {
        if (defaultConditionalChild == null) {
            return Collections.unmodifiableList(childrenWithConditions);
        }

        final List<NodeWithCondition> results = new ArrayList<>(childrenWithConditions);
        results.add(new NodeWithCondition(defaultConditionalChild, Condition.defaultCondition()));

        return Collections.unmodifiableList(results);
    }

    /**
     * Returns the default conditional child of this {@link Node}.
     * @return The default conditional child of this {@link Node}.
     */
    public Node getDefaultConditionalChild() {
        return defaultConditionalChild;
    }

    public static class NodeWithCondition {
        private final Node node;
        private final Condition condition;

        public NodeWithCondition(final Node node,
                                 final Condition condition) {
            this.node = node;
            this.condition = condition;
        }

        public Node getNode() {
            return node;
        }

        public Condition getCondition() {
            return condition;
        }
    }

    static class ConstructionData {

        ConstructionData(final Attributes attributes,
                         final ImmutableList<Node> parents,
                         final ImmutableList<NodeWithCondition> parentsWithConditions,
                         final ErrorHandler errorHandler) {
            this.attributes = attributes;
            this.parents = parents;
            this.parentsWithConditions = parentsWithConditions;
            this.errorHandler = errorHandler;
        }

        private final Attributes attributes;
        private final ImmutableList<Node> parents;
        private final ImmutableList<NodeWithCondition> parentsWithConditions;
        private final ErrorHandler errorHandler;
    }

    static class Attributes {
        private final String name;
        private final ImmutableList<Credential> credentials;
        private final Integer retryMax;
        private final Integer retryInterval;
        private final String retryPolicy;

        Attributes(final String name) {
            this(name, ImmutableList.of(), null, null, null);
        }

        Attributes(final String name,
                   final List<Credential> credentials,
                   final Integer retryMax,
                   final Integer retryInterval,
                   final String retryPolicy) {
            this.name = name;
            this.credentials = ImmutableList.copyOf(credentials);
            this.retryMax = retryMax;
            this.retryInterval = retryInterval;
            this.retryPolicy = retryPolicy;
        }
    }
}
