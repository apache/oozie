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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.oozie.fluentjob.api.Condition;
import org.apache.oozie.fluentjob.api.ModifyOnce;
import org.apache.oozie.fluentjob.api.workflow.Credential;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * An abstract base class for builders that build concrete instances of subclasses of {@link Node}. This class doesn't
 * implement the {@link Builder} interface as no type information as to the concrete node to build. The concrete node
 * builder classes of course should implement {@link Builder}.
 *
 * The concrete builders should provide a fluent API, and to facilitate this, the methods in this base class have to
 * return the concrete builder. Therefore it is templated on the type of the concrete builder class. Although it cannot
 * be enforced that the provided generic parameter is the same as the class deriving from this class, it definitely
 * should be, and the constraint on the type parameter tries to minimize the chance that the class is subclassed
 * incorrectly.
 *
 * The properties of the builder can only be set once, an attempt to set them a second time will trigger
 * an {@link IllegalStateException}. The properties that are lists are an exception to this rule, of course multiple
 * elements can be added / removed.
 *
 * @param <B> The type of the concrete builder class deriving from this class.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class NodeBuilderBaseImpl <B extends NodeBuilderBaseImpl<B>> {
    private final ModifyOnce<String> name;
    private final List<Credential> credentials;
    private final ModifyOnce<Integer> retryMax;
    private final ModifyOnce<Integer> retryInterval;
    private final ModifyOnce<String> retryPolicy;
    private final List<Node> parents;
    private final List<Node.NodeWithCondition> parentsWithConditions;

    private final ModifyOnce<ErrorHandler> errorHandler;

    NodeBuilderBaseImpl() {
        this(null);
    }

    NodeBuilderBaseImpl(final Node node) {
        if (node == null) {
            name = new ModifyOnce<>();
            credentials = new ArrayList<>();
            retryMax = new ModifyOnce<>();
            retryInterval = new ModifyOnce<>();
            retryPolicy = new ModifyOnce<>();
            parents = new ArrayList<>();
            parentsWithConditions = new ArrayList<>();
            errorHandler = new ModifyOnce<>();
        }
        else {
            // Names won't be copied as we need unique names within a workflow
            name = new ModifyOnce<>();
            credentials = new ArrayList<>(node.getCredentials());
            retryMax = new ModifyOnce<>(node.getRetryMax());
            retryInterval = new ModifyOnce<>(node.getRetryInterval());
            retryPolicy = new ModifyOnce<>(node.getRetryPolicy());
            parents = new ArrayList<>(node.getParentsWithoutConditions());
            parentsWithConditions = new ArrayList<>(node.getParentsWithConditions());
            errorHandler = new ModifyOnce<>(node.getErrorHandler());
        }
    }

    /**
     * Registers an error handler with this builder.
     * @param errorHandler The error handler to register.
     * @return This builder.
     */
    public B withErrorHandler(final ErrorHandler errorHandler) {
        this.errorHandler.set(errorHandler);
        return ensureRuntimeSelfReference();
    }

    /**
     * Removes the currently registered error handler if any.
     * @return This builder.
     */
    public B withoutErrorHandler() {
        errorHandler.set(null);
        return ensureRuntimeSelfReference();
    }

    /**
     * Registers a name that will be the name of the action built by this builder.
     * @param name The name of the action that will be built.
     * @return This builder.
     */
    public B withName(final String name) {
        this.name.set(name);
        return ensureRuntimeSelfReference();
    }

    /**
     * Registers a {@link Credential} that will be the {@code cred} of the action built by this builder.
     * @param credential The {@link Credential} of the action that will be built.
     * @return This builder.
     */
    public B withCredential(final Credential credential) {
        this.credentials.add(credential);
        return ensureRuntimeSelfReference();
    }

    /**
     * Removes a {@link Credential} registered with this builder. If the {@code credential} is not registered with this builder,
     * this method does nothing.
     * @param credential The {@link Credential} to remove.
     * @return This builder.
     */
    public B withoutCredential(final Credential credential) {
        this.credentials.remove(credential);
        return ensureRuntimeSelfReference();
    }

    /**
     * Removes all {@link Credential}s registered with this builder.
     * @return This builder.
     */
    public B clearCredentials() {
        this.credentials.clear();
        return ensureRuntimeSelfReference();
    }

    /**
     * Registers an {@link Integer} that will be the {@code retry-max} of the action built by this builder.
     * @param retryMax The {@code retry-max} of the action that will be built.
     * @return This builder.
     */
    public B withRetryMax(final Integer retryMax) {
        this.retryMax.set(retryMax);
        return ensureRuntimeSelfReference();
    }

    /**
     * Registers an {@link Integer} that will be the {@code retry-interval} of the action built by this builder.
     * @param retryInterval The {@code retry-interval} of the action that will be built.
     * @return This builder.
     */
    public B withRetryInterval(final Integer retryInterval) {
        this.retryInterval.set(retryInterval);
        return ensureRuntimeSelfReference();
    }

    /**
     * Registers a {@link String} that will be the {@code retry-policy} of the action built by this builder.
     * @param retryPolicy The {@code retry-policy} of the action that will be built.
     * @return This builder.
     */
    public B withRetryPolicy(final String retryPolicy) {
        this.retryPolicy.set(retryPolicy);
        return ensureRuntimeSelfReference();
    }

    /**
     * Registers an unconditional parent with this builder. If the parent is already registered with this builder,
     * {@link IllegalArgumentException} is thrown.
     * @param parent The node that will be the parent of the built action.
     * @return This builder.
     *
     * @throws IllegalArgumentException if the provided node is already registered as a parent.
     */
    public B withParent(final Node parent) {
        checkNoDuplicateParent(parent);

        parents.add(parent);
        return ensureRuntimeSelfReference();
    }

    /**
     * Registers a conditional parent with this builder. If the parent is already registered with this builder,
     * {@link IllegalArgumentException} is thrown.
     * @param parent The node that will be the parent of the built action.
     * @param condition The condition of the parent.
     * @return This builder.
     *
     * @throws IllegalArgumentException if the provided node is already registered as a parent.
     */
    public B withParentWithCondition(final Node parent, final String condition) {
        checkNoDuplicateParent(parent);

        parentsWithConditions.add(new Node.NodeWithCondition(parent, Condition.actualCondition(condition)));
        return ensureRuntimeSelfReference();
    }

    /**
     * Registers a conditional parent for which this node is the default transition. If the parent is already registered
     * with this builder, {@link IllegalArgumentException} is thrown.
     * {@link IllegalArgumentException} is thrown.
     * @param parent The node that will be the parent of the built action.
     * @return This builder.
     *
     * @throws IllegalArgumentException if the provided node is already registered as a parent.
     */
    public B withParentDefaultConditional(final Node parent) {
        checkNoDuplicateParent(parent);

        parentsWithConditions.add(new Node.NodeWithCondition(parent, Condition.defaultCondition()));
        return ensureRuntimeSelfReference();
    }

    /**
     * Removes a parent registered with this builder. If the parent is not registered with this builder, this method
     * does nothing.
     * @param parent The parent to remove.
     * @return This builder.
     */
    public B withoutParent(final Node parent) {
        if (parents.contains(parent)) {
            parents.remove(parent);
        } else {
            int index = indexOfParentAmongParentsWithConditions(parent);
            if (index >= 0) {
                parentsWithConditions.remove(index);
            }
        }

        return ensureRuntimeSelfReference();
    }

    /**
     * Removes all parents registered with this builder.
     * @return This builder.
     */
    public B clearParents() {
        parents.clear();
        parentsWithConditions.clear();
        return ensureRuntimeSelfReference();
    }

    final B ensureRuntimeSelfReference() {
        final B runtimeSelfReference = getRuntimeSelfReference();

        Preconditions.checkState(runtimeSelfReference == this, "The builder type B doesn't extend NodeBuilderBaseImpl<B>.");

        return runtimeSelfReference;
    }

    private void checkNoDuplicateParent(final Node parent) {
        boolean parentsContains = parents.contains(parent);
        boolean parentsWithConditionsContains = indexOfParentAmongParentsWithConditions(parent) != -1;

        Preconditions.checkArgument(!parentsContains && !parentsWithConditionsContains,
                "Trying to add a parent that is already a parent of this node.");
    }

    private int indexOfParentAmongParentsWithConditions(final Node parent) {
        for (int i = 0; i < parentsWithConditions.size(); ++i) {
            if (parent == parentsWithConditions.get(i).getNode()) {
                return i;
            }
        }

        return -1;
    }

    protected void addAsChildToAllParents(final Node child) {
        final List<Node> parentsList = child.getParentsWithoutConditions();
        if (parentsList != null) {
            for (final Node parent : parentsList) {
                parent.addChild(child);
            }
        }

        final List<Node.NodeWithCondition> parentsWithConditionsList = child.getParentsWithConditions();
        if (parentsWithConditionsList != null) {
            for (final Node.NodeWithCondition parentWithCondition : parentsWithConditionsList) {
                final Node parent = parentWithCondition.getNode();
                final Condition condition = parentWithCondition.getCondition();

                if (condition.isDefault()) {
                    parent.addChildAsDefaultConditional(child);
                }
                else {
                    parent.addChildWithCondition(child, condition.getCondition());
                }
            }
        }
    }

    Node.ConstructionData getConstructionData() {
        final String nameStr = ensureName();

        final ImmutableList<Node> parentsList = new ImmutableList.Builder<Node>().addAll(parents).build();
        final ImmutableList<Node.NodeWithCondition> parentsWithConditionsList
                = new ImmutableList.Builder<Node.NodeWithCondition>().addAll(parentsWithConditions).build();

        return new Node.ConstructionData(
                new Node.Attributes(nameStr,
                        credentials,
                        retryMax.get(),
                        retryInterval.get(),
                        retryPolicy.get()),
                parentsList,
                parentsWithConditionsList,
                errorHandler.get()
        );
    }

    private String ensureName() {
        if (Strings.isNullOrEmpty(this.name.get())) {
            final String type = getRuntimeSelfReference().getClass().getSimpleName()
                    .toLowerCase(Locale.getDefault()).replaceAll("actionbuilder", "");
            final int randomSuffix = new SecureRandom().nextInt(1_000_000_000);

            this.name.set(String.format("%s-%d", type, randomSuffix));
        }

        return this.name.get();
    }

    protected abstract B getRuntimeSelfReference();
}
