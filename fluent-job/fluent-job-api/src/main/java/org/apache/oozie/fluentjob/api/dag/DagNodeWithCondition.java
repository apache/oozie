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

import java.util.Collection;

/**
 * This is a class bundling together a {@link NodeBase} object and a {@link Condition} object. There are no restrictions
 * as to whether the node should be a parent with an outgoing conditional path or a child with an incoming conditional path.
 */
public class DagNodeWithCondition {
    private final NodeBase node;
    private final Condition condition;

    /**
     * Removes the first {@link DagNodeWithCondition} object from a collection that has the provided node as its node.
     * If there is no such object in the collection, this method returns false;
     * @param collection The collection from which to remove an element
     * @param node The node to remove together with its condition.
     * @return {@code true} if an element was removed; {@code false} if no matching element was contained in the collection.
     */
    public static boolean removeFromCollection(final Collection<DagNodeWithCondition> collection, final NodeBase node) {
        DagNodeWithCondition element = null;
        for (final DagNodeWithCondition nodeWithCondition : collection) {
            if (node.equals(nodeWithCondition.getNode())) {
                element = nodeWithCondition;
            }
        }

        if (element != null) {
            collection.remove(element);
        }

        return element != null;
    }

    /**
     * Creates a new {@link DagNodeWithCondition} object.
     * @param node A {@link NodeBase} object.
     * @param condition A {@link Condition} object.
     */
    public DagNodeWithCondition(final NodeBase node,
                                final Condition condition) {
        this.node = node;
        this.condition = condition;
    }

    /**
     * Returns the {@link NodeBase} object of this {@link DagNodeWithCondition}.
     * @return The {@link NodeBase} object of this {@link DagNodeWithCondition}.
     */
    public NodeBase getNode() {
        return node;
    }

    /**
     * Returns the {@link Condition} object of this {@link DagNodeWithCondition}.
     * @return The {@link Condition} object of this {@link DagNodeWithCondition}.
     */
    public Condition getCondition() {
        return condition;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final DagNodeWithCondition that = (DagNodeWithCondition) o;

        if (node != null ? !node.equals(that.node) : that.node != null) {
            return false;
        }

        return condition != null ? condition.equals(that.condition) : that.condition == null;
    }

    @Override
    public int hashCode() {
        int result = node != null ? node.hashCode() : 0;
        result = 31 * result + (condition != null ? condition.hashCode() : 0);

        return result;
    }
}
