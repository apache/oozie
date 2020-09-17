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

import java.util.List;

/**
 * An abstract base class for nodes in the intermediate graph representation of the workflow. Nodes in this graph may
 * contain information about nodes of the API level graph ({@link ExplicitNode}) or be generated control nodes
 * (for example {@link Fork}).
 * <p>
 * These nodes should not be used directly by the end user.
 */
public abstract class NodeBase {
    private final String name;

    /**
     * Creates a new {@link NodeBase} object with the given name.
     * @param name The name of the new {@link NodeBase} object.
     */
    protected NodeBase(final String name) {
        this.name = name;
    }

    /**
     * Returns the name of this {@link NodeBase} object.
     * @return The name of this {@link NodeBase} object.
     */
    public String getName() {
        return name;
    }

    /**
     * Adds the provided node as a parent of this {@link NodeBase} object.
     * @param parent The new parent of this {@link NodeBase} object.
     */
    public abstract void addParent(final NodeBase parent);

    /**
     * Adds the provided node as a conditional parent of this {@link NodeBase} object.
     * @param parent The new conditional parent of this {@link NodeBase} object.
     * @param condition The condition which must be true in addition the parent completing successfully for this node
     *                  to be executed.
     */
    public abstract void addParentWithCondition(final Decision parent, final Condition condition);

    /**
     * Adds the provided node as the default conditional parent of this {@link NodeBase} object.
     * @param parent The new conditional parent of this {@link NodeBase} object.
     */
    public abstract void addParentDefaultConditional(final Decision parent);

    /**
     * Removes a parent (whether or not conditional).
     * @param parent The parent to remove.
     * @throws IllegalStateException if {@code parent} is not the parent of this node.
     */
    public abstract void removeParent(final NodeBase parent);

    /**
     * Removes all parents (whether or not conditional) of this {@link NodeBase} object.
     */
    public abstract void clearParents();

    /**
     * Returns all the children (whether conditional or not) of this {@link NodeBase} object.
     * @return All the children (whether conditional or not) of this {@link NodeBase} object.
     */
    public abstract List<NodeBase> getChildren();

    /**
     * Adds a child to this {@link NodeBase} object. This method should only be used in the implementation of the
     * {@link NodeBase#addParent(NodeBase)} method of a subclass of {@link NodeBase}, so that whenever a node is assigned a new
     * parent, the parent also knows about the child.
     * @param child The node to be added as the child of this {@link NodeBase}.
     */
    protected abstract void addChild(final NodeBase child);

    /**
     * Removes the specified child from this {@link NodeBase} object. This method should only be used in the implementation of the
     * {@link NodeBase#removeParent(NodeBase)} method of a subclass of {@link NodeBase}, so that whenever a child is removed from a
     * node, the parent also knows about the child having been removed.
     * @param child The node to be removed as the child of this {@link NodeBase}.
     */
    protected abstract void removeChild(final NodeBase child);
}
