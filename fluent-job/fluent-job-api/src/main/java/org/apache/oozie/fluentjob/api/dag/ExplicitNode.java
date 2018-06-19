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

import com.google.common.base.Preconditions;
import org.apache.oozie.fluentjob.api.action.Node;
import org.apache.oozie.fluentjob.api.Condition;

import java.util.Arrays;
import java.util.List;

/**
 * A class representing action nodes in an Oozie workflow definition DAG. These are the nodes in the intermediate graph
 * representation that correspond to the nodes that are explicitly defined by the user.
 */
public class ExplicitNode extends NodeBase {
    private NodeBase parent;
    private NodeBase child;
    private final Node realNode;

    /**
     * Create a new explicit node with the given name.
     * @param name The name of the new explicit node.
     * @param realNode The API level {@link Node} object defined by the end user.
     */
    public ExplicitNode(final String name, final Node realNode) {
        super(name);
        this.realNode = realNode;
    }

    /**
     * Returns the API level {@link Node} object defined by the end user.
     * @return The API level {@link Node} object defined by the end user.
     */
    public Node getRealNode() {
        return realNode;
    }

    /**
     * Returns the parent of this node.
     * @return The parent of this node.
     */
    public NodeBase getParent() {
        return parent;
    }

    /**
     * Returns the child of this node.
     * @return The child of this node.
     */
    public NodeBase getChild() {
        return child;
    }

    /**
     * Adds the provided node as a parent of this node.
     * @param parent The new parent of this node.
     * @throws IllegalStateException if this node already has a parent.
     */
    @Override
    public void addParent(final NodeBase parent) {
        Preconditions.checkState(this.parent == null, "An explicit node cannot have multiple parents.");

        this.parent = parent;
        parent.addChild(this);
    }

    /**
     * Adds the provided node as a conditional parent of this node.
     * @param parent The new conditional parent of this node.
     * @param condition The condition which must be true in addition the parent completing successfully for this node
     *                  to be executed.
     * @throws IllegalStateException if this node already has a parent.
     */
    @Override
    public void addParentWithCondition(final Decision parent, final Condition condition) {
        Preconditions.checkState(this.parent == null, "An explicit node cannot have multiple parents.");

        this.parent = parent;
        parent.addChildWithCondition(this, condition);
    }

    /**
     * Adds the provided node as the default conditional parent of this node.
     * @param parent The new conditional parent of this node.
     * @throws IllegalStateException if this node already has a parent.
     */
    @Override
    public void addParentDefaultConditional(Decision parent) {
        Preconditions.checkState(this.parent == null, "An explicit node cannot have multiple parents.");

        this.parent = parent;
        parent.addDefaultChild(this);
    }

    @Override
    public void removeParent(final NodeBase parent) {
        Preconditions.checkArgument(this.parent == parent, "Trying to remove a nonexistent parent.");

        if (this.parent != null) {
            this.parent.removeChild(this);
        }

        this.parent = null;
    }

    @Override
    public void clearParents() {
        removeParent(parent);
    }

    @Override
    public List<NodeBase> getChildren() {
        if (child == null) {
            return Arrays.asList();
        }

        return Arrays.asList(child);
    }

    @Override
    protected void addChild(final NodeBase child) {
        Preconditions.checkState(this.child == null, "Normal nodes cannot have multiple children.");

        this.child = child;
    }

    @Override
    protected void removeChild(final NodeBase child) {
        Preconditions.checkArgument(this.child == child, "Trying to remove a nonexistent child.");

        this.child = null;
    }
}