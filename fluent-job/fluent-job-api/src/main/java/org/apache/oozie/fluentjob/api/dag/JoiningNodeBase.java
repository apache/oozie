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
import org.apache.oozie.fluentjob.api.Condition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * An abstract base class for {@link Join} and {@link DecisionJoin}.
 * @param <T> The type of the node for which this class is the closing pair.
 */
public abstract class JoiningNodeBase<T> extends NodeBase {
    private final List<NodeBase> parents;
    private NodeBase child;

    private final T branching;

    JoiningNodeBase(final String name, final T branching) {
        super(name);

        this.parents = new ArrayList<>();
        this.branching = branching;
    }

    /**
     * Returns an unmodifiable list of the parents of this node.
     * @return An unmodifiable list of the parents of this node.
     */
    public List<NodeBase> getParents() {
        return Collections.unmodifiableList(parents);
    }

    @Override
    public void addParent(final NodeBase parent) {
        if (parent != null) {
            parent.addChild(this);
        }

        parents.add(parent);
    }

    @Override
    public void addParentWithCondition(final Decision parent, final Condition condition) {
        if (parent != null) {
            parent.addChildWithCondition(this,  condition);
        }

        parents.add(parent);
    }

    @Override
    public void addParentDefaultConditional(final Decision parent) {
        if (parent != null) {
            parent.addDefaultChild(this);
        }

        parents.add(parent);
    }

    @Override
    public void removeParent(final NodeBase parent) {
        Preconditions.checkArgument(parents.remove(parent), "Trying to remove a nonexistent parent");

        parent.removeChild(this);
    }

    @Override
    public void clearParents() {
        final List<NodeBase> oldParents = new ArrayList<>(parents);
        for (final NodeBase parent : oldParents) {
            removeParent(parent);
        }
    }

    @Override
    public List<NodeBase> getChildren() {
        if (child == null) {
            return Arrays.asList();
        }

        return Arrays.asList(child);
    }

    public NodeBase getChild() {
        return child;
    }

    T getBranchingPair() {
        return branching;
    }

    @Override
    protected void addChild(final NodeBase child) {
        Preconditions.checkState(this.child == null, "JoiningNodeBase nodes cannot have multiple children.");

        this.child = child;
    }

    @Override
    protected void removeChild(final NodeBase child) {
        Preconditions.checkArgument(this.child == child, "Trying to remove a nonexistent child.");

        this.child = null;
    }
}
