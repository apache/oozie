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

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public abstract class TestJoiningNodeBase<B, J extends JoiningNodeBase<B>> extends TestNodeBase<JoiningNodeBase<B>> {
    protected abstract B getBranchingInstance(final String name);
    protected abstract J getJoiningInstance(final String name, final B branchingPair);

    protected  J getJoiningInstance(final String name) {
        return getJoiningInstance(name, getBranchingInstance("branching"));
    }

    @Test
    public void testCorrespondingBranchingIsCorrect() {
        B branching = getBranchingInstance("branching");
        J joining = getJoiningInstance("joining", branching);

        assertEquals(branching, joining.getBranchingPair());
    }

    @Test
    public void testAddParentWhenNoneAlreadyExists() {
        final ExplicitNode parent = new ExplicitNode("parent", null);
        final J instance = getJoiningInstance("instance");

        instance.addParent(parent);
        assertEquals(Arrays.asList(parent), instance.getParents());
        assertEquals(instance, parent.getChild());
    }

    @Test
    public void testAddParentWhenSomeAlreadyExist() {
        final NodeBase parent1 = new ExplicitNode("parent1", null);
        final NodeBase parent2 = new ExplicitNode("parent2", null);

        final J instance = getJoiningInstance("instance");

        instance.addParent(parent1);
        instance.addParent(parent2);

        assertEquals(Arrays.asList(parent1, parent2), instance.getParents());
    }

    @Test
    public void testRemoveExistingParent() {
        final ExplicitNode parent1 = new ExplicitNode("parent1", null);
        final ExplicitNode parent2 = new ExplicitNode("parent2", null);

        final J instance = getJoiningInstance("instance");

        instance.addParent(parent1);
        instance.addParent(parent2);

        instance.removeParent(parent2);
        assertEquals(Arrays.asList(parent1), instance.getParents());
        assertEquals(null, parent2.getChild());
    }

    @Test
    public void testRemoveNonexistentParentThrows() {
        final ExplicitNode parent = new ExplicitNode("parent", null);
        final J instance = getJoiningInstance("instance");

        expectedException.expect(IllegalArgumentException.class);
        instance.removeParent(parent);
    }

    @Test
    public void testClearExistingParent() {
        final ExplicitNode parent1 = new ExplicitNode("parent1", null);
        final ExplicitNode parent2 = new ExplicitNode("parent2", null);

        final J instance = getJoiningInstance("instance");

        instance.addParent(parent1);
        instance.addParent(parent2);

        instance.clearParents();
        assertEquals(0, instance.getParents().size());
        assertEquals(null, parent1.getChild());
        assertEquals(null, parent2.getChild());
    }

    @Test
    public void testClearNonExistentParent() {
        final J instance = getJoiningInstance("instance");

        instance.clearParents();
        assertEquals(0, instance.getParents().size());
    }

    @Test
    public void testJoinAddedAsParentWhenItHasNoChild() {
        final J instance = getJoiningInstance("instance");
        final NodeBase child = new ExplicitNode("child", null);

        child.addParent(instance);

        assertEquals(child, instance.getChild());
    }

    @Test
    public void testJoinAddedAsParentWhenItAlreadyHasAChildThrows() {
        final J instance = getJoiningInstance("instance");
        final NodeBase child1 = new ExplicitNode("child1", null);
        final NodeBase child2 = new ExplicitNode("child2", null);

        child1.addParent(instance);

        expectedException.expect(IllegalStateException.class);
        child2.addParent(instance);
    }

    @Test
    public void testJoinRemovedAsParent() {
        final J instance = getJoiningInstance("instance");
        final NodeBase child = new ExplicitNode("child", null);

        child.addParent(instance);

        child.removeParent(instance);

        assertEquals(null, instance.getChild());
    }

    @Test
    public void testGetChildren() {
        final J instance = getJoiningInstance("instance");
        final NodeBase child = new ExplicitNode("child", null);

        child.addParent(instance);

        assertEquals(Arrays.asList(child), instance.getChildren());
    }
}
