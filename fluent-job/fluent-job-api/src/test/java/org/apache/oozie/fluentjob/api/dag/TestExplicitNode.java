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

import org.apache.oozie.fluentjob.api.action.MapReduceActionBuilder;
import org.apache.oozie.fluentjob.api.action.Node;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class TestExplicitNode extends TestNodeBase<ExplicitNode> {
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Override
    protected ExplicitNode getInstance(final String name) {
        return new ExplicitNode(name, null);
    }

    @Test
    public void testRealNode() {
        final Node node = MapReduceActionBuilder.create().build();
        final ExplicitNode instance = new ExplicitNode(NAME, node);

        assertEquals(node, instance.getRealNode());
    }

    @Test
    public void testAddParentWhenNoneAlreadyExists() {
        final ExplicitNode parent = getInstance("parent");
        final ExplicitNode instance = getInstance("instance");

        instance.addParent(parent);
        assertEquals(parent, instance.getParent());
        assertEquals(instance, parent.getChild());
    }

    @Test
    public void testAddingParentWhenItAlreadyExistsThrows() {
        final NodeBase parent1 = getInstance("parent1");
        final NodeBase parent2 = getInstance("parent2");

        final ExplicitNode instance = getInstance("instance");

        instance.addParent(parent1);

        expectedException.expect(IllegalStateException.class);
        instance.addParent(parent2);
    }

    @Test
    public void testRemoveExistingParent() {
        final ExplicitNode parent = getInstance("parent");
        final ExplicitNode instance = getInstance("instance");

        instance.addParent(parent);

        instance.removeParent(parent);
        assertEquals(null, instance.getParent());
        assertEquals(null, parent.getChild());
    }

    @Test
    public void testRemoveNonexistentParentThrows() {
        final ExplicitNode parent = getInstance("parent");
        final ExplicitNode instance = getInstance("instance");

        expectedException.expect(IllegalArgumentException.class);
        instance.removeParent(parent);
    }

    @Test
    public void testClearExistingParent() {
        final Start parent = new Start("parent");
        final ExplicitNode instance = getInstance("instance");

        instance.addParent(parent);

        instance.clearParents();
        assertEquals(null, instance.getParent());
        assertEquals(null, parent.getChild());
    }

    @Test
    public void testClearNonExistentParent() {
        final Start parent = new Start("parent");
        final ExplicitNode instance = getInstance("instance");

        instance.clearParents();
        assertEquals(null, instance.getParent());
        assertEquals(null, parent.getChild());
    }

    @Test
    public void testNormalAddedAsParentWhenItHasNoChild() {
        final ExplicitNode instance = getInstance("start");
        final NodeBase child = getInstance("child");

        child.addParent(instance);

        assertEquals(child, instance.getChild());
    }

    @Test
    public void testNormalAddedAsParentWhenItAlreadyHasAChildThrows() {
        final ExplicitNode instance = getInstance("instance");
        final NodeBase child1 = new ExplicitNode("child1", null);
        final NodeBase child2 = new ExplicitNode("child2", null);

        child1.addParent(instance);

        expectedException.expect(IllegalStateException.class);
        child2.addParent(instance);
    }

    @Test
    public void testNormalRemovedAsParent() {
        final ExplicitNode instance = getInstance("instance");
        final NodeBase child = getInstance("child");

        child.addParent(instance);
        child.removeParent(instance);

        assertEquals(null, instance.getChild());
    }

    @Test
    public void testGetChildren() {
        final ExplicitNode instance = getInstance("start");
        final NodeBase child = getInstance("child");

        child.addParent(instance);

        assertEquals(Arrays.asList(child), instance.getChildren());
    }
}
