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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class TestStart extends TestNodeBase<Start> {
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Override
    protected Start getInstance(final String name) {
        return new Start(name);
    }

    @Test
    public void testAddParent() {
        final ExplicitNode parent = new ExplicitNode("parent", null);
        final Start start = getInstance("start");

        expectedException.expect(IllegalStateException.class);
        start.addParent(parent);
    }

    @Test
    public void testRemoveParent() {
        final Start start = getInstance("start");

        expectedException.expect(IllegalStateException.class);
        start.removeParent(null);
    }

    @Test
    public void testClearExistingParent() {
        new Start("parent");
        final Start instance = getInstance("instance");

        instance.clearParents();
    }

    @Test
    public void testClearNonExistentParent() {
        new Start("parent");
        final Start instance = getInstance("instance");

        instance.clearParents();
    }

    @Test
    public void testStartAddedAsParentWhenItHasNoChild() {
        final Start start = getInstance("start");
        final NodeBase child = new ExplicitNode("child", null);

        child.addParent(start);

        assertEquals(child, start.getChild());
    }

    @Test
    public void testStartAddedAsParentWhenItAlreadyHasAChildThrows() {
        final Start start = getInstance("start");
        final NodeBase child1 = new ExplicitNode("child1", null);
        final NodeBase child2 = new ExplicitNode("child2", null);

        child1.addParent(start);

        expectedException.expect(IllegalStateException.class);
        child2.addParent(start);
    }

    @Test
    public void testStartRemovedAsParent() {
        final Start instance = getInstance("instance");
        final NodeBase child = new ExplicitNode("child", null);

        child.addParent(instance);
        child.removeParent(instance);

        assertEquals(null, instance.getChild());
    }

    @Test
    public void testGetChildren() {
        final Start start = getInstance("start");
        final NodeBase child = new ExplicitNode("child", null);

        child.addParent(start);

        assertEquals(Arrays.asList(child), start.getChildren());
    }
}
