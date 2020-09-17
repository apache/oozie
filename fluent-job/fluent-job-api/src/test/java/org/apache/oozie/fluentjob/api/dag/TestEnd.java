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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestEnd extends TestNodeBase<End> {
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Override
    protected End getInstance(final String name) {
        return new End(name);
    }

    @Test
    public void testAddParentWhenNoneAlreadyExists() {
        final Start parent = new Start("parent");
        final End instance = getInstance("instance");

        instance.addParent(parent);
        assertEquals(parent, instance.getParent());
        assertEquals(instance, parent.getChild());
    }

    @Test
    public void testAddParentWhenItAlreadyExists() {
        final Start parent1 = new Start("parent1");
        final Start parent2 = new Start("parent2");
        final End instance = getInstance("instance");

        instance.addParent(parent1);

        expectedException.expect(IllegalStateException.class);
        instance.addParent(parent2);
    }

    @Test
    public void testRemoveExistingParent() {
        final Start parent = new Start("parent");
        final End instance = getInstance("instance");

        instance.addParent(parent);

        instance.removeParent(parent);
        assertEquals(null, instance.getParent());
        assertEquals(null, parent.getChild());
    }

    @Test
    public void testRemoveNonexistentParentThrows() {
        final Start parent = new Start("parent");
        final End instance = getInstance("instance");

        expectedException.expect(IllegalArgumentException.class);
        instance.removeParent(parent);
    }

    @Test
    public void testClearExistingParent() {
        final Start parent = new Start("parent");
        final End instance = getInstance("instance");

        instance.addParent(parent);

        instance.clearParents();
        assertEquals(null, instance.getParent());
        assertEquals(null, parent.getChild());
    }

    @Test
    public void testClearNonExistentParent() {
        final Start parent = new Start("parent");
        final End instance = getInstance("instance");

        instance.clearParents();
        assertEquals(null, instance.getParent());
        assertEquals(null, parent.getChild());
    }

    @Test
    public void testAddedAsParentThrows () {
        final End instance = getInstance("instance");
        final ExplicitNode child = new ExplicitNode("child", null);

        expectedException.expect(IllegalStateException.class);
        child.addParent(instance);
    }

    @Test
    public void testGetChildren() {
        final End instance = getInstance("end");

        assertTrue(instance.getChildren().isEmpty());
    }
}
