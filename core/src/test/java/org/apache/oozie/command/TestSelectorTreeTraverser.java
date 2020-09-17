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

package org.apache.oozie.command;

import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestSelectorTreeTraverser {
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private PurgeXCommand.JPAFunction<String, List<String>> noChildren = new PurgeXCommand.JPAFunction<String, List<String>>() {
        @Override
        public List<String> apply(String wId) {
            return new ArrayList<>();
        }
    };
    private PurgeXCommand.JPAFunction<List<String>, List<String>> noneSelector = new PurgeXCommand.JPAFunction<List<String>,
            List<String>>() {
        @Override
        public List<String> apply(List<String> jobBeans) {
            return new ArrayList<>();
        }
    };
    private PurgeXCommand.JPAFunction<List<String>, List<String>> allSelector = new PurgeXCommand.JPAFunction<List<String>,
            List<String>>() {
        @Override
        public List<String> apply(List<String> jobBeans) {
            return jobBeans;
        }
    };
    private PurgeXCommand.JPAFunction<String, List<String>> simpleTree = new PurgeXCommand.JPAFunction<String, List<String>>() {
        @Override
        public List<String> apply(String wId) {
            switch (wId) {
                case "A":
                    return Arrays.asList("B", "C");
                case "B:":
                case "C":
                    return new ArrayList<>();
            }
            return new ArrayList<>();
        }
    };
    private PurgeXCommand.JPAFunction<String, List<String>> invalidTree = new PurgeXCommand.JPAFunction<String, List<String>>() {
        @Override
        public List<String> apply(String wId)  {
            switch (wId) {
                case "A":
                    return Arrays.asList("B", "C", "D");
                case "B:":
                case "C":
                    return new ArrayList<>();
                case "D":
                    return Collections.singletonList("A");
            }
            return new ArrayList<>();
        }
    };

    @Test
    public void testSingleWorkflow() throws JPAExecutorException {
        PurgeXCommand.SelectorTreeTraverser<String, String> traverser = new PurgeXCommand.SelectorTreeTraverser<>("A",
                noChildren, noneSelector);
        List<String> descendants = traverser.findAllDescendantNodesIfSelectable();
        assertEquals(Collections.singletonList("A"), descendants);
    }

    @Test
    public void testOneDepthTreeNotSelectedChildren() throws JPAExecutorException {
        PurgeXCommand.SelectorTreeTraverser<String, String> traverser = new PurgeXCommand.SelectorTreeTraverser<>("A",
                simpleTree, noneSelector);
        List<String> descendants = traverser.findAllDescendantNodesIfSelectable();
        assertEquals(Collections.<String>emptyList(), descendants);
    }

    @Test
    public void testOneDepthTreeSelectedChildren() throws JPAExecutorException {
        PurgeXCommand.SelectorTreeTraverser<String, String> traverser = new PurgeXCommand.SelectorTreeTraverser<>("A",
                simpleTree, allSelector);
        List<String> descendants = traverser.findAllDescendantNodesIfSelectable();
        assertEquals(Arrays.asList("A", "B", "C"), descendants);
    }

    @Test
    public void testInvalidTree() throws JPAExecutorException {
        PurgeXCommand.SelectorTreeTraverser<String, String> traverser = new PurgeXCommand.SelectorTreeTraverser<>("A",
                invalidTree, allSelector);
        expectedException.expect(JPAExecutorException.class);
        traverser.findAllDescendantNodesIfSelectable();
    }

}