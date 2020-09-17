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

package org.apache.oozie.action.hadoop;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestShellSplitter {
    private ShellSplitter shellSplitter;
    private String input;
    private List<String> expectedOutput;
    private Class<? extends Exception> expectedExceptionClass;
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Parameterized.Parameters
    public static Collection<Object[]> params() {
        return Arrays.asList(new Object[][]{
                {null,
                        null,
                        null},
                {"",
                        Collections.<String>emptyList(),
                        null},
                {"  \t \n",
                        Collections.<String>emptyList(),
                        null},
                {"a\tbee  cee",
                        Arrays.asList("a", "bee", "cee"),
                        null},
                {" hello   world\t",
                        Arrays.asList("hello", "world"),
                        null},
                {"\"hello world\"",
                        Collections.singletonList("hello world"),
                        null},
                {"'hello world'",
                        Collections.singletonList("hello world"),
                        null},
                {"\"\\\"hello world\\\"\"",
                        Collections.singletonList("\"hello world\""),
                        null},
                {"'hello \\\" world'",
                        Collections.singletonList("hello \\\" world"),
                        null},
                {"\"foo\"'bar'baz",
                        Collections.singletonList("foobarbaz"),
                        null},
                {"\"three\"' 'four",
                        Collections.singletonList("three four"),
                        null},
                {"three\\ four",
                        Collections.singletonList("three four"),
                        null},
                {" '' one",
                        Arrays.asList("", "one"),
                        null},
                {"command -a aa -b -c",
                        Arrays.asList("command", "-a", "aa", "-b", "-c"),
                        null},
                {"command --longopt \"this is a single token\" --otherlongopt",
                        Arrays.asList("command", "--longopt", "this is a single token", "--otherlongopt"),
                        null},
                {"command --longopt 'this is a single token' --otherlongopt",
                        Arrays.asList("command", "--longopt", "this is a single token", "--otherlongopt"),
                        null},
                {"'",
                        null,
                        ShellSplitterException.class},
                {"'Hello world",
                        null,
                        ShellSplitterException.class},
                {"\"Hello world",
                        null,
                        ShellSplitterException.class},
                {"Hellow world\\",
                        null,
                        ShellSplitterException.class},
                {"\"Hello world'",
                        null,
                        ShellSplitterException.class},
        });
    }

    public TestShellSplitter(String input, List<String> expectedOutput, Class<? extends Exception> expectedException) {
        this.input = input;
        this.expectedOutput = expectedOutput;
        this.expectedExceptionClass = expectedException;
    }

    @Before
    public void setUp() throws Exception {
        shellSplitter = new ShellSplitter();
    }

    @Test
    public void test() throws ShellSplitterException {
        if (expectedExceptionClass != null) {
            expectedException.expect(expectedExceptionClass);
            shellSplitter.split(input);
        }
        else {
            assertEquals("Invalid splitting", expectedOutput, shellSplitter.split(input));
        }
    }
}