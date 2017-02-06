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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.util.Arrays;
import java.util.List;

@RunWith(Parameterized.class)
public class TestSparkOptionsSplitter {

    @Parameterized.Parameters
    public static List<Object[]> params() {
        return Arrays.asList(new Object[][]{
                {"--option1 value1", Arrays.asList(new String[]{"--option1", "value1"})},
                {"--option1   value1", Arrays.asList(new String[]{"--option1", "value1"})},
                {"   --option1 value1   ", Arrays.asList(new String[]{"--option1", "value1"})},
                {"--conf special=value1", Arrays.asList(new String[]{"--conf", "special=value1"})},
                {"--conf special=\"value1\"", Arrays.asList(new String[]{"--conf", "special=value1"})},
                {"--conf special=\"value1 value2\"", Arrays.asList(new String[]{"--conf", "special=value1 value2"})},
                {" --conf special=\"value1 value2\"  ", Arrays.asList(new String[]{"--conf", "special=value1 value2"})},
        });
    }

    private String input;

    private List<String> output;

    public TestSparkOptionsSplitter(String input, List<String> result) {
        this.input = input;
        this.output = result;
    }

    @Test
    public void test() {
        assertThat("Error for input >>" + input + "<<", SparkMain2.splitSparkOpts(input), is(output));
    }
}
