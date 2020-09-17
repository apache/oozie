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
                {"--option1 value1",
                        Arrays.asList("--option1", "value1")},
                {"--option1   value1",
                        Arrays.asList("--option1", "value1")},
                {"--option1 \"value1 value2\"",
                        Arrays.asList("--option1", "value1 value2")},
                {"--option1 \"value1 \"value2\" value3\"",
                        Arrays.asList("--option1", "\"value1 \"value2\" value3\"")},
                {"   --option1 value1   ",
                        Arrays.asList("--option1", "value1")},
                {"--conf special=value1",
                        Arrays.asList("--conf", "special=value1")},
                {"--conf special=\"value1\"",
                        Arrays.asList("--conf", "special=value1")},
                {"--conf special=\"value1 value2\"",
                        Arrays.asList("--conf", "special=value1 value2")},
                {" --conf special=\"value1 value2\"  ",
                        Arrays.asList("--conf", "special=value1 value2")},
                {"--conf key=value1 value2",
                        Arrays.asList("--conf", "key=value1 value2")},
                {"--conf key=value1 \"value2\"",
                        Arrays.asList("--conf", "key=value1 \"value2\"")},
                {"--conf key=\"value1 value2\" \"value3 value4\"",
                        Arrays.asList("--conf", "key=\"value1 value2\" \"value3 value4\"")},
                {"--conf key=\"value1 value2 value3 value4\"",
                        Arrays.asList("--conf", "key=value1 value2 value3 value4")},
                {"--conf special=value1 \"value2\"",
                        Arrays.asList("--conf", "special=value1 \"value2\"")},
                {"--conf special=value1 value2 --conf value3",
                        Arrays.asList("--conf", "special=value1 value2", "--conf", "value3")},
                {"--conf special1=value1 special2=\"value2 value3\" --conf value4",
                        Arrays.asList("--conf", "special1=value1 special2=\"value2 value3\"", "--conf", "value4")},
                {"--conf special1=value1 value2 special2=value3 value4",
                        Arrays.asList("--conf", "special1=value1 value2 special2=value3 value4")},
                {"--option1 value1 --option2=value2",
                        Arrays.asList("--option1", "value1", "--option2", "value2")},
                {"--conf spark.executor.extraJavaOptions=\"-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp\"",
                    Arrays.asList("--conf",
                            "spark.executor.extraJavaOptions=-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp")},
                {"--option1 value1 --verbose",
                        Arrays.asList("--option1", "value1", "--verbose")},
                {"--verbose --option1 value1",
                        Arrays.asList("--verbose", "--option1", "value1")}
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
        assertThat("Error for input >>" + input + "<<", SparkOptionsSplitter.splitSparkOpts(input), is(output));
    }
}
