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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class TestDuplicateFilteringInSparkMain {

    private static final String PREFIX = "hdfs://namenode.address:8020/folder/";

    static List<URI> getURIs(String... uriStrings) throws URISyntaxException {
        URI[] uris = new URI[uriStrings.length];
        for (int i = 0; i != uriStrings.length; ++i) {
            uris[i] = new URI(PREFIX + uriStrings[i]);
        }
        return Arrays.asList(uris);
    }

    static Object[] testCase(List<URI> inputs, List<URI> expectedOutputs) {
        return new Object[] {inputs, expectedOutputs};
    }

    @Parameterized.Parameters
    public static List<Object[]> params() throws Exception {
        return Arrays.asList(
                testCase(getURIs("file.io"),
                        getURIs("file.io")),

                testCase(getURIs("file.io", "file.io", "file.io"),
                        getURIs("file.io")),

                testCase(getURIs("file.io", "file3.io", "file.io"),
                        getURIs("file.io", "file3.io")),

                testCase(getURIs("file.io", "file3.io", "file2.io"),
                        getURIs("file.io", "file2.io", "file3.io"))
                );
    }

    private List<URI> input;

    private List<URI> expectedOutput;

    public TestDuplicateFilteringInSparkMain(List<URI> input, List<URI> result) {
        this.input = input;
        this.expectedOutput = result;
    }

    @Test
    public void test() throws Exception{
        Map<String, URI> uriMap = SparkMain.fixFsDefaultUrisAndFilterDuplicates(input.toArray(new URI[input.size()]));
        assertThat("Duplicate filtering failed for >>" + input + "<<", uriMap.size(), is(expectedOutput.size()));
        List<URI> outputList = Arrays.asList(uriMap.values().toArray(new URI[0]));
        Collections.sort(outputList);
        assertThat("Files are different in result ", outputList, is(expectedOutput));
    }

}
