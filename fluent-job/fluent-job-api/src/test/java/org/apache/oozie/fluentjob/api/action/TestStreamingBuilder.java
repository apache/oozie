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

package org.apache.oozie.fluentjob.api.action;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class TestStreamingBuilder {
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testWithMapper() {
        final String mapperName = "mapper-name.sh";

        final StreamingBuilder builder = new StreamingBuilder();
        builder.withMapper(mapperName);

        final Streaming streaming = builder.build();
        assertEquals(mapperName, streaming.getMapper());
    }

    @Test
    public void testWithMapperCalledTwiceThrows() {
        final String mapperName1 = "mapper-name-2.sh";
        final String mapperName2 = "mapper-name-2.sh";

        final StreamingBuilder builder = new StreamingBuilder();
        builder.withMapper(mapperName1);

        expectedException.expect(IllegalStateException.class);
        builder.withMapper(mapperName2);
    }

    @Test
    public void testWithReducer() {
        final String reducerName = "reducer-name.sh";

        final StreamingBuilder builder = new StreamingBuilder();
        builder.withReducer(reducerName);

        final Streaming streaming = builder.build();
        assertEquals(reducerName, streaming.getReducer());
    }

    @Test
    public void testWithReducerCalledTwiceThrows() {
        final String reducerName1 = "reducer-name1.sh";
        final String reducerName2 = "reducer-name2.sh";

        final StreamingBuilder builder = new StreamingBuilder();
        builder.withReducer(reducerName1);

        expectedException.expect(IllegalStateException.class);
        builder.withReducer(reducerName2);
    }

    @Test
    public void testWithRecordReader() {
        final String recordReaderName = "record-reader-name.sh";

        final StreamingBuilder builder = new StreamingBuilder();
        builder.withRecordReader(recordReaderName);

        final Streaming streaming = builder.build();
        assertEquals(recordReaderName, streaming.getRecordReader());
    }

    @Test
    public void testWithRecordReaderCalledTwiceThrows() {
        final String recordReaderName1 = "record-reader-name1.sh";
        final String recordReaderName2 = "record-reader-name2.sh";

        final StreamingBuilder builder = new StreamingBuilder();
        builder.withRecordReader(recordReaderName1);

        expectedException.expect(IllegalStateException.class);
        builder.withRecordReader(recordReaderName2);
    }

    @Test
    public void testWithRecordReaderMapping() {
        final String mapping1 = "mapping1";
        final String mapping2 = "mapping2";

        final StreamingBuilder builder = new StreamingBuilder();
        builder.withRecordReaderMapping(mapping1)
               .withRecordReaderMapping(mapping2);

        final Streaming streaming = builder.build();
        assertEquals(Arrays.asList(mapping1, mapping2), streaming.getRecordReaderMappings());
    }

    @Test
    public void testWithEnv() {
        final String env1 = "env1";
        final String env2 = "env2";

        final StreamingBuilder builder = new StreamingBuilder();
        builder.withEnv(env1)
                .withEnv(env2);

        final Streaming streaming = builder.build();
        assertEquals(Arrays.asList(env1, env2), streaming.getEnvs());
    }
}
