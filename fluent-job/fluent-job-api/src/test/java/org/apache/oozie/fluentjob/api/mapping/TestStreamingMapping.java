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

package org.apache.oozie.fluentjob.api.mapping;

import org.apache.oozie.fluentjob.api.generated.workflow.STREAMING;
import org.apache.oozie.fluentjob.api.action.Streaming;
import org.apache.oozie.fluentjob.api.action.StreamingBuilder;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestStreamingMapping {

    @Test
    public void testMappingStreaming() {
        final String mapper = "mapper";
        final String reducer = "reducer";
        final String recordReader = "recordReader";
        final List<String> recordReaderMappings = Arrays.asList("mapping1", "mapping2");
        final List<String> envs = Arrays.asList("env1", "env2");

        final StreamingBuilder builder = new StreamingBuilder();
        builder.withMapper(mapper)
                .withReducer(reducer)
                .withRecordReader(recordReader);

        for (final String recordReaderMapping : recordReaderMappings) {
            builder.withRecordReaderMapping(recordReaderMapping);
        }

        for (final String env : envs) {
            builder.withEnv(env);
        }

        final Streaming streaming = builder.build();

        final STREAMING streamingJAXB = DozerBeanMapperSingleton.instance().map(streaming, STREAMING.class);

        assertEquals(mapper, streamingJAXB.getMapper());
        assertEquals(reducer, streamingJAXB.getReducer());
        assertEquals(recordReader, streamingJAXB.getRecordReader());
        assertEquals(recordReaderMappings, streamingJAXB.getRecordReaderMapping());
        assertEquals(envs, streamingJAXB.getEnv());
    }
}
