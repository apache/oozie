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

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.oozie.fluentjob.api.ModifyOnce;

/**
 * A builder class for {@link Streaming}.
 *
 * The properties of the builder can only be set once, an attempt to set them a second time will trigger
 * an {@link IllegalStateException}. The properties that are lists are an exception to this rule, of course multiple
 * elements can be added / removed.
 *
 * Builder instances can be used to build several elements, although properties already set cannot be changed after
 * a call to {@link StreamingBuilder#build} either.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class StreamingBuilder implements Builder<Streaming> {
    private final ModifyOnce<String> mapper;
    private final ModifyOnce<String> reducer;
    private final ModifyOnce<String> recordReader;
    private final ImmutableList.Builder<String> recordReaderMappings;
    private final ImmutableList.Builder<String> envs;

    /**
     * Creates a new {@link StreamingBuilder}.
     */
    public StreamingBuilder() {
        mapper = new ModifyOnce<>();
        reducer = new ModifyOnce<>();
        recordReader = new ModifyOnce<>();

        recordReaderMappings = new ImmutableList.Builder<>();
        envs = new ImmutableList.Builder<>();
    }

    /**
     * Registers a mapper with this builder.
     * @param mapper The mapper to register with this builder.
     * @return This builder.
     */
    public StreamingBuilder withMapper(final String mapper) {
        this.mapper.set(mapper);
        return this;
    }

    /**
     * Registers a reducer with this builder.
     * @param reducer The reducer to register with this builder.
     * @return This builder.
     */
    public StreamingBuilder withReducer(final String reducer) {
        this.reducer.set(reducer);
        return this;
    }

    /**
     * Registers a record reader with this builder.
     * @param recordReader The record reader to register with this builder.
     * @return This builder.
     */
    public StreamingBuilder withRecordReader(final String recordReader) {
        this.recordReader.set(recordReader);
        return this;
    }

    /**
     * Registers a record reader mapping with this builder.
     * @param recordReaderMapping The record reader mapping to register with this builder.
     * @return This builder.
     */
    public StreamingBuilder withRecordReaderMapping(final String recordReaderMapping) {
        this.recordReaderMappings.add(recordReaderMapping);
        return this;
    }

    /**
     * Registers an environment variable with this builder.
     * @param env The environment variable to register with this builder.
     * @return This builder.
     */
    public StreamingBuilder withEnv(final String env) {
        this.envs.add(env);
        return this;
    }

    /**
     * Creates a new {@link Streaming} object with the properties stores in this builder.
     * The new {@link Streaming} object is independent of this builder and the builder can be used to build
     * new instances.
     * @return A new {@link Streaming} object with the properties stored in this builder.
     */
    @Override
    public Streaming build() {
        return new Streaming(mapper.get(), reducer.get(), recordReader.get(), recordReaderMappings.build(), envs.build());
    }
}
