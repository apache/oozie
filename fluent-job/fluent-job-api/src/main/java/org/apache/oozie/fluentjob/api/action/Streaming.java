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

import java.util.List;

/**
 * A class representing the streaming information within a {@link MapReduceAction}.
 *
 * Instances of this class should be built using the builder {@link StreamingBuilder}.
 *
 * The properties of the builder can only be set once, an attempt to set them a second time will trigger
 * an {@link IllegalStateException}.
 *
 * Builder instances can be used to build several elements, although properties already set cannot be changed after
 * a call to {@link StreamingBuilder#build} either.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class Streaming {
    private final String mapper;
    private final String reducer;
    private final String recordReader;
    private final ImmutableList<String> recordReaderMappings;
    private final ImmutableList<String> envs;

    Streaming(final String mapper,
              final String reducer,
              final String recordReader,
              final ImmutableList<String> recordReaderMappings,
              final ImmutableList<String> envs) {
        this.mapper = mapper;
        this.reducer = reducer;
        this.recordReader = recordReader;
        this.recordReaderMappings = recordReaderMappings;
        this.envs = envs;
    }

    /**
     * Returns the mapper of this {@link Streaming} object.
     * @return The mapper of this {@link Streaming} object.
     */
    public String getMapper() {
        return mapper;
    }

    /**
     * Returns the reducer of this {@link Streaming} object.
     * @return The reducer of this {@link Streaming} object.
     */
    public String getReducer() {
        return reducer;
    }

    /**
     * Returns the record reader of this {@link Streaming} object.
     * @return The record reader of this {@link Streaming} object.
     */
    public String getRecordReader() {
        return recordReader;
    }

    /**
     * Returns the record reader mappings of this {@link Streaming} object as a list.
     * @return The record reader mappings of this {@link Streaming} object as a list.
     */
    public List<String> getRecordReaderMappings() {
        return recordReaderMappings;
    }

    /**
     * Returns the environment variables of this {@link Streaming} object as a list.
     * @return The environment variables of this {@link Streaming} object as a list.
     */
    public List<String> getEnvs() {
        return envs;
    }
}
