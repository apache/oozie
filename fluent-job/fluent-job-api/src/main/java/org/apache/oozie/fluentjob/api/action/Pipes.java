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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A class representing the piping information within a {@link MapReduceAction}.
 *
 * Instances of this class should be built using the builder {@link PipesBuilder}.
 *
 * The properties of the builder can only be set once, an attempt to set them a second time will trigger
 * an {@link IllegalStateException}.
 *
 * Builder instances can be used to build several elements, although properties already set cannot be changed after
 * a call to {@link PipesBuilder#build} either.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class Pipes {
    private final String map;
    private final String reduce;
    private final String inputformat;
    private final String partitioner;
    private final String writer;
    private final String program;

    Pipes(final String map,
          final String reduce,
          final String inputformat,
          final String partitioner,
          final String writer,
          final String program) {
        this.map = map;
        this.reduce = reduce;
        this.inputformat = inputformat;
        this.partitioner = partitioner;
        this.writer = writer;
        this.program = program;
    }

    /**
     * Returns the mapper of this {@link Pipes} object.
     * @return The mapper of this {@link Pipes} object.
     */
    public String getMap() {
        return map;
    }

    /**
     * Returns the reducer of this {@link Pipes} object.
     * @return The reducer of this {@link Pipes} object.
     */
    public String getReduce() {
        return reduce;
    }

    /**
     * Returns the input format of this {@link Pipes} object.
     * @return The input format of this {@link Pipes} object.
     */
    public String getInputformat() {
        return inputformat;
    }

    /**
     * Returns the partitioner of this {@link Pipes} object.
     * @return The partitioner of this {@link Pipes} object.
     */
    public String getPartitioner() {
        return partitioner;
    }

    /**
     * Returns the writer of this {@link Pipes} object.
     * @return The writer of this {@link Pipes} object.
     */
    public String getWriter() {
        return writer;
    }

    /**
     * Returns the program of this {@link Pipes} object.
     * @return The program of this {@link Pipes} object.
     */
    public String getProgram() {
        return program;
    }
}
