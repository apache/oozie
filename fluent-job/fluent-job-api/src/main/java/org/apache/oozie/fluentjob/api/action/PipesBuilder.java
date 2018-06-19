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
import org.apache.oozie.fluentjob.api.ModifyOnce;

/**
 * A builder class for {@link Pipes}.
 *
 * The properties of the builder can only be set once, an attempt to set them a second time will trigger
 * an {@link IllegalStateException}. The properties that are lists are an exception to this rule, of course multiple
 * elements can be added / removed.
 *
 * Builder instances can be used to build several elements, although properties already set cannot be changed after
 * a call to {@link PipesBuilder#build} either.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PipesBuilder implements Builder<Pipes> {
    private final ModifyOnce<String> map;
    private final ModifyOnce<String> reduce;
    private final ModifyOnce<String> inputformat;
    private final ModifyOnce<String> partitioner;
    private final ModifyOnce<String> writer;
    private final ModifyOnce<String> program;

    /**
     * Creates a new {@link PipesBuilder}.
     */
    public PipesBuilder() {
        map = new ModifyOnce<>();
        reduce = new ModifyOnce<>();
        inputformat = new ModifyOnce<>();
        partitioner = new ModifyOnce<>();
        writer = new ModifyOnce<>();
        program = new ModifyOnce<>();
    }

    /**
     * Registers a mapper with this builder.
     * @param map The mapper to register with this builder.
     * @return This builder.
     */
    public PipesBuilder withMap(final String map) {
        this.map.set(map);
        return this;
    }

    /**
     * Registers a reducer with this builder.
     * @param reduce The reducer to register with this builder.
     * @return This builder.
     */
    public PipesBuilder withReduce(final String reduce) {
        this.reduce.set(reduce);
        return this;
    }

    /**
     * Registers an input format with this builder.
     * @param inputformat The input format to register with this builder.
     * @return This builder.
     */
    public PipesBuilder withInputformat(final String inputformat) {
        this.inputformat.set(inputformat);
        return this;
    }

    /**
     * Registers a partitioner with this builder.
     * @param partitioner The partitioner to register with this builder.
     * @return This builder.
     */
    public PipesBuilder withPartitioner(final String partitioner) {
        this.partitioner.set(partitioner);
        return this;
    }

    /**
     * Registers a writer with this builder.
     * @param writer The writer to register with this builder.
     * @return This builder.
     */
    public PipesBuilder withWriter(final String writer) {
        this.writer.set(writer);
        return this;
    }

    /**
     * Registers an executable program with this builder.
     * @param program The executable program to register with this builder.
     * @return This builder.
     */
    public PipesBuilder withProgram(final String program) {
        this.program.set(program);
        return this;
    }

    /**
     * Creates a new {@link Pipes} object with the properties stores in this builder.
     * The new {@link Pipes} object is independent of this builder and the builder can be used to build
     * new instances.
     * @return A new {@link Pipes} object with the properties stored in this builder.
     */
    @Override
    public Pipes build() {
        return new Pipes(map.get(), reduce.get(), inputformat.get(), partitioner.get(), writer.get(), program.get());
    }
}
