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

import static org.junit.Assert.assertEquals;

public class TestPipesBuilder {
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testWithMap() {
        final String map = "map";

        final PipesBuilder builder = new PipesBuilder();
        builder.withMap(map);

        final Pipes pipes = builder.build();
        assertEquals(map, pipes.getMap());
    }

    @Test
    public void testMapCalledTwiceThrows() {
        final String map1 = "map1";
        final String map2 = "map2";

        final PipesBuilder builder = new PipesBuilder();
        builder.withMap(map1);

        expectedException.expect(IllegalStateException.class);
        builder.withMap(map2);
    }

    @Test
    public void testWithReduce() {
        final String reduce = "reduce";

        final PipesBuilder builder = new PipesBuilder();
        builder.withReduce(reduce);

        final Pipes pipes = builder.build();
        assertEquals(reduce, pipes.getReduce());
    }

    @Test
    public void testWithReduceCalledTwiceThrows() {
        final String reduce1 = "reduce1";
        final String reduce2= "reduce2";

        final PipesBuilder builder = new PipesBuilder();
        builder.withReduce(reduce1);

        expectedException.expect(IllegalStateException.class);
        builder.withReduce(reduce2);
    }

    @Test
    public void testWithInputformat() {
        final String inputformat = "inputformat";

        final PipesBuilder builder = new PipesBuilder();
        builder.withInputformat(inputformat);

        final Pipes pipes = builder.build();
        assertEquals(inputformat, pipes.getInputformat());
    }

    @Test
    public void testWithInputformatCalledTwiceThrows() {
        final String inputformat1 = "inputformat1";
        final String inputformat2 = "inputformat2";

        final PipesBuilder builder = new PipesBuilder();
        builder.withInputformat(inputformat1);

        expectedException.expect(IllegalStateException.class);
        builder.withInputformat(inputformat2);
    }

    @Test
    public void testWithPartitioner() {
        final String partitioner = "partitioner";

        final PipesBuilder builder = new PipesBuilder();
        builder.withPartitioner(partitioner);

        final Pipes pipes = builder.build();
        assertEquals(partitioner, pipes.getPartitioner());
    }

    @Test
    public void testWithPartitionerCalledTwiceThrows() {
        final String partitioner1 = "partitioner1";
        final String partitioner2 = "partitioner2";

        final PipesBuilder builder = new PipesBuilder();
        builder.withPartitioner(partitioner1);

        expectedException.expect(IllegalStateException.class);
        builder.withPartitioner(partitioner2);
    }

    @Test
    public void testWithWriter() {
        final String writer = "writer";

        final PipesBuilder builder = new PipesBuilder();
        builder.withWriter(writer);

        final Pipes pipes = builder.build();
        assertEquals(writer, pipes.getWriter());
    }

    @Test
    public void testWithWriterCalledTwiceThrows() {
        final String writer1 = "writer1";
        final String writer2 = "writer2";

        final PipesBuilder builder = new PipesBuilder();
        builder.withWriter(writer1);

        expectedException.expect(IllegalStateException.class);
        builder.withWriter(writer2);
    }

    @Test
    public void testWithProgram() {
        final String program = "program";

        final PipesBuilder builder = new PipesBuilder();
        builder.withProgram(program);

        final Pipes pipes = builder.build();
        assertEquals(program, pipes.getProgram());
    }

    @Test
    public void testWithProgramCalledTwiceThrows() {
        final String program1 = "program1";
        final String program2 = "program2";

        final PipesBuilder builder = new PipesBuilder();
        builder.withProgram(program1);

        expectedException.expect(IllegalStateException.class);
        builder.withProgram(program2);
    }
}
