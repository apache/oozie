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

import org.apache.oozie.fluentjob.api.generated.workflow.PIPES;
import org.apache.oozie.fluentjob.api.action.Pipes;
import org.apache.oozie.fluentjob.api.action.PipesBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestPipesMapping {

    @Test
    public void testMappingPipes() {
        final String map = "map";
        final String reduce = "reduce";
        final String inputformat = "inputformat";
        final String partitioner = "partitioner";
        final String writer = "writer";
        final String program = "program";

        final Pipes pipes = new PipesBuilder()
                .withMap(map)
                .withReduce(reduce)
                .withInputformat(inputformat)
                .withPartitioner(partitioner)
                .withWriter(writer)
                .withProgram(program)
                .build();

        final PIPES pipesJAXB = DozerBeanMapperSingleton.instance().map(pipes, PIPES.class);

        assertEquals(map, pipesJAXB.getMap());
        assertEquals(reduce, pipesJAXB.getReduce());
        assertEquals(inputformat, pipesJAXB.getInputformat());
        assertEquals(partitioner, pipesJAXB.getPartitioner());
        assertEquals(writer, pipesJAXB.getWriter());
        assertEquals(program, pipesJAXB.getProgram());
    }
}
