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

import org.apache.oozie.fluentjob.api.generated.workflow.MAPREDUCE;
import org.apache.oozie.fluentjob.api.action.MapReduceAction;
import org.apache.oozie.fluentjob.api.action.MapReduceActionBuilder;
import org.apache.oozie.fluentjob.api.action.PipesBuilder;
import org.apache.oozie.fluentjob.api.action.PrepareBuilder;
import org.apache.oozie.fluentjob.api.action.StreamingBuilder;
import org.apache.oozie.fluentjob.api.generated.workflow.MAPREDUCE;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class TestMapReduceActionMapping {
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testMappingMapReduceAction() {
        final String resourceManager = "${resourceManager}";
        final String nameNode = "${nameNode}";

        final List<String> jobXmls = Arrays.asList("job1.xml", "job2.xml");

        final String configClass = "${configClass}";

        final List<String> files = Arrays.asList("file1", "file2");

        final List<String> archives = Arrays.asList("archive1", "archive2");

        final MapReduceActionBuilder builder = MapReduceActionBuilder.create();

        builder.withResourceManager(resourceManager)
                .withNameNode(nameNode)
                .withPrepare(new PrepareBuilder().build())
                .withStreaming(new StreamingBuilder().build())
                .withPipes(new PipesBuilder().build());

        for (final String jobXml : jobXmls) {
            builder.withJobXml(jobXml);
        }

        builder.withConfigProperty("propertyName1", "propertyValue1")
                .withConfigProperty("propertyName2", "propertyValue2");

        builder.withConfigClass(configClass);

        for (final String file : files) {
            builder.withFile(file);
        }

        for (final String archive : archives) {
            builder.withArchive(archive);
        }

        final MapReduceAction action = builder.build();

        final MAPREDUCE mapreduce = DozerBeanMapperSingleton.instance().map(action, MAPREDUCE.class);

        assertEquals(resourceManager, mapreduce.getResourceManager());
        assertEquals(nameNode, mapreduce.getNameNode());
        assertNotNull(mapreduce.getPrepare());
        assertNotNull(mapreduce.getStreaming());
        assertNotNull(mapreduce.getPipes());
        assertEquals(jobXmls, mapreduce.getJobXml());
        assertNotNull(mapreduce.getConfiguration());
        assertEquals(configClass, mapreduce.getConfigClass());
        assertEquals(files, mapreduce.getFile());
        assertEquals(archives, mapreduce.getArchive());
    }
}
