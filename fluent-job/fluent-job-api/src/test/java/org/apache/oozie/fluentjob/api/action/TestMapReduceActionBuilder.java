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

import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestMapReduceActionBuilder extends TestNodeBuilderBaseImpl<MapReduceAction, MapReduceActionBuilder> {
    private static final String NAME = "map-reduce-name";
    private static final String NAME_NODE = "${nameNode}";
    private static final String EXAMPLE_DIR = "/path/to/directory";
    private static final String CONFIG_CLASS = "AnyConfigClass.class";
    private static final String[] JOB_XMLS = {"jobXml1.xml", "jobXml2.xml", "jobXml3.xml", "jobXml4.xml"};
    private static final String[] FILES = {"file1.xml", "file2.xml", "file3.xml", "file4.xml"};
    private static final String[] ARCHIVES = {"archive1.jar", "archive2.jar", "archive3.jar", "archive4.jar"};
    private static final String RESOURCE_MANAGER = "${resourceManager}";

    private static final String MAPRED_JOB_QUEUE_NAME = "mapred.job.queue.name";
    private static final String DEFAULT = "default";

    @Override
    protected MapReduceActionBuilder getBuilderInstance() {
        return MapReduceActionBuilder.create();
    }

    @Override
    protected MapReduceActionBuilder getBuilderInstance(final MapReduceAction action) {
        return MapReduceActionBuilder.createFromExistingAction(action);
    }

    @Test
    public void testResourceManagerAdded() {
        final MapReduceActionBuilder builder = getBuilderInstance();
        builder.withResourceManager(RESOURCE_MANAGER);

        final MapReduceAction action = builder.build();
        assertEquals(RESOURCE_MANAGER, action.getResourceManager());
    }

    @Test
    public void testNameNodeAdded() {
        final MapReduceActionBuilder builder = getBuilderInstance();
        builder.withNameNode(NAME_NODE);

        final MapReduceAction mrAction = builder.build();
        assertEquals(NAME_NODE, mrAction.getNameNode());
    }

    @Test
    public void testNameNodeAddedTwiceThrows() {
        final MapReduceActionBuilder builder = getBuilderInstance();
        builder.withNameNode(NAME_NODE);

        expectedException.expect(IllegalStateException.class);
        builder.withNameNode("any_string");
    }

    @Test
    public void testPrepareAdded() {
        final MapReduceActionBuilder builder = getBuilderInstance();
        builder.withPrepare(new PrepareBuilder().withDelete(EXAMPLE_DIR).build());

        final MapReduceAction mrAction = builder.build();
        assertEquals(EXAMPLE_DIR, mrAction.getPrepare().getDeletes().get(0).getPath());
    }

    @Test
    public void testPrepareAddedTwiceThrows() {
        final MapReduceActionBuilder builder = getBuilderInstance();
        builder.withPrepare(new PrepareBuilder().withDelete(EXAMPLE_DIR).build());

        expectedException.expect(IllegalStateException.class);
        builder.withPrepare(new PrepareBuilder().withDelete("any_directory").build());
    }

    @Test
    public void testStreamingAdded() {
        final Streaming streaming = new StreamingBuilder().withMapper("mapper.sh").withReducer("reducer.sh").build();

        final MapReduceActionBuilder builder = getBuilderInstance();
        builder.withStreaming(streaming);

        final MapReduceAction mrAction = builder.build();
        assertEquals(streaming, mrAction.getStreaming());
    }

    @Test
    public void testStreamingAddedTwiceThrows() {
        final Streaming streaming1= new StreamingBuilder().withMapper("mapper1.sh").withReducer("reducer1.sh").build();
        final Streaming streaming2 = new StreamingBuilder().withMapper("mapper2.sh").withReducer("reducer2.sh").build();

        final MapReduceActionBuilder builder = getBuilderInstance();
        builder.withStreaming(streaming1);

        expectedException.expect(IllegalStateException.class);
        builder.withStreaming(streaming2);
    }

    @Test
    public void testPipesAdded() {
        final Pipes pipes = new PipesBuilder().withMap("map").withReduce("reduce").build();

        final MapReduceActionBuilder builder = getBuilderInstance();
        builder.withPipes(pipes);

        final MapReduceAction mrAction = builder.build();
        assertEquals(pipes, mrAction.getPipes());
    }

    @Test
    public void testPipesAddedTwiceThrows() {
        final Pipes pipes1 = new PipesBuilder().withMap("map1").withReduce("reduce1").build();
        final Pipes pipes2 = new PipesBuilder().withMap("map2").withReduce("reduce2").build();

        final MapReduceActionBuilder builder = getBuilderInstance();
        builder.withPipes(pipes1);

        expectedException.expect(IllegalStateException.class);
        builder.withPipes(pipes2);
    }

    @Test
    public void testConfigClassAdded() {
        final MapReduceActionBuilder builder = getBuilderInstance();
        builder.withConfigClass(CONFIG_CLASS);

        final MapReduceAction mrAction = builder.build();
        assertEquals(CONFIG_CLASS, mrAction.getConfigClass());
    }

    @Test
    public void testConfigClassAddedTwiceThrows() {
        final MapReduceActionBuilder builder = getBuilderInstance();
        builder.withConfigClass(CONFIG_CLASS);

        expectedException.expect(IllegalStateException.class);
        builder.withConfigClass("AnyClass");
    }

    @Test
    public void testSeveralJobXmlsAdded() {
        final MapReduceActionBuilder builder = getBuilderInstance();

        for (final String jobXml : JOB_XMLS) {
            builder.withJobXml(jobXml);
        }

        final MapReduceAction mrAction = builder.build();

        final List<String> jobXmlsList = mrAction.getJobXmls();
        assertEquals(JOB_XMLS.length, jobXmlsList.size());

        for (int i = 0; i < JOB_XMLS.length; ++i) {
            assertEquals(JOB_XMLS[i], jobXmlsList.get(i));
        }
    }

    @Test
    public void testWithoutJobXmls() {
        final MapReduceActionBuilder builder = getBuilderInstance();

        for (final String jobXml : JOB_XMLS) {
            builder.withJobXml(jobXml);
        }

        builder.withoutJobXml(JOB_XMLS[0]);

        final MapReduceAction mrAction = builder.build();

        final List<String> jobXmlsList = mrAction.getJobXmls();
        final String[] remainingJobXmls = Arrays.copyOfRange(JOB_XMLS, 1, JOB_XMLS.length);
        assertEquals(remainingJobXmls.length, jobXmlsList.size());

        for (int i = 0; i < remainingJobXmls.length; ++i) {
            assertEquals(remainingJobXmls[i], jobXmlsList.get(i));
        }
    }

    @Test
    public void testClearJobXmls() {
        final MapReduceActionBuilder builder = getBuilderInstance();

        for (final String jobXml : JOB_XMLS) {
            builder.withJobXml(jobXml);
        }

        builder.clearJobXmls();

        final MapReduceAction mrAction = builder.build();

        final List<String> jobXmlsList = mrAction.getJobXmls();
        assertEquals(0, jobXmlsList.size());
    }

    @Test
    public void testSameConfigPropertyAddedTwiceThrows() {
        final MapReduceActionBuilder builder = getBuilderInstance();
        builder.withConfigProperty(MAPRED_JOB_QUEUE_NAME, DEFAULT);

        expectedException.expect(IllegalStateException.class);
        builder.withConfigProperty(MAPRED_JOB_QUEUE_NAME, DEFAULT);
    }

    @Test
    public void testSeveralFilesAdded() {
        final MapReduceActionBuilder builder = getBuilderInstance();

        for (final String file : FILES) {
            builder.withFile(file);
        }

        final MapReduceAction mrAction = builder.build();

        final List<String> filesList = mrAction.getFiles();
        assertEquals(FILES.length, filesList.size());

        for (int i = 0; i < FILES.length; ++i) {
            assertEquals(FILES[i], filesList.get(i));
        }
    }

    @Test
    public void testRemoveFiles() {
        final MapReduceActionBuilder builder = getBuilderInstance();

        for (final String file : FILES) {
            builder.withFile(file);
        }

        builder.withoutFile(FILES[0]);

        final MapReduceAction mrAction = builder.build();

        final List<String> filesList = mrAction.getFiles();
        final String[] remainingFiles = Arrays.copyOfRange(FILES, 1, FILES.length);
        assertEquals(remainingFiles.length, filesList.size());

        for (int i = 0; i < remainingFiles.length; ++i) {
            assertEquals(remainingFiles[i], filesList.get(i));
        }
    }

    @Test
    public void testClearFiles() {
        final MapReduceActionBuilder builder = getBuilderInstance();

        for (final String file : FILES) {
            builder.withFile(file);
        }

        builder.clearFiles();

        final MapReduceAction mrAction = builder.build();

        final List<String> filesList = mrAction.getFiles();
        assertEquals(0, filesList.size());
    }

    @Test
    public void testSeveralArchivesAdded() {
        final MapReduceActionBuilder builder = getBuilderInstance();

        for (final String archive : ARCHIVES) {
            builder.withArchive(archive);
        }

        final MapReduceAction mrAction = builder.build();

        final List<String> filesList = mrAction.getArchives();
        assertEquals(ARCHIVES.length, filesList.size());

        for (int i = 0; i < ARCHIVES.length; ++i) {
            assertEquals(ARCHIVES[i], filesList.get(i));
        }
    }

    @Test
    public void testRemoveArchives() {
        final MapReduceActionBuilder builder = getBuilderInstance();

        for (final String archive : ARCHIVES) {
            builder.withArchive(archive);
        }

        builder.withoutArchive(ARCHIVES[0]);

        final MapReduceAction mrAction = builder.build();

        final List<String> archivesList = mrAction.getArchives();
        final String[] remainingArchives = Arrays.copyOfRange(ARCHIVES, 1, ARCHIVES.length);
        assertEquals(remainingArchives.length, archivesList.size());

        for (int i = 0; i < remainingArchives.length; ++i) {
            assertEquals(remainingArchives[i], archivesList.get(i));
        }
    }

    @Test
    public void testClearArchives() {
        final MapReduceActionBuilder builder = getBuilderInstance();

        for (final String archive : ARCHIVES) {
            builder.withArchive(archive);
        }

        builder.clearArchives();

        final MapReduceAction mrAction = builder.build();

        final List<String> archivesList = mrAction.getArchives();
        assertEquals(0, archivesList.size());
    }

    @Test
    public void testFromExistingActionMapReduceSpecific() {
        final Streaming streaming = new StreamingBuilder().withMapper("mapper.sh").withReducer("reducer.sh").build();
        final Pipes pipes = new PipesBuilder().withMap("map").withReduce("reduce").build();

        final MapReduceActionBuilder builder = getBuilderInstance();

        builder.withName(NAME)
                .withNameNode(NAME_NODE)
                .withStreaming(streaming)
                .withPipes(pipes)
                .withConfigProperty(MAPRED_JOB_QUEUE_NAME, DEFAULT)
                .withFile(FILES[0])
                .withFile(FILES[1]);

        final MapReduceAction mrAction = builder.build();

        final MapReduceActionBuilder fromExistingBuilder = getBuilderInstance(mrAction);

        final String newName = "fromExisting_" + NAME;
        fromExistingBuilder.withName(newName)
                .withoutFile(FILES[1])
                .withFile(FILES[2]);

        final MapReduceAction modifiedMrAction = fromExistingBuilder.build();

        assertEquals(newName, modifiedMrAction.getName());
        assertEquals(mrAction.getNameNode(), modifiedMrAction.getNameNode());
        assertEquals(streaming, modifiedMrAction.getStreaming());
        assertEquals(pipes, modifiedMrAction.getPipes());

        final Map<String, String> expectedConfiguration = new LinkedHashMap<>();
        expectedConfiguration.put(MAPRED_JOB_QUEUE_NAME, DEFAULT);
        assertEquals(expectedConfiguration, modifiedMrAction.getConfiguration());

        assertEquals(Arrays.asList(FILES[0], FILES[2]), modifiedMrAction.getFiles());
    }

    @Test
    public void testFromOtherAction() {
        final ShellAction parent = ShellActionBuilder.create()
                .withName("parent")
                .build();

        final ShellAction otherAction = ShellActionBuilder.createFromExistingAction(parent)
                .withName("shell")
                .withParent(parent)
                .build();

        final MapReduceAction fromOtherAction = MapReduceActionBuilder.createFromExistingAction(otherAction)
                .withName("map-reduce")
                .build();

        assertEquals("map-reduce", fromOtherAction.getName());
        assertEquals(parent, fromOtherAction.getParentsWithoutConditions().get(0));
    }
}
