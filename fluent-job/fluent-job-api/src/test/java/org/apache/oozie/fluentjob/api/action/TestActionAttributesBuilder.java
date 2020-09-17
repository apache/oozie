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

import com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestActionAttributesBuilder {
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private static final String NAME_NODE = "${nameNode}";
    private static final String EXAMPLE_DIR = "/path/to/directory";
    private static final String CONFIG_CLASS = "AnyConfigClass.class";
    private static final String[] JOB_XMLS = {"jobXml1.xml", "jobXml2.xml", "jobXml3.xml", "jobXml4.xml"};
    private static final String[] FILES = {"file1.xml", "file2.xml", "file3.xml", "file4.xml"};
    private static final String[] ARCHIVES = {"archive1.jar", "archive2.jar", "archive3.jar", "archive4.jar"};

    private static final String MAPRED_JOB_QUEUE_NAME = "mapred.job.queue.name";
    private static final String DEFAULT = "default";

    private static final ImmutableMap<String, String> CONFIG_EXAMPLE = getConfigExample();

    private static ImmutableMap<String, String> getConfigExample() {
        final ImmutableMap.Builder<String, String> configExampleBuilder = new ImmutableMap.Builder<>();

        final String[] keys = {"mapred.map.tasks", "mapred.input.dir", "mapred.output.dir"};
        final String[] values = {"1", "${inputDir}", "${outputDir}"};

        for (int i = 0; i < keys.length; ++i) {
            configExampleBuilder.put(keys[i], values[i]);
        }

        return configExampleBuilder.build();
    }

    private static final Delete[] DELETES = {new Delete("path0", null),
            new Delete("path1", true),
            new Delete("path2", false),
            new Delete("path3", null)
    };

    private static final Mkdir[] MKDIRS = {new Mkdir("path0"),
            new Mkdir("path1"),
            new Mkdir("path2"),
            new Mkdir("path3")
    };

    private static final Move[] MOVES = {new Move("source0", "target0"),
            new Move("source1", "target1"),
            new Move("source2", "target2"),
            new Move("source3", "target3")
    };

    private static final Chmod[] CHMODS = {new ChmodBuilder().withPermissions("711").build(),
            new ChmodBuilder().withPermissions("511").build(),
            new ChmodBuilder().withPermissions("551").build(),
            new ChmodBuilder().withPermissions("755").build()
    };

    private static final Touchz[] TOUCHZS = {new Touchz("path0"),
            new Touchz("path1"),
            new Touchz("path2"),
            new Touchz("path3")
    };

    private static final Chgrp[] CHGRPS = {new ChgrpBuilder().withGroup("user0").build(),
            new ChgrpBuilder().withGroup("user1").build(),
            new ChgrpBuilder().withGroup("user2").build(),
            new ChgrpBuilder().withGroup("user3").build()
    };

    private static final String RESOURCE_MANAGER = "${resourceManager}";

    private static final Launcher LAUNCHER = new LauncherBuilder()
            .withMemoryMb(1024)
            .withVCores(2)
            .withQueue(DEFAULT)
            .withSharelib(DEFAULT)
            .withViewAcl(DEFAULT)
            .withModifyAcl(DEFAULT)
            .build();

    private ActionAttributesBuilder getBuilderInstance() {
        return ActionAttributesBuilder.create();
    }

    @Test
    public void testFromExistingBuilder() {
        final ActionAttributesBuilder builder = getBuilderInstance();
        builder.withNameNode(NAME_NODE);

        final ActionAttributes fromExisting = ActionAttributesBuilder
                .createFromExisting(builder.build())
                .build();

        assertEquals(NAME_NODE, fromExisting.getNameNode());
    }

    @Test
    public void testNameNodeAdded() {
        final ActionAttributesBuilder builder = getBuilderInstance();
        builder.withNameNode(NAME_NODE);

        final ActionAttributes attributes = builder.build();
        assertEquals(NAME_NODE, attributes.getNameNode());
    }

    @Test
    public void testNameNodeAddedTwiceThrows() {
        final ActionAttributesBuilder builder = getBuilderInstance();
        builder.withNameNode(NAME_NODE);

        expectedException.expect(IllegalStateException.class);
        builder.withNameNode("any_string");
    }

    @Test
    public void testPrepareAdded() {
        final ActionAttributesBuilder builder = getBuilderInstance();
        builder.withPrepare(new PrepareBuilder().withDelete(EXAMPLE_DIR).build());

        final ActionAttributes attributes = builder.build();
        assertEquals(EXAMPLE_DIR, attributes.getPrepare().getDeletes().get(0).getPath());
    }

    @Test
    public void testPrepareAddedTwiceThrows() {
        final ActionAttributesBuilder builder = getBuilderInstance();
        builder.withPrepare(new PrepareBuilder().withDelete(EXAMPLE_DIR).build());

        expectedException.expect(IllegalStateException.class);
        builder.withPrepare(new PrepareBuilder().withDelete("any_directory").build());
    }

    @Test
    public void testStreamingAdded() {
        final Streaming streaming = new StreamingBuilder().withMapper("mapper.sh").withReducer("reducer.sh").build();

        final ActionAttributesBuilder builder = getBuilderInstance();
        builder.withStreaming(streaming);

        final ActionAttributes attributes = builder.build();
        assertEquals(streaming, attributes.getStreaming());
    }

    @Test
    public void testStreamingAddedTwiceThrows() {
        final Streaming streaming1= new StreamingBuilder().withMapper("mapper1.sh").withReducer("reducer1.sh").build();
        final Streaming streaming2 = new StreamingBuilder().withMapper("mapper2.sh").withReducer("reducer2.sh").build();

        final ActionAttributesBuilder builder = getBuilderInstance();
        builder.withStreaming(streaming1);

        expectedException.expect(IllegalStateException.class);
        builder.withStreaming(streaming2);
    }

    @Test
    public void testPipesAdded() {
        final Pipes pipes = new PipesBuilder().withMap("map").withReduce("reduce").build();

        final ActionAttributesBuilder builder = getBuilderInstance();
        builder.withPipes(pipes);

        final ActionAttributes attributes = builder.build();
        assertEquals(pipes, attributes.getPipes());
    }

    @Test
    public void testPipesAddedTwiceThrows() {
        final Pipes pipes1 = new PipesBuilder().withMap("map1").withReduce("reduce1").build();
        final Pipes pipes2 = new PipesBuilder().withMap("map2").withReduce("reduce2").build();

        final ActionAttributesBuilder builder = getBuilderInstance();
        builder.withPipes(pipes1);

        expectedException.expect(IllegalStateException.class);
        builder.withPipes(pipes2);
    }

    @Test
    public void testConfigClassAdded() {
        final ActionAttributesBuilder builder = getBuilderInstance();
        builder.withConfigClass(CONFIG_CLASS);

        final ActionAttributes attributes = builder.build();
        assertEquals(CONFIG_CLASS, attributes.getConfigClass());
    }

    @Test
    public void testConfigClassAddedTwiceThrows() {
        final ActionAttributesBuilder builder = getBuilderInstance();
        builder.withConfigClass(CONFIG_CLASS);

        expectedException.expect(IllegalStateException.class);
        builder.withConfigClass("AnyClass");
    }

    @Test
    public void testSeveralJobXmlsAdded() {
        final ActionAttributesBuilder builder = getBuilderInstance();

        for (final String jobXml : JOB_XMLS) {
            builder.withJobXml(jobXml);
        }

        final ActionAttributes attributes = builder.build();

        final List<String> jobXmlsList = attributes.getJobXmls();
        assertEquals(JOB_XMLS.length, jobXmlsList.size());

        for (int i = 0; i < JOB_XMLS.length; ++i) {
            assertEquals(JOB_XMLS[i], jobXmlsList.get(i));
        }
    }

    @Test
    public void testWithoutJobXmls() {
        final ActionAttributesBuilder builder = getBuilderInstance();

        for (final String jobXml : JOB_XMLS) {
            builder.withJobXml(jobXml);
        }

        builder.withoutJobXml(JOB_XMLS[0]);

        final ActionAttributes attributes = builder.build();

        final List<String> jobXmlsList = attributes.getJobXmls();
        final String[] remainingJobXmls = Arrays.copyOfRange(JOB_XMLS, 1, JOB_XMLS.length);
        assertEquals(remainingJobXmls.length, jobXmlsList.size());

        for (int i = 0; i < remainingJobXmls.length; ++i) {
            assertEquals(remainingJobXmls[i], jobXmlsList.get(i));
        }
    }

    @Test
    public void testClearJobXmls() {
        final ActionAttributesBuilder builder = getBuilderInstance();

        for (final String jobXml : JOB_XMLS) {
            builder.withJobXml(jobXml);
        }

        builder.clearJobXmls();

        final ActionAttributes attributes = builder.build();

        final List<String> jobXmlsList = attributes.getJobXmls();
        assertEquals(0, jobXmlsList.size());
    }



    @Test
    public void testConfigPropertyAdded() {
        final ActionAttributesBuilder builder = getBuilderInstance();
        builder.withConfigProperty(MAPRED_JOB_QUEUE_NAME, DEFAULT);

        final ActionAttributes attributes = builder.build();
        assertEquals(DEFAULT, attributes.getConfiguration().get(MAPRED_JOB_QUEUE_NAME));
    }

    @Test
    public void testSeveralConfigPropertiesAdded() {
        final ActionAttributesBuilder builder = getBuilderInstance();

        for (final Map.Entry<String, String> entry : CONFIG_EXAMPLE.entrySet()) {
            builder.withConfigProperty(entry.getKey(), entry.getValue());
        }

        final ActionAttributes attributes = builder.build();

        for (final Map.Entry<String, String> entry : CONFIG_EXAMPLE.entrySet()) {
            assertEquals(entry.getValue(), attributes.getConfiguration().get(entry.getKey()));
        }

        assertEquals(CONFIG_EXAMPLE, attributes.getConfiguration());
    }

    @Test
    public void testSameConfigPropertyAddedTwiceThrows() {
        final ActionAttributesBuilder builder = getBuilderInstance();
        builder.withConfigProperty(MAPRED_JOB_QUEUE_NAME, DEFAULT);

        expectedException.expect(IllegalStateException.class);
        builder.withConfigProperty(MAPRED_JOB_QUEUE_NAME, DEFAULT);
    }

    @Test
    public void testSeveralFilesAdded() {
        final ActionAttributesBuilder builder = getBuilderInstance();

        for (final String file : FILES) {
            builder.withFile(file);
        }

        final ActionAttributes attributes = builder.build();

        final List<String> filesList = attributes.getFiles();
        assertEquals(FILES.length, filesList.size());

        for (int i = 0; i < FILES.length; ++i) {
            assertEquals(FILES[i], filesList.get(i));
        }
    }

    @Test
    public void testRemoveFiles() {
        final ActionAttributesBuilder builder = getBuilderInstance();

        for (final String file : FILES) {
            builder.withFile(file);
        }

        builder.withoutFile(FILES[0]);

        final ActionAttributes attributes = builder.build();

        final List<String> filesList = attributes.getFiles();
        final String[] remainingFiles = Arrays.copyOfRange(FILES, 1, FILES.length);
        assertEquals(remainingFiles.length, filesList.size());

        for (int i = 0; i < remainingFiles.length; ++i) {
            assertEquals(remainingFiles[i], filesList.get(i));
        }
    }

    @Test
    public void testClearFiles() {
        final ActionAttributesBuilder builder = getBuilderInstance();

        for (final String file : FILES) {
            builder.withFile(file);
        }

        builder.clearFiles();

        final ActionAttributes attributes = builder.build();

        final List<String> filesList = attributes.getFiles();
        assertEquals(0, filesList.size());
    }

    @Test
    public void testSeveralArchivesAdded() {
        final ActionAttributesBuilder builder = getBuilderInstance();

        for (final String archive : ARCHIVES) {
            builder.withArchive(archive);
        }

        final ActionAttributes attributes = builder.build();

        final List<String> filesList = attributes.getArchives();
        assertEquals(ARCHIVES.length, filesList.size());

        for (int i = 0; i < ARCHIVES.length; ++i) {
            assertEquals(ARCHIVES[i], filesList.get(i));
        }
    }

    @Test
    public void testRemoveArchives() {
        final ActionAttributesBuilder builder = getBuilderInstance();

        for (final String archive : ARCHIVES) {
            builder.withArchive(archive);
        }

        builder.withoutArchive(ARCHIVES[0]);

        final ActionAttributes attributes = builder.build();

        final List<String> archivesList = attributes.getArchives();
        final String[] remainingArchives = Arrays.copyOfRange(ARCHIVES, 1, ARCHIVES.length);
        assertEquals(remainingArchives.length, archivesList.size());

        for (int i = 0; i < remainingArchives.length; ++i) {
            assertEquals(remainingArchives[i], archivesList.get(i));
        }
    }

    @Test
    public void testClearArchives() {
        final ActionAttributesBuilder builder = getBuilderInstance();

        for (final String archive : ARCHIVES) {
            builder.withArchive(archive);
        }

        builder.clearArchives();

        final ActionAttributes attributes = builder.build();

        final List<String> archivesList = attributes.getArchives();
        assertEquals(0, archivesList.size());
    }

    @Test
    public void testSeveralDeletesAdded() {
        final ActionAttributesBuilder builder = getBuilderInstance();

        for (final Delete delete : DELETES) {
            builder.withDelete(delete);
        }

        final ActionAttributes attributes = builder.build();

        assertEquals(Arrays.asList(DELETES), attributes.getDeletes());
    }

    @Test
    public void testWithoutDelete() {
        final ActionAttributesBuilder builder = getBuilderInstance();

        for (final Delete delete : DELETES) {
            builder.withDelete(delete);
        }

        builder.withoutDelete(DELETES[0]);

        final ActionAttributes attributes = builder.build();

        final List<Delete> expectedDeletes = Arrays.asList(DELETES).subList(1, DELETES.length);
        assertEquals(expectedDeletes, attributes.getDeletes());
    }

    @Test
    public void testClearDeletes() {
        final ActionAttributesBuilder builder = getBuilderInstance();

        for (final Delete delete : DELETES) {
            builder.withDelete(delete);
        }

        builder.clearDeletes();

        final ActionAttributes attributes = builder.build();

        assertTrue(attributes.getDeletes().isEmpty());
    }

    @Test
    public void testSeveralMkdirsAdded() {
        final ActionAttributesBuilder builder = getBuilderInstance();

        for (final Mkdir mkdir : MKDIRS) {
            builder.withMkdir(mkdir);
        }

        final ActionAttributes attributes = builder.build();

        assertEquals(Arrays.asList(MKDIRS), attributes.getMkdirs());
    }

    @Test
    public void testWithoutMkdir() {
        final ActionAttributesBuilder builder = getBuilderInstance();

        for (final Mkdir mkdir : MKDIRS) {
            builder.withMkdir(mkdir);
        }

        builder.withoutMkdir(MKDIRS[0]);

        final ActionAttributes attributes = builder.build();

        final List<Mkdir> expectedMkdirs = Arrays.asList(MKDIRS).subList(1, MKDIRS.length);
        assertEquals(expectedMkdirs, attributes.getMkdirs());
    }

    @Test
    public void testClearMkdirs() {
        final ActionAttributesBuilder builder = getBuilderInstance();

        for (final Mkdir mkdir : MKDIRS) {
            builder.withMkdir(mkdir);
        }

        builder.clearMkdirs();

        final ActionAttributes attributes = builder.build();

        assertTrue(attributes.getMkdirs().isEmpty());
    }

    @Test
    public void testSeveralMovesAdded() {
        final ActionAttributesBuilder builder = getBuilderInstance();

        for (final Move move : MOVES) {
            builder.withMove(move);
        }

        final ActionAttributes attributes = builder.build();

        assertEquals(Arrays.asList(MOVES), attributes.getMoves());
    }

    @Test
    public void testWithoutMove() {
        final ActionAttributesBuilder builder = getBuilderInstance();

        for (final Move move : MOVES) {
            builder.withMove(move);
        }

        builder.withoutMove(MOVES[0]);

        final ActionAttributes attributes = builder.build();

        final List<Move> expectedMoves = Arrays.asList(MOVES).subList(1, MOVES.length);
        assertEquals(expectedMoves, attributes.getMoves());
    }

    @Test
    public void testClearMoves() {
        final ActionAttributesBuilder builder = getBuilderInstance();

        for (final Move move : MOVES) {
            builder.withMove(move);
        }

        builder.clearMoves();

        final ActionAttributes attributes = builder.build();

        assertTrue(attributes.getMoves().isEmpty());
    }

    @Test
    public void testSeveralChmodsAdded() {
        final ActionAttributesBuilder builder = getBuilderInstance();

        for (final Chmod chmod : CHMODS) {
            builder.withChmod(chmod);
        }

        final ActionAttributes attributes = builder.build();

        assertEquals(Arrays.asList(CHMODS), attributes.getChmods());
    }

    @Test
    public void testWithoutChmod() {
        final ActionAttributesBuilder builder = getBuilderInstance();

        for (final Chmod chmod : CHMODS) {
            builder.withChmod(chmod);
        }

        builder.withoutChmod(CHMODS[0]);

        final ActionAttributes attributes = builder.build();

        final List<Chmod> expectedChmods = Arrays.asList(CHMODS).subList(1, CHMODS.length);
        assertEquals(expectedChmods, attributes.getChmods());
    }

    @Test
    public void testClearChmods() {
        final ActionAttributesBuilder builder = getBuilderInstance();

        for (final Chmod chmod : CHMODS) {
            builder.withChmod(chmod);
        }

        builder.clearChmods();

        final ActionAttributes attributes = builder.build();

        assertTrue(attributes.getChmods().isEmpty());
    }

    @Test
    public void testSeveralTouchzsAdded() {
        final ActionAttributesBuilder builder = getBuilderInstance();

        for (final Touchz touchz : TOUCHZS) {
            builder.withTouchz(touchz);
        }

        final ActionAttributes attributes = builder.build();

        assertEquals(Arrays.asList(TOUCHZS), attributes.getTouchzs());
    }

    @Test
    public void testWithoutTouchz() {
        final ActionAttributesBuilder builder = getBuilderInstance();

        for (final Touchz touchz : TOUCHZS) {
            builder.withTouchz(touchz);
        }

        builder.withoutTouchz(TOUCHZS[0]);

        final ActionAttributes attributes = builder.build();

        final List<Touchz> expectedTouchzs = Arrays.asList(TOUCHZS).subList(1, TOUCHZS.length);
        assertEquals(expectedTouchzs, attributes.getTouchzs());
    }

    @Test
    public void testClearTouchzs() {
        final ActionAttributesBuilder builder = getBuilderInstance();

        for (final Touchz touchz : TOUCHZS) {
            builder.withTouchz(touchz);
        }

        builder.clearTouchzs();

        final ActionAttributes attributes = builder.build();

        assertTrue(attributes.getTouchzs().isEmpty());
    }

    @Test
    public void testSeveralChgrpsAdded() {
        final ActionAttributesBuilder builder = getBuilderInstance();

        for (final Chgrp chgrp : CHGRPS) {
            builder.withChgrp(chgrp);
        }

        final ActionAttributes attributes = builder.build();

        assertEquals(Arrays.asList(CHGRPS), attributes.getChgrps());
    }

    @Test
    public void testWithoutChgrp() {
        final ActionAttributesBuilder builder = getBuilderInstance();

        for (final Chgrp chgrp : CHGRPS) {
            builder.withChgrp(chgrp);
        }

        builder.withoutChgrp(CHGRPS[0]);

        final ActionAttributes attributes = builder.build();

        final List<Chgrp> expectedChgrps = Arrays.asList(CHGRPS).subList(1, CHGRPS.length);
        assertEquals(expectedChgrps, attributes.getChgrps());
    }

    @Test
    public void testClearChgrps() {
        final ActionAttributesBuilder builder = getBuilderInstance();

        for (final Chgrp chgrp : CHGRPS) {
            builder.withChgrp(chgrp);
        }

        builder.clearChgrps();

        final ActionAttributes attributes = builder.build();

        assertTrue(attributes.getChgrps().isEmpty());
    }

    @Test
    public void testResourceManagerAdded() {
        final ActionAttributesBuilder builder = getBuilderInstance();
        builder.withResourceManager(RESOURCE_MANAGER);

        final ActionAttributes attributes = builder.build();
        assertEquals(RESOURCE_MANAGER, attributes.getResourceManager());
    }

    @Test
    public void testLauncherAdded() {
        final ActionAttributesBuilder builder = getBuilderInstance();
        builder.withLauncher(LAUNCHER);

        final ActionAttributes attributes = builder.build();
        assertEquals(LAUNCHER.getMemoryMb(), attributes.getLauncher().getMemoryMb());
        assertEquals(LAUNCHER.getVCores(), attributes.getLauncher().getVCores());
        assertEquals(LAUNCHER.getQueue(), attributes.getLauncher().getQueue());
        assertEquals(LAUNCHER.getSharelib(), attributes.getLauncher().getSharelib());
        assertEquals(LAUNCHER.getViewAcl(), attributes.getLauncher().getViewAcl());
        assertEquals(LAUNCHER.getModifyAcl(), attributes.getLauncher().getModifyAcl());
    }
}
