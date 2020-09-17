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
import static org.junit.Assert.assertTrue;

public class TestFSActionBuilder extends TestNodeBuilderBaseImpl<FSAction, FSActionBuilder> {
    private static final String NAME_NODE = "${nameNode}";
    private static final String[] JOB_XMLS = {"jobXml1.xml", "jobXml2.xml", "jobXml3.xml", "jobXml4.xml"};
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

    private static final String MAPRED_JOB_QUEUE_NAME = "mapred.job.queue.name";
    private static final String DEFAULT = "default";

    @Override
    protected FSActionBuilder getBuilderInstance() {
        return FSActionBuilder.create();
    }

    @Override
    protected FSActionBuilder getBuilderInstance(FSAction action) {
        return FSActionBuilder.createFromExistingAction(action);
    }

    @Test
    public void testNameNodeAdded() {
        final FSActionBuilder builder = getBuilderInstance();
        builder.withNameNode(NAME_NODE);

        final FSAction mrAction = builder.build();
        assertEquals(NAME_NODE, mrAction.getNameNode());
    }

    @Test
    public void testNameNodeAddedTwiceThrows() {
        final FSActionBuilder builder = getBuilderInstance();
        builder.withNameNode(NAME_NODE);

        expectedException.expect(IllegalStateException.class);
        builder.withNameNode("any_string");
    }

    @Test
    public void testSeveralJobXmlsAdded() {
        final FSActionBuilder builder = getBuilderInstance();

        for (final String jobXml : JOB_XMLS) {
            builder.withJobXml(jobXml);
        }

        final FSAction fsAction = builder.build();

        final List<String> jobXmlsList = fsAction.getJobXmls();
        assertEquals(JOB_XMLS.length, jobXmlsList.size());

        for (int i = 0; i < JOB_XMLS.length; ++i) {
            assertEquals(JOB_XMLS[i], jobXmlsList.get(i));
        }
    }

    @Test
    public void testWithoutJobXmls() {
        final FSActionBuilder builder = getBuilderInstance();

        for (final String jobXml : JOB_XMLS) {
            builder.withJobXml(jobXml);
        }

        builder.withoutJobXml(JOB_XMLS[0]);

        final FSAction fsAction = builder.build();

        final List<String> jobXmlsList = fsAction.getJobXmls();
        final String[] remainingJobXmls = Arrays.copyOfRange(JOB_XMLS, 1, JOB_XMLS.length);
        assertEquals(remainingJobXmls.length, jobXmlsList.size());

        for (int i = 0; i < remainingJobXmls.length; ++i) {
            assertEquals(remainingJobXmls[i], jobXmlsList.get(i));
        }
    }

    @Test
    public void testClearJobXmls() {
        final FSActionBuilder builder = getBuilderInstance();

        for (final String jobXml : JOB_XMLS) {
            builder.withJobXml(jobXml);
        }

        builder.clearJobXmls();

        final FSAction fsAction = builder.build();

        final List<String> jobXmlsList = fsAction.getJobXmls();
        assertEquals(0, jobXmlsList.size());
    }

    @Test
    public void testSameConfigPropertyAddedTwiceThrows() {
        final FSActionBuilder builder = getBuilderInstance();
        builder.withConfigProperty(MAPRED_JOB_QUEUE_NAME, DEFAULT);

        expectedException.expect(IllegalStateException.class);
        builder.withConfigProperty(MAPRED_JOB_QUEUE_NAME, DEFAULT);
    }

    @Test
    public void testSeveralDeletesAdded() {
        final FSActionBuilder builder = getBuilderInstance();

        for (final Delete delete : DELETES) {
            builder.withDelete(delete);
        }

        final FSAction fsAction = builder.build();

        assertEquals(Arrays.asList(DELETES), fsAction.getDeletes());
    }

    @Test
    public void testWithoutDelete() {
        final FSActionBuilder builder = getBuilderInstance();

        for (final Delete delete : DELETES) {
            builder.withDelete(delete);
        }

        builder.withoutDelete(DELETES[0]);

        final FSAction fsAction = builder.build();

        final List<Delete> expectedDeletes = Arrays.asList(DELETES).subList(1, DELETES.length);
        assertEquals(expectedDeletes, fsAction.getDeletes());
    }

    @Test
    public void testClearDeletes() {
        final FSActionBuilder builder = getBuilderInstance();

        for (final Delete delete : DELETES) {
            builder.withDelete(delete);
        }

        builder.clearDeletes();

        final FSAction fsAction = builder.build();

        assertTrue(fsAction.getDeletes().isEmpty());
    }

    @Test
    public void testSeveralMkdirsAdded() {
        final FSActionBuilder builder = getBuilderInstance();

        for (final Mkdir mkdir : MKDIRS) {
            builder.withMkdir(mkdir);
        }

        final FSAction fsAction = builder.build();

        assertEquals(Arrays.asList(MKDIRS), fsAction.getMkdirs());
    }

    @Test
    public void testWithoutMkdir() {
        final FSActionBuilder builder = getBuilderInstance();

        for (final Mkdir mkdir : MKDIRS) {
            builder.withMkdir(mkdir);
        }

        builder.withoutMkdir(MKDIRS[0]);

        final FSAction fsAction = builder.build();

        final List<Mkdir> expectedMkdirs = Arrays.asList(MKDIRS).subList(1, MKDIRS.length);
        assertEquals(expectedMkdirs, fsAction.getMkdirs());
    }

    @Test
    public void testClearMkdirs() {
        final FSActionBuilder builder = getBuilderInstance();

        for (final Mkdir mkdir : MKDIRS) {
            builder.withMkdir(mkdir);
        }

        builder.clearMkdirs();

        final FSAction fsAction = builder.build();

        assertTrue(fsAction.getMkdirs().isEmpty());
    }

    @Test
    public void testSeveralMovesAdded() {
        final FSActionBuilder builder = getBuilderInstance();

        for (final Move move : MOVES) {
            builder.withMove(move);
        }

        final FSAction fsAction = builder.build();

        assertEquals(Arrays.asList(MOVES), fsAction.getMoves());
    }

    @Test
    public void testWithoutMove() {
        final FSActionBuilder builder = getBuilderInstance();

        for (final Move move : MOVES) {
            builder.withMove(move);
        }

        builder.withoutMove(MOVES[0]);

        final FSAction fsAction = builder.build();

        final List<Move> expectedMoves = Arrays.asList(MOVES).subList(1, MOVES.length);
        assertEquals(expectedMoves, fsAction.getMoves());
    }

    @Test
    public void testClearMoves() {
        final FSActionBuilder builder = getBuilderInstance();

        for (final Move move : MOVES) {
            builder.withMove(move);
        }

        builder.clearMoves();

        final FSAction fsAction = builder.build();

        assertTrue(fsAction.getMoves().isEmpty());
    }

    @Test
    public void testSeveralChmodsAdded() {
        final FSActionBuilder builder = getBuilderInstance();

        for (final Chmod chmod : CHMODS) {
            builder.withChmod(chmod);
        }

        final FSAction fsAction = builder.build();

        assertEquals(Arrays.asList(CHMODS), fsAction.getChmods());
    }

    @Test
    public void testWithoutChmod() {
        final FSActionBuilder builder = getBuilderInstance();

        for (final Chmod chmod : CHMODS) {
            builder.withChmod(chmod);
        }

        builder.withoutChmod(CHMODS[0]);

        final FSAction fsAction = builder.build();

        final List<Chmod> expectedChmods = Arrays.asList(CHMODS).subList(1, CHMODS.length);
        assertEquals(expectedChmods, fsAction.getChmods());
    }

    @Test
    public void testClearChmods() {
        final FSActionBuilder builder = getBuilderInstance();

        for (final Chmod chmod : CHMODS) {
            builder.withChmod(chmod);
        }

        builder.clearChmods();

        final FSAction fsAction = builder.build();

        assertTrue(fsAction.getChmods().isEmpty());
    }

    @Test
    public void testSeveralTouchzsAdded() {
        final FSActionBuilder builder = getBuilderInstance();

        for (final Touchz touchz : TOUCHZS) {
            builder.withTouchz(touchz);
        }

        final FSAction fsAction = builder.build();

        assertEquals(Arrays.asList(TOUCHZS), fsAction.getTouchzs());
    }

    @Test
    public void testWithoutTouchz() {
        final FSActionBuilder builder = getBuilderInstance();

        for (final Touchz touchz : TOUCHZS) {
            builder.withTouchz(touchz);
        }

        builder.withoutTouchz(TOUCHZS[0]);

        final FSAction fsAction = builder.build();

        final List<Touchz> expectedTouchzs = Arrays.asList(TOUCHZS).subList(1, TOUCHZS.length);
        assertEquals(expectedTouchzs, fsAction.getTouchzs());
    }

    @Test
    public void testClearTouchzs() {
        final FSActionBuilder builder = getBuilderInstance();

        for (final Touchz touchz : TOUCHZS) {
            builder.withTouchz(touchz);
        }

        builder.clearTouchzs();

        final FSAction fsAction = builder.build();

        assertTrue(fsAction.getTouchzs().isEmpty());
    }

    @Test
    public void testSeveralChgrpsAdded() {
        final FSActionBuilder builder = getBuilderInstance();

        for (final Chgrp chgrp : CHGRPS) {
            builder.withChgrp(chgrp);
        }

        final FSAction fsAction = builder.build();

        assertEquals(Arrays.asList(CHGRPS), fsAction.getChgrps());
    }

    @Test
    public void testWithoutChgrp() {
        final FSActionBuilder builder = getBuilderInstance();

        for (final Chgrp chgrp : CHGRPS) {
            builder.withChgrp(chgrp);
        }

        builder.withoutChgrp(CHGRPS[0]);

        final FSAction fsAction = builder.build();

        final List<Chgrp> expectedChgrps = Arrays.asList(CHGRPS).subList(1, CHGRPS.length);
        assertEquals(expectedChgrps, fsAction.getChgrps());
    }

    @Test
    public void testClearChgrps() {
        final FSActionBuilder builder = getBuilderInstance();

        for (final Chgrp chgrp : CHGRPS) {
            builder.withChgrp(chgrp);
        }

        builder.clearChgrps();

        final FSAction fsAction = builder.build();

        assertTrue(fsAction.getChgrps().isEmpty());
    }

    @Test
    public void testFromExistingFSAction() {
        final String nameNode = "${nameNode}";

        final FSActionBuilder builder = getBuilderInstance();
        builder.withNameNode(nameNode)
                .withConfigProperty(MAPRED_JOB_QUEUE_NAME, DEFAULT);

        final FSAction action = builder.build();

        final FSActionBuilder fromExistingBuilder = getBuilderInstance(action);

        final FSAction modifiedAction = fromExistingBuilder.build();
        assertEquals(nameNode, modifiedAction.getNameNode());

        final Map<String, String> expectedConfiguration = new LinkedHashMap<>();
        expectedConfiguration.put(MAPRED_JOB_QUEUE_NAME, DEFAULT);
        assertEquals(expectedConfiguration, modifiedAction.getConfiguration());
    }

    @Test
    public void testFromEmailAction() {
        final EmailAction parent = EmailActionBuilder.create()
                .withName("parent")
                .build();

        final EmailAction other = EmailActionBuilder.createFromExistingAction(parent)
                .withName("other")
                .withParent(parent)
                .build();

        final FSAction fromEmail = FSActionBuilder.createFromExistingAction(other)
                .withName("fs")
                .withNameNode("${nameNode}")
                .build();

        assertEquals(parent, fromEmail.getParentsWithoutConditions().get(0));
        assertEquals("fs", fromEmail.getName());
        assertEquals("${nameNode}", fromEmail.getNameNode());
    }
}
