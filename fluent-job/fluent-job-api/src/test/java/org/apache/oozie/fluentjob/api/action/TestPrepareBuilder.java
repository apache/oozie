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

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestPrepareBuilder {
    private static final String[] TEST_FOLDER_NAMES = {
            "/user/testpath/testdir1",
            "/user/testpath/testdir2",
            "/user/testpath/testdir3"
    };

    private PrepareBuilder pb;

    @Before
    public void setUp() {
        pb = new PrepareBuilder();
    }

    @Test
    public void testOneDeleteIsAddedWithSkipTrashTrue() {
        pb.withDelete(TEST_FOLDER_NAMES[0], true);

        final Prepare prepare = pb.build();

        assertEquals(1, prepare.getDeletes().size());

        final Delete delete = prepare.getDeletes().get(0);
        assertEquals(TEST_FOLDER_NAMES[0], delete.getPath());
        assertEquals(true, delete.getSkipTrash());

        assertEquals(0, prepare.getMkdirs().size());
    }

    @Test
    public void testSeveralDeletesAreAddedWithSkipTrashNotSpecified() {
        for (final String testDir : TEST_FOLDER_NAMES) {
            pb.withDelete(testDir);
        }

        final Prepare prepare = pb.build();

        assertEquals(TEST_FOLDER_NAMES.length, prepare.getDeletes().size());

        for (int i = 0; i < TEST_FOLDER_NAMES.length; ++i) {
            final Delete delete = prepare.getDeletes().get(i);
            assertEquals(TEST_FOLDER_NAMES[i], delete.getPath());
            assertEquals(null, delete.getSkipTrash());
        }

        assertEquals(0, prepare.getMkdirs().size());
    }

    @Test
    public void testOneMkdirIsAdded() {
        pb.withMkdir(TEST_FOLDER_NAMES[0]);

        final Prepare prepare = pb.build();

        assertEquals(1, prepare.getMkdirs().size());

        final Mkdir mkdir = prepare.getMkdirs().get(0);
        assertEquals(TEST_FOLDER_NAMES[0], mkdir.getPath());

        assertEquals(0, prepare.getDeletes().size());
    }

    @Test
    public void testSeveralMkdirsAreAdded() {
        for (final String testDir : TEST_FOLDER_NAMES) {
            pb.withMkdir(testDir);
        }

        final Prepare prepare = pb.build();

        assertEquals(TEST_FOLDER_NAMES.length, prepare.getMkdirs().size());

        for (int i = 0; i < TEST_FOLDER_NAMES.length; ++i) {
            final Mkdir mkdir = prepare.getMkdirs().get(i);
            assertEquals(TEST_FOLDER_NAMES[i], mkdir.getPath());
        }

        assertEquals(0, prepare.getDeletes().size());
    }
}
