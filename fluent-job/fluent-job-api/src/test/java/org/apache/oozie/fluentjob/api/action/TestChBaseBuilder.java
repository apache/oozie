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

public abstract class TestChBaseBuilder<A extends ChFSBase, B extends ChFSBaseBuilder<B> & Builder<A>> {
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    protected abstract B getBuilderInstance();

    @Test
    public void testSetRecursive() {
        final B builder = getBuilderInstance();
        builder.setRecursive();

        final A chmod = builder.build();
        assertEquals(true, chmod.isRecursive());
    }

    @Test
    public void testSetRecursiveCalledTwiceThrows() {
        final B builder = getBuilderInstance();
        builder.setRecursive();

        expectedException.expect(IllegalStateException.class);
        builder.setRecursive();
    }

    @Test
    public void testSetNonRecursive() {
        final B builder = getBuilderInstance();
        builder.setNonRecursive();

        final A chmod = builder.build();
        assertEquals(false, chmod.isRecursive());
    }

    @Test
    public void testSetNonRecursiveCalledTwiceThrows() {
        final B builder = getBuilderInstance();
        builder.setNonRecursive();

        expectedException.expect(IllegalStateException.class);
        builder.setNonRecursive();
    }

    @Test
    public void testSetRecursiveCalledAfterSetNonRecursive() {
        final B builder = getBuilderInstance();
        builder.setNonRecursive();

        expectedException.expect(IllegalStateException.class);
        builder.setRecursive();
    }

    @Test
    public void testSetNonRecursiveCalledAfterSetRecursive() {
        final B builder = getBuilderInstance();
        builder.setRecursive();

        expectedException.expect(IllegalStateException.class);
        builder.setNonRecursive();
    }

    @Test
    public void testWithPath() {
        final String path = "path";

        final B builder = getBuilderInstance();
        builder.withPath(path);

        final A chmod = builder.build();
        assertEquals(path, chmod.getPath());
    }

    @Test
    public void testWithPathCalledTwiceThrows() {
        final String path1 = "path1";
        final String path2 = "path2";

        final B builder = getBuilderInstance();
        builder.withPath(path1);

        expectedException.expect(IllegalStateException.class);
        builder.withPath(path2);
    }

    @Test
    public void testSetDirFiles() {
        final B builder = getBuilderInstance();
        builder.setDirFiles(false);

        final A chmod = builder.build();
        assertEquals("false", chmod.getDirFiles());
    }

    @Test
    public void testSetDirFilesCalledTwiceThrows() {
        final B builder = getBuilderInstance();
        builder.setDirFiles(false);

        expectedException.expect(IllegalStateException.class);
        builder.setDirFiles(true);
    }
}
