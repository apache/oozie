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
package org.apache.oozie.action.hadoop;

import java.util.Arrays;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestDependencyDeduplicator {
    private String originalList;
    private String deduplicatedList;

    public TestDependencyDeduplicator(String testName, String deduplicatedList, String originalList) {
        this.originalList = originalList;
        this.deduplicatedList = deduplicatedList;
    }
    private static final String KEY = "key";

    private static final String WITHOUT_SYMLINKS = "/a/a.jar,/b/a.jar";
    private static final String WITHOUT_SYMLINKS_DEDUPLICATED = "/a/a.jar";

    private static final String FILENAME_ONLY = "a.jar,b.jar#a.jar";
    private static final String FILENAME_ONLY_DEDUPLICATED = "a.jar";

    private static final String SAME_SYMLINK = "/a/a.jar#a.jar,/a/b.jar#a.jar";
    private static final String SAME_SYMLINK_DEDUPLICATED = "/a/a.jar#a.jar";

    private static final String DIFFERENT_SYMLINK = "/a/a.jar#a.jar,/b/a.jar#b.jar";
    private static final String DIFFERENT_SYMLINK_DEDUPLICATED = DIFFERENT_SYMLINK;

    private static final String NOT_JARS = "/b/a.txt,/a/a.txt";
    private static final String NOT_JARS_DEDUPLICATED = "/b/a.txt";

    private static final String EMPTY = "";
    private static final String COMMAS_ONLY = ",,,,,";

    private static final String DIFFERENT_EXTENSON_SYMLINK = "/a/a.txt#a,/b/b.txt/a";
    private static final String DIFFERENT_EXTENSON_SYMLINK_DEDUPLICATED = "/a/a.txt#a";

    private static final String UNICODE_PATHS = "/音/a.txt#a,/音/b.txt/a";
    private static final String UNICODE_PATHS_DEDUPLICATED = "/音/a.txt#a";

    private static final String UNICODE_FILENAMES = "/a/a.txt#音,/b/音";
    private static final String UNICODE_FILENAMES_DEDUPLICATED = "/a/a.txt#音";

    private Configuration conf;
    private DependencyDeduplicator deduplicator;

    @Parameterized.Parameters(name = "{0}")
    public static Collection testCases() {
        return Arrays.asList(new Object[][] {
                {"Test without symliks", WITHOUT_SYMLINKS_DEDUPLICATED, WITHOUT_SYMLINKS},
                {"Test only with filenames", FILENAME_ONLY_DEDUPLICATED, FILENAME_ONLY},
                {"Test with symliks", SAME_SYMLINK_DEDUPLICATED, SAME_SYMLINK},
                {"Test with different symliks", DIFFERENT_SYMLINK_DEDUPLICATED, DIFFERENT_SYMLINK},
                {"Test not jars", NOT_JARS_DEDUPLICATED, NOT_JARS},
                {"Test with empty list", EMPTY, EMPTY},
                {"Test with list of empty values", EMPTY, COMMAS_ONLY},
                {"Test with different extension in symlink", DIFFERENT_EXTENSON_SYMLINK_DEDUPLICATED, DIFFERENT_EXTENSON_SYMLINK},
                {"Test with unicode characters in path", UNICODE_PATHS_DEDUPLICATED, UNICODE_PATHS},
                {"Test with unicode characters in path", UNICODE_FILENAMES_DEDUPLICATED, UNICODE_FILENAMES}
        });
    }

    @Before
    public void init() {
        deduplicator = new DependencyDeduplicator();
        conf = new Configuration();
    }

    @Test
    public void testDeduplication() {
        conf.set(KEY, originalList);
        deduplicator.deduplicate(conf, KEY);
        Assert.assertEquals(
                String.format("Deduplicator should provide [%s] for input [%s]", deduplicatedList, originalList),
                deduplicatedList, conf.get(KEY));
    }
}
