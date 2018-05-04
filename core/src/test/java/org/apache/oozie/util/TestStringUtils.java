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

package org.apache.oozie.util;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;

/**
 * Test cases for the {@link StringUtils} class.
 */
public class TestStringUtils {
    /**
     * Tests if a new line character is replaced by an empty string.
     */
    @Test
    public void testTrimNewLineReplacement() {
        assertEquals("A  string", StringUtils.trim("A \n string"));
    }

    /**
     * Tests if a tab character is replaced by an empty string.
     */
    @Test
    public void testTrimTabReplacement() {
        assertEquals("A  string", StringUtils.trim("A \t string"));
    }

    /**
     * Tests if surrounding whitespaces are trimmed.
     */
    @Test
    public void testTrimWhitespaceReplacement() {
        assertEquals("A string", StringUtils.trim("  A string     "));
    }

    /**
     * Tests if a valid string, meaning one without leading and trailing whitespaces, no new line characters and no tab character,
     * is returned umodified.
     */
    @Test
    public void testTrimUnmodifiedString() {
        assertEquals("A string", StringUtils.trim("A string"));
    }
}
