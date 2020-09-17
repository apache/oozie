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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test cases for the {@link StringUtils} class.
 */
public class TestStringUtils {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public static final String ASSERT_MESSAGE_CHECK_STATIC_EXISTENCE = "Invalid checkStaticExistence return value";

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

    @Test
    public void testCheckStaticExistenceWithValidExpressions() throws ELEvaluationException {
        assertFalse(ASSERT_MESSAGE_CHECK_STATIC_EXISTENCE, StringUtils.checkStaticExistence("ab", ","));
        assertTrue(ASSERT_MESSAGE_CHECK_STATIC_EXISTENCE, StringUtils.checkStaticExistence("${a:a()}${a:a()}!", "!"));
        assertTrue(ASSERT_MESSAGE_CHECK_STATIC_EXISTENCE, StringUtils.checkStaticExistence("${a:a()},${a:a()}", ","));
        assertFalse(ASSERT_MESSAGE_CHECK_STATIC_EXISTENCE, StringUtils.checkStaticExistence("${a:d('foo', 'bar')}", ","));
        assertTrue(ASSERT_MESSAGE_CHECK_STATIC_EXISTENCE, StringUtils.checkStaticExistence("${a:a(), a:a()}", ","));
        assertTrue(ASSERT_MESSAGE_CHECK_STATIC_EXISTENCE, StringUtils.checkStaticExistence("a,b", ","));
        assertTrue(ASSERT_MESSAGE_CHECK_STATIC_EXISTENCE, StringUtils.checkStaticExistence("'a,b'", ","));
        assertFalse(ASSERT_MESSAGE_CHECK_STATIC_EXISTENCE, StringUtils.checkStaticExistence("${func(a,b)}", ","));
        assertFalse(ASSERT_MESSAGE_CHECK_STATIC_EXISTENCE, StringUtils.checkStaticExistence("${func('a','b')}", ","));
        assertTrue(ASSERT_MESSAGE_CHECK_STATIC_EXISTENCE, StringUtils.checkStaticExistence("${func('abc')},${func('def')}", ","));
    }

    @Test
    public void testUnclosedCurlyBracket() throws ELEvaluationException {
        expectedException.expect(ELEvaluationException.class);
        StringUtils.checkStaticExistence("${", ",");
    }

    @Test
    public void testClosingTooManyCurlyBracket() throws ELEvaluationException {
        expectedException.expect(ELEvaluationException.class);
        StringUtils.checkStaticExistence("${a:a()}}", ",");
    }

    @Test
    public void testUnclosedFunctionCall() throws ELEvaluationException {
        expectedException.expect(ELEvaluationException.class);
        StringUtils.checkStaticExistence("${a:a(}", ",");
    }

    @Test
    public void testTooManyParanthesisClosing() throws ELEvaluationException {
        expectedException.expect(ELEvaluationException.class);
        StringUtils.checkStaticExistence("${a:a())}", ",");
    }

    @Test
    public void testSequenceBeforeInvalidPart() throws ELEvaluationException {
        expectedException.expect(ELEvaluationException.class);
        StringUtils.checkStaticExistence("${a:a(),b:b(),c:c(}", ",");
    }
}
