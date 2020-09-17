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

package org.apache.oozie;

import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertFalse;

public class TestErrorCode {

    // MessageFormat requires that single quotes are escaped by another single quote; otherwise, it (a) doesn't render the single
    // quote and (b) doesn't parse the {#} tokens after the single quote.  For example, foo("{0} don't have {1}", "I", "a problem")
    // would render as "I dont have {1}".  This test checks that we don't accidently do this.  It should be
    // foo("{0} don''t have {1}", "I", "a problem"), which would result in "I dont have a problem".
    @Test
    public void testEscapedSingleQuotes() {
        Pattern singleQuotePattern = Pattern.compile("^'[^']|[^']'[^']|[^']'$");
        for (ErrorCode ec : ErrorCode.values()) {
            String tmpl = ec.getTemplate();
            Matcher m = singleQuotePattern.matcher(tmpl);
            assertFalse("Found an unescaped single quote in " + ec + " (" + tmpl + ").\nMake sure to replace all ' with ''",
                    m.find());
        }
    }
}
