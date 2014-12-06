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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class for rule mapping
 */
public class MappingRule {

    private static Pattern variableNamePattern = Pattern.compile("\\$\\{[0-9]\\}");
    private Pattern fromPattern;
    private String fromString;
    private String toString;
    private boolean patternMatch;

    /**
     * Maps from source rule to destination rule
     * @param fromRule - Rule for which input needs to be matched
     * @param toRule - Rule for value to be returned
     */
    public MappingRule(String fromRule, String toRule) {
        if (fromRule.contains("$")) {
            patternMatch = true;
            fromRule = fromRule.replaceAll("\\.", "\\\\.");
            Matcher match = variableNamePattern.matcher(fromRule);
            fromRule = match.replaceAll("(.*)");
            fromPattern = Pattern.compile(fromRule);
        }
        else {
            fromString = fromRule;
        }
        toString = toRule;
    }

    /**
     * Gets the from rule
     * @return
     */
    public String getFromRule() {
        return fromString;
    }

    /**
     * Gets the to rule
     * @return
     */
    public String getToRule() {
        return toString;
    }

    /**
     * Applies rules based on the input
     * @param input
     * @return
     */
    public String applyRule(String input) {
        if (patternMatch) {
            Matcher match = fromPattern.matcher(input);
            if (match.matches()) {
                String result = toString;
                int count = match.groupCount();
                for (int i = 1; i <= count; i++) {
                    result = result.replace("${" + (i) + "}", match.group(i));
                }
                return result;
            }
        }
        else if (input.equals(fromString)) {
            return toString;
        }
        return null;
    }
}
