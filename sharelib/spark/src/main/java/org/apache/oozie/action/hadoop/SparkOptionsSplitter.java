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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class SparkOptionsSplitter {

    /**
     * Matches an option key.
     * <p/>
     * Some examples:
     * <ul>
     *     <li>{@code key1}</li>
     *     <li>{@code key2=}</li>
     *     <li>{@code key3a-key3b}</li>
     *     <li>{@code key4a-KEY4b=}</li>
     * </ul>
     */
    private static final Pattern OPTION_KEY_PREFIX = Pattern.compile("\\s*--[a-zA-Z0-9.]+[\\-a-zA-Z0-9.]*[=]?");

    /**
     * Matches an option key / value pair, where the whole value part is quoted.
     * <p/>
     * Some examples:
     * <ul>
     *     <li>{@code spark.executor.extraJavaOptions="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp"}</li>
     *     <li>{@code spark.executor.extraJavaOptions="-XX:HeapDumpPath=/tmp"}</li>
     * </ul>
     */
    private static final String VALUE_HAS_QUOTES_AT_ENDS_REGEX = "([a-zA-Z0-9.]+=)?\".+\"";

    /**
     * Matches an option key / value pair, where the value part has quotes in between.
     * <p/>
     * Some examples:
     * <ul>
     *     <li>{@code spark.executor.extraJavaOptions=-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp
     *     -Dlog4j.configuration="/some path/spark-log4j.properties"}</li>
     *     <li>{@code spark.executor.extraJavaOptions=-XX:+HeapDumpOnOutOfMemoryError
     *     -Dlog4j.configuration="/some path/spark-log4j.properties" -XX:HeapDumpPath=/tmp}</li>
     *     <li>{@code spark.executor.extraJavaOptions=-Dlog4j.configuration="/some path/spark-log4j.properties"
     *     -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp}</li>
     * </ul>
     */
    private static final String VALUE_HAS_QUOTES_IN_BETWEEN_REGEX = "([a-zA-Z0-9.]+=)?\".*\".*\"";

    /**
     * Converts the options to be Spark-compatible.
     * <p/>
     * Following will be considered when converting a single option:
     *  <ul>
     *      <li>parameter keys should begin with {@link #OPTION_KEY_PREFIX}</li>
     *      <li>parameter values should be separated by whitespace(s) and can be grouped using double quotes</li>
     *      <li>double quotes should be removed when the entire value part of a parameter key / value pair is double quoted,
     *      and no partial value is quoted. {@see #VALUE_HAS_QUOTES_AT_ENDS_REGEX}
     *      and {@see #VALUE_HAS_QUOTES_IN_BETWEEN_REGEX}</li>
     *      <li>double quotes should be kept when only a value part of a parameter key / value pair is double quoted</li>
     *      <li>adjacent whitespace separators are treated as one</li>
     *  </ul>
     * <p/>
     * Please have a look at the scenarios within {@code TestSparkOptionsSplitter} for details.
     *
     * @param sparkOpts the options for Spark
     * @return the options parsed into a {@link List}
     */
    static List<String> splitSparkOpts(final String sparkOpts) {
        final List<String> result = new ArrayList<>();
        final Matcher matcher = OPTION_KEY_PREFIX.matcher(sparkOpts);

        int start = 0, end;
        while (matcher.find()) {
            end = matcher.start();

            if (start > 0) {
                final String maybeQuotedValue = sparkOpts.substring(start, end).trim();
                if (StringUtils.isNotEmpty(maybeQuotedValue)) {
                    result.add(unquoteEntirelyQuotedValue(maybeQuotedValue));
                }
            }

            String sparkOpt = matchSparkOpt(sparkOpts, matcher);
            if (sparkOpt.endsWith("=")) {
                sparkOpt = sparkOpt.replaceAll("=", "");
            }
            result.add(sparkOpt);

            start = matcher.end();
        }

        final String maybeEntirelyQuotedValue = sparkOpts.substring(start).trim();
        if (StringUtils.isNotEmpty(maybeEntirelyQuotedValue)) {
            result.add(unquoteEntirelyQuotedValue(maybeEntirelyQuotedValue));
        }

        return result;
    }

    private static String matchSparkOpt(final String sparkOpts, final Matcher matcher) {
        return sparkOpts.substring(matcher.start(), matcher.end()).trim();
    }

    /**
     * Unquote an option value if and only if it has quotes at both ends, and doesn't have any quotes in between.
     * <p/>
     * Some examples:
     * <ul>
     *     <li>{@code key=value1 value2}: remains unquoted (isn't quoted at all)</li>
     *     <li>{@code key=value1 "value2"}: remains quoted (has quotes in between)</li>
     *     <li>{@code key="value1 value2" "value3 value4"}: remains quoted (has quotes both ends, but has also some in between)</li>
     *     <li>{@code key="value1 value2 value3 value4"}: gets unquoted (has quotes both ends, and no quotes in between)</li>
     * </ul>
     * @param maybeEntirelyQuotedValue a {@code String} that is a parameter value but not necessarily quoted
     * @return an unquoted version of the input {@code String}, when {@code maybeEntirelyQuotedValue} had quotes at both ends,
     * and didn't have any quotes in between. Else {@code maybeEntirelyQuotedValue}
     */
    @SuppressFBWarnings(value = {"REDOS"}, justification = "Complex regular expression")
    private static String unquoteEntirelyQuotedValue(final String maybeEntirelyQuotedValue) {
        final boolean hasQuotesAtEnds = maybeEntirelyQuotedValue.matches(VALUE_HAS_QUOTES_AT_ENDS_REGEX);
        final boolean hasQuotesInBetween = maybeEntirelyQuotedValue.matches(VALUE_HAS_QUOTES_IN_BETWEEN_REGEX);
        final boolean isEntirelyQuoted = hasQuotesAtEnds && !hasQuotesInBetween;

        if (isEntirelyQuoted) {
            return maybeEntirelyQuotedValue.replaceAll("\"", "");
        }

        return maybeEntirelyQuotedValue;
    }
}
