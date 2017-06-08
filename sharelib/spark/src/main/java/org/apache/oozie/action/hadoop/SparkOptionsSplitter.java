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

import java.util.ArrayList;
import java.util.List;

class SparkOptionsSplitter {

    /**
     * Converts the options to be Spark-compatible.
     * <ul>
     * <li>Parameters are separated by whitespace and can be groupped using double quotes</li>
     * <li>Quotes should be removed</li>
     * <li>Adjacent whitespace separators are treated as one</li>
     * </ul>
     *
     * @param sparkOpts the options for Spark
     * @return the options parsed into a list
     */
    static List<String> splitSparkOpts(final String sparkOpts) {
        final List<String> result = new ArrayList<String>();
        final StringBuilder currentWord = new StringBuilder();

        boolean insideQuote = false;
        for (int i = 0; i < sparkOpts.length(); i++) {
            final char c = sparkOpts.charAt(i);
            if (c == '"') {
                insideQuote = !insideQuote;
            }
            else if (Character.isWhitespace(c) && !insideQuote) {
                if (currentWord.length() > 0) {
                    result.add(currentWord.toString());
                    currentWord.setLength(0);
                }
            }
            else {
                currentWord.append(c);
            }
        }

        if (currentWord.length() > 0) {
            result.add(currentWord.toString());
        }

        return result;
    }
}
