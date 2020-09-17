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

import java.util.ArrayList;

/**
 * A wrapper over log line to save redundant use of regex
 */
public class LogLine {
    /**
     * List to hold parts of log message.
     */
    private ArrayList<String> logParts;
    private String line;
    /**
     * Boolean to indicate if this log line has matched to the given log pattern
     */
    private MATCHED_PATTERN pattern = MATCHED_PATTERN.NONE;

    public ArrayList<String> getLogParts() {
        return logParts;
    }

    public void setLogParts(ArrayList<String> logParts) {
        this.logParts = logParts;
    }

    public String getLine() {
        return line;
    }

    public void setLine(String line) {
        this.line = line;
    }

    public MATCHED_PATTERN getMatchedPattern() {
        return pattern;
    }

    public void setMatchedPattern(MATCHED_PATTERN pattern) {
        this.pattern = pattern;
    }

    enum MATCHED_PATTERN {
        SPLIT, GENENRIC, NONE;
    }
}
