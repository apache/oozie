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

public class ShellSplitter {
    private static final char BACKSPACE = '\\';
    private static final char SINGLE_QUOTE = '\'';
    private static final char DOUBLE_QUOTE = '"';
    private static final char SPACE = ' ';
    private boolean escaping;
    private char quoteChar;
    private boolean quoting;
    private boolean addTokenEvenIfEmpty;
    private StringBuilder current;
    private List<String> tokens;

    List<String> split(final String commandLine) throws ShellSplitterException {
        if (commandLine == null) {
            return null;
        }
        ensureFields();
        for (int i = 0; i < commandLine.length(); i++) {
            char c = commandLine.charAt(i);
            processCharacter(c);
        }
        addLastToken();
        if (quoting || escaping) {
            final String errorMessage = String.format("Unable to parse command %s", commandLine);
            throw new ShellSplitterException(errorMessage);
        }
        return tokens;
    }

    private void ensureFields() {
        tokens = new ArrayList<>();
        escaping = false;
        quoteChar = SPACE;
        quoting = false;
        addTokenEvenIfEmpty = false;
        current = new StringBuilder();
    }

    private void processCharacter(char c) {
        if (escaping) {
            current.append(c);
            escaping = false;
        } else if (c == BACKSPACE && !(quoting && quoteChar == SINGLE_QUOTE)) {
            escaping = true;
        } else if (quoting && c == quoteChar) {
            quoting = false;
            addTokenEvenIfEmpty = true;
        } else if (!quoting && (c == SINGLE_QUOTE || c == DOUBLE_QUOTE)) {
            quoting = true;
            quoteChar = c;
        } else if (!quoting && Character.isWhitespace(c)) {
            if (current.length() > 0 || addTokenEvenIfEmpty ) {
                addNewToken();
            }
        } else {
            current.append(c);
        }
    }

    private void addLastToken() {
        if (current.length() > 0) {
            addNewToken();
        }
    }

    private void addNewToken() {
        tokens.add(current.toString());
        current = new StringBuilder();
        addTokenEvenIfEmpty = false;
    }
}
