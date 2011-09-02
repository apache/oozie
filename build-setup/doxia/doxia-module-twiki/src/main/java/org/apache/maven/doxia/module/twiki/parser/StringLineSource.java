/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.maven.doxia.module.twiki.parser;

import org.apache.maven.doxia.util.ByLineSource;

public class StringLineSource implements ByLineSource {


    private String source;
    private int nextLineBreakIndex;
    private int lastLineBreakIndex;
    private int lineNumber;


    public StringLineSource(String source) {
        this.source = source;
        lastLineBreakIndex = -1;
        nextLineBreakIndex = source.indexOf("\n");
        lineNumber = 1;
    }

    public String getNextLine() {
        String line = null;
        if(nextLineBreakIndex >= 0) {
            line = source.substring(lastLineBreakIndex + 1, nextLineBreakIndex);
            lastLineBreakIndex = nextLineBreakIndex;
            nextLineBreakIndex = source.indexOf("\n", nextLineBreakIndex + 1);
            lineNumber++;
        }
        return line;
    }

    public String getName() {
        return "";
    }

    public int getLineNumber() {
        return lineNumber;
    }

    public void ungetLine() {
        if(nextLineBreakIndex > 0) {
            nextLineBreakIndex = lastLineBreakIndex;
        }
        lastLineBreakIndex = source.substring(0, lastLineBreakIndex).lastIndexOf("\n");

    }

    public void unget(String s) throws IllegalStateException {
        if ( s == null )
        {
            throw new IllegalArgumentException( "argument can't be null" );
        }
        if ( s.length() != 0 )
        {
            ungetLine();
            int newNext = source.substring(0, nextLineBreakIndex).lastIndexOf(s);
            if(newNext > 0) {
                nextLineBreakIndex = newNext;
            }
        }
    }

    public void close() {

    }

}
