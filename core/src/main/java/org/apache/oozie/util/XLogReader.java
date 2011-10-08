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

import org.apache.oozie.util.XLogStreamer;

import java.util.ArrayList;
import java.io.Writer;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.InputStream;

/**
 * Reads the input stream(log file) and applies the filters and writes it to output stream. The filtering will also
 * consider the log messages spilling over multiline.
 */
public class XLogReader {
    private BufferedReader logReader;
    private Writer logWriter;
    private boolean noFilter = false;
    private XLogStreamer.Filter logFilter;

    public XLogReader(InputStream logFileIS, XLogStreamer.Filter filter, Writer logWriter) {
        logReader = new BufferedReader(new InputStreamReader(logFileIS));
        logFilter = filter;
        this.logWriter = logWriter;
    }

    /**
     * Processes the Given Log and writes the output after applying the filters.
     *
     * @throws IOException
     */
    public void processLog() throws IOException {
        String line = logReader.readLine();
        boolean patternMatched = false;
        int lcnt = 0;
        if (logFilter == null || !logFilter.isFilterPresent()) {
            noFilter = true;
        }
        else {
            logFilter.constructPattern();
        }
        while (line != null) {
            if (noFilter) {
                logWriter.write(line + "\n");
            }
            else {
                ArrayList<String> logParts = logFilter.splitLogMessage(line);
                if (logParts != null) {
                    patternMatched = logFilter.matches(logParts);
                }
                if (patternMatched) {
                    logWriter.write(line + "\n");
                }
            }
            lcnt++;
            if (lcnt % 20 == 0) {
                logWriter.flush();
            }
            line = logReader.readLine();
        }
        logWriter.flush();
    }

    public void close() throws IOException {
        logReader.close();
    }
}
