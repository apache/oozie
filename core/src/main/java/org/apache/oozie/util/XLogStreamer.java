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
package org.apache.oozie.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.Date;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XLogReader;

/**
 * XLogStreamer streams the given log file to logWriter after applying the given
 * filter.
 */
public class XLogStreamer {

    /**
     * Filter that will construct the regular expression that will be used to
     * filter the log statement. And also checks if the given log message go
     * through the filter. Filters that can be used are logLevel(Multi values
     * separated by "|") jobId appName actionId token
     */
    public static class Filter {
        private Map<String, Integer> logLevels;
        private Map<String, String> filterParams;
        private static List<String> parameters = new ArrayList<String>();
        private boolean noFilter;
        private Pattern filterPattern;

        //TODO Patterns to be read from config file
        private static final String DEFAULT_REGEX = "[^\\]]*";

        public static final String ALLOW_ALL_REGEX = "(.*)";
        private static final String TIMESTAMP_REGEX = "(\\d\\d\\d\\d-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d,\\d\\d\\d)";
        private static final String WHITE_SPACE_REGEX = "\\s+";
        private static final String LOG_LEVEL_REGEX = "(\\w+)";
        private static final String PREFIX_REGEX = TIMESTAMP_REGEX + WHITE_SPACE_REGEX + LOG_LEVEL_REGEX
                + WHITE_SPACE_REGEX;
        private static final Pattern SPLITTER_PATTERN = Pattern.compile(PREFIX_REGEX + ALLOW_ALL_REGEX);

        public Filter() {
            filterParams = new HashMap<String, String>();
            for (int i = 0; i < parameters.size(); i++) {
                filterParams.put(parameters.get(i), DEFAULT_REGEX);
            }
            logLevels = null;
            noFilter = true;
            filterPattern = null;
        }

        public void setLogLevel(String logLevel) {
            if (logLevel != null && logLevel.trim().length() > 0) {
                this.logLevels = new HashMap<String, Integer>();
                String[] levels = logLevel.split("\\|");
                for (int i = 0; i < levels.length; i++) {
                    String s = levels[i].trim().toUpperCase();
                    try {
                        XLog.Level.valueOf(s);
                    }
                    catch (Exception ex) {
                        continue;
                    }
                    this.logLevels.put(levels[i].toUpperCase(), 1);
                }
            }
        }

        public void setParameter(String filterParam, String value) {
            if (filterParams.containsKey(filterParam)) {
                noFilter = false;
                filterParams.put(filterParam, value);
            }
        }

        public static void defineParameter(String filterParam) {
            parameters.add(filterParam);
        }

        public boolean isFilterPresent() {
            if (noFilter && logLevels == null) {
                return false;
            }
            return true;
        }

        /**
         * Checks if the logLevel and logMessage goes through the logFilter.
         * 
         * @param logParts
         * @return
         */
        public boolean matches(ArrayList<String> logParts) {
            String logLevel = logParts.get(0);
            String logMessage = logParts.get(1);
            if (this.logLevels == null || this.logLevels.containsKey(logLevel.toUpperCase())) {
                Matcher logMatcher = filterPattern.matcher(logMessage);
                return logMatcher.matches();
            }
            else {
                return false;
            }
        }

        /**
         * Splits the log line into timestamp, logLevel and remaining log
         * message. Returns array containing logLevel and logMessage if the
         * pattern matches i.e A new log statement, else returns null.
         * 
         * @param logLine
         * @return Array containing log level and log message
         */
        public ArrayList<String> splitLogMessage(String logLine) {
            Matcher splitter = SPLITTER_PATTERN.matcher(logLine);
            if (splitter.matches()) {
                ArrayList<String> logParts = new ArrayList<String>();
                logParts.add(splitter.group(2));// log level
                logParts.add(splitter.group(3));// Log Message
                return logParts;
            }
            else {
                return null;
            }
        }

        /**
         * Constructs the regular expression according to the filter and assigns
         * it to fileterPattarn. ".*" will be assigned if no filters are set.
         */
        public void constructPattern() {
            if (noFilter && logLevels == null) {
                filterPattern = Pattern.compile(ALLOW_ALL_REGEX);
                return;
            }
            StringBuilder sb = new StringBuilder();
            if (noFilter) {
                sb.append("(.*)");
            }
            else {
                sb.append("(.* - ");
                for (int i = 0; i < parameters.size(); i++) {
                    sb.append(parameters.get(i) + "\\[");
                    sb.append(filterParams.get(parameters.get(i)) + "\\] ");
                }
                sb.append(".*)");
            }
            filterPattern = Pattern.compile(sb.toString());
        }

        public static void reset() {
            parameters.clear();
        }
    }

    private String logFile;
    private String logPath;
    private Filter logFilter;
    private Writer logWriter;
    private long logRotation;

    public XLogStreamer(Filter logFilter, Writer logWriter, String logPath, String logFile, long logRotationSecs) {
        this.logWriter = logWriter;
        this.logFilter = logFilter;
        if (logFile == null) {
            logFile = "oozie-app.log";
        }
        this.logFile = logFile;
        this.logPath = logPath;
        this.logRotation = logRotationSecs * 1000l;
    }

    /**
     * Gets the files that are modified between startTime and endTime in the
     * given logPath and streams the log after applying the filters.
     * 
     * @param startTime
     * @param endTime
     * @throws IOException
     */
    public void streamLog(Date startTime, Date endTime) throws IOException {
        long startTimeMillis = 0;
        long endTimeMillis;
        if (startTime != null) {
            startTimeMillis = startTime.getTime();
        }
        if (endTime == null) {
            endTimeMillis = System.currentTimeMillis();
        }
        else {
            endTimeMillis = endTime.getTime();
        }
        File dir = new File(logPath);
        ArrayList<FileInfo> fileList = getFileList(dir, startTimeMillis, endTimeMillis, logRotation, logFile);
        for (int i = 0; i < fileList.size(); i++) {
            InputStream ifs;
            ifs = new FileInputStream(fileList.get(i).getFileName());
            XLogReader logReader = new XLogReader(ifs, logFilter, logWriter);
            logReader.processLog();
        }
    }

    /**
     * File name along with the modified time which will be used to sort later.
     */
    class FileInfo implements Comparable<FileInfo> {
        String fileName;
        long modTime;

        public FileInfo(String fileName, long modTime) {
            this.fileName = fileName;
            this.modTime = modTime;
        }

        public String getFileName() {
            return fileName;
        }

        public long getModTime() {
            return modTime;
        }

        public int compareTo(FileInfo fileInfo) {
            return (int) (this.modTime - fileInfo.modTime);
        }
    }

    /**
     * Gets the file list that will have the logs between startTime and endTime.
     * 
     * @param dir
     * @param startTime
     * @param endTime
     * @param logRotationTime
     * @param logFile
     * @return List of files to be streamed
     */
    private ArrayList<FileInfo> getFileList(File dir, long startTime, long endTime, long logRotationTime,
                                            String logFile) {
        String[] children = dir.list();
        ArrayList<FileInfo> fileList = new ArrayList<FileInfo>();
        if (children == null) {
            return fileList;
        }
        else {
            for (int i = 0; i < children.length; i++) {
                String filename = children[i];
                if (!filename.startsWith(logFile) && !filename.equals(logFile)) {
                    continue;
                }
                File file = new File(dir.getAbsolutePath(), filename);
                long modTime = file.lastModified();
                if (modTime < startTime) {
                    continue;
                }
                if (modTime / logRotationTime > (endTime / logRotationTime + 1)) {
                    continue;
                }
                fileList.add(new FileInfo(file.getAbsolutePath(), modTime));
            }
        }
        Collections.sort(fileList);
        return fileList;
    }
}
