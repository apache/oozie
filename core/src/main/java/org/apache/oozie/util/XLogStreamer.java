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

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.BufferedReader;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.XLogService;

/**
 * XLogStreamer streams the given log file to writer after applying the given filter.
 */
public class XLogStreamer {
    private static XLog LOG = XLog.getLog(XLogStreamer.class);
    protected static final String CONF_PREFIX = Service.CONF_PREFIX + "XLogStreamingService.";
    public static final String STREAM_BUFFER_LEN = CONF_PREFIX + "buffer.len";

    private String logFile;
    private String logPath;
    protected XLogFilter logFilter;
    private long logRotation;
    Map<String, String[]> requestParam;
    protected int totalDataWritten;
    protected int bufferLen;

    public XLogStreamer(XLogFilter logFilter, String logPath, String logFile, long logRotationSecs) {
        if (logFile == null) {
            logFile = "oozie-app.log";
        }

        this.logFilter = logFilter;
        this.logFile = logFile;
        this.logPath = logPath;
        this.logRotation = logRotationSecs * 1000l;
        bufferLen = ConfigurationService.getInt(STREAM_BUFFER_LEN, 4096);
    }

    public XLogStreamer(XLogFilter logFilter) {
        this(logFilter, Services.get().get(XLogService.class).getOozieLogPath(), Services.get().get(XLogService.class)
                .getOozieLogName(), Services.get().get(XLogService.class).getOozieLogRotation());

    }

    public XLogStreamer(XLogFilter logFilter, Map<String, String[]> params) {
        this(logFilter, Services.get().get(XLogService.class).getOozieLogPath(), Services.get().get(XLogService.class)
                .getOozieLogName(), Services.get().get(XLogService.class).getOozieLogRotation());
        this.requestParam = params;

    }


    public XLogStreamer(Map<String, String[]> params) throws CommandException {
        this(new XLogFilter(new XLogUserFilterParam(params)));
        this.requestParam = params;
    }

    /**
     * Gets the files that are modified between startTime and endTime in the given logPath and streams the log after
     * applying the filters.
     *
     * @param writer the target writer
     * @param startTime the start time
     * @param endTime the end time
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public void streamLog(Writer writer, Date startTime, Date endTime) throws IOException {
        streamLog(writer, startTime, endTime, true);
    }

    /**
     * Gets the files that are modified between startTime and endTime in the given logPath and streams the log after
     * applying the filters
     *
     * @param writer the writer
     * @param startTime the start time
     * @param endTime the end time
     * @param appendDebug the append debug
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public void streamLog(Writer writer, Date startTime, Date endTime, boolean appendDebug) throws IOException {
        // Get a Reader for the log file(s)
        BufferedReader reader = new BufferedReader(getReader(startTime, endTime));
        try {
            if (appendDebug) {
                if (!StringUtils.isEmpty(logFilter.getTruncatedMessage())) {
                    writer.write(StringEscapeUtils.escapeHtml(logFilter.getTruncatedMessage()));
                }
                if (logFilter.isDebugMode()) {
                    writer.write(StringEscapeUtils.escapeHtml(logFilter.getDebugMessage()));
                }
            }
            // Process the entire logs from the reader using the logFilter
            new TimestampedMessageParser(reader, logFilter).processRemaining(writer, this);
        }
        finally {
            reader.close();
        }
    }

    /**
     * Returns a BufferedReader configured to read the log files based on the given startTime and endTime.
     *
     * @param startTime the start time
     * @param endTime the end time
     * @return A BufferedReader for the log files
     * @throws IOException Signals that an I/O exception has occurred.
     */

    private MultiFileReader getReader(Date startTime, Date endTime) throws IOException {
        calculateAndValidateDateRange(startTime, endTime);
        return new MultiFileReader(getFileList(logFilter.getStartDate(), logFilter.getEndDate()));
    }

    protected void calculateAndValidateDateRange(Date startTime, Date endTime) throws IOException {
        logFilter.calculateAndCheckDates(startTime, endTime);
        logFilter.validateDateRange(startTime, endTime);
    }

    public BufferedReader makeReader(Date startTime, Date endTime) throws IOException {
        return new BufferedReader(getReader(startTime, endTime));
    }

    /**
     * Gets the log file list for specific date range.
     *
     * @param startTime the start time
     * @param endTime the end time
     * @return log file list
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private ArrayList<File> getFileList(Date startTime, Date endTime) throws IOException {
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
        return getFileList(dir, startTimeMillis, endTimeMillis, logRotation, logFile);
    }

    /**
     * File along with the modified time which will be used to sort later.
     */
    public class FileInfo implements Comparable<FileInfo> {
        File file;
        long modTime;

        public FileInfo(File file, long modTime) {
            this.file = file;
            this.modTime = modTime;
        }

        public File getFile() {
            return file;
        }

        public long getModTime() {
            return modTime;
        }

        public int compareTo(FileInfo fileInfo) {
            long diff = this.modTime - fileInfo.modTime;
            if (diff > 0) {
                return 1;
            }
            else if (diff < 0) {
                return -1;
            }
            else {
                return 0;
            }
        }
    }

    /**
     * Gets the file list that will have the logs between startTime and endTime.
     *
     * @param dir the directory to list
     * @param startTime the start time
     * @param endTime the end time
     * @param logRotationTime the log rotation time
     * @param logFile the file to look up
     * @return List of files to be streamed
     */
    private ArrayList<File> getFileList(File dir, long startTime, long endTime, long logRotationTime, String logFile) {
        String[] children = dir.list();
        ArrayList<FileInfo> fileList = new ArrayList<FileInfo>();
        if (children == null) {
            return new ArrayList<File>();
        }
        else {
            for (int i = 0; i < children.length; i++) {
                String fileName = children[i];
                if (!fileName.startsWith(logFile) && !fileName.equals(logFile)) {
                    continue;
                }
                File file = new File(dir.getAbsolutePath(), fileName);
                if (fileName.endsWith(".gz")) {
                    long gzFileCreationTime = getGZFileCreationTime(fileName, startTime, endTime);
                    if (gzFileCreationTime != -1) {
                        fileList.add(new FileInfo(file, gzFileCreationTime));
                    }
                    continue;
                }
                long modTime = file.lastModified();
                if (modTime < startTime) {
                    continue;
                }
                if (modTime / logRotationTime > (endTime / logRotationTime + 1)) {
                    continue;
                }
                fileList.add(new FileInfo(file, modTime));
            }
        }
        Collections.sort(fileList);
        ArrayList<File> files = new ArrayList<File>(fileList.size());
        for (FileInfo info : fileList) {
            files.add(info.getFile());
        }
        return files;
    }

    /**
     * This pattern matches the end of a gzip filename to have a format like "-YYYY-MM-dd-HH.gz" with capturing groups for each part
     * of the date
     */
    public static final Pattern gzTimePattern = Pattern.compile(".*-(\\d\\d\\d\\d)-(\\d\\d)-(\\d\\d)-(\\d\\d)\\.gz");

    /**
     * Returns the creation time of the .gz archive if it is relevant to the job
     *
     * @param fileName
     * @param startTime
     * @param endTime
     * @return Modification time of .gz file after checking if it is relevant to the job
     */
    private long getGZFileCreationTime(String fileName, long startTime, long endTime) {
        // Default return value of -1 to exclude the file
        long returnVal = -1;

        // Include oozie.log as oozie.log.gz if it is accidentally GZipped
        if (fileName.equals("oozie.log.gz")) {
            LOG.warn("oozie.log has been GZipped, which is unexpected");
            // Return a value other than -1 to include the file in list
            returnVal = 0;
        }
        else {
            Matcher m = gzTimePattern.matcher(fileName);
            if (m.matches() && m.groupCount() == 4) {
                int year = Integer.parseInt(m.group(1));
                int month = Integer.parseInt(m.group(2));
                int day = Integer.parseInt(m.group(3));
                int hour = Integer.parseInt(m.group(4));
                int minute = 0;
                Calendar calendarEntry = Calendar.getInstance();
                calendarEntry.set(year, month - 1, day, hour, minute); // give month-1(Say, 7 for August)
                long logFileStartTime = calendarEntry.getTimeInMillis();
                long milliSecondsPerHour = 3600000;
                long logFileEndTime = logFileStartTime + milliSecondsPerHour;
                /*  To check whether the log content is there in the initial or later part of the log file or
                    the log content is contained entirely within this log file or
                    the entire log file contains the event log where the event spans across hours
                */
                if ((startTime >= logFileStartTime && startTime <= logFileEndTime)
                        || (endTime >= logFileStartTime && endTime <= logFileEndTime)
                        || (startTime <= logFileStartTime && endTime >= logFileEndTime)) {
                    returnVal = logFileStartTime;
                }
            }
            else {
                LOG.debug("Filename " + fileName + " does not match the expected format");
                returnVal = -1;
            }
        }
        return returnVal;
    }

    public boolean isLogEnabled() {
        return Services.get().get(XLogService.class).getLogOverWS();
    }

    public String getLogType() {
        return RestConstants.JOB_SHOW_LOG;
    }

    public XLogFilter getXLogFilter() {
        return logFilter;
    }

    public String getLogDisableMessage() {
        return "Log streaming disabled!!";
    }

    public Map<String, String[]> getRequestParam() {
        return requestParam;
    }

    public boolean shouldFlushOutput(int writtenBytes) {
        this.totalDataWritten += writtenBytes;
        if (this.totalDataWritten > getBufferLen()) {
            this.totalDataWritten = 0;
            return true;
        }
        else {
            return false;
        }
    }

    public int getBufferLen() {
        return bufferLen;
    }
}
