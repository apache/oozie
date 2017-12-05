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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.util.LogLine.MATCHED_PATTERN;

import com.google.common.annotations.VisibleForTesting;

/**
 * Filter that will construct the regular expression that will be used to filter the log statement. And also checks if
 * the given log message go through the filter. Filters that can be used are logLevel(Multi values separated by "|")
 * jobId appName actionId token
 */
public class XLogFilter {

    private static final int LOG_TIME_BUFFER = 2; // in min
    public static String MAX_ACTIONLIST_SCAN_DURATION = "oozie.service.XLogStreamingService.actionlist.max.log.scan.duration";
    public static String MAX_SCAN_DURATION = "oozie.service.XLogStreamingService.max.log.scan.duration";
    private Map<String, Integer> logLevels;
    private final Map<String, String> filterParams;
    private static List<String> parameters = new ArrayList<String>();
    private boolean noFilter;
    private Pattern filterPattern;
    private XLogUserFilterParam userLogFilter;
    private Date endDate;
    private Date startDate;
    private boolean isActionList = false;
    private String formattedEndDate;
    private String formattedStartDate;
    private String truncatedMessage;

    // TODO Patterns to be read from config file
    private static final String DEFAULT_REGEX = "[^\\]]*";

    public static final String ALLOW_ALL_REGEX = "(.*)";
    private static final String TIMESTAMP_REGEX = "(\\d\\d\\d\\d-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d,\\d\\d\\d)";
    private static final String WHITE_SPACE_REGEX = "\\s+";
    private static final String LOG_LEVEL_REGEX = "(\\w+)";
    static final String PREFIX_REGEX = TIMESTAMP_REGEX + WHITE_SPACE_REGEX + LOG_LEVEL_REGEX
            + WHITE_SPACE_REGEX;
    private static final Pattern SPLITTER_PATTERN = Pattern.compile(PREFIX_REGEX + ALLOW_ALL_REGEX);

    public XLogFilter() {
        this(new XLogUserFilterParam());
    }

    public XLogFilter(XLogUserFilterParam userLogFilter) {
        filterParams = new HashMap<String, String>();
        for (int i = 0; i < parameters.size(); i++) {
            filterParams.put(parameters.get(i), DEFAULT_REGEX);
        }
        logLevels = null;
        noFilter = true;
        filterPattern = null;
        setUserLogFilter(userLogFilter);
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
     * @param logLine the log line
     * @return true if line contains the permitted logLevel
     */
    public boolean splitsMatches(LogLine logLine) {
        // Check whether logLine matched with filter
        if (logLine.getMatchedPattern() != MATCHED_PATTERN.SPLIT) {
            return false;
        }
        ArrayList<String> logParts = logLine.getLogParts();
        if (getStartDate() != null) {
            if (logParts.get(0).substring(0, 19).compareTo(getFormattedStartDate()) < 0) {
                return false;
            }
        }
        String logLevel = logParts.get(1);
        if (this.logLevels == null || this.logLevels.containsKey(logLevel.toUpperCase(Locale.ENGLISH))) {
            // line contains the permitted logLevel
            return true;
        }
        else {
            return false;
        }
    }

    /**
     * Checks if the logLevel and logMessage goes through the logFilter.
     *
     * @param logParts the arrayList of log parts
     * @return true if the logLevel and logMessage goes through the logFilter
     */
    public boolean matches(ArrayList<String> logParts) {
        if (getStartDate() != null) {
            if (logParts.get(0).substring(0, 19).compareTo(getFormattedStartDate()) < 0) {
                return false;
            }
        }
        String logLevel = logParts.get(1);
        String logMessage = logParts.get(2);
        if (this.logLevels == null || this.logLevels.containsKey(logLevel.toUpperCase())) {
            Matcher logMatcher = filterPattern.matcher(logMessage);
            return logMatcher.matches();
        }
        else {
            return false;
        }
    }

    /**
     * Splits the log line into timestamp, logLevel and remaining log message.
     * Returns array containing timestamp, logLevel, and logMessage if the
     * pattern matches i.e A new log statement, else returns null.
     *
     * @param logLine the line
     * @return Array containing log level and log message
     */
    public ArrayList<String> splitLogMessage(String logLine) {
        Matcher splitter = SPLITTER_PATTERN.matcher(logLine);
        if (splitter.matches()) {
            ArrayList<String> logParts = new ArrayList<String>();
            logParts.add(splitter.group(1));// timestamp
            logParts.add(splitter.group(2));// log level
            logParts.add(splitter.group(3));// Log Message
            return logParts;
        }
        else {
            return null;
        }
    }

    /**
     * If <code>logLine</code> matches with <code>splitPattern</code>,
     * <ol>
     * <li>Split the log line into timestamp, logLevel and remaining log
     * message.</li>
     * <li>Record the parts of message in <code>logLine</code> to avoid regex
     * matching in future.</li>
     * <li>Record the pattern to which <code>logLine</code> has matched.</li>
     * </ol>
     * @param logLine the line to split
     * @param splitPattern the pattern to use
     */
    public void splitLogMessage(LogLine logLine, Pattern splitPattern) {
        Matcher splitterWithJobId = splitPattern.matcher(logLine.getLine());
        Matcher allowAll = SPLITTER_PATTERN.matcher(logLine.getLine());
        if (splitterWithJobId.matches()) {
            ArrayList<String> logParts = new ArrayList<String>(3);
            logParts.add(splitterWithJobId.group(1));// timestamp
            logParts.add(splitterWithJobId.group(2));// log level
            logParts.add(splitterWithJobId.group(3));// log message
            logLine.setLogParts(logParts);
            logLine.setMatchedPattern(MATCHED_PATTERN.SPLIT);
        }
        else if (allowAll.matches()) {
            logLine.setMatchedPattern(MATCHED_PATTERN.GENENRIC);
        }
        else {
            logLine.setMatchedPattern(MATCHED_PATTERN.NONE);
        }
    }

    /**
     * Constructs the regular expression according to the filter and assigns it
     * to fileterPattarn. ".*" will be assigned if no filters are set.
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
            sb.append("(.* ");
            for (int i = 0; i < parameters.size(); i++) {
                sb.append(parameters.get(i) + "\\[");
                sb.append(filterParams.get(parameters.get(i)) + "\\] ");
            }
            sb.append(".*)");
        }
        if (!StringUtils.isEmpty(userLogFilter.getSearchText())) {
            sb.append(userLogFilter.getSearchText() + ".*");
        }
        filterPattern = Pattern.compile(sb.toString());
    }

    public static void reset() {
        parameters.clear();
    }

    public final Map<String, String> getFilterParams() {
        return filterParams;
    }

    public XLogUserFilterParam getUserLogFilter() {
        return userLogFilter;
    }

    public void setUserLogFilter(XLogUserFilterParam userLogFilter) {
        this.userLogFilter = userLogFilter;
        setLogLevel(userLogFilter.getLogLevel());
    }

    public Date getEndDate() {
        return endDate;
    }

    public String getFormattedEndDate() {
        return formattedEndDate;
    }

    public String getFormattedStartDate() {
        return formattedStartDate;
    }

    public Date getStartDate() {
        return startDate;
    }

    public boolean isDebugMode() {
        return userLogFilter.isDebug();
    }

    public int getLogLimit() {
        return userLogFilter.getLimit();
    }

    public String getDebugMessage() {
        return new StringBuilder("Log start time = ").append(getStartDate()).append(". Log end time = ")
                .append(getEndDate()).append(". User Log Filter = ").append(getUserLogFilter())
                .append(System.getProperty("line.separator")).toString();
    }

    public boolean isActionList() {
        return isActionList;
    }

    public void setActionList(boolean isActionList) {
        this.isActionList = isActionList;
    }

    /**
     * Calculate scan date
     *
     * @param jobStartTime the job start time
     * @param jobEndTime the job end time
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public void calculateAndCheckDates(Date jobStartTime, Date jobEndTime) throws IOException {

        // for testcase, otherwise jobStartTime and jobEndTime will be always
        // set
        if (jobStartTime == null || jobEndTime == null) {
            return;
        }

        if (userLogFilter.getStartDate() != null) {
            startDate = userLogFilter.getStartDate();
        }
        else if (userLogFilter.getStartOffset() != -1) {
            startDate = adjustOffset(jobStartTime, userLogFilter.getStartOffset());
        }
        else {
            startDate = new Date(jobStartTime.getTime());
        }

        if (userLogFilter.getEndDate() != null) {
            endDate = userLogFilter.getEndDate();
        }
        else if (userLogFilter.getEndOffset() != -1) {
            // If user has specified startdate as absolute then end offset will
            // be on user start date,
            // else end offset will be calculated on job startdate.
            if (userLogFilter.getStartDate() != null) {
                endDate = adjustOffset(startDate, userLogFilter.getEndOffset());
            }
            else {
                endDate = adjustOffset(jobStartTime, userLogFilter.getEndOffset());
            }
        }
        else {
            endDate = new Date(jobEndTime.getTime());
        }
        // if recent offset is specified then start time = endtime - offset
        if (getUserLogFilter().getRecent() != -1) {
            startDate = adjustOffset(endDate, userLogFilter.getRecent() * -1);
        }

        // add buffer if dates are not absolute
        if (userLogFilter.getStartDate() == null) {
            startDate = adjustOffset(startDate, -LOG_TIME_BUFFER);
        }
        if (userLogFilter.getEndDate() == null) {
            endDate = adjustOffset(endDate, LOG_TIME_BUFFER);
        }

        formattedEndDate = XLogUserFilterParam.dt.get().format(getEndDate());
        formattedStartDate = XLogUserFilterParam.dt.get().format(getStartDate());

        if (startDate.after(endDate)) {
            throw new IOException(
                    "Start time should be less than end time. startTime = " + startDate + " endTime = " + endDate);
        }
    }

    /**
     * validate date range.
     *
     * @param jobStartTime the job start time
     * @param jobEndTime the job end time
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public void validateDateRange(Date jobStartTime, Date jobEndTime) throws IOException {
        // for testcase, otherwise jobStartTime and jobEndTime will be always
        // set
        if (jobStartTime == null || jobEndTime == null) {
            return;
        }

        long diffHours = (endDate.getTime() - startDate.getTime()) / (60 * 60 * 1000);
        if (isActionList) {
            int actionLogDuration = ConfigurationService.getInt(MAX_ACTIONLIST_SCAN_DURATION);
            if (actionLogDuration == -1) {
                return;
            }
            if (diffHours > actionLogDuration) {
                setTruncatedMessage("Truncated logs to max log scan duration " + actionLogDuration + " hrs");
                startDate = adjustOffset(endDate, -1 * actionLogDuration * 60);
                startDate = adjustOffset(startDate, -1 * LOG_TIME_BUFFER);
            }
        }
        else {
            int logDuration = ConfigurationService.getInt(MAX_SCAN_DURATION);
            if (logDuration == -1) {
                return;
            }
            if (diffHours > logDuration) {
                setTruncatedMessage("Truncated logs to max log scan duration " + logDuration + " hrs");
                startDate = adjustOffset(endDate, -1 * logDuration * 60);
                startDate = adjustOffset(startDate, -1 * LOG_TIME_BUFFER);
            }
        }
    }

    protected void setTruncatedMessage(String message) {
        truncatedMessage = message;

    }

    public String getTruncatedMessage() {
        if (StringUtils.isEmpty(truncatedMessage)) {
            return truncatedMessage;
        }
        else {
            return truncatedMessage + System.getProperty("line.separator");
        }
    }

    /**
     * Adjust offset, offset will always be in min.
     *
     * @param date the date
     * @param offset the offset
     * @return the date
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public Date adjustOffset(Date date, int offset) throws IOException {
        return org.apache.commons.lang.time.DateUtils.addMinutes(date, offset);
    }

    public void setFilterPattern(Pattern filterPattern) {
        this.filterPattern = filterPattern;
    }

    public Pattern getFilterPattern() {
        return this.filterPattern;
    }

}
