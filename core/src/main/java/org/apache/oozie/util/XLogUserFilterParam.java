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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.command.CommandException;

public class XLogUserFilterParam {

    public static final String START_TIME = "START";

    public static final String END_TIME = "END";

    public static final String SEARCH_TEXT = "TEXT";

    public static final String LOG_LEVEL = "LOGLEVEL";

    public static final String LIMIT = "LIMIT";

    public static final String RECENT_LOG_OFFSET = "RECENT";

    public static final String DEBUG = "DEBUG";

    private Date startTime;
    private Date endTime;
    private int startOffset;
    private int endOffset = -1;
    private int recent = -1;
    private String logLevel;
    private int limit = -1;
    private boolean isDebug = false;
    private String searchText;

    private String params;

    public static final ThreadLocal<SimpleDateFormat> dt = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };

    static final HashSet<String> LOG_LEVELS = new HashSet<String>();
    static {
        LOG_LEVELS.add("ALL");
        LOG_LEVELS.add("DEBUG");
        LOG_LEVELS.add("ERROR");
        LOG_LEVELS.add("INFO");
        LOG_LEVELS.add("TRACE");
        LOG_LEVELS.add("WARN");
        LOG_LEVELS.add("FATAL");
    }

    public XLogUserFilterParam() {
    }

    /**
     * Instantiates a new log user param.
     *
     * @param params the params
     * @throws CommandException the command exception
     */
    public XLogUserFilterParam(Map<String, String[]> params) throws CommandException {
        if (params != null && params.get(RestConstants.LOG_FILTER_OPTION) != null
                && params.get(RestConstants.LOG_FILTER_OPTION).length > 0) {
            try {
                parseFilterParam(params.get(RestConstants.LOG_FILTER_OPTION)[0]);
            }
            catch (Exception e) {
               throw new CommandException(ErrorCode.E0302, e.getMessage());

            }
        }
    }

    /**
     * Parse filter param
     *
     * @param param the param
     * @param map the map
     * @throws Exception
     */
    private void parseFilterParam(String param) throws Exception {
        this.params = param;

        if (StringUtils.isEmpty(param) || StringUtils.equalsIgnoreCase(param, "null")) {
            return;
        }
        for (String keyValue : param.split(";")) {
            String[] pairs = keyValue.split("=");
            String key = pairs[0].toUpperCase();
            String value = pairs.length == 1 ? "" : pairs[1];
            if (key.equals(START_TIME)) {
                startTime = getDate(value);
                if (startTime == null) {
                    startOffset = getOffsetInMinute(value);
                }
            }
            else if (key.equals(END_TIME)) {
                endTime = getDate(value);
                if (endTime == null) {
                    endOffset = getOffsetInMinute(value);
                }

            }
            else if (key.equals(RECENT_LOG_OFFSET)) {
                recent = getOffsetInMinute(value);
            }
            else if (key.equals(LIMIT)) {
                limit = Integer.parseInt(value);

            }
            else if (key.equals(LOG_LEVEL)) {
                logLevel = value;
                validateLogLevel(logLevel);

            }
            else if (key.equals(DEBUG)) {
                isDebug = true;

            }
            else if (key.equals(SEARCH_TEXT)) {
                searchText = value;

            }
            else {
                throw new Exception("Unsupported log filter " + key);
            }
        }
    }

    /**
     * Gets the log level.
     *
     * @return the log level
     */
    public String getLogLevel() {
        return logLevel;

    }

    /**
     * Gets the start date.
     *
     * @return the start date
     */
    public Date getStartDate() {
        return startTime;
    }

    /**
     * Gets the end date.
     *
     * @return the end date
     */
    public Date getEndDate() {
        return endTime;
    }

    /**
     * Gets the search text.
     *
     * @return the search text
     */
    public String getSearchText() {
        return searchText;
    }

    /**
     * Validate log level.
     *
     * @throws CommandException
     */
    public void validateLogLevel(String loglevel) throws CommandException {
        if (StringUtils.isEmpty(loglevel)) {
            return;
        }
        for (String level : getLogLevel().split("\\|")) {
            if (!LOG_LEVELS.contains(level)) {
                throw new CommandException(ErrorCode.E0302, "Supported log level are " + LOG_LEVELS.toString());
            }
        }
    }

    /**
     * Validate search text.
     *
     * @throws CommandException the command exception
     */
    public void validateSearchText() throws CommandException {
        // No restriction on search text.

    }

    /**
     * Gets the date. Date can in TZ or yyyy-MM-dd HH:mm:ss,SSS format
     *
     * @param String date
     * @return the date
     */
    public Date getDate(String date) {
        try {
            return DateUtils.parseDateOozieTZ(date);
        }
        catch (ParseException e) {
            try {
                return dt.get().parse(date);
            }
            catch (ParseException e1) {
                return null;
            }
        }
    }

    /**
     * Checks if is debug.
     *
     * @return true, if it's debug
     */
    public boolean isDebug() {
        return isDebug;
    }

    public Date getEndTime() {
        return endTime;
    }

    public int getEndOffset() {
        return endOffset;
    }

    public int getRecent() {
        return recent;
    }

    public int getLimit() {
        return limit;
    }

    public int getStartOffset() {
        return startOffset;
    }

    @Override
    public String toString() {
        return params;
    }

    private int getOffsetInMinute(String offset) throws IOException {

        if (Character.isLetter(offset.charAt(offset.length() - 1))) {
            switch (offset.charAt(offset.length() - 1)) {
                case 'h':
                    return Integer.parseInt(offset.substring(0, offset.length() - 1)) * 60;
                case 'm':
                    return Integer.parseInt(offset.substring(0, offset.length() - 1));
                default:
                    throw new IOException("Unsupported offset " + offset);
            }
        }
        else {
            if (StringUtils.isNumeric(offset)) {
                return Integer.parseInt(offset) * 60;
            }
            else {
                throw new IOException("Unsupported time : " + offset);
            }
        }
    }

}
