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

package org.apache.oozie.coord;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.dependency.URIHandler;
import org.apache.oozie.dependency.URIHandler.Context;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.URIHandlerService;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;

import java.net.URI;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

/**
 * This class implements the EL function related to coordinator
 */

public class CoordELFunctions {
    final public static String DATASET = "oozie.coord.el.dataset.bean";
    final public static String COORD_ACTION = "oozie.coord.el.app.bean";
    final public static String CONFIGURATION = "oozie.coord.el.conf";
    final public static String LATEST_EL_USE_CURRENT_TIME = "oozie.service.ELService.latest-el.use-current-time";
    // INSTANCE_SEPARATOR is used to separate multiple directories into one tag.
    final public static String INSTANCE_SEPARATOR = "#";
    final public static String DIR_SEPARATOR = ",";
    // TODO: in next release, support flexibility
    private static String END_OF_OPERATION_INDICATOR_FILE = "_SUCCESS";

    public static final long MINUTE_MSEC = 60 * 1000L;
    public static final long HOUR_MSEC = 60 * MINUTE_MSEC;
    public static final long DAY_MSEC = 24 * HOUR_MSEC;
    public static final long MONTH_MSEC = 30 * DAY_MSEC;
    public static final long YEAR_MSEC = 365 * DAY_MSEC;

    /**
     * Used in defining the frequency in 'day' unit. <p/> domain: <code> val &gt; 0</code> and should be integer.
     *
     * @param val frequency in number of days.
     * @return number of days and also set the frequency timeunit to "day"
     */
    public static int ph1_coord_days(int val) {
        val = ParamChecker.checkGTZero(val, "n");
        ELEvaluator eval = ELEvaluator.getCurrent();
        eval.setVariable("timeunit", TimeUnit.DAY);
        eval.setVariable("endOfDuration", TimeUnit.NONE);
        return val;
    }

    /**
     * Used in defining the frequency in 'month' unit. <p/> domain: <code> val &gt; 0</code> and should be integer.
     *
     * @param val frequency in number of months.
     * @return number of months and also set the frequency timeunit to "month"
     */
    public static int ph1_coord_months(int val) {
        val = ParamChecker.checkGTZero(val, "n");
        ELEvaluator eval = ELEvaluator.getCurrent();
        eval.setVariable("timeunit", TimeUnit.MONTH);
        eval.setVariable("endOfDuration", TimeUnit.NONE);
        return val;
    }

    /**
     * Used in defining the frequency in 'hour' unit. <p/> parameter value domain: <code> val &gt; 0</code> and should
     * be integer.
     *
     * @param val frequency in number of hours.
     * @return number of minutes and also set the frequency timeunit to "minute"
     */
    public static int ph1_coord_hours(int val) {
        val = ParamChecker.checkGTZero(val, "n");
        ELEvaluator eval = ELEvaluator.getCurrent();
        eval.setVariable("timeunit", TimeUnit.MINUTE);
        eval.setVariable("endOfDuration", TimeUnit.NONE);
        return val * 60;
    }

    /**
     * Used in defining the frequency in 'minute' unit. <p/> domain: <code> val &gt; 0</code> and should be integer.
     *
     * @param val frequency in number of minutes.
     * @return number of minutes and also set the frequency timeunit to "minute"
     */
    public static int ph1_coord_minutes(int val) {
        val = ParamChecker.checkGTZero(val, "n");
        ELEvaluator eval = ELEvaluator.getCurrent();
        eval.setVariable("timeunit", TimeUnit.MINUTE);
        eval.setVariable("endOfDuration", TimeUnit.NONE);
        return val;
    }

    /**
     * Used in defining the frequency in 'day' unit and specify the "end of day" property. <p/> Every instance will
     * start at 00:00 hour of each day. <p/> domain: <code> val &gt; 0</code> and should be integer.
     *
     * @param val frequency in number of days.
     * @return number of days and also set the frequency timeunit to "day" and end_of_duration flag to "day"
     */
    public static int ph1_coord_endOfDays(int val) {
        val = ParamChecker.checkGTZero(val, "n");
        ELEvaluator eval = ELEvaluator.getCurrent();
        eval.setVariable("timeunit", TimeUnit.DAY);
        eval.setVariable("endOfDuration", TimeUnit.END_OF_DAY);
        return val;
    }

    /**
     * Used in defining the frequency in 'month' unit and specify the "end of month" property. <p/> Every instance will
     * start at first day of each month at 00:00 hour. <p/> domain: <code> val &gt; 0</code> and should be integer.
     *
     * @param val: frequency in number of months.
     * @return number of months and also set the frequency timeunit to "month" and end_of_duration flag to "month"
     */
    public static int ph1_coord_endOfMonths(int val) {
        val = ParamChecker.checkGTZero(val, "n");
        ELEvaluator eval = ELEvaluator.getCurrent();
        eval.setVariable("timeunit", TimeUnit.MONTH);
        eval.setVariable("endOfDuration", TimeUnit.END_OF_MONTH);
        return val;
    }

    /**
     * Calculate the difference of timezone offset in minutes between dataset and coordinator job. <p/> Depends on: <p/>
     * 1. Timezone of both dataset and job <p/> 2. Action creation Time
     *
     * @return difference in minutes (DataSet TZ Offset - Application TZ offset)
     */
    public static int ph2_coord_tzOffset() {
        long actionCreationTime = getActionCreationtime().getTime();
        TimeZone dsTZ = ParamChecker.notNull(getDatasetTZ(), "DatasetTZ");
        TimeZone jobTZ = ParamChecker.notNull(getJobTZ(), "JobTZ");
        return (dsTZ.getOffset(actionCreationTime) - jobTZ.getOffset(actionCreationTime)) / (1000 * 60);
    }

    public static int ph3_coord_tzOffset() {
        return ph2_coord_tzOffset();
    }

    /**
     * Returns a date string that is offset from 'strBaseDate' by the amount specified.  The unit can be one of
     * DAY, MONTH, HOUR, MINUTE, MONTH.
     *
     * @param strBaseDate The base date
     * @param offset any number
     * @param unit one of DAY, MONTH, HOUR, MINUTE, MONTH
     * @return the offset date string
     * @throws Exception
     */
    public static String ph2_coord_dateOffset(String strBaseDate, int offset, String unit) throws Exception {
        Calendar baseCalDate = DateUtils.getCalendar(strBaseDate);
        StringBuilder buffer = new StringBuilder();
        baseCalDate.add(TimeUnit.valueOf(unit).getCalendarUnit(), offset);
        buffer.append(DateUtils.formatDateOozieTZ(baseCalDate));
        return buffer.toString();
    }

    public static String ph3_coord_dateOffset(String strBaseDate, int offset, String unit) throws Exception {
        return ph2_coord_dateOffset(strBaseDate, offset, unit);
    }

    /**
     * Returns a date string that is offset from 'strBaseDate' by the difference from Oozie processing timezone to the given
     * timezone. It will account for daylight saving time based on the given 'strBaseDate' and 'timezone'.
     *
     * @param strBaseDate The base date
     * @param timezone
     * @return the offset date string
     * @throws Exception
     */
    public static String ph2_coord_dateTzOffset(String strBaseDate, String timezone) throws Exception {
        Calendar baseCalDate = DateUtils.getCalendar(strBaseDate);
        StringBuilder buffer = new StringBuilder();
        baseCalDate.setTimeZone(DateUtils.getTimeZone(timezone));
        buffer.append(DateUtils.formatDate(baseCalDate));
        return buffer.toString();
    }

    public static String ph3_coord_dateTzOffset(String strBaseDate, String timezone) throws Exception{
        return ph2_coord_dateTzOffset(strBaseDate, timezone);
    }

    /**
     * Determine the date-time in Oozie processing timezone of n-th future available dataset instance
     * from nominal Time but not beyond the instance specified as 'instance.
     * <p/>
     * It depends on:
     * <p/>
     * 1. Data set frequency
     * <p/>
     * 2. Data set Time unit (day, month, minute)
     * <p/>
     * 3. Data set Time zone/DST
     * <p/>
     * 4. End Day/Month flag
     * <p/>
     * 5. Data set initial instance
     * <p/>
     * 6. Action Creation Time
     * <p/>
     * 7. Existence of dataset's directory
     *
     * @param n :instance count
     *        <p/>
     *        domain: n >= 0, n is integer
     * @param instance: How many future instance it should check? value should
     *        be >=0
     * @return date-time in Oozie processing timezone of the n-th instance
     *         <p/>
     * @throws Exception
     */
    public static String ph3_coord_future(int n, int instance) throws Exception {
        ParamChecker.checkGEZero(n, "future:n");
        ParamChecker.checkGTZero(instance, "future:instance");
        if (isSyncDataSet()) {// For Sync Dataset
            return coord_future_sync(n, instance);
        }
        else {
            throw new UnsupportedOperationException("Asynchronous Dataset is not supported yet");
        }
    }

    /**
     * Determine the date-time in Oozie processing timezone of the future available dataset instances
     * from start to end offsets from nominal Time but not beyond the instance specified as 'instance'.
     * <p/>
     * It depends on:
     * <p/>
     * 1. Data set frequency
     * <p/>
     * 2. Data set Time unit (day, month, minute)
     * <p/>
     * 3. Data set Time zone/DST
     * <p/>
     * 4. End Day/Month flag
     * <p/>
     * 5. Data set initial instance
     * <p/>
     * 6. Action Creation Time
     * <p/>
     * 7. Existence of dataset's directory
     *
     * @param start : start instance offset
     *        <p/>
     *        domain: start >= 0, start is integer
     * @param end : end instance offset
     *        <p/>
     *        domain: end >= 0, end is integer
     * @param instance: How many future instance it should check? value should
     *        be >=0
     * @return date-time in Oozie processing timezone of the instances from start to end offsets
     *        delimited by comma.
     *         <p/>
     * @throws Exception
     */
    public static String ph3_coord_futureRange(int start, int end, int instance) throws Exception {
        ParamChecker.checkGEZero(start, "future:n");
        ParamChecker.checkGEZero(end, "future:n");
        ParamChecker.checkGTZero(instance, "future:instance");
        if (isSyncDataSet()) {// For Sync Dataset
            return coord_futureRange_sync(start, end, instance);
        }
        else {
            throw new UnsupportedOperationException("Asynchronous Dataset is not supported yet");
        }
    }

    private static String coord_future_sync(int n, int instance) throws Exception {
        return coord_futureRange_sync(n, n, instance);
    }

    private static String coord_futureRange_sync(int startOffset, int endOffset, int instance) throws Exception {
        final XLog LOG = XLog.getLog(CoordELFunctions.class);
        final Thread currentThread = Thread.currentThread();
        ELEvaluator eval = ELEvaluator.getCurrent();
        String retVal = "";
        int datasetFrequency = (int) getDSFrequency();// in minutes
        TimeUnit dsTimeUnit = getDSTimeUnit();
        int[] instCount = new int[1];
        Calendar nominalInstanceCal = getCurrentInstance(getActionCreationtime(), instCount);
        StringBuilder resolvedInstances = new StringBuilder();
        StringBuilder resolvedURIPaths = new StringBuilder();
        if (nominalInstanceCal != null) {
            Calendar initInstance = getInitialInstanceCal();
            nominalInstanceCal = (Calendar) initInstance.clone();
            nominalInstanceCal.add(dsTimeUnit.getCalendarUnit(), instCount[0] * datasetFrequency);

            SyncCoordDataset ds = (SyncCoordDataset) eval.getVariable(DATASET);
            if (ds == null) {
                throw new RuntimeException("Associated Dataset should be defined with key " + DATASET);
            }
            String uriTemplate = ds.getUriTemplate();
            Configuration conf = (Configuration) eval.getVariable(CONFIGURATION);
            if (conf == null) {
                throw new RuntimeException("Associated Configuration should be defined with key " + CONFIGURATION);
            }
            int available = 0, checkedInstance = 0;
            boolean resolved = false;
            String user = ParamChecker
                    .notEmpty((String) eval.getVariable(OozieClient.USER_NAME), OozieClient.USER_NAME);
            String doneFlag = ds.getDoneFlag();
            URIHandlerService uriService = Services.get().get(URIHandlerService.class);
            URIHandler uriHandler = null;
            Context uriContext = null;
            try {
                while (instance >= checkedInstance && !currentThread.isInterrupted()) {
                    ELEvaluator uriEval = getUriEvaluator(nominalInstanceCal);
                    String uriPath = uriEval.evaluate(uriTemplate, String.class);
                    if (uriHandler == null) {
                        URI uri = new URI(uriPath);
                        uriHandler = uriService.getURIHandler(uri);
                        uriContext = uriHandler.getContext(uri, conf, user, true);
                    }
                    String uriWithDoneFlag = uriHandler.getURIWithDoneFlag(uriPath, doneFlag);
                    if (uriHandler.exists(new URI(uriWithDoneFlag), uriContext)) {
                        if (available == endOffset) {
                            LOG.debug("Matched future(" + available + "): " + uriWithDoneFlag);
                            resolved = true;
                            resolvedInstances.append(DateUtils.formatDateOozieTZ(nominalInstanceCal));
                            resolvedURIPaths.append(uriPath);
                            retVal = resolvedInstances.toString();
                            eval.setVariable("resolved_path", resolvedURIPaths.toString());
                            break;
                        }
                        else if (available >= startOffset) {
                            LOG.debug("Matched future(" + available + "): " + uriWithDoneFlag);
                            resolvedInstances.append(DateUtils.formatDateOozieTZ(nominalInstanceCal)).append(
                                    INSTANCE_SEPARATOR);
                            resolvedURIPaths.append(uriPath).append(INSTANCE_SEPARATOR);
                        }
                        available++;
                    }
                    // nominalInstanceCal.add(dsTimeUnit.getCalendarUnit(), datasetFrequency);
                    nominalInstanceCal = (Calendar) initInstance.clone();
                    instCount[0]++;
                    nominalInstanceCal.add(dsTimeUnit.getCalendarUnit(), instCount[0] * datasetFrequency);
                    checkedInstance++;
                    // DateUtils.moveToEnd(nominalInstanceCal, getDSEndOfFlag());
                }
            }
            finally {
                if (uriContext != null) {
                    uriContext.destroy();
                }
            }
            if (!resolved) {
                // return unchanged future function with variable 'is_resolved'
                // to 'false'
                eval.setVariable("is_resolved", Boolean.FALSE);
                if (startOffset == endOffset) {
                    retVal = "${coord:future(" + startOffset + ", " + instance + ")}";
                }
                else {
                    retVal = "${coord:futureRange(" + startOffset + ", " + endOffset + ", " + instance + ")}";
                }
            }
            else {
                eval.setVariable("is_resolved", Boolean.TRUE);
            }
        }
        else {// No feasible nominal time
            eval.setVariable("is_resolved", Boolean.TRUE);
            retVal = "";
        }
        return retVal;
    }

    /**
     * Return nominal time or Action Creation Time.
     * <p/>
     *
     * @return coordinator action creation or materialization date time
     * @throws Exception if unable to format the Date object to String
     */
    public static String ph2_coord_nominalTime() throws Exception {
        ELEvaluator eval = ELEvaluator.getCurrent();
        SyncCoordAction action = ParamChecker.notNull((SyncCoordAction) eval.getVariable(COORD_ACTION),
                "Coordinator Action");
        return DateUtils.formatDateOozieTZ(action.getNominalTime());
    }

    public static String ph3_coord_nominalTime() throws Exception {
        return ph2_coord_nominalTime();
    }

    /**
     * Convert from standard date-time formatting to a desired format.
     * <p/>
     * @param dateTimeStr - A timestamp in standard (ISO8601) format.
     * @param format - A string representing the desired format.
     * @return coordinator action creation or materialization date time
     * @throws Exception if unable to format the Date object to String
     */
    public static String ph2_coord_formatTime(String dateTimeStr, String format)
            throws Exception {
        Date dateTime = DateUtils.parseDateOozieTZ(dateTimeStr);
        return DateUtils.formatDateCustom(dateTime, format);
    }

    public static String ph3_coord_formatTime(String dateTimeStr, String format)
            throws Exception {
        return ph2_coord_formatTime(dateTimeStr, format);
    }

    /**
     * Return Action Id. <p/>
     *
     * @return coordinator action Id
     */
    public static String ph2_coord_actionId() throws Exception {
        ELEvaluator eval = ELEvaluator.getCurrent();
        SyncCoordAction action = ParamChecker.notNull((SyncCoordAction) eval.getVariable(COORD_ACTION),
                "Coordinator Action");
        return action.getActionId();
    }

    public static String ph3_coord_actionId() throws Exception {
        return ph2_coord_actionId();
    }

    /**
     * Return Job Name. <p/>
     *
     * @return coordinator name
     */
    public static String ph2_coord_name() throws Exception {
        ELEvaluator eval = ELEvaluator.getCurrent();
        SyncCoordAction action = ParamChecker.notNull((SyncCoordAction) eval.getVariable(COORD_ACTION),
                "Coordinator Action");
        return action.getName();
    }

    public static String ph3_coord_name() throws Exception {
        return ph2_coord_name();
    }

    /**
     * Return Action Start time. <p/>
     *
     * @return coordinator action start time
     * @throws Exception if unable to format the Date object to String
     */
    public static String ph2_coord_actualTime() throws Exception {
        ELEvaluator eval = ELEvaluator.getCurrent();
        SyncCoordAction coordAction = (SyncCoordAction) eval.getVariable(COORD_ACTION);
        if (coordAction == null) {
            throw new RuntimeException("Associated Application instance should be defined with key " + COORD_ACTION);
        }
        return DateUtils.formatDateOozieTZ(coordAction.getActualTime());
    }

    public static String ph3_coord_actualTime() throws Exception {
        return ph2_coord_actualTime();
    }

    /**
     * Used to specify a list of URI's that are used as input dir to the workflow job. <p/> Look for two evaluator-level
     * variables <p/> A) .datain.<DATAIN_NAME> B) .datain.<DATAIN_NAME>.unresolved <p/> A defines the current list of
     * URI. <p/> B defines whether there are any unresolved EL-function (i.e latest) <p/> If there are something
     * unresolved, this function will echo back the original function <p/> otherwise it sends the uris.
     *
     * @param dataInName : Datain name
     * @return the list of URI's separated by INSTANCE_SEPARATOR <p/> if there are unresolved EL function (i.e. latest)
     *         , echo back <p/> the function without resolving the function.
     */
    public static String ph3_coord_dataIn(String dataInName) {
        String uris = "";
        ELEvaluator eval = ELEvaluator.getCurrent();
        uris = (String) eval.getVariable(".datain." + dataInName);
        Boolean unresolved = (Boolean) eval.getVariable(".datain." + dataInName + ".unresolved");
        if (unresolved != null && unresolved.booleanValue() == true) {
            return "${coord:dataIn('" + dataInName + "')}";
        }
        return uris;
    }

    /**
     * Used to specify a list of URI's that are output dir of the workflow job. <p/> Look for one evaluator-level
     * variable <p/> dataout.<DATAOUT_NAME> <p/> It defines the current list of URI. <p/> otherwise it sends the uris.
     *
     * @param dataOutName : Dataout name
     * @return the list of URI's separated by INSTANCE_SEPARATOR
     */
    public static String ph3_coord_dataOut(String dataOutName) {
        String uris = "";
        ELEvaluator eval = ELEvaluator.getCurrent();
        uris = (String) eval.getVariable(".dataout." + dataOutName);
        return uris;
    }

    /**
     * Determine the date-time in Oozie processing timezone of n-th dataset instance. <p/> It depends on: <p/> 1.
     * Data set frequency <p/> 2.
     * Data set Time unit (day, month, minute) <p/> 3. Data set Time zone/DST <p/> 4. End Day/Month flag <p/> 5. Data
     * set initial instance <p/> 6. Action Creation Time
     *
     * @param n instance count domain: n is integer
     * @return date-time in Oozie processing timezone of the n-th instance returns 'null' means n-th instance is
     * earlier than Initial-Instance of DS
     * @throws Exception
     */
    public static String ph2_coord_current(int n) throws Exception {
        if (isSyncDataSet()) { // For Sync Dataset
            return coord_current_sync(n);
        }
        else {
            throw new UnsupportedOperationException("Asynchronous Dataset is not supported yet");
        }
    }

    /**
     * Determine the date-time in Oozie processing timezone of current dataset instances
     * from start to end offsets from the nominal time. <p/> It depends
     * on: <p/> 1. Data set frequency <p/> 2. Data set Time unit (day, month, minute) <p/> 3. Data set Time zone/DST
     * <p/> 4. End Day/Month flag <p/> 5. Data set initial instance <p/> 6. Action Creation Time
     *
     * @param start :start instance offset <p/> domain: start <= 0, start is integer
     * @param end :end instance offset <p/> domain: end <= 0, end is integer
     * @return date-time in Oozie processing timezone of the instances from start to end offsets
     *        delimited by comma. <p/> If the current instance time of the dataset based on the Action Creation Time
     *        is earlier than the Initial-Instance of DS an empty string is returned.
     *        If an instance within the range is earlier than Initial-Instance of DS that instance is ignored
     * @throws Exception
     */
    public static String ph2_coord_currentRange(int start, int end) throws Exception {
        if (isSyncDataSet()) { // For Sync Dataset
            return coord_currentRange_sync(start, end);
        }
        else {
            throw new UnsupportedOperationException("Asynchronous Dataset is not supported yet");
        }
    }
    /**
     * Determine the date-time in Oozie processing timezone of the given offset from the dataset effective nominal time. <p/> It
     * depends on: <p> 1. Data set frequency <p/> 2. Data set Time Unit <p/> 3. Data set Time zone/DST
     * <p/> 4. Data set initial instance <p/> 5. Action Creation Time
     *
     * @param n offset amount (integer)
     * @param timeUnit TimeUnit for offset n ("MINUTE", "HOUR", "DAY", "MONTH", "YEAR")
     * @return date-time in Oozie processing timezone of the given offset from the dataset effective nominal time
     * @throws Exception if there was a problem formatting
     */
    public static String ph2_coord_offset(int n, String timeUnit) throws Exception {
        if (isSyncDataSet()) { // For Sync Dataset
            return coord_offset_sync(n, timeUnit);
        }
        else {
            throw new UnsupportedOperationException("Asynchronous Dataset is not supported yet");
        }
    }

    /**
     * Determine how many hours is on the date of n-th dataset instance. <p/> It depends on: <p/> 1. Data set frequency
     * <p/> 2. Data set Time unit (day, month, minute) <p/> 3. Data set Time zone/DST <p/> 4. End Day/Month flag <p/> 5.
     * Data set initial instance <p/> 6. Action Creation Time
     *
     * @param n instance count <p/> domain: n is integer
     * @return number of hours on that day <p/> returns -1 means n-th instance is earlier than Initial-Instance of DS
     * @throws Exception
     */
    public static int ph2_coord_hoursInDay(int n) throws Exception {
        int datasetFrequency = (int) getDSFrequency();
        // /Calendar nominalInstanceCal =
        // getCurrentInstance(getActionCreationtime());
        Calendar nominalInstanceCal = getEffectiveNominalTime();
        if (nominalInstanceCal == null) {
            return -1;
        }
        nominalInstanceCal.add(getDSTimeUnit().getCalendarUnit(), datasetFrequency * n);
        /*
         * if (nominalInstanceCal.getTime().compareTo(getInitialInstance()) < 0)
         * { return -1; }
         */
        nominalInstanceCal.setTimeZone(getDatasetTZ());// Use Dataset TZ
        // DateUtils.moveToEnd(nominalInstanceCal, getDSEndOfFlag());
        return DateUtils.hoursInDay(nominalInstanceCal);
    }

    public static int ph3_coord_hoursInDay(int n) throws Exception {
        return ph2_coord_hoursInDay(n);
    }

    /**
     * Calculate number of days in one month for n-th dataset instance. <p/> It depends on: <p/> 1. Data set frequency .
     * <p/> 2. Data set Time unit (day, month, minute) <p/> 3. Data set Time zone/DST <p/> 4. End Day/Month flag <p/> 5.
     * Data set initial instance <p/> 6. Action Creation Time
     *
     * @param n instance count. domain: n is integer
     * @return number of days in that month <p/> returns -1 means n-th instance is earlier than Initial-Instance of DS
     * @throws Exception
     */
    public static int ph2_coord_daysInMonth(int n) throws Exception {
        int datasetFrequency = (int) getDSFrequency();// in minutes
        // Calendar nominalInstanceCal =
        // getCurrentInstance(getActionCreationtime());
        Calendar nominalInstanceCal = getEffectiveNominalTime();
        if (nominalInstanceCal == null) {
            return -1;
        }
        nominalInstanceCal.add(getDSTimeUnit().getCalendarUnit(), datasetFrequency * n);
        /*
         * if (nominalInstanceCal.getTime().compareTo(getInitialInstance()) < 0)
         * { return -1; }
         */
        nominalInstanceCal.setTimeZone(getDatasetTZ());// Use Dataset TZ
        // DateUtils.moveToEnd(nominalInstanceCal, getDSEndOfFlag());
        return nominalInstanceCal.getActualMaximum(Calendar.DAY_OF_MONTH);
    }

    public static int ph3_coord_daysInMonth(int n) throws Exception {
        return ph2_coord_daysInMonth(n);
    }

    /**
     * Determine the date-time in Oozie processing timezone of n-th latest available dataset instance. <p/> It depends
     * on: <p/> 1. Data set frequency <p/> 2. Data set Time unit (day, month, minute) <p/> 3. Data set Time zone/DST
     * <p/> 4. End Day/Month flag <p/> 5. Data set initial instance <p/> 6. Action Creation Time <p/> 7. Existence of
     * dataset's directory
     *
     * @param n :instance count <p/> domain: n <= 0, n is integer
     * @return date-time in Oozie processing timezone of the n-th instance <p/> returns 'null' means n-th instance is
     * earlier than Initial-Instance of DS
     * @throws Exception
     */
    public static String ph3_coord_latest(int n) throws Exception {
        ParamChecker.checkLEZero(n, "latest:n");
        if (isSyncDataSet()) {// For Sync Dataset
            return coord_latest_sync(n);
        }
        else {
            throw new UnsupportedOperationException("Asynchronous Dataset is not supported yet");
        }
    }

    /**
     * Determine the date-time in Oozie processing timezone of latest available dataset instances
     * from start to end offsets from the nominal time. <p/> It depends
     * on: <p/> 1. Data set frequency <p/> 2. Data set Time unit (day, month, minute) <p/> 3. Data set Time zone/DST
     * <p/> 4. End Day/Month flag <p/> 5. Data set initial instance <p/> 6. Action Creation Time <p/> 7. Existence of
     * dataset's directory
     *
     * @param start :start instance offset <p/> domain: start <= 0, start is integer
     * @param end :end instance offset <p/> domain: end <= 0, end is integer
     * @return date-time in Oozie processing timezone of the instances from start to end offsets
     *        delimited by comma. <p/> returns 'null' means start offset instance is
     *        earlier than Initial-Instance of DS
     * @throws Exception
     */
    public static String ph3_coord_latestRange(int start, int end) throws Exception {
        ParamChecker.checkLEZero(start, "latest:n");
        ParamChecker.checkLEZero(end, "latest:n");
        if (isSyncDataSet()) {// For Sync Dataset
            return coord_latestRange_sync(start, end);
        }
        else {
            throw new UnsupportedOperationException("Asynchronous Dataset is not supported yet");
        }
    }

    /**
     * Configure an evaluator with data set and application specific information. <p/> Helper method of associating
     * dataset and application object
     *
     * @param evaluator : to set variables
     * @param ds : Data Set object
     * @param coordAction : Application instance
     */
    public static void configureEvaluator(ELEvaluator evaluator, SyncCoordDataset ds, SyncCoordAction coordAction) {
        evaluator.setVariable(COORD_ACTION, coordAction);
        evaluator.setVariable(DATASET, ds);
    }

    /**
     * Helper method to wrap around with "${..}". <p/>
     *
     *
     * @param eval :EL evaluator
     * @param expr : expression to evaluate
     * @return Resolved expression or echo back the same expression
     * @throws Exception
     */
    public static String evalAndWrap(ELEvaluator eval, String expr) throws Exception {
        try {
            eval.setVariable(".wrap", null);
            String result = eval.evaluate(expr, String.class);
            if (eval.getVariable(".wrap") != null) {
                return "${" + result + "}";
            }
            else {
                return result;
            }
        }
        catch (Exception e) {
            throw new Exception("Unable to evaluate :" + expr + ":\n", e);
        }
    }

    // Set of echo functions

    public static String ph1_coord_current_echo(String n) {
        return echoUnResolved("current", n);
    }

    public static String ph1_coord_absolute_echo(String date) {
        return echoUnResolved("absolute", date);
    }

    public static String ph1_coord_currentRange_echo(String start, String end) {
        return echoUnResolved("currentRange", start + ", " + end);
    }

    public static String ph1_coord_offset_echo(String n, String timeUnit) {
        return echoUnResolved("offset", n + " , " + timeUnit);
    }

    public static String ph2_coord_current_echo(String n) {
        return echoUnResolved("current", n);
    }

    public static String ph2_coord_currentRange_echo(String start, String end) {
        return echoUnResolved("currentRange", start + ", " + end);
    }

    public static String ph2_coord_offset_echo(String n, String timeUnit) {
        return echoUnResolved("offset", n + " , " + timeUnit);
    }

    public static String ph2_coord_absolute_echo(String date) {
        return echoUnResolved("absolute", date);
    }

    public static String ph2_coord_absolute_range(String startInstance, int end) throws Exception {
        int[] instanceCount = new int[1];

        // getCurrentInstance() returns null, which means startInstance is less
        // than initial instance
        if (getCurrentInstance(DateUtils.getCalendar(startInstance).getTime(), instanceCount) == null) {
            throw new CommandException(ErrorCode.E1010,
                    "intial-instance should be equal or earlier than the start-instance. intial-instance is "
                            + getInitialInstance() + " and start-instance is " + startInstance);
        }
        int[] nominalCount = new int[1];
        if (getCurrentInstance(getActionCreationtime(), nominalCount) == null) {
            throw new CommandException(ErrorCode.E1010,
                    "intial-instance should be equal or earlier than the nominal time. intial-instance is "
                            + getInitialInstance() + " and nominal time is " + getActionCreationtime());
        }
        // getCurrentInstance return offset relative to initial instance.
        // start instance offset - nominal offset = start offset relative to
        // nominal time-stamp.
        int start = instanceCount[0] - nominalCount[0];
        if (start > end) {
            throw new CommandException(ErrorCode.E1010,
                    "start-instance should be equal or earlier than the end-instance. startInstance is "
                            + startInstance + " which is equivalent to current (" + instanceCount[0]
                            + ") but end is specified as current (" + end + ")");
        }
        return ph2_coord_currentRange(start, end);
    }

    public static String ph1_coord_dateOffset_echo(String n, String offset, String unit) {
        return echoUnResolved("dateOffset", n + " , " + offset + " , " + unit);
    }

    public static String ph1_coord_dateTzOffset_echo(String n, String timezone) {
        return echoUnResolved("dateTzOffset", n + " , " + timezone);
    }

    public static String ph1_coord_formatTime_echo(String dateTime, String format) {
        // Quote the dateTime value since it would contain a ':'.
        return echoUnResolved("formatTime", "'"+dateTime+"'" + " , " + format);
    }

    public static String ph1_coord_latest_echo(String n) {
        return echoUnResolved("latest", n);
    }

    public static String ph2_coord_latest_echo(String n) {
        return ph1_coord_latest_echo(n);
    }

    public static String ph1_coord_future_echo(String n, String instance) {
        return echoUnResolved("future", n + ", " + instance + "");
    }

    public static String ph2_coord_future_echo(String n, String instance) {
        return ph1_coord_future_echo(n, instance);
    }

    public static String ph1_coord_latestRange_echo(String start, String end) {
        return echoUnResolved("latestRange", start + ", " + end);
    }

    public static String ph2_coord_latestRange_echo(String start, String end) {
        return ph1_coord_latestRange_echo(start, end);
    }

    public static String ph1_coord_futureRange_echo(String start, String end, String instance) {
        return echoUnResolved("futureRange", start + ", " + end + ", " + instance);
    }

    public static String ph2_coord_futureRange_echo(String start, String end, String instance) {
        return ph1_coord_futureRange_echo(start, end, instance);
    }

    public static String ph1_coord_dataIn_echo(String n) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        String val = (String) eval.getVariable("oozie.dataname." + n);
        if (val == null || val.equals("data-in") == false) {
            XLog.getLog(CoordELFunctions.class).error("data_in_name " + n + " is not valid");
            throw new RuntimeException("data_in_name " + n + " is not valid");
        }
        return echoUnResolved("dataIn", "'" + n + "'");
    }

    public static String ph1_coord_dataOut_echo(String n) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        String val = (String) eval.getVariable("oozie.dataname." + n);
        if (val == null || val.equals("data-out") == false) {
            XLog.getLog(CoordELFunctions.class).error("data_out_name " + n + " is not valid");
            throw new RuntimeException("data_out_name " + n + " is not valid");
        }
        return echoUnResolved("dataOut", "'" + n + "'");
    }

    public static String ph1_coord_nominalTime_echo() {
        return echoUnResolved("nominalTime", "");
    }

    public static String ph1_coord_nominalTime_echo_wrap() {
        // return "${coord:nominalTime()}"; // no resolution
        return echoUnResolved("nominalTime", "");
    }

    public static String ph1_coord_nominalTime_echo_fixed() {
        return "2009-03-06T010:00"; // Dummy resolution
    }

    public static String ph1_coord_actualTime_echo_wrap() {
        // return "${coord:actualTime()}"; // no resolution
        return echoUnResolved("actualTime", "");
    }

    public static String ph1_coord_actionId_echo() {
        return echoUnResolved("actionId", "");
    }

    public static String ph1_coord_name_echo() {
        return echoUnResolved("name", "");
    }

    // The following echo functions are not used in any phases yet
    // They are here for future purpose.
    public static String coord_minutes_echo(String n) {
        return echoUnResolved("minutes", n);
    }

    public static String coord_hours_echo(String n) {
        return echoUnResolved("hours", n);
    }

    public static String coord_days_echo(String n) {
        return echoUnResolved("days", n);
    }

    public static String coord_endOfDay_echo(String n) {
        return echoUnResolved("endOfDay", n);
    }

    public static String coord_months_echo(String n) {
        return echoUnResolved("months", n);
    }

    public static String coord_endOfMonth_echo(String n) {
        return echoUnResolved("endOfMonth", n);
    }

    public static String coord_actualTime_echo() {
        return echoUnResolved("actualTime", "");
    }

    // This echo function will always return "24" for validation only.
    // This evaluation ****should not**** replace the original XML
    // Create a temporary string and validate the function
    // This is **required** for evaluating an expression like
    // coord:HoursInDay(0) + 3
    // actual evaluation will happen in phase 2 or phase 3.
    public static String ph1_coord_hoursInDay_echo(String n) {
        return "24";
        // return echoUnResolved("hoursInDay", n);
    }

    // This echo function will always return "30" for validation only.
    // This evaluation ****should not**** replace the original XML
    // Create a temporary string and validate the function
    // This is **required** for evaluating an expression like
    // coord:daysInMonth(0) + 3
    // actual evaluation will happen in phase 2 or phase 3.
    public static String ph1_coord_daysInMonth_echo(String n) {
        // return echoUnResolved("daysInMonth", n);
        return "30";
    }

    // This echo function will always return "3" for validation only.
    // This evaluation ****should not**** replace the original XML
    // Create a temporary string and validate the function
    // This is **required** for evaluating an expression like coord:tzOffset + 2
    // actual evaluation will happen in phase 2 or phase 3.
    public static String ph1_coord_tzOffset_echo() {
        // return echoUnResolved("tzOffset", "");
        return "3";
    }

    // Local methods
    /**
     * @param n
     * @return n-th instance Date-Time from current instance for data-set <p/> return empty string ("") if the
     *         Action_Creation_time or the n-th instance <p/> is earlier than the Initial_Instance of dataset.
     * @throws Exception
     */
    private static String coord_current_sync(int n) throws Exception {
        return coord_currentRange_sync(n, n);
    }

    private static String coord_currentRange_sync(int start, int end) throws Exception {
        final XLog LOG = XLog.getLog(CoordELFunctions.class);
        int datasetFrequency = getDSFrequency();// in minutes
        TimeUnit dsTimeUnit = getDSTimeUnit();
        int[] instCount = new int[1];// used as pass by ref
        Calendar nominalInstanceCal = getCurrentInstance(getActionCreationtime(), instCount);
        if (nominalInstanceCal == null) {
            LOG.warn("If the initial instance of the dataset is later than the nominal time, an empty string is"
                    + " returned. This means that no data is available at the current-instance specified by the user"
                    + " and the user could try modifying his initial-instance to an earlier time.");
            return "";
        } else {
            Calendar initInstance = getInitialInstanceCal();
            // Add in the reverse order - newest instance first.
            nominalInstanceCal = (Calendar) initInstance.clone();
            nominalInstanceCal.add(dsTimeUnit.getCalendarUnit(), (instCount[0] + start) * datasetFrequency);
            List<String> instances = new ArrayList<String>();
            for (int i = start; i <= end; i++) {
                if (nominalInstanceCal.compareTo(initInstance) < 0) {
                    LOG.warn("If the initial instance of the dataset is later than the current-instance specified,"
                            + " such as coord:current({0}) in this case, an empty string is returned. This means that"
                            + " no data is available at the current-instance specified by the user and the user could"
                            + " try modifying his initial-instance to an earlier time.", start);
                }
                else {
                    instances.add(DateUtils.formatDateOozieTZ(nominalInstanceCal));
                }
                nominalInstanceCal.add(dsTimeUnit.getCalendarUnit(), datasetFrequency);
            }
            instances = Lists.reverse(instances);
            return StringUtils.join(instances, CoordELFunctions.INSTANCE_SEPARATOR);
        }
    }

    /**
     *
     * @param n offset amount (integer)
     * @param timeUnit TimeUnit for offset n ("MINUTE", "HOUR", "DAY", "MONTH", "YEAR")
     * @return the offset time from the effective nominal time <p/> return empty string ("") if the Action_Creation_time or the
     *         offset instance <p/> is earlier than the Initial_Instance of dataset.
     * @throws Exception
     */
    private static String coord_offset_sync(int n, String timeUnit) throws Exception {
        Calendar rawCal = resolveOffsetRawTime(n, TimeUnit.valueOf(timeUnit), null);
        if (rawCal == null) {
            // warning already logged by resolveOffsetRawTime()
            return "";
        }

        int freq = getDSFrequency();
        TimeUnit freqUnit = getDSTimeUnit();
        int freqCount = 0;
        // We're going to manually turn back/forward cal by decrements/increments of freq and then check that it gives the same
        // time as rawCal; this is to check that the offset time resolves to a frequency offset of the effective nominal time
        // In other words, that there exists an integer x, such that coord:offset(n, timeUnit) == coord:current(x) is true
        // If not, then we'll "rewind" rawCal to the latest instance earlier than rawCal and use that.
        Calendar cal = getInitialInstanceCal();
        if (rawCal.before(cal)) {
            while (cal.after(rawCal)) {
                cal.add(freqUnit.getCalendarUnit(), -freq);
                freqCount--;
            }
        }
        else if (rawCal.after(cal)) {
            while (cal.before(rawCal)) {
                cal.add(freqUnit.getCalendarUnit(), freq);
                freqCount++;
            }
        }
        if (cal.before(rawCal)) {
            rawCal = cal;
        }
        else if (cal.after(rawCal)) {
            cal.add(freqUnit.getCalendarUnit(), -freq);
            rawCal = cal;
            freqCount--;
        }
        String rawCalStr = DateUtils.formatDateOozieTZ(rawCal);

        Calendar nominalInstanceCal = getInitialInstanceCal();
        nominalInstanceCal.add(freqUnit.getCalendarUnit(), freq * freqCount);
        if (nominalInstanceCal.getTime().compareTo(getInitialInstance()) < 0) {
            XLog.getLog(CoordELFunctions.class).warn("If the initial instance of the dataset is later than the offset instance"
                    + " specified, such as coord:offset({0}, {1}) in this case, an empty string is returned. This means that no"
                    + " data is available at the offset instance specified by the user and the user could try modifying his"
                    + " initial-instance to an earlier time.", n, timeUnit);
            return "";
        }
        String nominalCalStr = DateUtils.formatDateOozieTZ(nominalInstanceCal);

        if (!rawCalStr.equals(nominalCalStr)) {
            throw new RuntimeException("Shouldn't happen");
        }
        return rawCalStr;
    }

    /**
     * @param offset
     * @return n-th available latest instance Date-Time for SYNC data-set
     * @throws Exception
     */
    private static String coord_latest_sync(int offset) throws Exception {
        return coord_latestRange_sync(offset, offset);
    }

    private static String coord_latestRange_sync(int startOffset, int endOffset) throws Exception {
        final XLog LOG = XLog.getLog(CoordELFunctions.class);
        final Thread currentThread = Thread.currentThread();
        ELEvaluator eval = ELEvaluator.getCurrent();
        String retVal = "";
        int datasetFrequency = (int) getDSFrequency();// in minutes
        TimeUnit dsTimeUnit = getDSTimeUnit();
        int[] instCount = new int[1];
        boolean useCurrentTime = Services.get().getConf().getBoolean(LATEST_EL_USE_CURRENT_TIME, false);
        Calendar nominalInstanceCal;
        if (useCurrentTime) {
            nominalInstanceCal = getCurrentInstance(new Date(), instCount);
        }
        else {
            nominalInstanceCal = getCurrentInstance(getActualTime(), instCount);
        }
        StringBuilder resolvedInstances = new StringBuilder();
        StringBuilder resolvedURIPaths = new StringBuilder();
        if (nominalInstanceCal != null) {
            Calendar initInstance = getInitialInstanceCal();
            SyncCoordDataset ds = (SyncCoordDataset) eval.getVariable(DATASET);
            if (ds == null) {
                throw new RuntimeException("Associated Dataset should be defined with key " + DATASET);
            }
            String uriTemplate = ds.getUriTemplate();
            Configuration conf = (Configuration) eval.getVariable(CONFIGURATION);
            if (conf == null) {
                throw new RuntimeException("Associated Configuration should be defined with key " + CONFIGURATION);
            }
            int available = 0;
            boolean resolved = false;
            String user = ParamChecker
                    .notEmpty((String) eval.getVariable(OozieClient.USER_NAME), OozieClient.USER_NAME);
            String doneFlag = ds.getDoneFlag();
            URIHandlerService uriService = Services.get().get(URIHandlerService.class);
            URIHandler uriHandler = null;
            Context uriContext = null;
            try {
                while (nominalInstanceCal.compareTo(initInstance) >= 0 && !currentThread.isInterrupted()) {
                    ELEvaluator uriEval = getUriEvaluator(nominalInstanceCal);
                    String uriPath = uriEval.evaluate(uriTemplate, String.class);
                    if (uriHandler == null) {
                        URI uri = new URI(uriPath);
                        uriHandler = uriService.getURIHandler(uri);
                        uriContext = uriHandler.getContext(uri, conf, user, true);
                    }
                    String uriWithDoneFlag = uriHandler.getURIWithDoneFlag(uriPath, doneFlag);
                    if (uriHandler.exists(new URI(uriWithDoneFlag), uriContext)) {
                        XLog.getLog(CoordELFunctions.class)
                        .debug("Found latest(" + available + "): " + uriWithDoneFlag);
                        if (available == startOffset) {
                            LOG.debug("Matched latest(" + available + "): " + uriWithDoneFlag);
                            resolved = true;
                            resolvedInstances.append(DateUtils.formatDateOozieTZ(nominalInstanceCal));
                            resolvedURIPaths.append(uriPath);
                            retVal = resolvedInstances.toString();
                            eval.setVariable("resolved_path", resolvedURIPaths.toString());
                            break;
                        }
                        else if (available <= endOffset) {
                            LOG.debug("Matched latest(" + available + "): " + uriWithDoneFlag);
                            resolvedInstances.append(DateUtils.formatDateOozieTZ(nominalInstanceCal)).append(
                                    INSTANCE_SEPARATOR);
                            resolvedURIPaths.append(uriPath).append(INSTANCE_SEPARATOR);
                        }

                        available--;
                    }
                    // nominalInstanceCal.add(dsTimeUnit.getCalendarUnit(), -datasetFrequency);
                    nominalInstanceCal = (Calendar) initInstance.clone();
                    instCount[0]--;
                    nominalInstanceCal.add(dsTimeUnit.getCalendarUnit(), instCount[0] * datasetFrequency);
                    // DateUtils.moveToEnd(nominalInstanceCal, getDSEndOfFlag());
                }
            }
            finally {
                if (uriContext != null) {
                    uriContext.destroy();
                }
            }
            if (!resolved) {
                // return unchanged latest function with variable 'is_resolved'
                // to 'false'
                eval.setVariable("is_resolved", Boolean.FALSE);
                if (startOffset == endOffset) {
                    retVal = "${coord:latest(" + startOffset + ")}";
                }
                else {
                    retVal = "${coord:latestRange(" + startOffset + "," + endOffset + ")}";
                }
            }
            else {
                eval.setVariable("is_resolved", Boolean.TRUE);
            }
        }
        else {// No feasible nominal time
            eval.setVariable("is_resolved", Boolean.FALSE);
        }
        return retVal;
    }

    /**
     * @param tm
     * @return a new Evaluator to be used for URI-template evaluation
     */
    private static ELEvaluator getUriEvaluator(Calendar tm) {
        tm.setTimeZone(DateUtils.getOozieProcessingTimeZone());
        ELEvaluator retEval = new ELEvaluator();
        retEval.setVariable("YEAR", tm.get(Calendar.YEAR));
        retEval.setVariable("MONTH", (tm.get(Calendar.MONTH) + 1) < 10 ? "0" + (tm.get(Calendar.MONTH) + 1) : (tm
                .get(Calendar.MONTH) + 1));
        retEval.setVariable("DAY", tm.get(Calendar.DAY_OF_MONTH) < 10 ? "0" + tm.get(Calendar.DAY_OF_MONTH) : tm
                .get(Calendar.DAY_OF_MONTH));
        retEval.setVariable("HOUR", tm.get(Calendar.HOUR_OF_DAY) < 10 ? "0" + tm.get(Calendar.HOUR_OF_DAY) : tm
                .get(Calendar.HOUR_OF_DAY));
        retEval.setVariable("MINUTE", tm.get(Calendar.MINUTE) < 10 ? "0" + tm.get(Calendar.MINUTE) : tm
                .get(Calendar.MINUTE));
        return retEval;
    }

    /**
     * @return whether a data set is SYNCH or ASYNC
     */
    private static boolean isSyncDataSet() {
        ELEvaluator eval = ELEvaluator.getCurrent();
        SyncCoordDataset ds = (SyncCoordDataset) eval.getVariable(DATASET);
        if (ds == null) {
            throw new RuntimeException("Associated Dataset should be defined with key " + DATASET);
        }
        return ds.getType().equalsIgnoreCase("SYNC");
    }

    /**
     * Check whether a function should be resolved.
     *
     * @param functionName
     * @param n
     * @return null if the functionName needs to be resolved otherwise return the calling function unresolved.
     */
    private static String checkIfResolved(String functionName, String n) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        String replace = (String) eval.getVariable("resolve_" + functionName);
        if (replace == null || (replace != null && replace.equalsIgnoreCase("false"))) { // Don't
            // resolve
            // return "${coord:" + functionName + "(" + n +")}"; //Unresolved
            eval.setVariable(".wrap", "true");
            return "coord:" + functionName + "(" + n + ")"; // Unresolved
        }
        return null; // Resolved it
    }

    private static String echoUnResolved(String functionName, String n) {
        return echoUnResolvedPre(functionName, n, "coord:");
    }

    private static String echoUnResolvedPre(String functionName, String n, String prefix) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        eval.setVariable(".wrap", "true");
        return prefix + functionName + "(" + n + ")"; // Unresolved
    }

    /**
     * @return the initial instance of a DataSet in DATE
     */
    private static Date getInitialInstance() {
        ELEvaluator eval = ELEvaluator.getCurrent();
        return getInitialInstance(eval);
    }

    /**
     * @return the initial instance of a DataSet in DATE
     */
    private static Date getInitialInstance(ELEvaluator eval) {
        return getInitialInstanceCal(eval).getTime();
        // return ds.getInitInstance();
    }

    /**
     * @return the initial instance of a DataSet in Calendar
     */
    private static Calendar getInitialInstanceCal() {
        ELEvaluator eval = ELEvaluator.getCurrent();
        return getInitialInstanceCal(eval);
    }

    /**
     * @return the initial instance of a DataSet in Calendar
     */
    private static Calendar getInitialInstanceCal(ELEvaluator eval) {
        SyncCoordDataset ds = (SyncCoordDataset) eval.getVariable(DATASET);
        if (ds == null) {
            throw new RuntimeException("Associated Dataset should be defined with key " + DATASET);
        }
        Calendar effInitTS = new GregorianCalendar(ds.getTimeZone());
        effInitTS.setTime(ds.getInitInstance());
        // To adjust EOD/EOM
        DateUtils.moveToEnd(effInitTS, getDSEndOfFlag(eval));
        return effInitTS;
        // return ds.getInitInstance();
    }

    /**
     * @return Nominal or action creation Time when all the dependencies of an application instance are met.
     */
    private static Date getActionCreationtime() {
        ELEvaluator eval = ELEvaluator.getCurrent();
        return getActionCreationtime(eval);
    }

    /**
     * @return Nominal or action creation Time when all the dependencies of an application instance are met.
     */
    private static Date getActionCreationtime(ELEvaluator eval) {
        SyncCoordAction coordAction = (SyncCoordAction) eval.getVariable(COORD_ACTION);
        if (coordAction == null) {
            throw new RuntimeException("Associated Application instance should be defined with key " + COORD_ACTION);
        }
        return coordAction.getNominalTime();
    }

    /**
     * @return Actual Time when all the dependencies of an application instance are met.
     */
    private static Date getActualTime() {
        ELEvaluator eval = ELEvaluator.getCurrent();
        SyncCoordAction coordAction = (SyncCoordAction) eval.getVariable(COORD_ACTION);
        if (coordAction == null) {
            throw new RuntimeException("Associated Application instance should be defined with key " + COORD_ACTION);
        }
        return coordAction.getActualTime();
    }

    /**
     * @return TimeZone for the application or job.
     */
    private static TimeZone getJobTZ() {
        ELEvaluator eval = ELEvaluator.getCurrent();
        SyncCoordAction coordAction = (SyncCoordAction) eval.getVariable(COORD_ACTION);
        if (coordAction == null) {
            throw new RuntimeException("Associated Application instance should be defined with key " + COORD_ACTION);
        }
        return coordAction.getTimeZone();
    }

    /**
     * Find the current instance based on effectiveTime (i.e Action_Creation_Time or Action_Start_Time)
     *
     * @return current instance i.e. current(0) returns null if effectiveTime is earlier than Initial Instance time of
     *         the dataset.
     */
    public static Calendar getCurrentInstance(Date effectiveTime, int instanceCount[]) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        return getCurrentInstance(effectiveTime, instanceCount, eval);
    }

    /**
     * Find the current instance based on effectiveTime (i.e Action_Creation_Time or Action_Start_Time)
     *
     * @return current instance i.e. current(0) returns null if effectiveTime is earlier than Initial Instance time of
     *         the dataset.
     */
    private static Calendar getCurrentInstance(Date effectiveTime, int instanceCount[], ELEvaluator eval) {
        Date datasetInitialInstance = getInitialInstance(eval);
        TimeUnit dsTimeUnit = getDSTimeUnit(eval);
        TimeZone dsTZ = getDatasetTZ(eval);
        int dsFreq = getDSFrequency(eval);
        // Convert Date to Calendar for corresponding TZ
        Calendar current = Calendar.getInstance(dsTZ);
        current.setTime(datasetInitialInstance);

        Calendar calEffectiveTime = new GregorianCalendar(dsTZ);
        calEffectiveTime.setTime(effectiveTime);
        if (instanceCount == null) {    // caller doesn't care about this value
            instanceCount = new int[1];
        }
        instanceCount[0] = 0;
        if (current.compareTo(calEffectiveTime) > 0) {
            return null;
        }

        switch(dsTimeUnit) {
            case MINUTE:
                instanceCount[0] = (int) ((effectiveTime.getTime() - datasetInitialInstance.getTime()) / MINUTE_MSEC);
                break;
            case HOUR:
                instanceCount[0] = (int) ((effectiveTime.getTime() - datasetInitialInstance.getTime()) / HOUR_MSEC);
                break;
            case DAY:
            case END_OF_DAY:
                instanceCount[0] = (int) ((effectiveTime.getTime() - datasetInitialInstance.getTime()) / DAY_MSEC);
                break;
            case MONTH:
            case END_OF_MONTH:
                instanceCount[0] = (int) ((effectiveTime.getTime() - datasetInitialInstance.getTime()) / MONTH_MSEC);
                break;
            case YEAR:
                instanceCount[0] = (int) ((effectiveTime.getTime() - datasetInitialInstance.getTime()) / YEAR_MSEC);
                break;
            default:
                throw new IllegalArgumentException("Unhandled dataset time unit " + dsTimeUnit);
        }

        if (instanceCount[0] > 2) {
            instanceCount[0] = (instanceCount[0] / dsFreq);
            current.add(dsTimeUnit.getCalendarUnit(), instanceCount[0] * dsFreq);
        } else {
            instanceCount[0] = 0;
        }
        while (!current.getTime().after(effectiveTime)) {
            current.add(dsTimeUnit.getCalendarUnit(), dsFreq);
            instanceCount[0]++;
        }
        current.add(dsTimeUnit.getCalendarUnit(), -dsFreq);
        instanceCount[0]--;
        return current;
    }

    /**
     * Find the current instance based on effectiveTime (i.e Action_Creation_Time or Action_Start_Time)
     *
     * @return current instance i.e. current(0) returns null if effectiveTime is earlier than Initial Instance time of
     *         the dataset.
     */
    private static Calendar getCurrentInstance_old(Date effectiveTime, int instanceCount[], ELEvaluator eval) {
        Date datasetInitialInstance = getInitialInstance(eval);
        TimeUnit dsTimeUnit = getDSTimeUnit(eval);
        TimeZone dsTZ = getDatasetTZ(eval);
        int dsFreq = getDSFrequency(eval);
        // Convert Date to Calendar for corresponding TZ
        Calendar current = Calendar.getInstance();
        current.setTime(datasetInitialInstance);
        current.setTimeZone(dsTZ);

        Calendar calEffectiveTime = Calendar.getInstance();
        calEffectiveTime.setTime(effectiveTime);
        calEffectiveTime.setTimeZone(dsTZ);
        if (instanceCount == null) {    // caller doesn't care about this value
            instanceCount = new int[1];
        }
        instanceCount[0] = 0;
        if (current.compareTo(calEffectiveTime) > 0) {
            return null;
        }
        Calendar origCurrent = (Calendar) current.clone();
        while (current.compareTo(calEffectiveTime) <= 0) {
            current = (Calendar) origCurrent.clone();
            instanceCount[0]++;
            current.add(dsTimeUnit.getCalendarUnit(), instanceCount[0] * dsFreq);
        }
        instanceCount[0]--;

        current = (Calendar) origCurrent.clone();
        current.add(dsTimeUnit.getCalendarUnit(), instanceCount[0] * dsFreq);
        return current;
    }

    public static Calendar getEffectiveNominalTime() {
        Date datasetInitialInstance = getInitialInstance();
        TimeZone dsTZ = getDatasetTZ();
        // Convert Date to Calendar for corresponding TZ
        Calendar current = Calendar.getInstance();
        current.setTime(datasetInitialInstance);
        current.setTimeZone(dsTZ);

        Calendar calEffectiveTime = Calendar.getInstance();
        calEffectiveTime.setTime(getActionCreationtime());
        calEffectiveTime.setTimeZone(dsTZ);
        if (current.compareTo(calEffectiveTime) > 0) {
            // Nominal Time < initial Instance
            // TODO: getClass() call doesn't work from static method.
            // XLog.getLog("CoordELFunction.class").warn("ACTION CREATED BEFORE INITIAL INSTACE "+
            // current.getTime());
            return null;
        }
        return calEffectiveTime;
    }

    /**
     * @return dataset frequency in minutes
     */
    private static int getDSFrequency() {
        ELEvaluator eval = ELEvaluator.getCurrent();
        return getDSFrequency(eval);
    }

    /**
     * @return dataset frequency in minutes
     */
    private static int getDSFrequency(ELEvaluator eval) {
        SyncCoordDataset ds = (SyncCoordDataset) eval.getVariable(DATASET);
        if (ds == null) {
            throw new RuntimeException("Associated Dataset should be defined with key " + DATASET);
        }
        return ds.getFrequency();
    }

    /**
     * @return dataset TimeUnit
     */
    private static TimeUnit getDSTimeUnit() {
        ELEvaluator eval = ELEvaluator.getCurrent();
        return getDSTimeUnit(eval);
    }

    /**
     * @return dataset TimeUnit
     */
    private static TimeUnit getDSTimeUnit(ELEvaluator eval) {
        SyncCoordDataset ds = (SyncCoordDataset) eval.getVariable(DATASET);
        if (ds == null) {
            throw new RuntimeException("Associated Dataset should be defined with key " + DATASET);
        }
        return ds.getTimeUnit();
    }

    /**
     * @return dataset TimeZone
     */
    public static TimeZone getDatasetTZ() {
        ELEvaluator eval = ELEvaluator.getCurrent();
        return getDatasetTZ(eval);
    }

    /**
     * @return dataset TimeZone
     */
    private static TimeZone getDatasetTZ(ELEvaluator eval) {
        SyncCoordDataset ds = (SyncCoordDataset) eval.getVariable(DATASET);
        if (ds == null) {
            throw new RuntimeException("Associated Dataset should be defined with key " + DATASET);
        }
        return ds.getTimeZone();
    }

    /**
     * @return dataset TimeUnit
     */
    private static TimeUnit getDSEndOfFlag() {
        ELEvaluator eval = ELEvaluator.getCurrent();
        return getDSEndOfFlag(eval);
    }

    /**
     * @return dataset TimeUnit
     */
    private static TimeUnit getDSEndOfFlag(ELEvaluator eval) {
        SyncCoordDataset ds = (SyncCoordDataset) eval.getVariable(DATASET);
        if (ds == null) {
            throw new RuntimeException("Associated Dataset should be defined with key " + DATASET);
        }
        return ds.getEndOfDuration();// == null ? "": ds.getEndOfDuration();
    }

    /**
     * Return a job configuration property for the coordinator.
     *
     * @param property property name.
     * @return the value of the property, <code>null</code> if the property is undefined.
     */
    public static String coord_conf(String property) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        return (String) eval.getVariable(property);
    }

    /**
     * Return the user that submitted the coordinator job.
     *
     * @return the user that submitted the coordinator job.
     */
    public static String coord_user() {
        ELEvaluator eval = ELEvaluator.getCurrent();
        return (String) eval.getVariable(OozieClient.USER_NAME);
    }

    /**
     * Takes two offset times and returns a list of multiples of the frequency offset from the effective nominal time that occur
     * between them.  The caller should make sure that startCal is earlier than endCal.
     * <p>
     * As a simple example, assume its the same day: startCal is 1:00, endCal is 2:00, frequency is 20min, and effective nominal
     * time is 1:20 -- then this method would return a list containing: -20, 0, 20, 40, 60
     *
     * @param startCal The earlier offset time
     * @param endCal The later offset time
     * @param eval The ELEvaluator to use; cannot be null
     * @return A list of multiple of the frequency offset from the effective nominal time that occur between the startCal and endCal
     */
    public static List<Integer> expandOffsetTimes(Calendar startCal, Calendar endCal, ELEvaluator eval) {
        List<Integer> expandedFreqs = new ArrayList<Integer>();
        // Use eval because the "current" eval isn't set
        int freq = getDSFrequency(eval);
        TimeUnit freqUnit = getDSTimeUnit(eval);
        Calendar cal = getCurrentInstance(getActionCreationtime(eval), null, eval);
        int totalFreq = 0;
        if (startCal.before(cal)) {
            while (cal.after(startCal)) {
                cal.add(freqUnit.getCalendarUnit(), -freq);
                totalFreq += -freq;
            }
            if (cal.before(startCal)) {
                cal.add(freqUnit.getCalendarUnit(), freq);
                totalFreq += freq;
            }
        }
        else if (startCal.after(cal)) {
            while (cal.before(startCal)) {
                cal.add(freqUnit.getCalendarUnit(), freq);
                totalFreq += freq;
            }
        }
        // At this point, cal is the smallest multiple of the dataset frequency that is >= to the startCal and offset from the
        // effective nominal time.  Now we can find all of the instances that occur between startCal and endCal, inclusive.
        while (cal.before(endCal) || cal.equals(endCal)) {
            expandedFreqs.add(totalFreq);
            cal.add(freqUnit.getCalendarUnit(), freq);
            totalFreq += freq;
        }
        return expandedFreqs;
    }

    /**
     * Resolve the offset time from the effective nominal time
     *
     * @param n offset amount (integer)
     * @param timeUnit TimeUnit for offset n ("MINUTE", "HOUR", "DAY", "MONTH", "YEAR")
     * @param eval The ELEvaluator to use; or null to use the "current" eval
     * @return A Calendar of the offset time
     */
    public static Calendar resolveOffsetRawTime(int n, TimeUnit timeUnit, ELEvaluator eval) {
        // Use eval if given (for when the "current" eval isn't set)
        Calendar cal;
        if (eval == null) {
            cal = getCurrentInstance(getActionCreationtime(), null);
        }
        else {
            cal = getCurrentInstance(getActionCreationtime(eval), null, eval);
        }
        if (cal == null) {
            XLog.getLog(CoordELFunctions.class).warn("If the initial instance of the dataset is later than the nominal time, an"
                    + " empty string is returned. This means that no data is available at the offset instance specified by the user"
                    + " and the user could try modifying his or her initial-instance to an earlier time.");
            return null;
        }
        cal.add(timeUnit.getCalendarUnit(), n);
        return cal;
    }
}
