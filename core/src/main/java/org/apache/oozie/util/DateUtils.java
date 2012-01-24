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

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.oozie.coord.TimeUnit;

public class DateUtils {

    private static final String[] W3CDATETIME_MASKS = {"yyyy-MM-dd'T'HH:mmz"};

    /**
     * Parses a Date out of a String with a date in W3C date-time format.
     * <p/>
     * It parsers the following formats:
     * <ul>
     * <li>"yyyy-MM-dd'T'HH:mm:ssz"</li>
     * <li>"yyyy-MM-dd'T'HH:mmz"</li>
     * <li>"yyyy-MM-dd"</li>
     * <li>"yyyy-MM"</li>
     * <li>"yyyy"</li>
     * </ul>
     * <p/>
     * Refer to the java.text.SimpleDateFormat javadocs for details on the
     * format of each element.
     * <p/>
     *
     * @param sDate string to parse for a date.
     * @return the Date represented by the given W3C date-time string. It
     * returns <b>null</b> if it was not possible to parse the given
     * string into a Date.
     */
    /*
     * public static Date parseW3CDateTime(String sDate) { // if sDate has time
     * on it, it injects 'GTM' before de TZ displacement to // allow the
     * SimpleDateFormat parser to parse it properly int tIndex =
     * sDate.indexOf("T"); if (tIndex > -1) { if (sDate.endsWith("Z")) { sDate =
     * sDate.substring(0, sDate.length() - 1) + "+00:00"; } int tzdIndex =
     * sDate.indexOf("+", tIndex); if (tzdIndex == -1) { tzdIndex =
     * sDate.indexOf("-", tIndex); } if (tzdIndex > -1) { String pre =
     * sDate.substring(0, tzdIndex); int secFraction = pre.indexOf(","); if
     * (secFraction > -1) { pre = pre.substring(0, secFraction); } String post =
     * sDate.substring(tzdIndex); sDate = pre + "GMT" + post; } } else { sDate
     * += "T00:00GMT"; } return parseUsingMask(W3CDATETIME_MASKS, sDate); }
     */
    /**
     * Parses a Date out of a string using an array of masks. <p/> It uses the masks in order until one of them succedes
     * or all fail. <p/>
     *
     * @param masks array of masks to use for parsing the string
     * @param sDate string to parse for a date.
     * @return the Date represented by the given string using one of the given masks. It returns <b>null</b> if it was
     *         not possible to parse the the string with any of the masks.
     */
    private static Date parseUsingMask(String[] masks, String sDate) {
        sDate = (sDate != null) ? sDate.trim() : null;
        ParsePosition pp;
        Date d = null;
        if (sDate != null) {
            for (int i = 0; d == null && i < masks.length; i++) {
                DateFormat df = new SimpleDateFormat(masks[i], Locale.US);
                df.setLenient(true);
                pp = new ParsePosition(0);
                d = df.parse(sDate, pp);
                if (pp.getIndex() != sDate.length()) {
                    d = null;
                }
            }
        }
        return d;
    }

    private static final TimeZone UTC = getTimeZone("UTC");

    private static DateFormat getISO8601DateFormat() {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        dateFormat.setTimeZone(UTC);
        return dateFormat;
    }

    private static DateFormat getSpecificDateFormat(String format) {
        DateFormat dateFormat = new SimpleDateFormat(format);
        dateFormat.setTimeZone(UTC);
        return dateFormat;
    }

    public static TimeZone getTimeZone(String tzId) {
        if (tzId == null) {
            throw new IllegalArgumentException("Invalid TimeZone: " + tzId);
        }
        TimeZone tz = TimeZone.getTimeZone(tzId);
        if (!tz.getID().equals(tzId)) {
            throw new IllegalArgumentException("Invalid TimeZone: " + tzId);
        }
        return tz;
    }

    public static Date parseDateUTC(String s) throws ParseException {
        return getISO8601DateFormat().parse(s);
    }

    public static String formatDateUTC(Date d) throws Exception {
        return (d != null) ? getISO8601DateFormat().format(d) : "NULL";
    }

    public static String formatDateCustom(Date d, String format) throws Exception {
        return (d != null) ? getSpecificDateFormat(format).format(d) : "NULL";
    }

    public static String formatDateUTC(Calendar c) throws Exception {
        return (c != null) ? formatDateUTC(c.getTime()) : "NULL";
    }

    /**
     * This function returns number of hour in a day when given a Calendar with appropriate TZ. It consider DST to find
     * the number of hours. Generally it is 24. At some tZ, in one day of a year it is 23 and another day it is 25
     *
     * @param cal: The date for which the number of hours is requested
     * @return number of hour in that day.
     */
    public static int hoursInDay(Calendar cal) {
        Calendar localCal = new GregorianCalendar(cal.getTimeZone());
        localCal.set(Calendar.MILLISECOND, 0);
        localCal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.get(Calendar.DAY_OF_MONTH), 0, 30, 0);
        localCal.add(Calendar.HOUR_OF_DAY, 24);
        switch (localCal.get(Calendar.HOUR_OF_DAY)) {
            case 1:
                return 23;
            case 23:
                return 25;
            default: // Case 0
                return 24;
        }
    }

    /**
     * Determine whether a specific date is on DST change day
     *
     * @param cal: Date to know if it is DST change day. Appropriate TZ is specified
     * @return true , if it DST change date otherwise false
     */
    public static boolean isDSTChangeDay(Calendar cal) {
        return hoursInDay(cal) != 24;
    }

    /**
     * Move the any date-time to the end of the duration. If endOfFlag == day, move the date to the end of day (24:00 on
     * the same day or 00:00 on the next day) If endOf Flag = month. move the date to then end of current month
     * Otherwise do nothing
     *
     * @param cal : Date-time needs to be moved to the end
     * @param endOfFlag : day (for end of day) or month (for end of month) or empty
     */
    public static void moveToEnd(Calendar cal, TimeUnit endOfFlag) {
        // TODO: Both logic needs to be checked
        if (endOfFlag == TimeUnit.END_OF_DAY) { // 24:00:00
            cal.add(Calendar.DAY_OF_MONTH, 1);
            // cal.set(Calendar.HOUR_OF_DAY, cal
            // .getActualMaximum(Calendar.HOUR_OF_DAY) + 1);// TODO:
            cal.set(Calendar.HOUR_OF_DAY, 0);
            cal.set(Calendar.MINUTE, 0);
            cal.set(Calendar.SECOND, 0);
        }
        else {
            if (endOfFlag == TimeUnit.END_OF_MONTH) {
                cal.add(Calendar.MONTH, 1);
                cal.set(Calendar.DAY_OF_MONTH, 1);
                cal.set(Calendar.HOUR_OF_DAY, 0);
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.SECOND, 0);
            }
        }
    }

    /**
     * Create a Calendar instance using the specified date and Time zone
     * @param dateString
     * @param tz : TimeZone
     * @return appropriate Calendar object
     * @throws Exception
     */
    public static Calendar getCalendar(String dateString, TimeZone tz) throws Exception {
        Date date = DateUtils.parseDateUTC(dateString);
        Calendar calDate = Calendar.getInstance();
        calDate.setTime(date);
        calDate.setTimeZone(tz);
        return calDate;
    }

    /**
     * Create a Calendar instance for UTC time zone using the specified date.
     * @param dateString
     * @return appropriate Calendar object
     * @throws Exception
     */
    public static Calendar getCalendar(String dateString) throws Exception {
        return getCalendar(dateString, DateUtils.getTimeZone("UTC"));
    }

    /**
     * Convert java.sql.Timestamp to java.util.Date
     *
     * @param timestamp java.sql.Timestamp
     * @return java.util.Date
     */
    public static java.util.Date toDate(java.sql.Timestamp timestamp) {
        if (timestamp != null) {
            long milliseconds = timestamp.getTime();
            return new java.util.Date(milliseconds);
        }
        return null;
    }

    /**
     * Convert java.util.Date to java.sql.Timestamp
     *
     * @param d java.util.Date
     * @return java.sql.Timestamp
     */
    public static Timestamp convertDateToTimestamp(Date d) {
        if (d != null) {
            return new Timestamp(d.getTime());
        }
        return null;
    }

    /**
     * Return the UTC date and time in W3C format down to second
     * (yyyy-MM-ddTHH:mmZ). i.e.: 1997-07-16T19:20:30Z
     *
     * @return the formatted time string.
     */
    public static String convertDateToString(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.format(date);
    }

    /**
     * Return the UTC date and time in W3C format down to second
     * (yyyy-MM-ddTHH:mmZ). i.e.: 1997-07-16T19:20Z The input date is a
     * long (Unix Time Stamp)
     *
     * @return the formatted time string.
     */
    public static String convertDateToString(long timeStamp) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.format(new Date(timeStamp));
    }

}
