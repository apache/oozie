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

package org.apache.oozie.example;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

public class DateList {
    private static final TimeZone UTC = getTimeZone("UTC");
    private static String DATE_LIST_SEPARATOR = ",";

    public static void main(String[] args) throws IOException, ParseException {
        if (!checkArgsOk(args)) {
            System.exit(1);
        }

        String dateList = createDateListFromArgs(args);

        System.out.println("datelist :" + dateList+ ":");
        writeWorkflowOutput(dateList);
    }

    private static boolean checkArgsOk(final String[] args) {
        if (args.length < 5) {
            System.out
                    .println("Usage: java DateList <start_time>  <end_time> <frequency> <timeunit> <timezone>");
            System.out
                    .println("Example: java DateList 2009-02-01T01:00Z 2009-02-01T02:00Z 15 MINUTES UTC");
            return false;
        }

        return true;
    }

    private static String createDateListFromArgs(final String[] args) throws ParseException {
        final Date startTime = parseDateUTC(args[0]);
        final Date endTime = parseDateUTC(args[1]);
        final int frequency = Integer.parseInt(args[2]);
        final TimeUnit timeUnit = TimeUnit.valueOf(args[3]);
        final TimeZone timeZone = getTimeZone(args[4]);
        final Repeatable rep = createRepeatable(startTime, frequency, timeUnit, timeZone);

        return getDateList(startTime, endTime, rep);
    }

    private static Repeatable createRepeatable(final Date startTime, final int frequency,
                                               final TimeUnit timeUnit, final TimeZone timeZone) {
        Repeatable rep = new Repeatable();
        rep.setBaseline(startTime);
        rep.setFrequency(frequency);
        rep.setTimeUnit(timeUnit);
        rep.setTimeZone(timeZone);

        return rep;
    }

    private static String getDateList(final Date startTime, final Date endTime, final Repeatable rep) {
        int occurrence = 0;
        List<String> dates = new ArrayList<>();
        Date date = rep.getOccurrenceTime(startTime, occurrence++, null);

        while (date != null && date.before(endTime)) {
            dates.add(formatDateUTC(date));
            date = rep.getOccurrenceTime(startTime, occurrence++, null);
        }

        return String.join(DATE_LIST_SEPARATOR, dates);
    }

    private static void writeWorkflowOutput(final String dateList) throws IOException {
        //Passing the variable to WF that could be referred by subsequent actions
        File file = new File(System.getProperty("oozie.action.output.properties"));
        Properties props = new Properties();
        props.setProperty("datelist", dateList);
        try (OutputStream os = new FileOutputStream(file)) {
            props.store(os, "");
        }
    }

    //Utility methods
    private static DateFormat getISO8601DateFormat() {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        dateFormat.setTimeZone(UTC);
        return dateFormat;
    }

    private static TimeZone getTimeZone(String tzId) {
        TimeZone tz = TimeZone.getTimeZone(tzId);
        if (!tz.getID().equals(tzId)) {
            throw new IllegalArgumentException("Invalid TimeZone: " + tzId);
        }
        return tz;
    }

    private static Date parseDateUTC(String s) throws ParseException {
        return getISO8601DateFormat().parse(s);
    }

    private static String formatDateUTC(Date d) {
        return (d != null) ? getISO8601DateFormat().format(d) : "NULL";
    }
}
