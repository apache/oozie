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
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;

public class DateList {
	private static final TimeZone UTC = getTimeZone("UTC");
	private static String DATE_LIST_SEPARATOR = ",";

	public static void main(String[] args) throws Exception {
		if (args.length < 5) {
			System.out
					.println("Usage: java DateList <start_time>  <end_time> <frequency> <timeunit> <timezone>");
			System.out
					.println("Example: java DateList 2009-02-01T01:00Z 2009-02-01T02:00Z 15 MINUTES UTC");
			System.exit(1);
		}
		Date startTime = parseDateUTC(args[0]);
		Date endTime = parseDateUTC(args[1]);
		Repeatable rep = new Repeatable();
		rep.setBaseline(startTime);
		rep.setFrequency(Integer.parseInt(args[2]));
		rep.setTimeUnit(TimeUnit.valueOf(args[3]));
		rep.setTimeZone(getTimeZone(args[4]));
		Date date = null;
		int occurrence = 0;
		StringBuilder dateList = new StringBuilder();
		do {
			date = rep.getOccurrenceTime(startTime, occurrence++, null);
			if (!date.before(endTime)) {
				break;
			}
			if (occurrence > 1) {
				dateList.append(DATE_LIST_SEPARATOR);
			}
			dateList.append(formatDateUTC(date));
		} while (date != null);

		System.out.println("datelist :" + dateList+ ":");
		//Passing the variable to WF that could be referred by subsequent actions
		File file = new File(System.getProperty("oozie.action.output.properties"));
		Properties props = new Properties();
		props.setProperty("datelist", dateList.toString());
		OutputStream os = new FileOutputStream(file);
        	props.store(os, "");
        	os.close();
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

	private static Date parseDateUTC(String s) throws Exception {
		return getISO8601DateFormat().parse(s);
	}
	private static String formatDateUTC(Date d) throws Exception {
		return (d != null) ? getISO8601DateFormat().format(d) : "NULL";
	}

	private static String formatDateUTC(Calendar c) throws Exception {
		return (c != null) ? formatDateUTC(c.getTime()) : "NULL";
	}

}
