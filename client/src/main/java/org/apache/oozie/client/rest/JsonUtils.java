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

package org.apache.oozie.client.rest;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


/**
 * Json utils methods.
 */
public class JsonUtils {

    /*
     * GMT is the most commonly used timezone; we can save on parsing and
     * creating a TimeZone by creating a GMT TimeZone once
     */
    private static TimeZone GMT_TZ = TimeZone.getTimeZone("GMT");
    /**
     * Format a Date in RFC822 with the given time zone.
     *
     * @param date date to format.
     * @param timeZoneId the time zone to use
     * @return RFC822 for the date, &lt;code&gt;null&lt;/code&gt; if the date was &lt;code&gt;null&lt;/code&gt;.
     */
    public static String formatDateRfc822(Date date, String timeZoneId) {
        if (date != null) {
            TimeZone tZone = "GMT".equals(timeZoneId) ? GMT_TZ : TimeZone.getTimeZone(timeZoneId);
            SimpleDateFormat dateFormater = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);
            dateFormater.setTimeZone(tZone);
            return dateFormater.format(date);
        }
        return null;
    }

    /**
     * Format a Date in RFC822 GMT.
     *
     * @param date date to format.
     * @return RFC822 GMT for the date, &lt;code&gt;null&lt;/code&gt; if the date was &lt;code&gt;null&lt;/code&gt;.
     */
    public static String formatDateRfc822(Date date) {
        return formatDateRfc822(date, "GMT");
    }

    /**
     * Parse a string in RFC822 GMT format.
     *
     * @param str string to parse.
     * @return parsed date, &lt;code&gt;null&lt;/code&gt; if the string was &lt;code&gt;null&lt;/code&gt; or in an invalid format.
     */
    public static Date parseDateRfc822(String str) {
        if (str != null) {
            try {
                SimpleDateFormat dateFormater = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.US);
                dateFormater.setTimeZone(GMT_TZ);
                return dateFormater.parse(str);
            }
            catch (ParseException ex) {
                return null;
            }
        }
        return null;
    }

}
