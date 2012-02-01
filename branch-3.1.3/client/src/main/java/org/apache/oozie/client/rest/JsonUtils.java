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

    /**
     * Format a Date in RFC822 GMT.
     *
     * @param date date to format.
     * @return RFC822 GMT for the date, <code>null</code> if the date was <code>null</null>.
     */
    public static String formatDateRfc822(Date date) {
        if (date != null) {
            SimpleDateFormat dateFormater = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.US);
            dateFormater.setTimeZone(TimeZone.getTimeZone("GMT"));
            return dateFormater.format(date);
        }
        return null;
    }

    /**
     * Parse a string in RFC822 GMT format.
     *
     * @param str string to parse.
     * @return parsed date, <code>null</code> if the string was <code>null</null> or in an invalid format.
     */
    static Date parseDateRfc822(String str) {
        if (str != null) {
            try {
                SimpleDateFormat dateFormater = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.US);
                dateFormater.setTimeZone(TimeZone.getTimeZone("GMT"));
                return dateFormater.parse(str);
            }
            catch (ParseException ex) {
                return null;
            }
        }
        return null;
    }

}
