/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
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

    /**
     * Return a long value from a JSONObject.
     *
     * @param map JSON object.
     * @param name name of the property.
     * @return the long value associated with it, or 0 if not defined.
     */
    public static long getLongValue(JSONObject map, String name) {
        Long l = (Long) map.get(name);
        return (l != null) ? l : 0;
    }

    /**
     * Return a List<String> value from a JSONObject.
     *
     * @param map JSON object.
     * @param name name of the property.
     * @return the List<String> value associated with it, or null if not defined.
     */
    public static List<String> getListString(JSONObject json, String name) {
        ArrayList<String> values = new ArrayList();
        JSONArray array = (JSONArray) json.get(name);
        if (array == null) {
            return null;
        }

        for (Object o : array) {
            values.add((String) o);
        }
        return values;
    }

}
