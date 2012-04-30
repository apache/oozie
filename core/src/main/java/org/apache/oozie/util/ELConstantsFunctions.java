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

import org.apache.hadoop.conf.Configuration;
import org.jdom.Element;
import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.Date;
import java.net.URLEncoder;
import java.io.UnsupportedEncodingException;

/**
 * Base EL constants and functions.
 */
public class ELConstantsFunctions {

    /**
     * KiloByte constant (1024). Defined for EL as 'KB'.
     */
    public static final long KB = 1024;

    /**
     * MegaByte constant (1024 KB). Defined for EL as 'MB'.
     */
    public static final long MB = KB * 1024;

    /**
     * GigaByte constant (1024 MB). Defined for EL as 'GB'.
     */
    public static final long GB = MB * 1024;

    /**
     * TeraByte constant (1024 GB). Defined for EL as 'TB'.
     */
    public static final long TB = GB * 1024;

    /**
     * PetaByte constant (1024 TB). Defined for EL as 'PB'.
     */
    public static final long PB = TB * 1024;

    public static final int SUBMIT_MINUTES = 1;
    public static final int SUBMIT_HOURS = 60;
    public static final int SUBMIT_DAYS = 24 * 60;

    /**
     * Return the first not <code>null</code> value, or <code>null</code> if both are <code>null</code>. Defined for EL
     * as 'Object firstNotNull(Object, Object)'.
     *
     * @param o1 first value.
     * @param o2 second value.
     * @return the first not <code>null</code> value, or or <code>null</code> if both are <code>null</code>
     */
    public static Object firstNotNull(Object o1, Object o2) {
        return (o1 != null) ? o1 : o2;
    }

    /**
     * Return the concatenation of 2 strings. <p/> A string with <code>null</code> value is considered as an empty
     * string.
     *
     * @param s1 first string.
     * @param s2 second string.
     * @return the concatenation of <code>s1</code> and <code>s2</code>.
     */
    public static String concat(String s1, String s2) {
        StringBuilder sb = new StringBuilder();
        if (s1 != null) {
            sb.append(s1);
        }
        if (s2 != null) {
            sb.append(s2);
        }
        return sb.toString();
    }

    /**
     * Return the trimmed version of the given string.
     *
     * @param input string to be trimmed
     * @return the trimmed version of the given string or the empty string if the given string was <code>null</code>
     */
    public static String trim(String input) {
        return (input == null) ? "" : input.trim();
    }

    /**
     * Return the UTC current date and time in W3C format down to second (yyyy-MM-ddTHH:mm:ssZ). i.e.:
     * 1997-07-16T19:20:30Z
     *
     * @return the formatted time string.
     */
    public static String timestamp() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.format(new Date());
    }

    /**
     * Translates a string into <code>application/x-www-form-urlencoded</code> format using UTF-8 encoding scheme. Bytes
     * for unsafe characters are also obtained using UTF-8 scheme.
     *
     * @param input string to be encoded
     * @return the encoded <code>String</code>
     */
    public static String urlEncode(String input) {
        try {
            return (input == null) ? "" : URLEncoder.encode(input, "UTF-8");
        }
        catch (UnsupportedEncodingException uee) {
            throw new RuntimeException("It should never happen");
        }
    }

    public static String toJsonStr(Map<String, String> map) {
        JSONObject json = new JSONObject(map);
        return XmlUtils.escapeCharsForXML(json.toString());
    }

    public static String toPropertiesStr(Map<String, String> map) {
        Properties props = new Properties();
        for (Map.Entry<String, String> entry: map.entrySet()) {
            props.setProperty(entry.getKey(), entry.getValue());
        }
        return XmlUtils.escapeCharsForXML(PropertiesUtils.propertiesToString(props));
    }

    public static String toConfigurationStr(Map<String, String> map) {
        Configuration conf = new Configuration(false);
        for (Map.Entry<String, String> entry: map.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        return XmlUtils.escapeCharsForXML(XmlUtils.prettyPrint(conf).toString());
    }

}
