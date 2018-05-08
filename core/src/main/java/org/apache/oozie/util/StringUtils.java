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

/**
 * Utility methods for working with {@link String} objects.
 */
public class StringUtils {
    /**
     * Trims leading and trailing whitespaces of a {@link String} object. Also replaces new lines characters and tab characters by
     * empty Strings.
     *
     * @param str the {@link String} to trim
     * @return the trimmed {@link String}
     */
    public static String trim(String str) {
        if (str != null) {
            str = str.replaceAll("\\n", "");
            str = str.replaceAll("\\t", "");
            str = str.trim();
        }
        return str;
    }

    /**
     * Return the internalized string, or null if the given string is null.
     * @param str The string to intern
     * @return The identical string cached in the JVM string pool.
     */
    public static String intern(String str) {
        if (str == null) {
            return null;
        }
        return str.intern();
    }
}

