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

package org.apache.oozie;

import com.google.common.base.Strings;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.oozie.servlet.XServletException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import java.util.StringTokenizer;

public class FilterParser {

    private static final String PARAMETER_DELIMITER = ";";
    private static final String PARAMETER_EQUALS = "=";

    /**
     * Parse filter string to a map with key = filter name and values = filter values
     *
     * @param filterString the filter string
     * @return filter key and values map
     * @throws ServletException thrown if failed to parse filter string
     */
    public static ListMultimap<String, String> parseFilter(String filterString) throws ServletException {
        ListMultimap<String, String> filterFieldMap = LinkedListMultimap.create();
        if (filterString != null) {
            StringTokenizer st = new StringTokenizer(filterString, PARAMETER_DELIMITER);
            while (st.hasMoreTokens()) {
                String token = st.nextToken();
                if (token.contains(PARAMETER_EQUALS)) {
                    String[] nameValuePair = token.split(PARAMETER_EQUALS);
                    if (nameValuePair.length != 2) {
                        filterFormatError(filterString);
                    }
                    String key = nameValuePair[0];
                    String value = nameValuePair[1];
                    if (Strings.isNullOrEmpty(key)) {
                        filterFormatError(filterString);
                    }
                    filterFieldMap.put(key, value);
                }
                else {
                    filterFormatError(filterString);
                }
            }
        }
        return filterFieldMap;
    }

    private static void filterFormatError(String filterString) throws ServletException {
        throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0401,
                String.format("elements must be semicolon-separated name=value pairs but was %s", filterString));

    }
}
