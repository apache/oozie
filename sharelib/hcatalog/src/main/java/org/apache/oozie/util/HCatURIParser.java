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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HCatURIParser {

    public static String[] splitHCatUris(String uri, Pattern pattern) {
        List<String> list = new ArrayList<>();
        Matcher matcher = pattern.matcher(uri);
        while (matcher.find()) {
            String s = matcher.group();
            list.add(s);
        }
        return list.toArray(new String[list.size()]);
    }

    static URI parseURI(URI uri) throws URISyntaxException {
        String uriStr = uri.toString();
        int index = uriStr.indexOf("://");
        String scheme = uriStr.substring(0, index + 3);
        uriStr = uriStr.replaceAll(scheme, "");
        uriStr = scheme.concat(uriStr);
        return new URI(uriStr);
    }
}