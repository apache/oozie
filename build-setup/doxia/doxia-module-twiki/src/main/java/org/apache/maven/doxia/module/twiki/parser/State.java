/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.maven.doxia.module.twiki.parser;

import org.apache.maven.doxia.sink.Sink;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

public class State {

    private static Map titleMap = new HashMap();
    private static boolean isVerbatimMode = false;
    private static boolean isAutoLinkEnabled = true;

    public static void init() {
        titleMap.clear();
        isVerbatimMode = false;
        isAutoLinkEnabled = true;
    }

    static void setVerbatimMode() {
        isVerbatimMode = true;
    }

    static void clearVerbatimMode() {
        isVerbatimMode = false;
    }

    static boolean isVerbatimMode() {
        return isVerbatimMode;
    }

    static boolean isInTitleList(Sink sink, String title) {
        List titleList = (List)titleMap.get(sink);
        if(titleList == null) {
            return false;
        }
        return titleList.contains(title);
    }

    static void addToTitleList(Sink sink, String title) {
        List titleList = (List)titleMap.get(sink);
        if(titleList == null) {
            titleList = new ArrayList();
            titleMap.put(sink, titleList);
        }
        titleList.add(title);
    }

    static void disableAutoLinking() {
        isAutoLinkEnabled = false;
    }

    static void enableAutoLinking() {
        isAutoLinkEnabled = true;
    }

    static boolean isAutoLinkEnabled() {
        return isAutoLinkEnabled;
    }

}
