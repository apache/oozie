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
package org.apache.oozie.store;

import java.util.List;
import java.util.Map;

import org.apache.oozie.client.OozieClient;

public class StoreStatusFilter {
    public static final String coordSeletStr = "Select w.id, w.appName, w.status, w.user, w.group, w.startTimestamp, w.endTimestamp, w.appPath, w.concurrency, w.frequency, w.lastActionTimestamp, w.nextMaterializedTimestamp, w.createdTimestamp, w.timeUnitStr, w.timeZone, w.timeOut from CoordinatorJobBean w";

    public static final String coordCountStr = "Select count(w) from CoordinatorJobBean w";

    public static final String wfSeletStr = "Select w.id, w.appName, w.status, w.run, w.user, w.group, w.createdTimestamp, w.startTimestamp, w.lastModifiedTimestamp, w.endTimestamp from WorkflowJobBean w";

    public static final String wfCountStr = "Select count(w) from WorkflowJobBean w";

    public static void filter(Map<String, List<String>> filter, List<String> orArray, List<String> colArray, List<String> valArray, StringBuilder sb, String seletStr, String countStr) {
        boolean isStatus = false;
        boolean isGroup = false;
        boolean isAppName = false;
        boolean isUser = false;
        boolean isEnabled = false;

        int index = 0;

        for (Map.Entry<String, List<String>> entry : filter.entrySet()) {
            String colName = null;
            String colVar = null;
            if (entry.getKey().equals(OozieClient.FILTER_GROUP)) {
                List<String> values = filter.get(OozieClient.FILTER_GROUP);
                colName = "group";
                for (int i = 0; i < values.size(); i++) {
                    colVar = "group";
                    colVar = colVar + index;
                    if (!isEnabled && !isGroup) {
                        sb.append(seletStr).append(" where w.group IN (:group" + index);
                        isGroup = true;
                        isEnabled = true;
                    }
                    else {
                        if (isEnabled && !isGroup) {
                            sb.append(" and w.group IN (:group" + index);
                            isGroup = true;
                        }
                        else {
                            if (isGroup) {
                                sb.append(", :group" + index);
                            }
                        }
                    }
                    if (i == values.size() - 1) {
                        sb.append(")");
                    }
                    index++;
                    valArray.add(values.get(i));
                    orArray.add(colName);
                    colArray.add(colVar);
                }
            }
            else {
                if (entry.getKey().equals(OozieClient.FILTER_STATUS)) {
                    List<String> values = filter.get(OozieClient.FILTER_STATUS);
                    colName = "status";
                    for (int i = 0; i < values.size(); i++) {
                        colVar = "status";
                        colVar = colVar + index;
                        if (!isEnabled && !isStatus) {
                            sb.append(seletStr).append(" where w.status IN (:status" + index);
                            isStatus = true;
                            isEnabled = true;
                        }
                        else {
                            if (isEnabled && !isStatus) {
                                sb.append(" and w.status IN (:status" + index);
                                isStatus = true;
                            }
                            else {
                                if (isStatus) {
                                    sb.append(", :status" + index);
                                }
                            }
                        }
                        if (i == values.size() - 1) {
                            sb.append(")");
                        }
                        index++;
                        valArray.add(values.get(i));
                        orArray.add(colName);
                        colArray.add(colVar);
                    }
                }
                else {
                    if (entry.getKey().equals(OozieClient.FILTER_NAME)) {
                        List<String> values = filter.get(OozieClient.FILTER_NAME);
                        colName = "appName";
                        for (int i = 0; i < values.size(); i++) {
                            colVar = "appName";
                            colVar = colVar + index;
                            if (!isEnabled && !isAppName) {
                                sb.append(seletStr).append(" where w.appName IN (:appName" + index);
                                isAppName = true;
                                isEnabled = true;
                            }
                            else {
                                if (isEnabled && !isAppName) {
                                    sb.append(" and w.appName IN (:appName" + index);
                                    isAppName = true;
                                }
                                else {
                                    if (isAppName) {
                                        sb.append(", :appName" + index);
                                    }
                                }
                            }
                            if (i == values.size() - 1) {
                                sb.append(")");
                            }
                            index++;
                            valArray.add(values.get(i));
                            orArray.add(colName);
                            colArray.add(colVar);
                        }
                    }
                    else {
                        if (entry.getKey().equals(OozieClient.FILTER_USER)) {
                            List<String> values = filter.get(OozieClient.FILTER_USER);
                            colName = "user";
                            for (int i = 0; i < values.size(); i++) {
                                colVar = "user";
                                colVar = colVar + index;
                                if (!isEnabled && !isUser) {
                                    sb.append(seletStr).append(" where w.user IN (:user" + index);
                                    isUser = true;
                                    isEnabled = true;
                                }
                                else {
                                    if (isEnabled && !isUser) {
                                        sb.append(" and w.user IN (:user" + index);
                                        isUser = true;
                                    }
                                    else {
                                        if (isUser) {
                                            sb.append(", :user" + index);
                                        }
                                    }
                                }
                                if (i == values.size() - 1) {
                                    sb.append(")");
                                }
                                index++;
                                valArray.add(values.get(i));
                                orArray.add(colName);
                                colArray.add(colVar);
                            }
                        }
                    }
                }
            }
        }
    }
}
