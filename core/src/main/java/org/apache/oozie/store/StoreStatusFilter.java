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

package org.apache.oozie.store;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.XLog;

public class StoreStatusFilter {
    public static final String coordSeletStr = "Select w.id, w.appName, w.statusStr, w.user, w.group, w.startTimestamp, " +
            "w.endTimestamp, w.appPath, w.concurrency, w.frequency, w.lastActionTimestamp, w.nextMaterializedTimestamp, " +
            "w.createdTimestamp, w.timeUnitStr, w.timeZone, w.timeOut, w.bundleId from CoordinatorJobBean w";

    public static final String coordCountStr = "Select count(w) from CoordinatorJobBean w";

    public static final String wfSeletStr = "Select w.id, w.appName, w.statusStr, w.run, w.user, w.group, w.createdTimestamp, " +
            "w.startTimestamp, w.lastModifiedTimestamp, w.endTimestamp from WorkflowJobBean w";

    public static final String wfCountStr = "Select count(w) from WorkflowJobBean w";

    public static final String bundleSeletStr = "Select w.id, w.appName, w.appPath, w.conf, w.statusStr, w.kickoffTimestamp, " +
            "w.startTimestamp, w.endTimestamp, w.pauseTimestamp, w.createdTimestamp, w.user, w.group, w.timeUnitStr, " +
            "w.timeOut from BundleJobBean w";

    public static final String bundleCountStr = "Select count(w) from BundleJobBean w";

    public static final String TIME_FORMAT = " Specify time either in UTC format (yyyy-MM-dd'T'HH:mm'Z') or " +
            "a offset value in days/hours/minutes e.g. (-2d/h/m) from the current time.";

    private static final String textFilterStr = "(w.appName LIKE :text1 OR w.user LIKE :text2 OR w.id = :text3)";


    public static void filter(Map<String, List<String>> filter, List<String> orArray, List<String> colArray,
           List<Object> valArray, StringBuilder sb, String seletStr, String countStr) throws JPAExecutorException {
        boolean isStatus = false;
        boolean isAppName = false;
        boolean isUser = false;
        boolean isEnabled = false;
        boolean isFrequency = false;
        boolean isId = false;
        boolean isUnit = false;

        int index = 0;

        for (Map.Entry<String, List<String>> entry : filter.entrySet()) {
            String colName = null;
            String colVar = null;
            if (entry.getKey().equals(OozieClient.FILTER_GROUP)) {
                XLog.getLog(StoreStatusFilter.class).warn("Filter by 'group' is not supported anymore");
            }
            else {
                if (entry.getKey().equals(OozieClient.FILTER_STATUS)) {
                    List<String> values = filter.get(OozieClient.FILTER_STATUS);
                    colName = "status";
                    for (int i = 0; i < values.size(); i++) {
                        colVar = "status";
                        colVar = colVar + index;
                        if (!isEnabled && !isStatus) {
                            sb.append(seletStr).append(" where w.statusStr IN (:status" + index);
                            isStatus = true;
                            isEnabled = true;
                        }
                        else {
                            if (isEnabled && !isStatus) {
                                sb.append(" and w.statusStr IN (:status" + index);
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
                        else if (entry.getKey().equals(OozieClient.FILTER_FREQUENCY)) {
                            List<String> values = filter.get(OozieClient.FILTER_FREQUENCY);
                            colName = "frequency";
                            for (int i = 0; i < values.size(); i++) {
                                colVar = "frequency";
                                colVar = colVar + index;
                                if (!isEnabled && !isFrequency) {
                                    sb.append(seletStr).append(" where w.frequency IN (:frequency" + index);
                                    isFrequency = true;
                                    isEnabled = true;
                                }
                                else {
                                    if (isEnabled && !isFrequency) {
                                        sb.append(" and w.frequency IN (:frequency" + index);
                                        isFrequency = true;
                                    }
                                    else {
                                        if (isFrequency) {
                                            sb.append(", :frequency" + index);
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
                        else if (entry.getKey().equals(OozieClient.FILTER_ID)) {
                            List<String> values = filter.get(OozieClient.FILTER_ID);
                            colName = "id";
                            for (int i = 0; i < values.size(); i++) {
                                colVar = "id";
                                colVar = colVar + index;
                                if (!isEnabled && !isId) {
                                    sb.append(seletStr).append(" where w.id IN (:id" + index);
                                    isId = true;
                                    isEnabled = true;
                                }
                                else {
                                    if (isEnabled && !isId) {
                                        sb.append(" and w.id IN (:id" + index);
                                        isId = true;
                                    }
                                    else {
                                        if (isId) {
                                            sb.append(", :id" + index);
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
                        // Filter map has time unit filter specified
                        else if (entry.getKey().equals(OozieClient.FILTER_UNIT)) {
                            List<String> values = filter.get(OozieClient.FILTER_UNIT);
                            colName = "timeUnitStr";
                            for (int i = 0; i < values.size(); ++i) {
                                colVar = colName + index;
                                // This unit filter value is the first condition to be added to the where clause of
                                // query
                                if (!isEnabled && !isUnit) {
                                    sb.append(seletStr).append(" where w.timeUnitStr IN (:timeUnitStr" + index);
                                    isUnit = true;
                                    isEnabled = true;
                                } else {
                                    // Unit filter is neither the first nor the last condition to be added to the where
                                    // clause of query
                                    if (isEnabled && !isUnit) {
                                        sb.append(" and w.timeUnitStr IN (:timeUnitStr" + index);
                                        isUnit = true;
                                    } else {
                                        if (isUnit) {
                                            sb.append(", :timeUnitStr" + index);
                                        }
                                    }
                                }
                                // This unit filter value is the last condition to be added to the where clause of query
                                if (i == values.size() - 1) {
                                    sb.append(")");
                                }
                                ++index;
                                valArray.add(values.get(i));
                                orArray.add(colName);
                                colArray.add(colVar);
                            }
                        }
                        else if (entry.getKey().equalsIgnoreCase(OozieClient.FILTER_CREATED_TIME_START)) {
                            List<String> values = filter.get(OozieClient.FILTER_CREATED_TIME_START);
                            colName = "createdTimestampStart";
                            if (values.size() > 1) {
                                throw new JPAExecutorException(ErrorCode.E0302,
                                        "cannot specify multiple startcreatedtime");
                            }
                            colVar = colName;
                            colVar = colVar + index;
                            if (!isEnabled) {
                                sb.append(seletStr).append(" where w.createdTimestamp >= :" + colVar);
                                isEnabled = true;
                            }
                            else {
                                sb.append(" and w.createdTimestamp >= :" + colVar);
                            }
                            index++;
                            Date createdTime = null;
                            try {
                                createdTime = parseCreatedTimeString(values.get(0));
                            }
                            catch (Exception e) {
                                throw new JPAExecutorException(ErrorCode.E0302, e.getMessage());
                            }
                            Timestamp createdTimeStamp = new Timestamp(createdTime.getTime());
                            valArray.add(createdTimeStamp);
                            orArray.add(colName);
                            colArray.add(colVar);

                        }
                        else if (entry.getKey().equalsIgnoreCase(OozieClient.FILTER_CREATED_TIME_END)) {
                            List<String> values = filter.get(OozieClient.FILTER_CREATED_TIME_END);
                            colName = "createdTimestampEnd";
                            if (values.size() > 1) {
                                throw new JPAExecutorException(ErrorCode.E0302,
                                        "cannot specify multiple endcreatedtime");
                            }
                            colVar = colName;
                            colVar = colVar + index;
                            if (!isEnabled) {
                                sb.append(seletStr).append(" where w.createdTimestamp <= :" + colVar);
                                isEnabled = true;
                            }
                            else {
                                sb.append(" and w.createdTimestamp <= :" + colVar);
                            }
                            index++;
                            Date createdTime = null;
                            try {
                                createdTime = parseCreatedTimeString(values.get(0));
                            }
                            catch (Exception e) {
                                throw new JPAExecutorException(ErrorCode.E0302, e.getMessage());
                            }
                            Timestamp createdTimeStamp = new Timestamp(createdTime.getTime());
                            valArray.add(createdTimeStamp);
                            orArray.add(colName);
                            colArray.add(colVar);
                        }
                        // job.id = text || job.appName.contains(text) || job.user.contains(text)
                        else if (entry.getKey().equalsIgnoreCase(OozieClient.FILTER_TEXT)) {
                            filterJobsUsingText(filter, sb, isEnabled, seletStr, valArray, orArray, colArray);
                            isEnabled = true;
                        }
                    }
                }
            }
        }
    }

    private static Date parseCreatedTimeString(String time) throws Exception{
        Date createdTime = null;
        int offset = 0;
        if (Character.isLetter(time.charAt(time.length() - 1))) {
            switch (time.charAt(time.length() - 1)) {
                case 'd':
                    offset = Integer.parseInt(time.substring(0, time.length() - 1));
                    if(offset > 0) {
                        throw new IllegalArgumentException("offset must be minus from currentTime.");
                    }
                    createdTime = org.apache.commons.lang.time.DateUtils.addDays(new Date(), offset);
                    break;
                case 'h':
                    offset =  Integer.parseInt(time.substring(0, time.length() - 1));
                    if(offset > 0) {
                        throw new IllegalArgumentException("offset must be minus from currentTime.");
                    }
                    createdTime = org.apache.commons.lang.time.DateUtils.addHours(new Date(), offset);
                    break;
                case 'm':
                    offset =  Integer.parseInt(time.substring(0, time.length() - 1));
                    if(offset > 0) {
                        throw new IllegalArgumentException("offset must be minus from currentTime.");
                    }
                    createdTime = org.apache.commons.lang.time.DateUtils.addMinutes(new Date(), offset);
                    break;
                case 'Z':
                    createdTime = DateUtils.parseDateUTC(time);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported time format: " + time + TIME_FORMAT);
            }
        } else {
            throw new IllegalArgumentException("The format of time is wrong: " + time + TIME_FORMAT);
        }
        return createdTime;
    }

    public static String getSortBy(Map<String, List<String>> filter, String sortByStr) throws JPAExecutorException {
        if (filter.containsKey(OozieClient.FILTER_SORT_BY)) {
            List<String> values = filter.get(OozieClient.FILTER_SORT_BY);
            if (values.size() > 1) {
                throw new JPAExecutorException(ErrorCode.E0302,
                        "cannot specify multiple sortby parameter");
            }
            String value = values.get(0);
            for (OozieClient.SORT_BY sortBy : OozieClient.SORT_BY.values()) {
                if (sortBy.toString().equalsIgnoreCase(value)) {
                    value = sortBy.getFullname();
                    sortByStr = " order by w.".concat(value).concat(" desc ");
                    break;
                }
            }
        }
        return sortByStr;
    }

    public static void filterJobsUsingText(Map<String, List<String>> filter, StringBuilder sb, boolean isEnabled,
           String seletStr, List<Object> valArray, List<String> orArray, List<String> colArray) throws JPAExecutorException {
        List<String> values = filter.get(OozieClient.FILTER_TEXT);
        if (values.size() != 1) {
            throw new JPAExecutorException(ErrorCode.E0302,
                    "cannot specify multiple strings to search");
        }
        if (!isEnabled) {
            sb.append(seletStr).append(" where " + textFilterStr);
        }
        else {
            sb.append(" and " + textFilterStr);
        }

        valArray.add("%" + values.get(0) + "%");
        orArray.add("appName");
        colArray.add("text1");

        valArray.add("%" + values.get(0) + "%");
        orArray.add("user");
        colArray.add("text2");

        valArray.add(values.get(0));
        orArray.add("id");
        colArray.add("text3");
    }
}
