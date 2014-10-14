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
package org.apache.oozie.sla;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.apache.oozie.AppType;
import org.apache.oozie.client.event.Event.MessageType;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.util.DateUtils;
import org.apache.openjpa.persistence.jdbc.Index;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

@Entity
@Table(name = "SLA_REGISTRATION")
@NamedQueries({

 @NamedQuery(name = "UPDATE_SLA_REG_ALL", query = "update SLARegistrationBean w set w.jobId = :jobId, w.nominalTimeTS = :nominalTime, w.expectedStartTS = :expectedStartTime, w.expectedEndTS = :expectedEndTime, w.expectedDuration = :expectedDuration, w.slaConfig = :slaConfig, w.notificationMsg = :notificationMsg, w.upstreamApps = :upstreamApps, w.appType = :appType, w.appName = :appName, w.user = :user, w.parentId = :parentId, w.jobData = :jobData where w.jobId = :jobId"),

 @NamedQuery(name = "GET_SLA_REG_ON_RESTART", query = "select w.notificationMsg, w.upstreamApps, w.slaConfig, w.jobData from SLARegistrationBean w where w.jobId = :id"),

 @NamedQuery(name = "GET_SLA_REG_ALL", query = "select OBJECT(w) from SLARegistrationBean w where w.jobId = :id") })
public class SLARegistrationBean implements JsonBean {

    @Id
    @Basic
    @Column(name = "job_id")
    private String jobId;

    @Basic
    @Column(name = "parent_id")
    private String parentId = null;

    @Basic
    @Column(name = "app_name")
    private String appName = null;

    @Basic
    @Column(name = "app_type")
    private String appType = null;

    @Basic
    @Column(name = "created_time")
    private Timestamp createdTimeTS = null;

    @Basic
    @Index
    @Column(name = "nominal_time")
    private Timestamp nominalTimeTS = null;

    @Basic
    @Column(name = "expected_start")
    private Timestamp expectedStartTS = null;

    @Basic
    @Column(name = "expected_end")
    private Timestamp expectedEndTS = null;

    @Basic
    @Column(name = "expected_duration")
    private long expectedDuration = -1;

    @Basic
    @Column(name = "user_name")
    private String user = null;

    @Basic
    @Column(name = "upstream_apps")
    private String upstreamApps = null;

    @Basic
    @Column(name = "job_data")
    private String jobData = null;

    @Basic
    @Column(name = "sla_config")
    private String slaConfig = null;

    @Basic
    @Column(name = "notification_msg")
    private String notificationMsg = null;

    @Transient
    private Map<String, String> slaConfigMap;

    @Transient
    private MessageType msgType;

    private final String ALERT_EVENTS = "alert_events";
    private final String ALERT_CONTACT = "alert_contact";

    public SLARegistrationBean() {
        slaConfigMap = new HashMap<String, String>();
        msgType = MessageType.SLA;
    }

    public SLARegistrationBean(JSONObject obj) {
        // TODO use JSONObject
        this();
    }

    public String getId() {
        return jobId;
    }

    public void setId(String jobId) {
        this.jobId = jobId;
    }

    public String getParentId() {
        return parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public AppType getAppType() {
        return AppType.valueOf(appType);
    }

    public void setAppType(AppType appType) {
        this.appType = appType.toString();
    }

    public Timestamp getCreatedTimestamp() {
        return createdTimeTS;
    }

    public void setCreatedTimestamp(Timestamp createdTime) {
        this.createdTimeTS = createdTime;
    }

    public Date getCreatedTime() {
        return DateUtils.toDate(createdTimeTS);
    }

    public void setCreatedTime(Date createdTime) {
        this.createdTimeTS = DateUtils.convertDateToTimestamp(createdTime);
    }

    public Date getNominalTime() {
        return DateUtils.toDate(nominalTimeTS);
    }

    public Timestamp getNominalTimestamp() {
        return this.nominalTimeTS;
    }

    public void setNominalTime(Date nominalTime) {
        this.nominalTimeTS = DateUtils.convertDateToTimestamp(nominalTime);
    }

    public Date getExpectedStart() {
        return DateUtils.toDate(expectedStartTS);
    }

    public Timestamp getExpectedStartTimestamp() {
        return this.expectedStartTS;
    }

    public void setExpectedStart(Date expectedStart) {
        this.expectedStartTS = DateUtils.convertDateToTimestamp(expectedStart);
    }

    public Date getExpectedEnd() {
        return DateUtils.toDate(expectedEndTS);
    }

    public Timestamp getExpectedEndTimestamp() {
        return this.expectedEndTS;
    }

    public void setExpectedEnd(Date expectedEnd) {
        this.expectedEndTS = DateUtils.convertDateToTimestamp(expectedEnd);
    }

    public long getExpectedDuration() {
        return expectedDuration;
    }

    public void setExpectedDuration(long expectedDuration) {
        this.expectedDuration = expectedDuration;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getUpstreamApps() {
        return upstreamApps;
    }

    public void setUpstreamApps(String upstreamApps) {
        this.upstreamApps = upstreamApps;
    }

    public String getJobData() {
        return jobData;
    }

    public void setJobData(String jobData) {
        this.jobData = jobData;
    }

    public String getSlaConfig() {
        return slaConfig;
    }

    public void setSlaConfig(String configStr) {
        this.slaConfig = configStr;
        slaConfigStringToMap();
    }

    public String getNotificationMsg() {
        return notificationMsg;
    }

    public void setNotificationMsg(String notificationMsg) {
        this.notificationMsg = notificationMsg;
    }

    public String getAlertEvents() {
        return slaConfigMap.get(ALERT_EVENTS);
    }

    public void setAlertEvents(String alertEvents) {
        slaConfigMap.put(ALERT_EVENTS, alertEvents);
        slaConfig = slaConfigMapToString();
    }

    public String getAlertContact() {
        return slaConfigMap.get(ALERT_CONTACT);
    }

    public void setAlertContact(String alertContact) {
        slaConfigMap.put(ALERT_CONTACT, alertContact);
        slaConfig = slaConfigMapToString();
    }

    public Map<String, String> getSlaConfigMap() {
        return slaConfigMap;
    }

    private void slaConfigStringToMap() {
        if (slaConfig != null) {
            String[] splitString = slaConfig.split("},");
            for (String config : splitString) {
                String[] pair = config.split("=");
                if (pair.length == 2) {
                    slaConfigMap.put(pair[0].substring(1), pair[1]);
                }
            }
        }
    }

    public String slaConfigMapToString() {
        StringBuilder sb = new StringBuilder();
        for (Entry<String, String> e : slaConfigMap.entrySet()) {
            sb.append("{" + e.getKey() + "=" + e.getValue() + "},");
        }
        return sb.toString();
    }

    @Override
    public JSONObject toJSONObject() {
        // TODO
        return null;
    }

    @Override
    public JSONObject toJSONObject(String timeZoneId) {
        // TODO
        return null;
    }

    /**
     * Convert a SLARegistrationBean list into a JSONArray.
     *
     * @param events SLARegistrationBean list.
     * @param timeZoneId time zone to use for dates in the JSON array.
     * @return the corresponding JSON array.
     */
    @SuppressWarnings("unchecked")
    public static JSONArray toJSONArray(List<? extends SLARegistrationBean> events, String timeZoneId) {
        JSONArray array = new JSONArray();
        if (events != null) {
            for (SLARegistrationBean node : events) {
                array.add(node.toJSONObject(timeZoneId));
            }
        }
        return array;
    }

    /**
     * Convert a JSONArray into a SLARegistrationBean list.
     *
     * @param array JSON array.
     * @return the corresponding SLA SLARegistrationBean list.
     */
    public static List<SLARegistrationBean> fromJSONArray(JSONArray array) {
        List<SLARegistrationBean> list = new ArrayList<SLARegistrationBean>();
        for (Object obj : array) {
            list.add(new SLARegistrationBean((JSONObject) obj));
        }
        return list;
    }

    public MessageType getMsgType(){
        return this.msgType;
    }

    public void setMsgType(MessageType msgType){
        this.msgType = msgType;
    }
}
