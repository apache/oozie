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
package org.apache.oozie.client.rest.sla;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.DiscriminatorColumn;
import javax.persistence.DiscriminatorType;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.apache.oozie.AppType;
import org.apache.oozie.client.event.SLAEvent;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.openjpa.persistence.jdbc.Index;
import org.json.simple.JSONObject;

@Entity
@Table(name = "SLA_REGISTRATION")
public class JsonSLARegistrationEvent extends SLAEvent implements JsonBean {

    @Id
    @Basic
    @Column(name = "job_id")
    private String jobId;

    @Transient
    private AppType appType = null;

    @Basic
    @Column(name = "app_name")
    private String appName = null;

    @Basic
    @Column(name = "user_name")
    private String user = null;

    @Transient
    private Date nominalTime = null;

    @Transient
    private Date expectedStart = null;

    @Transient
    private Date expectedEnd = null;

    @Basic
    @Column(name = "expected_duration")
    private long expectedDuration = 0;

    @Basic
    @Column(name = "job_data")
    private String jobData = null;

    @Basic
    @Column(name = "parent_id")
    private String parentId = null;

    @Basic
    @Column(name = "notification_msg")
    private String notificationMsg = null;

    @Basic
    @Column(name = "upstream_apps")
    private String upstreamApps = null;

    @Basic
    @Column(name = "sla_config")
    private String slaConfig = null;

    @Transient
    private Map<String, String> slaConfigMap;
    private final String ALERT_EVENTS = "alert_events";
    private final String ALERT_CONTACT = "alert_contact";

    public JsonSLARegistrationEvent() {
        slaConfigMap = new HashMap<String, String>();
        msgType = MessageType.SLA;
    }

    public JsonSLARegistrationEvent(JSONObject json) {
        this();
        // TODO read jsonobject
    }

    @Override
    public String getId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    @Override
    public AppType getAppType() {
        return appType;
    }

    public void setAppType(AppType appType) {
        this.appType = appType;
    }

    @Override
    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    @Override
    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    @Override
    public Date getNominalTime() {
        return nominalTime;
    }

    public void setNominalTime(Date nomTime) {
        this.nominalTime = nomTime;
    }

    @Override
    public Date getExpectedStart() {
        return expectedStart;
    }

    public void setExpectedStart(Date expectedStart) {
        this.expectedStart = expectedStart;
    }

    @Override
    public Date getExpectedEnd() {
        return expectedEnd;
    }

    public void setExpectedEnd(Date expectedEnd) {
        this.expectedEnd = expectedEnd;
    }

    @Override
    public long getExpectedDuration() {
        return expectedDuration;
    }

    public void setExpectedDuration(long expectedDuration) {
        this.expectedDuration = expectedDuration;
    }

    @Override
    public String getParentId() {
        return parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    @Override
    public String getNotificationMsg() {
        return notificationMsg;
    }

    public void setNotificationMsg(String notificationMsg) {
        this.notificationMsg = notificationMsg;
    }

    @Override
    public String getAlertEvents() {
        return slaConfigMap.get(ALERT_EVENTS);
    }

    public void setAlertEvents(String alertEvents) {
        slaConfigMap.put(ALERT_EVENTS, alertEvents);
        slaConfig = slaConfigMapToString();
    }

    @Override
    public String getAlertContact() {
        return slaConfigMap.get(ALERT_CONTACT);
    }

    public void setAlertContact(String alertContact) {
        slaConfigMap.put(ALERT_CONTACT, alertContact);
        slaConfig = slaConfigMapToString();
    }

    @Override
    public String getUpstreamApps() {
        return upstreamApps;
    }

    public void setUpstreamApps(String upstreamApps) {
        this.upstreamApps = upstreamApps;
    }

    @Override
    public String getJobData() {
        return jobData;
    }

    public void setJobData(String jobData) {
        this.jobData = jobData;
    }

    public Map<String, String> getSlaConfigMap() {
        return slaConfigMap;
    }

    @Override
    public String getSlaConfig() {
        return slaConfig;
    }

    public void setSlaConfig(String configStr) {
        this.slaConfig = configStr;
        slaConfigStringToMap();
    }

    @Override
    public SLAStatus getSLAStatus() {
        return null;
    }

    @Override
    public EventStatus getEventStatus() {
        return null;
    }

    @Override
    public Date getActualStart() {
        return null;
    }

    @Override
    public Date getActualEnd() {
        return null;
    }

    @Override
    public long getActualDuration() {
        return 0;
    }

    @Override
    public String getJobStatus() {
        return null;
    }

    private void slaConfigStringToMap() {
        StringTokenizer st = new StringTokenizer(slaConfig, "},");
        while (st.hasMoreTokens()) {
            String token = st.nextToken();
            String[] pair = token.split("=");
            if (pair.length == 2) {
                slaConfigMap.put(pair[0].substring(1), pair[1]);
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

}
