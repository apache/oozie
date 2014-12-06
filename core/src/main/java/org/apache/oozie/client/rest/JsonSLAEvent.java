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

package org.apache.oozie.client.rest;

import java.util.Date;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.DiscriminatorColumn;
import javax.persistence.DiscriminatorType;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.apache.oozie.client.SLAEvent;
import org.json.simple.JSONObject;

@Deprecated
@Entity
@Table(name = "SLA_EVENTS")
@DiscriminatorColumn(name = "bean_type", discriminatorType = DiscriminatorType.STRING)
public class JsonSLAEvent implements SLAEvent, JsonBean {
    // Primary key
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "EVENT_SEQ")
    @SequenceGenerator(name = "EVENT_SEQ", sequenceName = "EVENT_SEQ", initialValue = 1, allocationSize = 50)
    private long event_id;

    @Basic
    @Column(name = "sla_id")
    private String slaId;

    @Transient
    private SlaAppType appType = null;

    @Basic
    @Column(name = "app_name")
    private String appName = null;

    @Basic
    @Column(name = "user_name")
    private String user = null;

    @Basic
    @Column(name = "group_name")
    private String groupName = null;

    @Basic
    @Column(name = "parent_client_id")
    private String parentClientId = null;

    @Basic
    @Column(name = "parent_sla_id")
    private String parentSlaId = null;

    @Transient
    private Date expectedStart = null;

    @Transient
    private Date expectedEnd = null;

    @Transient
    private Date statusTimestamp = null;

    @Column(name = "notification_msg")
    @Lob
    private String notificationMsg = null;

    @Basic
    @Column(name = "alert_contact")
    private String alertContact = null;

    @Basic
    @Column(name = "dev_contact")
    private String devContact = null;

    @Basic
    @Column(name = "qa_contact")
    private String qaContact = null;

    @Basic
    @Column(name = "se_contact")
    private String seContact = null;

    @Basic
    @Column(name = "alert_frequency")
    private String alertFrequency = null;

    @Basic
    @Column(name = "alert_percentage")
    private String alertPercentage = null;

    @Column(name = "upstream_apps")
    @Lob
    private String upstreamApps = null;

    @Transient
    private Status jobStatus = null;

    @Column(name = "job_data")
    @Lob
    private String jobData = null;

    public long getEvent_id() {
        return event_id;
    }

    public void setEvent_id(long id) {
        this.event_id = id;
    }

    public String getSlaId() {
        return slaId;
    }

    public void setSlaId(String slaId) {
        this.slaId = slaId;
    }

    /*
     * public String getClientId() { return clientId; }
     *
     * public void setClientId(String clientId) { this.clientId = clientId; }
     */
    public SlaAppType getAppType() {
        return appType;
    }

    public void setAppType(SlaAppType appType) {
        this.appType = appType;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getParentClientId() {
        return parentClientId;
    }

    public void setParentClientId(String parentClientId) {
        this.parentClientId = parentClientId;
    }

    public String getParentSlaId() {
        return parentSlaId;
    }

    public void setParentSlaId(String parentSlaId) {
        this.parentSlaId = parentSlaId;
    }

    public Date getExpectedStart() {
        return expectedStart;
    }

    public void setExpectedStart(Date expectedStart) {
        this.expectedStart = expectedStart;
    }

    public Date getExpectedEnd() {
        return expectedEnd;
    }

    public void setExpectedEnd(Date expectedEnd) {
        this.expectedEnd = expectedEnd;
    }

    public Date getStatusTimestamp() {
        return statusTimestamp;
    }

    public void setStatusTimestamp(Date statusTimestamp) {
        this.statusTimestamp = statusTimestamp;
    }

    public String getNotificationMsg() {
        return notificationMsg;
    }

    public void setNotificationMsg(String notificationMsg) {
        this.notificationMsg = notificationMsg;
    }

    public String getAlertContact() {
        return alertContact;
    }

    public void setAlertContact(String alertContact) {
        this.alertContact = alertContact;
    }

    public String getDevContact() {
        return devContact;
    }

    public void setDevContact(String devContact) {
        this.devContact = devContact;
    }

    public String getQaContact() {
        return qaContact;
    }

    public void setQaContact(String qaContact) {
        this.qaContact = qaContact;
    }

    public String getSeContact() {
        return seContact;
    }

    public void setSeContact(String seContact) {
        this.seContact = seContact;
    }

    public String getAlertFrequency() {
        return alertFrequency;
    }

    public void setAlertFrequency(String alertFrequency) {
        this.alertFrequency = alertFrequency;
    }

    public String getAlertPercentage() {
        return alertPercentage;
    }

    public void setAlertPercentage(String alertPercentage) {
        this.alertPercentage = alertPercentage;
    }

    public String getUpstreamApps() {
        return upstreamApps;
    }

    public void setUpstreamApps(String upstreamApps) {
        this.upstreamApps = upstreamApps;
    }

    public Status getJobStatus() {
        return jobStatus;
    }

    public void setJobStatus(Status jobStatus) {
        this.jobStatus = jobStatus;
    }

    public String getJobData() {
        return jobData;
    }

    public void setJobData(String jobData) {
        this.jobData = jobData;
    }

    @Override
    public JSONObject toJSONObject() {
        // TODO Auto-generated method stub
        return null;
    }

    @SuppressWarnings("unchecked")
    public JSONObject toJSONObject(String timeZoneId) {
        return null;
    }

    public JsonSLAEvent() {

    }

    @SuppressWarnings("unchecked")
    public JsonSLAEvent(JSONObject json) {

    }

}
