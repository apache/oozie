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
import java.util.Date;
import java.util.List;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;

import org.apache.oozie.AppType;
import org.apache.oozie.client.event.SLAEvent;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.JsonUtils;
import org.apache.oozie.util.DateUtils;
import org.apache.openjpa.persistence.jdbc.Index;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

@Entity
@Table(name = "SLA_SUMMARY")
@NamedQueries({

 @NamedQuery(name = "UPDATE_SLA_SUMMARY_FOR_SLA_STATUS", query = "update  SLASummaryBean w set w.slaStatus = :slaStatus, w.eventStatus = :eventStatus, w.eventProcessed = :eventProcessed, w.lastModifiedTS = :lastModifiedTS where w.jobId = :jobId"),

 @NamedQuery(name = "UPDATE_SLA_SUMMARY_FOR_STATUS_ACTUAL_TIMES", query = "update SLASummaryBean w set w.slaStatus = :slaStatus, w.eventStatus = :eventStatus, w.eventProcessed = :eventProcessed, w.jobStatus = :jobStatus, w.lastModifiedTS = :lastModifiedTS, w.actualStartTS = :actualStartTS, w.actualEndTS = :actualEndTS, w.actualDuration = :actualDuration where w.jobId = :jobId"),

 @NamedQuery(name = "UPDATE_SLA_SUMMARY_FOR_ACTUAL_TIMES", query = "update SLASummaryBean w set w.eventProcessed = :eventProcessed, w.actualStartTS = :actualStartTS, w.actualEndTS = :actualEndTS, w.actualEndTS = :actualEndTS, w.actualDuration = :actualDuration, w.lastModifiedTS = :lastModifiedTS where w.jobId = :jobId"),

 @NamedQuery(name = "UPDATE_SLA_SUMMARY_EVENTPROCESSED", query = "update SLASummaryBean w set w.eventProcessed = :eventProcessed where w.jobId = :jobId"),

 @NamedQuery(name = "UPDATE_SLA_SUMMARY_ALL", query = "update SLASummaryBean w set w.jobId = :jobId, w.appName = :appName, w.appType = :appType, w.nominalTimeTS = :nominalTime, w.expectedStartTS = :expectedStartTime, w.expectedEndTS = :expectedEndTime, w.expectedDuration = :expectedDuration, w.jobStatus = :jobStatus, w.slaStatus = :slaStatus, w.eventStatus = :eventStatus, w.lastModifiedTS = :lastModTime, w.user = :user, w.parentId = :parentId, w.eventProcessed = :eventProcessed, w.actualDuration = :actualDuration, w.actualEndTS = :actualEndTS, w.actualStartTS = :actualStartTS where w.jobId = :jobId"),

 @NamedQuery(name = "GET_SLA_SUMMARY", query = "select OBJECT(w) from SLASummaryBean w where w.jobId = :id"),

 @NamedQuery(name = "GET_SLA_SUMMARY_RECORDS_RESTART", query = "select OBJECT(w) from SLASummaryBean w where w.eventProcessed <= 7 AND w.lastModifiedTS >= :lastModifiedTime"),

 @NamedQuery(name = "GET_SLA_SUMMARY_EVENTPROCESSED", query = "select w.eventProcessed from SLASummaryBean w where w.jobId = :id")
})

/**
 * Class to store all the SLA related details (summary) per job
 */
public class SLASummaryBean implements JsonBean {

    @Id
    @Basic
    @Column(name = "job_id")
    private String jobId;

    @Basic
    @Index
    @Column(name = "parent_id")
    private String parentId;

    @Basic
    @Index
    @Column(name = "app_name")
    private String appName;

    @Basic
    @Column(name = "app_type")
    private String appType;

    @Basic
    @Column(name = "user_name")
    private String user;

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
    @Column(name = "actual_start")
    private Timestamp actualStartTS = null;

    @Basic
    @Column(name = "actual_end")
    private Timestamp actualEndTS = null;

    @Basic
    @Column(name = "actual_duration")
    private long actualDuration = -1;

    @Basic
    @Column(name = "job_status")
    private String jobStatus;

    @Basic
    @Column(name = "event_status")
    private String eventStatus;

    @Basic
    @Column(name = "sla_status")
    private String slaStatus;

    @Basic
    @Index
    @Column(name = "event_processed")
    private byte eventProcessed = 0;

    @Basic
    @Index
    @Column(name = "last_modified")
    private Timestamp lastModifiedTS = null;

    public SLASummaryBean() {
    }

    public SLASummaryBean(SLACalcStatus slaCalc) {
        SLARegistrationBean reg = slaCalc.getSLARegistrationBean();
        setId(slaCalc.getId());
        setAppName(reg.getAppName());
        setAppType(reg.getAppType());
        setNominalTime(reg.getNominalTime());
        setExpectedStart(reg.getExpectedStart());
        setExpectedEnd(reg.getExpectedEnd());
        setExpectedDuration(reg.getExpectedDuration());
        setJobStatus(slaCalc.getJobStatus());
        setSLAStatus(slaCalc.getSLAStatus());
        setEventStatus(slaCalc.getEventStatus());
        setLastModifiedTime(slaCalc.getLastModifiedTime());
        setUser(reg.getUser());
        setParentId(reg.getParentId());
        setEventProcessed(slaCalc.getEventProcessed());
        setActualDuration(slaCalc.getActualDuration());
        setActualEnd(slaCalc.getActualEnd());
        setActualStart(slaCalc.getActualStart());
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

    public Date getActualStart() {
        return DateUtils.toDate(actualStartTS);
    }

    public Timestamp getActualStartTimestamp() {
        return this.actualStartTS;
    }

    public void setActualStart(Date actualStart) {
        this.actualStartTS = DateUtils.convertDateToTimestamp(actualStart);
    }

    public Date getActualEnd() {
        return DateUtils.toDate(actualEndTS);
    }

    public Timestamp getActualEndTimestamp() {
        return this.actualEndTS;
    }

    public void setActualEnd(Date actualEnd) {
        this.actualEndTS = DateUtils.convertDateToTimestamp(actualEnd);
    }

    public long getActualDuration() {
        return actualDuration;
    }

    public void setActualDuration(long actualDuration) {
        this.actualDuration = actualDuration;
    }

    public String getJobStatus() {
        return jobStatus;
    }

    public void setJobStatus(String status) {
        this.jobStatus = status;
    }

    public SLAEvent.EventStatus getEventStatus() {
        return (eventStatus != null ? SLAEvent.EventStatus.valueOf(eventStatus) : null);
    }

    public void setEventStatus(SLAEvent.EventStatus eventStatus) {
        this.eventStatus = (eventStatus != null ? eventStatus.name() : null);
    }

    public SLAEvent.SLAStatus getSLAStatus() {
        return (slaStatus != null ? SLAEvent.SLAStatus.valueOf(slaStatus) : null);
    }

    public String getSLAStatusString() {
        return slaStatus;
    }

    public String getEventStatusString() {
        return eventStatus;
    }

    public void setSLAStatus(SLAEvent.SLAStatus stage) {
        this.slaStatus = (stage != null ? stage.name() : null);
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
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

    public byte getEventProcessed() {
        return eventProcessed;
    }

    public void setEventProcessed(int eventProcessed) {
        this.eventProcessed = (byte)eventProcessed;
    }

    public Date getLastModifiedTime() {
        return DateUtils.toDate(lastModifiedTS);
    }

    public Timestamp getLastModifiedTimestamp() {
        return this.lastModifiedTS;
    }

    public void setLastModifiedTime(Date lastModified) {
        this.lastModifiedTS = DateUtils.convertDateToTimestamp(lastModified);
    }

    @SuppressWarnings("unchecked")
    @Override
    public JSONObject toJSONObject() {
        JSONObject json = new JSONObject();
        json.put(JsonTags.SLA_SUMMARY_ID, jobId);
        if (parentId != null) {
            json.put(JsonTags.SLA_SUMMARY_PARENT_ID, parentId);
        }
        json.put(JsonTags.SLA_SUMMARY_APP_NAME, appName);
        json.put(JsonTags.SLA_SUMMARY_APP_TYPE, appType);
        json.put(JsonTags.SLA_SUMMARY_USER, user);
        json.put(JsonTags.SLA_SUMMARY_NOMINAL_TIME, nominalTimeTS.getTime());
        if (expectedStartTS != null) {
            json.put(JsonTags.SLA_SUMMARY_EXPECTED_START, expectedStartTS.getTime());
        }
        else {
            json.put(JsonTags.SLA_SUMMARY_EXPECTED_START, null);
        }
        if (actualStartTS != null) {
            json.put(JsonTags.SLA_SUMMARY_ACTUAL_START, actualStartTS.getTime());
        }
        else {
            json.put(JsonTags.SLA_SUMMARY_ACTUAL_START, null);
        }
        json.put(JsonTags.SLA_SUMMARY_EXPECTED_END, expectedEndTS.getTime());
        if (actualEndTS != null) {
            json.put(JsonTags.SLA_SUMMARY_ACTUAL_END, actualEndTS.getTime());
        }
        else {
            json.put(JsonTags.SLA_SUMMARY_ACTUAL_END, null);
        }
        json.put(JsonTags.SLA_SUMMARY_EXPECTED_DURATION, expectedDuration);
        json.put(JsonTags.SLA_SUMMARY_ACTUAL_DURATION, actualDuration);
        json.put(JsonTags.SLA_SUMMARY_JOB_STATUS, jobStatus);
        json.put(JsonTags.SLA_SUMMARY_SLA_STATUS, slaStatus);
        json.put(JsonTags.SLA_SUMMARY_LAST_MODIFIED, lastModifiedTS.getTime());
        return json;
    }

    @SuppressWarnings("unchecked")
    @Override
    public JSONObject toJSONObject(String timeZoneId) {
        if (timeZoneId == null) {
            return toJSONObject();
        }
        else {
            JSONObject json = new JSONObject();
            json.put(JsonTags.SLA_SUMMARY_ID, jobId);
            if (parentId != null) {
                json.put(JsonTags.SLA_SUMMARY_PARENT_ID, parentId);
            }
            json.put(JsonTags.SLA_SUMMARY_APP_NAME, appName);
            json.put(JsonTags.SLA_SUMMARY_APP_TYPE, appType);
            json.put(JsonTags.SLA_SUMMARY_USER, user);
            json.put(JsonTags.SLA_SUMMARY_NOMINAL_TIME, JsonUtils.formatDateRfc822(nominalTimeTS, timeZoneId));
            if (expectedStartTS != null) {
                json.put(JsonTags.SLA_SUMMARY_EXPECTED_START, JsonUtils.formatDateRfc822(expectedStartTS, timeZoneId));
            }
            else {
                json.put(JsonTags.SLA_SUMMARY_EXPECTED_START, null);
            }
            if (actualStartTS != null) {
                json.put(JsonTags.SLA_SUMMARY_ACTUAL_START, JsonUtils.formatDateRfc822(actualStartTS, timeZoneId));
            }
            else {
                json.put(JsonTags.SLA_SUMMARY_ACTUAL_START, null);
            }
            json.put(JsonTags.SLA_SUMMARY_EXPECTED_END, JsonUtils.formatDateRfc822(expectedEndTS, timeZoneId));
            if (actualEndTS != null) {
                json.put(JsonTags.SLA_SUMMARY_ACTUAL_END, JsonUtils.formatDateRfc822(actualEndTS, timeZoneId));
            }
            else {
                json.put(JsonTags.SLA_SUMMARY_ACTUAL_END, null);
            }
            json.put(JsonTags.SLA_SUMMARY_EXPECTED_DURATION, expectedDuration);
            json.put(JsonTags.SLA_SUMMARY_ACTUAL_DURATION, actualDuration);
            json.put(JsonTags.SLA_SUMMARY_JOB_STATUS, jobStatus);
            json.put(JsonTags.SLA_SUMMARY_SLA_STATUS, slaStatus);
            json.put(JsonTags.SLA_SUMMARY_LAST_MODIFIED, JsonUtils.formatDateRfc822(lastModifiedTS, timeZoneId));
            return json;
        }
    }

    /**
     * Convert a sla summary list into a json object.
     *
     * @param slaSummaryList sla summary list.
     * @param timeZoneId time zone to use for dates in the JSON array.
     * @return the corresponding JSON object.
     */
    @SuppressWarnings("unchecked")
    public static JSONObject toJSONObject(List<? extends SLASummaryBean> slaSummaryList, String timeZoneId) {
        JSONObject json = new JSONObject();
        JSONArray array = new JSONArray();
        if (slaSummaryList != null) {
            for (SLASummaryBean summary : slaSummaryList) {
                array.add(summary.toJSONObject(timeZoneId));
            }
        }
        json.put(JsonTags.SLA_SUMMARY_LIST, array);
        return json;
    }

}
