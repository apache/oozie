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
package org.apache.oozie;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;

import org.apache.hadoop.io.Writable;
import org.apache.oozie.client.SLAEvent;
import org.apache.oozie.client.rest.JsonSLAEvent;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.XLog;
import org.jdom.Element;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

@Entity
@NamedQueries({

    @NamedQuery(name = "GET_SLA_EVENT_NEWER_SEQ_LIMITED", query = "select OBJECT(w) from SLAEventBean w where w.event_id > :id order by w.event_id")})
public class SLAEventBean extends JsonSLAEvent implements Writable {

    @Basic
    @Column(name = "job_status")
    private String jobStatusStr = null;

    @Basic
    @Column(name = "app_type")
    private String appTypeStr = null;

    @Basic
    @Column(name = "expected_start")
    private java.sql.Timestamp expectedStartTS = null;

    @Basic
    @Column(name = "expected_end")
    private java.sql.Timestamp expectedEndTS = null;

    @Basic
    @Column(name = "status_timestamp")
    private java.sql.Timestamp statusTimestampTS = null;

    @Basic
    @Column(name = "event_type")
    private String eventType = null;

    public SLAEventBean() {

    }

    public String getJobStatusStr() {
        return jobStatusStr;
    }

    public void setJobStatusStr(String jobStatusStr) {
        this.jobStatusStr = jobStatusStr;
    }

    public Status getJobStatus() {
        return Status.valueOf(this.jobStatusStr);
    }

    public void setJobStatus(Status jobStatus) {
        super.setJobStatus(jobStatus);
        this.jobStatusStr = jobStatus.toString();
    }

    public String getAppTypeStr() {
        return appTypeStr;
    }

    public void setAppTypeStr(String appTypeStr) {
        this.appTypeStr = appTypeStr;
    }

    public SlaAppType getAppType() {
        return SlaAppType.valueOf(appTypeStr);
    }

    public void setAppType(SlaAppType appType) {
        super.setAppType(appType);
        this.appTypeStr = appType.toString();
    }

    public java.sql.Timestamp getExpectedStartTS() {
        return expectedStartTS;
    }

    public Date getExpectedStart() {
        return DateUtils.toDate(expectedStartTS);
    }

    public void setExpectedStart(Date expectedStart) {
        super.setExpectedStart(expectedStart);
        this.expectedStartTS = DateUtils.convertDateToTimestamp(expectedStart);
    }

    public java.sql.Timestamp getExpectedEndTS() {
        return expectedEndTS;
    }

    public Date getExpectedEnd() {
        return DateUtils.toDate(expectedEndTS);
    }

    public void setExpectedEnd(Date expectedEnd) {
        super.setExpectedEnd(expectedEnd);
        this.expectedEndTS = DateUtils.convertDateToTimestamp(expectedEnd);
    }

    public java.sql.Timestamp getStatusTimestampTS() {
        return statusTimestampTS;
    }

    public Date getStatusTimestamp() {
        return DateUtils.toDate(statusTimestampTS);
    }

    public void setStatusTimestamp(Date statusTimestamp) {
        super.setStatusTimestamp(statusTimestamp);
        this.statusTimestampTS = DateUtils.convertDateToTimestamp(statusTimestamp);
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        // TODO Auto-generated method stub

    }

    public String toString() {
        return MessageFormat.format("Event id[{0}] status[{1}]", getEvent_id(),
                                    getJobStatus());
    }

    /**
     * Convert a SLAEvent list into a JSONArray.
     *
     * @param SLAEVent list.
     * @return the corresponding JSON array.
     */
    @SuppressWarnings("unchecked")
    public static JSONArray toJSONArray(List<? extends SLAEventBean> events) {
        JSONArray array = new JSONArray();
        if (events != null) {
            for (JsonSLAEvent node : events) {
                array.add(node.toJSONObject());
            }
        }
        return array;
    }

    /**
     * Convert a JSONArray into a SLAEvent list.
     *
     * @param array JSON array.
     * @return the corresponding SLA event list.
     */
    @SuppressWarnings("unchecked")
    public static List<SLAEvent> fromJSONArray(JSONArray array) {
        List<SLAEvent> list = new ArrayList<SLAEvent>();
        for (Object obj : array) {
            list.add(new JsonSLAEvent((JSONObject) obj));
        }
        return list;
    }

    /* public String toXml2() {
            String ret = "";
            if (getJobStatus() == Status.CREATED) {
                ret = getRegistrationEventXml();
            }
            else {
                ret = getStatusEventXml();
            }
            return createATag("event", ret);
        }

        private String getStatusEventXml() {
            StringBuilder statXml = new StringBuilder();
            statXml
                    .append(createATag("sequence-id", String.valueOf(getEvent_id())));
            statXml.append("<status>");
            statXml.append(createATag("sla-id", getSlaId()));
            statXml.append(createATag("status-timestamp",
                    getDateString(getStatusTimestamp())));
            statXml.append(createATag("job-status", getJobStatus().toString()));
            statXml.append("</status>");
            return statXml.toString();
        }

        private String getRegistrationEventXml() {
            StringBuilder regXml = new StringBuilder();
            regXml.append(createATag("sequence-id", String.valueOf(getEvent_id())));
            regXml.append("<registration>");
            regXml.append(createATag("sla-id", String.valueOf(getSlaId())));
            regXml.append(createATag("app-type", getAppType().toString()));
            regXml.append(createATag("app-name", getAppName()));
            regXml.append(createATag("user", getUser()));
            regXml.append(createATag("group", getGroupName()));
            regXml.append(createATag("parent-sla-id", String
                    .valueOf(getParentSlaId())));
            regXml.append(createATag("expected-start",
                    getDateString(getExpectedStart())));
            regXml.append(createATag("expected-end",
                    getDateString(getExpectedEnd())));
            regXml.append(createATag("status-timestamp",
                    getDateString(getStatusTimestamp())));
            regXml.append(createATag("job-status", getJobStatus().toString()));

            regXml.append(createATag("alert-contact", getAlertContact()));
            regXml.append(createATag("dev-contact", getDevContact()));
            regXml.append(createATag("qa-contact", getQaContact()));
            regXml.append(createATag("se-contact", getSeContact()));
            regXml.append(createATag("notification-msg", getNotificationMsg()));
            regXml.append(createATag("alert-percentage", getAlertPercentage()));
            regXml.append(createATag("alert-frequency", getAlertFrequency()));
            regXml.append(createATag("upstream-apps", getUpstreamApps()));
            regXml.append("</registration>");
            return regXml.toString();
        }
        private String createATag(String tag, String content) {
            if (content == null) {
                content = "";
            }
            return "<" + tag + ">" + content + "</" + tag + ">";
        }
    */
    public Element toXml() {
        Element retElem = null;
        if (getJobStatus() == Status.CREATED) {
            retElem = getRegistrationEvent("event");
        }
        else {
            retElem = getStatusEvent("event");
        }
        return retElem;
    }

    private Element getRegistrationEvent(String tag) {
        Element eReg = new Element(tag);
        eReg.addContent(createATagElement("sequence-id", String.valueOf(getEvent_id())));
        Element e = new Element("registration");
        e.addContent(createATagElement("sla-id", getSlaId()));
        //e.addContent(createATagElement("sla-id", String.valueOf(getSlaId())));
        e.addContent(createATagElement("app-type", getAppType().toString()));
        e.addContent(createATagElement("app-name", getAppName()));
        e.addContent(createATagElement("user", getUser()));
        e.addContent(createATagElement("group", getGroupName()));
        e.addContent(createATagElement("parent-sla-id", String
                .valueOf(getParentSlaId())));
        e.addContent(createATagElement("expected-start",
                                       getDateString(getExpectedStart())));
        e.addContent(createATagElement("expected-end",
                                       getDateString(getExpectedEnd())));
        e.addContent(createATagElement("status-timestamp",
                                       getDateString(getStatusTimestamp())));
        e.addContent(createATagElement("notification-msg", getNotificationMsg()));

        e.addContent(createATagElement("alert-contact", getAlertContact()));
        e.addContent(createATagElement("dev-contact", getDevContact()));
        e.addContent(createATagElement("qa-contact", getQaContact()));
        e.addContent(createATagElement("se-contact", getSeContact()));

        e.addContent(createATagElement("alert-percentage", getAlertPercentage()));
        e.addContent(createATagElement("alert-frequency", getAlertFrequency()));

        e.addContent(createATagElement("upstream-apps", getUpstreamApps()));
        e.addContent(createATagElement("job-status", getJobStatus().toString()));
        e.addContent(createATagElement("job-data", getJobData()));
        eReg.addContent(e);
        return eReg;
    }

    private Element getStatusEvent(String tag) {
        Element eStat = new Element(tag);
        eStat.addContent(createATagElement("sequence-id", String.valueOf(getEvent_id())));
        Element e = new Element("status");
        e.addContent(createATagElement("sla-id", getSlaId()));
        e.addContent(createATagElement("status-timestamp",
                                       getDateString(getStatusTimestamp())));
        e.addContent(createATagElement("job-status", getJobStatus().toString()));
        e.addContent(createATagElement("job-data", getJobData()));
        eStat.addContent(e);
        return eStat;
    }

    private Element createATagElement(String tag, String content) {
        if (content == null) {
            content = "";
        }
        Element e = new Element(tag);
        e.addContent(content);
        return e;
    }

    private Element createATagElement(String tag, Element content) {
        Element e = new Element(tag);
        e.addContent(content);
        return e;
    }

    private String getDateString(Date d) {
        try {
            return DateUtils.formatDateUTC(d);
        }
        catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            XLog.getLog(getClass()).error("Date formatting error " + d, e);
            throw new RuntimeException("Date formatting error " + d + e);
        }
    }

}
