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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Transient;

import org.apache.hadoop.io.Writable;
import org.apache.oozie.AppType;
import org.apache.oozie.client.event.SLAEvent;
import org.apache.oozie.client.rest.sla.JsonSLARegistrationEvent;
import org.apache.oozie.util.DateUtils;
import org.apache.openjpa.persistence.jdbc.Index;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

@Entity
@NamedQueries({
 @NamedQuery(name = "GET_SLA_REG_ALL", query = "select w.jobId, w.appType, w.appName, w.user, w.nominalTimeTS," +
        "w.expectedStartTS, w.expectedEndTS, w.expectedDuration, w.jobData, w.parentId, w.notificationMsg," +
        "w.upstreamApps, w.slaConfig from SLARegistrationBean w where w.jobId = :id") })
public class SLARegistrationBean extends JsonSLARegistrationEvent implements Writable {

    @Basic
    @Column(name = "app_type")
    private String appType = null;

    @Basic
    @Index
    @Column(name = "nominal_time")
    private java.sql.Timestamp nominalTimeTS = null;

    @Basic
    @Column(name = "expected_start")
    private java.sql.Timestamp expectedStartTS = null;

    @Basic
    @Column(name = "expected_end")
    private java.sql.Timestamp expectedEndTS = null;

    @Transient
    private long expectedDuration;

    public SLARegistrationBean() {
    }

    @Override
    public AppType getAppType() {
        return AppType.valueOf(appType);
    }

    @Override
    public void setAppType(AppType appType) {
        super.setAppType(appType);
        this.appType = appType.toString();
    }

    @Override
    public Date getNominalTime() {
        return DateUtils.toDate(nominalTimeTS);
    }

    @Override
    public void setNominalTime(Date nominalTime) {
        super.setNominalTime(nominalTime);
        this.nominalTimeTS = DateUtils.convertDateToTimestamp(nominalTime);
    }

    @Override
    public Date getExpectedStart() {
        return DateUtils.toDate(expectedStartTS);
    }

    @Override
    public void setExpectedStart(Date expectedStart) {
        super.setExpectedStart(expectedStart);
        this.expectedStartTS = DateUtils.convertDateToTimestamp(expectedStart);
    }

    @Override
    public Date getExpectedEnd() {
        return DateUtils.toDate(expectedEndTS);
    }

    @Override
    public void setExpectedEnd(Date expectedEnd) {
        super.setExpectedEnd(expectedEnd);
        this.expectedEndTS = DateUtils.convertDateToTimestamp(expectedEnd);
    }

    @Override
    public long getExpectedDuration() {
        return expectedDuration;
    }

    @Override
    public void setExpectedDuration(long expectedDuration) {
        super.setExpectedDuration(expectedDuration);
        this.expectedDuration = expectedDuration;
    }

    @Override
    public void write(DataOutput dataOut) throws IOException {
        // required?
    }

    @Override
    public void readFields(DataInput dataIn) throws IOException {
        // required?
    }

    /**
     * Convert a SLAEvent list into a JSONArray.
     *
     * @param SLAEVent list.
     * @param timeZoneId time zone to use for dates in the JSON array.
     * @return the corresponding JSON array.
     */
    @SuppressWarnings("unchecked")
    public static JSONArray toJSONArray(List<? extends SLARegistrationBean> events, String timeZoneId) {
        JSONArray array = new JSONArray();
        if (events != null) {
            for (JsonSLARegistrationEvent node : events) {
                array.add(node.toJSONObject(timeZoneId));
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
    public static List<SLAEvent> fromJSONArray(JSONArray array) {
        List<SLAEvent> list = new ArrayList<SLAEvent>();
        for (Object obj : array) {
            list.add(new JsonSLARegistrationEvent((JSONObject) obj));
        }
        return list;
    }

}
