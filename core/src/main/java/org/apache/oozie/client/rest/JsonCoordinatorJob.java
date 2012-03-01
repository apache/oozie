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

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.DiscriminatorColumn;
import javax.persistence.DiscriminatorType;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

@Entity
@Table(name = "COORD_JOBS")
@DiscriminatorColumn(name = "bean_type", discriminatorType = DiscriminatorType.STRING)
public class JsonCoordinatorJob implements CoordinatorJob, JsonBean {

    @Id
    private String id;

    @Basic
    @Column(name = "app_path")
    private String appPath = null;

    @Basic
    @Column(name = "app_name")
    private String appName = null;

    @Basic
    @Column(name = "external_id")
    private String externalId = null;

    @Column(name = "conf")
    @Lob
    private String conf = null;

    @Transient
    private Status status = CoordinatorJob.Status.PREP;

    @Transient
    private Execution executionOrder = CoordinatorJob.Execution.FIFO;

    @Transient
    private Date startTime;

    @Transient
    private Date endTime;

    @Transient
    private Date pauseTime;

    @Basic
    @Column(name = "frequency")
    private int frequency = 0;

    @Basic
    @Column(name = "time_zone")
    private String timeZone = null;

    @Basic
    @Column(name = "concurrency")
    private int concurrency = 0;

    @Basic
    @Column(name = "mat_throttling")
    private int matThrottling = 0;

    @Transient
    private Timeunit timeUnit = CoordinatorJob.Timeunit.MINUTE;

    @Basic
    @Column(name = "time_out")
    private int timeOut = 0;

    @Transient
    private Date lastAction;

    @Basic
    @Column(name = "last_action_number")
    private int lastActionNumber;

    @Transient
    private Date nextMaterializedTime;

    @Basic
    @Column(name = "user_name")
    private String user = null;

    @Basic
    @Column(name = "group_name")
    private String group = null;

    @Basic
    @Column(name = "bundle_id")
    private String bundleId = null;

    @Transient
    private String consoleUrl;

    @Transient
    private List<? extends JsonCoordinatorAction> actions;

    @Transient
    private int pending = 0;


    public JsonCoordinatorJob() {
        actions = new ArrayList<JsonCoordinatorAction>();
    }

    @SuppressWarnings("unchecked")
    public JSONObject toJSONObject() {
        JSONObject json = new JSONObject();
        json.put(JsonTags.COORDINATOR_JOB_PATH, getAppPath());
        json.put(JsonTags.COORDINATOR_JOB_NAME, getAppName());
        json.put(JsonTags.COORDINATOR_JOB_ID, getId());
        json.put(JsonTags.COORDINATOR_JOB_EXTERNAL_ID, getExternalId());
        json.put(JsonTags.COORDINATOR_JOB_CONF, getConf());
        json.put(JsonTags.COORDINATOR_JOB_STATUS, getStatus().toString());
        json.put(JsonTags.COORDINATOR_JOB_EXECUTIONPOLICY, getExecutionOrder().toString());
        json.put(JsonTags.COORDINATOR_JOB_FREQUENCY, getFrequency());
        json.put(JsonTags.COORDINATOR_JOB_TIMEUNIT, getTimeUnit().toString());
        json.put(JsonTags.COORDINATOR_JOB_TIMEZONE, getTimeZone());
        json.put(JsonTags.COORDINATOR_JOB_CONCURRENCY, getConcurrency());
        json.put(JsonTags.COORDINATOR_JOB_TIMEOUT, getTimeout());
        json.put(JsonTags.COORDINATOR_JOB_LAST_ACTION_TIME, JsonUtils.formatDateRfc822(getLastActionTime()));
        json.put(JsonTags.COORDINATOR_JOB_NEXT_MATERIALIZED_TIME, JsonUtils.formatDateRfc822(getNextMaterializedTime()));
        json.put(JsonTags.COORDINATOR_JOB_START_TIME, JsonUtils.formatDateRfc822(getStartTime()));
        json.put(JsonTags.COORDINATOR_JOB_END_TIME, JsonUtils.formatDateRfc822(getEndTime()));
        json.put(JsonTags.COORDINATOR_JOB_PAUSE_TIME, JsonUtils.formatDateRfc822(getPauseTime()));
        json.put(JsonTags.COORDINATOR_JOB_USER, getUser());
        json.put(JsonTags.COORDINATOR_JOB_GROUP, getGroup());
        json.put(JsonTags.COORDINATOR_JOB_ACL, getAcl());
        json.put(JsonTags.COORDINATOR_JOB_CONSOLE_URL, getConsoleUrl());
        json.put(JsonTags.COORDINATOR_JOB_MAT_THROTTLING, getMatThrottling());
        json.put(JsonTags.COORDINATOR_ACTIONS, JsonCoordinatorAction.toJSONArray(actions));
        json.put(JsonTags.TO_STRING,toString());

        return json;
    }

    public String getAppPath() {
        return appPath;
    }

    public void setAppPath(String appPath) {
        this.appPath = appPath;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setExternalId(String externalId) {
        this.externalId = externalId;
    }

    public String getExternalId() {
        return externalId;
    }

    public String getConf() {
        return conf;
    }

    public void setConf(String conf) {
        this.conf = conf;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    public int getFrequency() {
        return frequency;
    }

    public void setTimeUnit(Timeunit timeUnit) {
        this.timeUnit = timeUnit;
    }

    public Timeunit getTimeUnit() {
        return timeUnit;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    public int getConcurrency() {
        return concurrency;
    }

    public int getMatThrottling() {
        return matThrottling;
    }

    public void setMatThrottling(int matThrottling) {
        this.matThrottling = matThrottling;
    }

    public void setExecutionOrder(Execution order) {
        this.executionOrder = order;
    }

    public Execution getExecutionOrder() {
        return executionOrder;
    }

    public void setTimeout(int timeOut) {
        this.timeOut = timeOut;
    }

    public int getTimeout() {
        return timeOut;
    }

    public void setLastActionTime(Date lastAction) {
        this.lastAction = lastAction;
    }

    public Date getLastActionTime() {
        return lastAction;
    }

    public Date getNextMaterializedTime() {
        return nextMaterializedTime;
    }

    public void setNextMaterializedTime(Date nextMaterializedTime) {
        this.nextMaterializedTime = nextMaterializedTime;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public Date getPauseTime() {
        return pauseTime;
    }

    public void setPauseTime(Date pauseTime) {
        this.pauseTime = pauseTime;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getGroup() {
        return group;
    }

    @Override
    public String getAcl() {
        return getGroup();
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getBundleId() {
        return bundleId;
    }

    public void setBundleId(String bundleId) {
        this.bundleId = bundleId;
    }

    /**
     * Return the coordinate application console URL.
     *
     * @return the coordinate application console URL.
     */
    public String getConsoleUrl() {
        return consoleUrl;
    }

    /**
     * Set the coordinate application console URL.
     *
     * @param consoleUrl the coordinate application console URL.
     */
    public void setConsoleUrl(String consoleUrl) {
        this.consoleUrl = consoleUrl;
    }

    @Override
    public String toString() {
        return MessageFormat.format("Coornidator application id[{0}] status[{1}]", getId(), getStatus());
    }

    public void setActions(List<? extends JsonCoordinatorAction> nodes) {
        this.actions = (nodes != null) ? nodes : new ArrayList<JsonCoordinatorAction>();
    }

    @SuppressWarnings("unchecked")
    public List<CoordinatorAction> getActions() {
        return (List) actions;
    }

    /**
     * Convert a coordinator application list into a JSONArray.
     *
     * @param applications list.
     * @return the corresponding JSON array.
     */
    @SuppressWarnings("unchecked")
    public static JSONArray toJSONArray(List<? extends JsonCoordinatorJob> applications) {
        JSONArray array = new JSONArray();
        if (applications != null) {
            for (JsonCoordinatorJob application : applications) {
                array.add(application.toJSONObject());
            }
        }
        return array;
    }

    public int getLastActionNumber() {
        return lastActionNumber;
    }

    public void setLastActionNumber(int lastActionNumber) {
        this.lastActionNumber = lastActionNumber;
    }

    /**
     * Set pending to true
     *
     * @param pending set pending to true
     */
    public void setPending() {
        this.pending = 1;
    }

    /**
     * Set pending to false
     *
     * @param pending set pending to false
     */
    public void resetPending() {
        this.pending = 0;
    }

}
