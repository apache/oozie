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

import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

@Entity
@Table(name = "BUNDLE_JOBS")
@DiscriminatorColumn(name = "bean_type", discriminatorType = DiscriminatorType.STRING)
public class JsonBundleJob implements BundleJob, JsonBean {
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
    private Status status = Job.Status.PREP;

    @Transient
    private Date kickoffTime;

    @Transient
    private Date startTime;

    @Transient
    private Date endTime;

    @Transient
    private Date pauseTime;

    @Transient
    private Date createdTime;

    @Transient
    private Timeunit timeUnit = BundleJob.Timeunit.MINUTE;

    @Basic
    @Column(name = "time_out")
    private int timeOut = 0;

    @Basic
    @Column(name = "user_name")
    private String user = null;

    @Basic
    @Column(name = "group_name")
    private String group = null;

    @Transient
    private String consoleUrl;

    @Transient
    private int pending = 0;

    @Transient
    private List<? extends JsonCoordinatorJob> coordJobs;

    public JsonBundleJob() {
        coordJobs = new ArrayList<JsonCoordinatorJob>();
    }

    /**
     * Get the value from the json object.
     *
     * @param json
    public JsonBundleJob(JSONObject json) {
        appPath = (String) json.get(JsonTags.BUNDLE_JOB_PATH);
        appName = (String) json.get(JsonTags.BUNDLE_JOB_NAME);
        id = (String) json.get(JsonTags.BUNDLE_JOB_ID);
        externalId = (String) json.get(JsonTags.BUNDLE_JOB_EXTERNAL_ID);
        conf = (String) json.get(JsonTags.BUNDLE_JOB_CONF);
        status = Status.valueOf((String) json.get(JsonTags.BUNDLE_JOB_STATUS));
        kickoffTime = JsonUtils.parseDateRfc822((String) json.get(JsonTags.BUNDLE_JOB_KICKOFF_TIME));
        startTime = JsonUtils.parseDateRfc822((String) json.get(JsonTags.BUNDLE_JOB_START_TIME));
        endTime = JsonUtils.parseDateRfc822((String) json.get(JsonTags.BUNDLE_JOB_END_TIME));
        pauseTime = JsonUtils.parseDateRfc822((String) json.get(JsonTags.BUNDLE_JOB_PAUSE_TIME));
        createdTime = JsonUtils.parseDateRfc822((String) json.get(JsonTags.BUNDLE_JOB_CREATED_TIME));
        timeUnit = Timeunit.valueOf((String) json.get(JsonTags.BUNDLE_JOB_TIMEUNIT));
        timeOut = (int) JsonUtils.getLongValue(json, JsonTags.BUNDLE_JOB_TIMEOUT);
        user = (String) json.get(JsonTags.BUNDLE_JOB_USER);
        group = (String) json.get(JsonTags.BUNDLE_JOB_GROUP);
        consoleUrl = (String) json.get(JsonTags.BUNDLE_JOB_CONSOLE_URL);
        coordJobs = JsonCoordinatorJob.fromJSONArray((JSONArray) json.get(JsonTags.BUNDLE_COORDINATOR_JOBS));
    }
*/

    /* (non-Javadoc)
     * @see org.apache.oozie.client.rest.JsonBean#toJSONObject()
     */
    @Override
    @SuppressWarnings("unchecked")
    public JSONObject toJSONObject() {
        return toJSONObject("GMT");
    }
    
    /* (non-Javadoc)
     * @see org.apache.oozie.client.rest.JsonBean#toJSONObject(String timeZoneId)
     */
    @Override
    @SuppressWarnings("unchecked")
    public JSONObject toJSONObject(String timeZoneId) {
        JSONObject json = new JSONObject();
        json.put(JsonTags.BUNDLE_JOB_PATH, appPath);
        json.put(JsonTags.BUNDLE_JOB_NAME, appName);
        json.put(JsonTags.BUNDLE_JOB_ID, id);
        json.put(JsonTags.BUNDLE_JOB_EXTERNAL_ID, externalId);
        json.put(JsonTags.BUNDLE_JOB_CONF, conf);
        json.put(JsonTags.BUNDLE_JOB_STATUS, getStatus().toString());
        json.put(JsonTags.BUNDLE_JOB_TIMEUNIT, getTimeUnit().toString());
        json.put(JsonTags.BUNDLE_JOB_TIMEOUT, timeOut);
        json.put(JsonTags.BUNDLE_JOB_KICKOFF_TIME, JsonUtils.formatDateRfc822(getKickoffTime(), timeZoneId));
        json.put(JsonTags.BUNDLE_JOB_START_TIME, JsonUtils.formatDateRfc822(getStartTime(), timeZoneId));
        json.put(JsonTags.BUNDLE_JOB_END_TIME, JsonUtils.formatDateRfc822(getEndTime(), timeZoneId));
        json.put(JsonTags.BUNDLE_JOB_PAUSE_TIME, JsonUtils.formatDateRfc822(getPauseTime(), timeZoneId));
        json.put(JsonTags.BUNDLE_JOB_CREATED_TIME, JsonUtils.formatDateRfc822(getCreatedTime(), timeZoneId));
        json.put(JsonTags.BUNDLE_JOB_USER, getUser());
        json.put(JsonTags.BUNDLE_JOB_GROUP, getGroup());
        json.put(JsonTags.BUNDLE_JOB_ACL, getAcl());
        json.put(JsonTags.BUNDLE_JOB_CONSOLE_URL, getConsoleUrl());
        json.put(JsonTags.BUNDLE_COORDINATOR_JOBS, JsonCoordinatorJob.toJSONArray(coordJobs, timeZoneId));
        json.put(JsonTags.TO_STRING, toString());

        return json;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.client.Job#getAppName()
     */
    @Override
    public String getAppName() {
        return appName;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.client.Job#getAppPath()
     */
    @Override
    public String getAppPath() {
        return appPath;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.client.Job#getConf()
     */
    @Override
    public String getConf() {
        return conf;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.client.Job#getConsoleUrl()
     */
    @Override
    public String getConsoleUrl() {
        return consoleUrl;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.client.BundleJob#getCoordinators()
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<CoordinatorJob> getCoordinators() {
        return (List) coordJobs;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.client.Job#getEndTime()
     */
    @Override
    public Date getEndTime() {
        return endTime;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.client.Job#getGroup()
     */
    @Override
    @Deprecated
    public String getGroup() {
        return group;
    }

    @Override
    public String getAcl() {
        return getGroup();
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.client.Job#getId()
     */
    @Override
    public String getId() {
        return id;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.client.Job#getKickoffTime()
     */
    @Override
    public Date getKickoffTime() {
        return kickoffTime;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.client.Job#getStatus()
     */
    @Override
    public Status getStatus() {
        return status;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.client.BundleJob#getTimeUnit()
     */
    @Override
    public Timeunit getTimeUnit() {
        return timeUnit;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.client.BundleJob#getTimeout()
     */
    @Override
    public int getTimeout() {
        return timeOut;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.client.Job#getUser()
     */
    @Override
    public String getUser() {
        return user;
    }

    /**
     * Set id
     *
     * @param id the id to set
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Set bundlePath
     *
     * @param bundlePath the bundlePath to set
     */
    public void setAppPath(String bundlePath) {
        this.appPath = bundlePath;
    }

    /**
     * Set bundleName
     *
     * @param bundleName the bundleName to set
     */
    public void setAppName(String bundleName) {
        this.appName = bundleName;
    }

    /**
     * Return externalId
     *
     * @return externalId
     */
    public String getExternalId() {
        return this.externalId;
    }

    /**
     * Set externalId
     *
     * @param externalId the externalId to set
     */
    public void setExternalId(String externalId) {
        this.externalId = externalId;
    }

    /**
     * Set conf
     *
     * @param conf the conf to set
     */
    public void setConf(String conf) {
        this.conf = conf;
    }

    /**
     * Set status
     *
     * @param status the status to set
     */
    @Override
    public void setStatus(Status status) {
        this.status = status;
    }

    /**
     * Set kickoffTime
     *
     * @param kickoffTime the kickoffTime to set
     */
    public void setKickoffTime(Date kickoffTime) {
        this.kickoffTime = kickoffTime;
    }

    /**
     * Set startTime
     *
     * @param kickoffTime the kickoffTime to set
     */
    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    /**
     * Set endTime
     *
     * @param endTime the endTime to set
     */
    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    /**
     * Get pauseTime
     *
     * @return pauseTime
     */
    public Date getPauseTime() {
        return pauseTime;
    }

    /**
     * Set pauseTime
     *
     * @param pauseTime the pauseTime to set
     */
    public void setPauseTime(Date pauseTime) {
        this.pauseTime = pauseTime;
    }

    /**
     * Get createdTime
     *
     * @return createdTime
     */
    public Date getCreatedTime() {
        return createdTime;
    }

    /**
     * Set createdTime
     *
     * @param createdTime the createdTime to set
     */
    public void setCreatedTime(Date createdTime) {
        this.createdTime = createdTime;
    }

    /**
     * Set timeUnit
     *
     * @param timeUnit the timeUnit to set
     */
    public void setTimeUnit(Timeunit timeUnit) {
        this.timeUnit = timeUnit;
    }

    /**
     * Set timeOut
     *
     * @param timeOut the timeOut to set
     */
    public void setTimeOut(int timeOut) {
        this.timeOut = timeOut;
    }

    /**
     * Set user
     *
     * @param user the user to set
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * Set group
     *
     * @param group the group to set
     */
    public void setGroup(String group) {
        this.group = group;
    }

    /**
     * Set consoleUrl
     *
     * @param consoleUrl the consoleUrl to set
     */
    public void setConsoleUrl(String consoleUrl) {
        this.consoleUrl = consoleUrl;
    }

    /**
     * Set coordJobs
     *
     * @param coordJobs the coordJobs to set
     */
    public void setCoordJobs(List<? extends JsonCoordinatorJob> coordJobs) {
        this.coordJobs = (coordJobs != null) ? coordJobs : new ArrayList<JsonCoordinatorJob>();
    }

    /**
     * Convert a Bundle job list into a JSONArray.
     *
     * @param application list.
     * @param timeZoneId time zone to use for dates in the JSON array.
     * @return the corresponding JSON array.
     */
    @SuppressWarnings("unchecked")
    public static JSONArray toJSONArray(List<? extends JsonBundleJob> applications, String timeZoneId) {
        JSONArray array = new JSONArray();
        if (applications != null) {
            for (JsonBundleJob application : applications) {
                array.add(application.toJSONObject(timeZoneId));
            }
        }
        return array;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.client.Job#getStartTime()
     */
    @Override
    public Date getStartTime() {
        return startTime;
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

    @Override
    public String toString() {
        return MessageFormat.format("Bundle id[{0}] status[{1}]", getId(), getStatus());
    }
}
