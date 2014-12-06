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
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.apache.hadoop.io.Writable;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.JsonUtils;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.WritableUtils;
import org.apache.openjpa.persistence.jdbc.Index;
import org.apache.openjpa.persistence.jdbc.Strategy;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

@Entity
@NamedQueries( {
        @NamedQuery(name = "UPDATE_BUNDLE_JOB", query = "update BundleJobBean w set w.appName = :appName, w.appPath = :appPath, w.conf = :conf, w.externalId = :externalId, w.timeOut = :timeOut, w.createdTimestamp = :createdTime, w.endTimestamp = :endTime, w.jobXml = :jobXml, w.lastModifiedTimestamp = :lastModifiedTime, w.origJobXml = :origJobXml, w.startTimestamp = :startTime, w.statusStr = :status, w.timeUnitStr = :timeUnit, w.pending = :pending where w.id = :id"),

        @NamedQuery(name = "UPDATE_BUNDLE_JOB_STATUS", query = "update BundleJobBean w set w.statusStr = :status, w.lastModifiedTimestamp = :lastModifiedTime, w.pending = :pending where w.id = :id"),

        @NamedQuery(name = "UPDATE_BUNDLE_JOB_STATUS_PENDING", query = "update BundleJobBean w set w.statusStr = :status, w.pending = :pending where w.id = :id"),

        @NamedQuery(name = "UPDATE_BUNDLE_JOB_STATUS_PENDING_MODTIME", query = "update BundleJobBean w set w.statusStr = :status, w.lastModifiedTimestamp = :lastModifiedTime, w.pending = :pending where w.id = :id"),

        @NamedQuery(name = "UPDATE_BUNDLE_JOB_STATUS_PENDING_SUSP_MOD_TIME", query = "update BundleJobBean w set w.statusStr = :status, w.lastModifiedTimestamp = :lastModifiedTime, w.pending = :pending, w.suspendedTimestamp = :suspendedTime where w.id = :id"),

        @NamedQuery(name = "UPDATE_BUNDLE_JOB_STATUS_PAUSE_ENDTIME", query = "update BundleJobBean w set w.statusStr = :status, w.pauseTimestamp = :pauseTime, w.endTimestamp = :endTime where w.id = :id"),

        @NamedQuery(name = "UPDATE_BUNDLE_JOB_PAUSE_KICKOFF", query = "update BundleJobBean w set w.kickoffTimestamp = :kickoffTime, w.pauseTimestamp = :pauseTime where w.id = :id"),

        @NamedQuery(name = "DELETE_BUNDLE_JOB", query = "delete from BundleJobBean w where w.id IN (:id)"),

        @NamedQuery(name = "GET_BUNDLE_JOBS", query = "select OBJECT(w) from BundleJobBean w"),

        @NamedQuery(name = "GET_BUNDLE_JOB", query = "select OBJECT(w) from BundleJobBean w where w.id = :id"),

        @NamedQuery(name = "GET_BUNDLE_JOB_STATUS", query = "select w.statusStr from BundleJobBean w where w.id = :id"),

        @NamedQuery(name = "GET_BUNDLE_JOB_ID_STATUS_PENDING_MODTIME", query = "select w.id, w.statusStr, w.pending, w.lastModifiedTimestamp from BundleJobBean w where w.id = :id"),

        @NamedQuery(name = "GET_BUNDLE_JOB_ID_JOBXML_CONF", query = "select w.id, w.jobXml, w.conf from BundleJobBean w where w.id = :id"),

        @NamedQuery(name = "GET_BUNDLE_JOBS_COUNT", query = "select count(w) from BundleJobBean w"),

        @NamedQuery(name = "GET_BUNDLE_JOBS_COLUMNS", query = "select w.id, w.appName, w.appPath, w.conf, w.statusStr, w.kickoffTimestamp, w.startTimestamp, w.endTimestamp, w.pauseTimestamp, w.createdTimestamp, w.user, w.group, w.timeUnitStr, w.timeOut from BundleJobBean w order by w.createdTimestamp desc"),

        @NamedQuery(name = "GET_BUNDLE_JOBS_RUNNING_OR_PENDING", query = "select OBJECT(w) from BundleJobBean w where w.statusStr = 'RUNNING' OR w.statusStr = 'RUNNINGWITHERROR' OR w.pending = 1 order by w.lastModifiedTimestamp"),

        @NamedQuery(name = "GET_BUNDLE_JOBS_NEED_START", query = "select OBJECT(w) from BundleJobBean w where w.statusStr = 'PREP' AND (w.kickoffTimestamp IS NULL OR (w.kickoffTimestamp IS NOT NULL AND w.kickoffTimestamp <= :currentTime)) order by w.lastModifiedTimestamp"),

        @NamedQuery(name = "GET_BUNDLE_JOBS_PAUSED", query = "select OBJECT(w) from BundleJobBean w where w.statusStr = 'PAUSED' OR w.statusStr = 'PAUSEDWITHERROR' OR w.statusStr = 'PREPPAUSED' order by w.lastModifiedTimestamp"),

        @NamedQuery(name = "GET_BUNDLE_JOBS_UNPAUSED", query = "select OBJECT(w) from BundleJobBean w where w.statusStr = 'RUNNING' OR w.statusStr = 'RUNNINGWITHERROR' OR w.statusStr = 'PREP' order by w.lastModifiedTimestamp"),

        @NamedQuery(name = "GET_BUNDLE_JOBS_OLDER_THAN", query = "select OBJECT(w) from BundleJobBean w where w.startTimestamp <= :matTime AND (w.statusStr = 'PREP' OR w.statusStr = 'RUNNING' or w.statusStr = 'RUNNINGWITHERROR')  order by w.lastModifiedTimestamp"),

        @NamedQuery(name = "GET_BUNDLE_JOBS_OLDER_THAN_STATUS", query = "select OBJECT(w) from BundleJobBean w where w.statusStr = :status AND w.lastModifiedTimestamp <= :lastModTime order by w.lastModifiedTimestamp"),

        @NamedQuery(name = "GET_COMPLETED_BUNDLE_JOBS_OLDER_THAN", query = "select w.id from BundleJobBean w where ( w.statusStr = 'SUCCEEDED' OR w.statusStr = 'FAILED' OR w.statusStr = 'KILLED' OR w.statusStr = 'DONEWITHERROR') AND w.lastModifiedTimestamp <= :lastModTime order by w.lastModifiedTimestamp"),

        @NamedQuery(name = "BULK_MONITOR_BUNDLE_QUERY", query = "SELECT b.id, b.appName, b.statusStr, b.user FROM BundleJobBean b"),

        // Join query
        @NamedQuery(name = "BULK_MONITOR_ACTIONS_QUERY", query = "SELECT a.id, a.actionNumber, a.errorCode, a.errorMessage, a.externalId, " +
                "a.externalStatus, a.statusStr, a.createdTimestamp, a.nominalTimestamp, a.missingDependencies, " +
                "c.id, c.appName, c.statusStr FROM CoordinatorActionBean a, CoordinatorJobBean c " +
                "WHERE a.jobId = c.id AND c.bundleId = :bundleId ORDER BY a.jobId, a.createdTimestamp"),

        @NamedQuery(name = "BULK_MONITOR_COUNT_QUERY", query = "SELECT COUNT(a) FROM CoordinatorActionBean a, CoordinatorJobBean c"),

        @NamedQuery(name = "GET_BUNDLE_IDS_FOR_STATUS_TRANSIT", query = "select DISTINCT w.id from BundleActionBean a , BundleJobBean w where a.lastModifiedTimestamp >= :lastModifiedTime and w.id = a.bundleId and (w.statusStr = 'RUNNING' OR w.statusStr = 'RUNNINGWITHERROR' OR w.statusStr = 'PAUSED' OR w.statusStr = 'PAUSEDWITHERROR' OR w.pending = 1)"),


        @NamedQuery(name = "GET_BUNDLE_JOB_FOR_USER", query = "select w.user from BundleJobBean w where w.id = :id") })
@Table(name = "BUNDLE_JOBS")
public class BundleJobBean implements Writable, BundleJob, JsonBean {

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

    @Basic
    @Column(name = "conf")
    @Lob
    @Strategy("org.apache.oozie.executor.jpa.StringBlobValueHandler")
    private StringBlob conf;

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

    @Basic
    @Index
    @Column(name = "status")
    private String statusStr = Job.Status.PREP.toString();

    @Basic
    @Column(name = "kickoff_time")
    private java.sql.Timestamp kickoffTimestamp = null;

    @Basic
    @Column(name = "start_time")
    private java.sql.Timestamp startTimestamp = null;

    @Basic
    @Column(name = "end_time")
    private java.sql.Timestamp endTimestamp = null;

    @Basic
    @Column(name = "pause_time")
    private java.sql.Timestamp pauseTimestamp = null;

    @Basic
    @Index
    @Column(name = "created_time")
    private java.sql.Timestamp createdTimestamp = null;

    @Basic
    @Column(name = "time_unit")
    private String timeUnitStr = BundleJob.Timeunit.NONE.toString();

    @Basic
    @Column(name = "pending")
    private int pending = 0;

    @Basic
    @Index
    @Column(name = "last_modified_time")
    private java.sql.Timestamp lastModifiedTimestamp = null;

    @Basic
    @Index
    @Column(name = "suspended_time")
    private java.sql.Timestamp suspendedTimestamp = null;

    @Basic
    @Column(name = "job_xml")
    @Lob
    @Strategy("org.apache.oozie.executor.jpa.StringBlobValueHandler")
    private StringBlob jobXml;

    @Basic
    @Column(name = "orig_job_xml")
    @Lob
    @Strategy("org.apache.oozie.executor.jpa.StringBlobValueHandler")
    private StringBlob origJobXml = null;


    @Transient
    private List<CoordinatorJobBean> coordJobs;

    public BundleJobBean() {
        coordJobs = new ArrayList<CoordinatorJobBean>();
    }

    /**
     * @return the kickoffTimestamp
     */
    public java.sql.Timestamp getKickoffTimestamp() {
        return kickoffTimestamp;
    }

    /**
     * @return the startTimestamp
     */
    public java.sql.Timestamp getstartTimestamp() {
        return startTimestamp;
    }

    /**
     * @param kickoffTimestamp the kickoffTimestamp to set
     */
    public void setKickoffTimestamp(java.sql.Timestamp kickoffTimestamp) {
        this.kickoffTimestamp = kickoffTimestamp;
    }

    /**
     * @param startTimestamp the startTimestamp to set
     */
    public void setStartTimestamp(java.sql.Timestamp startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    /**
     * Set startTime
     *
     * @param startTime the startTime to set
     */
    public void setStartTime(Date startTime) {
        this.startTimestamp = DateUtils.convertDateToTimestamp(startTime);
    }

    /**
     * @return the endTimestamp
     */
    public java.sql.Timestamp getEndTimestamp() {
        return endTimestamp;
    }

    /**
     * @param endTimestamp the endTimestamp to set
     */
    public void setEndTimestamp(java.sql.Timestamp endTimestamp) {
        this.endTimestamp = endTimestamp;
    }

    /**
     * @return the pauseTimestamp
     */
    public java.sql.Timestamp getPauseTimestamp() {
        return pauseTimestamp;
    }

    /**
     * @param pauseTimestamp the pauseTimestamp to set
     */
    public void setPauseTimestamp(java.sql.Timestamp pauseTimestamp) {
        this.pauseTimestamp = pauseTimestamp;
    }

    /**
     * @return the createdTimestamp
     */
    public java.sql.Timestamp getCreatedTimestamp() {
        return createdTimestamp;
    }

    /**
     * @return the createdTime
     */
    @Override
    public Date getCreatedTime() {
        return DateUtils.toDate(createdTimestamp);
    }

    /**
     * @return the timeUnitStr
     */
    public String getTimeUnitStr() {
        return timeUnitStr;
    }

    /**
     * @return the pending
     */
    public int getPending() {
        return pending;
    }

    /**
     * Set pending to true
     *
     * @param pending set pending to true
     */
    @Override
    public void setPending() {
        this.pending = 1;
    }

    /**
     * Set pending value
     *
     * @param pending set pending value
     */
    public void setPending(int i) {
        this.pending = i;
    }

    /**
     * Set pending to false
     *
     * @param pending set pending to false
     */
    @Override
    public void resetPending() {
        this.pending = 0;
    }

    /**
     * Return if the action is pending.
     *
     * @return if the action is pending.
     */
    public boolean isPending() {
        return pending == 1 ? true : false;
    }

    /**
     * @return the lastModifiedTimestamp
     */
    public java.sql.Timestamp getLastModifiedTimestamp() {
        return lastModifiedTimestamp;
    }

    /**
     * @param lastModifiedTimestamp the lastModifiedTimestamp to set
     */
    public void setLastModifiedTimestamp(java.sql.Timestamp lastModifiedTimestamp) {
        this.lastModifiedTimestamp = lastModifiedTimestamp;
    }

    /**
     * @return the suspendedTimestamp
     */
    public Timestamp getSuspendedTimestamp() {
        return suspendedTimestamp;
    }

    /**
     * @param suspendedTimestamp the suspendedTimestamp to set
     */
    public void setSuspendedTimestamp(Timestamp suspendedTimestamp) {
        this.suspendedTimestamp = suspendedTimestamp;
    }

    /**
     * @return the jobXml
     */
    public String getJobXml() {
        return jobXml == null ? null : jobXml.getString();
    }

    /**
     * @param jobXml the jobXml to set
     */
    public void setJobXml(String jobXml) {
        if (this.jobXml == null) {
            this.jobXml = new StringBlob(jobXml);
        }
        else {
            this.jobXml.setString(jobXml);
        }

    }

    public void setJobXmlBlob (StringBlob jobXmlBlob) {
        this.jobXml = jobXmlBlob;
    }

    public StringBlob getJobXmlBlob() {
        return jobXml;
    }

    /**
     * @return the origJobXml
     */
    public String getOrigJobXml() {
        return origJobXml == null ? null : origJobXml.getString();
    }

    /**
     * @param origJobXml the origJobXml to set
     */
    public void setOrigJobXml(String origJobXml) {
        if (this.origJobXml == null) {
            this.origJobXml = new StringBlob(origJobXml);
        }
        else {
            this.origJobXml.setString(origJobXml);
        }
    }

    public void setOrigJobXmlBlob (StringBlob origJobXml) {
        this.origJobXml = origJobXml;
    }

    public StringBlob getOrigJobXmlBlob() {
        return origJobXml;
    }

    /**
     * @param createTime the createdTime to set
     */
    public void setCreatedTime(Date createTime) {
        this.createdTimestamp = DateUtils.convertDateToTimestamp(createTime);
    }

    /**
     * @param lastModifiedTime
     */
    public void setLastModifiedTime(Date lastModifiedTime) {
        this.lastModifiedTimestamp = DateUtils.convertDateToTimestamp(lastModifiedTime);
    }

    /**
     * Get last modified time
     *
     * @return last modified time
     */
    public Date getLastModifiedTime() {
        return DateUtils.toDate(lastModifiedTimestamp);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeStr(dataOutput, getAppPath());
        WritableUtils.writeStr(dataOutput, getAppName());
        WritableUtils.writeStr(dataOutput, getId());
        WritableUtils.writeStr(dataOutput, getConf());
        WritableUtils.writeStr(dataOutput, getStatusStr());
        WritableUtils.writeStr(dataOutput, getTimeUnit().toString());
        dataOutput.writeLong((getKickoffTime() != null) ? getKickoffTime().getTime() : -1);
        dataOutput.writeLong((getStartTime() != null) ? getStartTime().getTime() : -1);
        dataOutput.writeLong((getEndTime() != null) ? getEndTime().getTime() : -1);
        WritableUtils.writeStr(dataOutput, getUser());
        WritableUtils.writeStr(dataOutput, getGroup());
        WritableUtils.writeStr(dataOutput, getExternalId());
        dataOutput.writeInt(getTimeout());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        setAppPath(WritableUtils.readStr(dataInput));
        setAppName(WritableUtils.readStr(dataInput));
        setId(WritableUtils.readStr(dataInput));
        setConf(WritableUtils.readStr(dataInput));
        setStatus(BundleJob.Status.valueOf(WritableUtils.readStr(dataInput)));
        setTimeUnit(BundleJob.Timeunit.valueOf(WritableUtils.readStr(dataInput)));

        long d = dataInput.readLong();
        if (d != -1) {
            setKickoffTime(new Date(d));
        }
        d = dataInput.readLong();
        if (d != -1) {
            setStartTime(new Date(d));
        }
        d = dataInput.readLong();
        if (d != -1) {
            setEndTime(new Date(d));
        }
        setUser(WritableUtils.readStr(dataInput));
        setGroup(WritableUtils.readStr(dataInput));
        setExternalId(WritableUtils.readStr(dataInput));
        setTimeOut(dataInput.readInt());
    }


    public Date getEndTime() {
        return DateUtils.toDate(endTimestamp);
    }

    @Override
    public Date getKickoffTime() {
        return DateUtils.toDate(kickoffTimestamp);
    }

    @Override
    public Timeunit getTimeUnit() {
        return Timeunit.valueOf(this.timeUnitStr);
    }

    public void setEndTime(Date endTime) {
        this.endTimestamp = DateUtils.convertDateToTimestamp(endTime);
    }

    public void setKickoffTime(Date kickoffTime) {
        this.kickoffTimestamp = DateUtils.convertDateToTimestamp(kickoffTime);
    }

    @Override
    public Date getPauseTime() {
        return DateUtils.toDate(pauseTimestamp);
    }

    public void setPauseTime(Date pauseTime) {
        this.pauseTimestamp = DateUtils.convertDateToTimestamp(pauseTime);
    }

    /**
     * @param return the suspendTime
     */
    public void setSuspendedTime(Date suspendTime) {
        this.suspendedTimestamp = DateUtils.convertDateToTimestamp(suspendTime);
    }

    @Override
    @SuppressWarnings("unchecked")
    public JSONObject toJSONObject() {
        return toJSONObject("GMT");
    }

    @Override
    @SuppressWarnings("unchecked")
    public JSONObject toJSONObject(String timeZoneId) {
        JSONObject json = new JSONObject();
        json.put(JsonTags.BUNDLE_JOB_PATH, appPath);
        json.put(JsonTags.BUNDLE_JOB_NAME, appName);
        json.put(JsonTags.BUNDLE_JOB_ID, id);
        json.put(JsonTags.BUNDLE_JOB_EXTERNAL_ID, externalId);
        json.put(JsonTags.BUNDLE_JOB_CONF, getConf());
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
        json.put(JsonTags.BUNDLE_COORDINATOR_JOBS, CoordinatorJobBean.toJSONArray(coordJobs, timeZoneId));
        json.put(JsonTags.TO_STRING, toString());

        return json;
    }

    @Override
    public String getAppName() {
        return appName;
    }

    @Override
    public String getAppPath() {
        return appPath;
    }

    @Override
    public String getConf() {
        return conf == null ? null : conf.getString();
    }

    @Override
    public String getConsoleUrl() {
        return consoleUrl;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<CoordinatorJob> getCoordinators() {
        return (List) coordJobs;
    }

    @Override
    @Deprecated
    public String getGroup() {
        return group;
    }

    @Override
    public String getAcl() {
        return getGroup();
    }

    @Override
    public String getId() {
        return id;
    }

    public int getTimeout() {
        return timeOut;
    }

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
        if (this.conf == null) {
            this.conf = new StringBlob(conf);
        }
        else {
            this.conf.setString(conf);
        }
    }

    public void setConfBlob(StringBlob conf) {
        this.conf = conf;
    }

    public StringBlob getConfBlob() {
        return conf;
    }

    /**
     * Set status
     *
     * @param status the status to set
     */
    public void setStatus(Status status) {
        this.statusStr = status.toString();
    }


    @Override
    public Status getStatus() {
        return Status.valueOf(this.statusStr);
    }

    /**
     * Set status
     *
     * @param status the status to set
     */
    public void setStatus(String statusStr) {
        this.statusStr = statusStr;
    }


    /**
     * @return status string
     */
    public String getStatusStr() {
        return statusStr;
    }


    /**
     * Set timeUnit
     *
     * @param timeUnit the timeUnit to set
     */
    public void setTimeUnit(Timeunit timeUnit) {
        this.timeUnitStr = timeUnit.toString();
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
    public void setCoordJobs(List<CoordinatorJobBean> coordJobs) {
        this.coordJobs = (coordJobs != null) ? coordJobs : new ArrayList<CoordinatorJobBean>();
    }

    /**
     * Convert a Bundle job list into a JSONArray.
     *
     * @param application list.
     * @param timeZoneId time zone to use for dates in the JSON array.
     * @return the corresponding JSON array.
     */
    @SuppressWarnings("unchecked")
    public static JSONArray toJSONArray(List<BundleJobBean> applications, String timeZoneId) {
        JSONArray array = new JSONArray();
        if (applications != null) {
            for (BundleJobBean application : applications) {
                array.add(application.toJSONObject(timeZoneId));
            }
        }
        return array;
    }


    @Override
    public String toString() {
        return MessageFormat.format("Bundle id[{0}] status[{1}]", getId(), getStatus());
    }

    @Override
    public Date getStartTime() {
        return DateUtils.toDate(startTimestamp);
    }

    /**
     * @return true if in terminal status
     */
    public boolean isTerminalStatus() {
        boolean isTerminal = false;
        switch (getStatus()) {
            case SUCCEEDED:
            case FAILED:
            case KILLED:
            case DONEWITHERROR:
                isTerminal = true;
                break;
            default:
                isTerminal = false;
                break;
        }
        return isTerminal;
    }
}
