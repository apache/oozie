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
import java.util.Date;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Lob;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;

import org.apache.hadoop.io.Writable;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.rest.JsonBundleJob;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.WritableUtils;
import org.apache.openjpa.persistence.jdbc.Index;

@Entity
@NamedQueries( {
        @NamedQuery(name = "UPDATE_BUNDLE_JOB", query = "update BundleJobBean w set w.appName = :appName, w.appPath = :appPath, w.conf = :conf, w.externalId = :externalId, w.timeOut = :timeOut, w.authToken = :authToken, w.createdTimestamp = :createdTimestamp, w.endTimestamp = :endTimestamp, w.jobXml = :jobXml, w.lastModifiedTimestamp = :lastModifiedTimestamp, w.origJobXml = :origJobXml, w.startTimestamp = :startTimestamp, w.status = :status, w.timeUnitStr = :timeUnit, w.pending = :pending where w.id = :id"),

        @NamedQuery(name = "UPDATE_BUNDLE_JOB_STATUS", query = "update BundleJobBean w set w.status = :status, w.lastModifiedTimestamp = :lastModifiedTimestamp, w.pending = :pending where w.id = :id"),

        @NamedQuery(name = "DELETE_BUNDLE_JOB", query = "delete from BundleJobBean w where w.id = :id"),

        @NamedQuery(name = "GET_BUNDLE_JOBS", query = "select OBJECT(w) from BundleJobBean w"),

        @NamedQuery(name = "GET_BUNDLE_JOB", query = "select OBJECT(w) from BundleJobBean w where w.id = :id"),

        @NamedQuery(name = "GET_BUNDLE_JOBS_COUNT", query = "select count(w) from BundleJobBean w"),

        @NamedQuery(name = "GET_BUNDLE_JOBS_COLUMNS", query = "select w.id, w.appName, w.appPath, w.conf, w.status, w.kickoffTimestamp, w.startTimestamp, w.endTimestamp, w.pauseTimestamp, w.createdTimestamp, w.user, w.group, w.timeUnitStr, w.timeOut from BundleJobBean w order by w.createdTimestamp desc"),

        @NamedQuery(name = "GET_BUNDLE_JOBS_RUNNING_OR_PENDING", query = "select OBJECT(w) from BundleJobBean w where w.status = 'RUNNING' OR w.status = 'RUNNINGWITHERROR' OR w.pending = 1 order by w.lastModifiedTimestamp"),

        @NamedQuery(name = "GET_BUNDLE_JOBS_NEED_START", query = "select OBJECT(w) from BundleJobBean w where w.status = 'PREP' AND (w.kickoffTimestamp IS NULL OR (w.kickoffTimestamp IS NOT NULL AND w.kickoffTimestamp <= :currentTime)) order by w.lastModifiedTimestamp"),

        @NamedQuery(name = "GET_BUNDLE_JOBS_PAUSED", query = "select OBJECT(w) from BundleJobBean w where w.status = 'PAUSED' OR w.status = 'PAUSEDWITHERROR' OR w.status = 'PREPPAUSED' order by w.lastModifiedTimestamp"),

        @NamedQuery(name = "GET_BUNDLE_JOBS_UNPAUSED", query = "select OBJECT(w) from BundleJobBean w where w.status = 'RUNNING' OR w.status = 'RUNNINGWITHERROR' OR w.status = 'PREP' order by w.lastModifiedTimestamp"),

        @NamedQuery(name = "GET_BUNDLE_JOBS_OLDER_THAN", query = "select OBJECT(w) from BundleJobBean w where w.startTimestamp <= :matTime AND (w.status = 'PREP' OR w.status = 'RUNNING' or w.status = 'RUNNINGWITHERROR')  order by w.lastModifiedTimestamp"),

        @NamedQuery(name = "GET_BUNDLE_JOBS_OLDER_THAN_STATUS", query = "select OBJECT(w) from BundleJobBean w where w.status = :status AND w.lastModifiedTimestamp <= :lastModTime order by w.lastModifiedTimestamp"),

        @NamedQuery(name = "GET_COMPLETED_BUNDLE_JOBS_OLDER_THAN", query = "select OBJECT(w) from BundleJobBean w where ( w.status = 'SUCCEEDED' OR w.status = 'FAILED' OR w.status = 'KILLED' OR w.status = 'DONEWITHERROR') AND w.lastModifiedTimestamp <= :lastModTime order by w.lastModifiedTimestamp"),

        @NamedQuery(name = "BULK_MONITOR_BUNDLE_QUERY", query = "SELECT b.id, b.status FROM BundleJobBean b WHERE b.appName = :appName"),

        // Join query
        @NamedQuery(name = "BULK_MONITOR_ACTIONS_QUERY", query = "SELECT a.id, a.actionNumber, a.errorCode, a.errorMessage, a.externalId, " +
                "a.externalStatus, a.status, a.createdTimestamp, a.nominalTimestamp, a.missingDependencies, " +
                "c.id, c.appName, c.status FROM CoordinatorActionBean a, CoordinatorJobBean c " +
                "WHERE a.jobId = c.id AND c.bundleId = :bundleId ORDER BY a.jobId, a.createdTimestamp"),

        @NamedQuery(name = "BULK_MONITOR_COUNT_QUERY", query = "SELECT COUNT(a) FROM CoordinatorActionBean a, CoordinatorJobBean c") })
public class BundleJobBean extends JsonBundleJob implements Writable {

    @Basic
    @Index
    @Column(name = "status")
    private String status = Job.Status.PREP.toString();

    @Basic
    @Column(name = "auth_token")
    @Lob
    private String authToken = null;

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

    @Column(name = "job_xml")
    @Lob
    private String jobXml = null;

    @Column(name = "orig_job_xml")
    @Lob
    private String origJobXml = null;

    /**
     * @return the authToken
     */
    public String getAuthToken() {
        return authToken;
    }

    /**
     * @param authToken the authToken to set
     */
    public void setAuthToken(String authToken) {
        this.authToken = authToken;
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
        super.setKickoffTime(DateUtils.toDate(kickoffTimestamp));
        this.kickoffTimestamp = kickoffTimestamp;
    }

    /**
     * @param startTimestamp the startTimestamp to set
     */
    public void setStartTimestamp(java.sql.Timestamp startTimestamp) {
        super.setStartTime(DateUtils.toDate(startTimestamp));
        this.startTimestamp = startTimestamp;
    }

    /**
     * Set startTime
     *
     * @param startTime the startTime to set
     */
    @Override
    public void setStartTime(Date startTime) {
        super.setStartTime(startTime);
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
        super.setEndTime(DateUtils.toDate(endTimestamp));
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
        super.setPauseTime(DateUtils.toDate(pauseTimestamp));
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
        super.setPending();
        this.pending = 1;
    }

    /**
     * Set pending to false
     *
     * @param pending set pending to false
     */
    @Override
    public void resetPending() {
        super.resetPending();
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
        return jobXml;
    }

    /**
     * @param jobXml the jobXml to set
     */
    public void setJobXml(String jobXml) {
        this.jobXml = jobXml;
    }

    /**
     * @return the origJobXml
     */
    public String getOrigJobXml() {
        return origJobXml;
    }

    /**
     * @param origJobXml the origJobXml to set
     */
    public void setOrigJobXml(String origJobXml) {
        this.origJobXml = origJobXml;
    }

    /**
     * @param createTime the createdTime to set
     */
    @Override
    public void setCreatedTime(Date createTime) {
        super.setCreatedTime(createTime);
        this.createdTimestamp = DateUtils.convertDateToTimestamp(createTime);
    }

    /**
     * @param lastModifiedTime
     */
    public void setLastModifiedTime(Date lastModifiedTime) {
        this.lastModifiedTimestamp = DateUtils.convertDateToTimestamp(lastModifiedTime);
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
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

    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
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

    /* (non-Javadoc)
     * @see org.apache.oozie.client.rest.JsonBundleJob#getStatus()
     */
    @Override
    public Status getStatus() {
        return Status.valueOf(this.status);
    }

    /**
     * @return status string
     */
    public String getStatusStr() {
        return status;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.client.rest.JsonBundleJob#getEndTime()
     */
    @Override
    public Date getEndTime() {
        return DateUtils.toDate(endTimestamp);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.client.rest.JsonBundleJob#getKickoffTime()
     */
    @Override
    public Date getKickoffTime() {
        return DateUtils.toDate(kickoffTimestamp);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.client.rest.JsonBundleJob#getTimeUnit()
     */
    @Override
    public Timeunit getTimeUnit() {
        return Timeunit.valueOf(this.timeUnitStr);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.client.rest.JsonBundleJob#setEndTime(java.util.Date)
     */
    @Override
    public void setEndTime(Date endTime) {
        super.setEndTime(endTime);
        this.endTimestamp = DateUtils.convertDateToTimestamp(endTime);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.client.rest.JsonBundleJob#setKickoffTime(java.util.Date)
     */
    @Override
    public void setKickoffTime(Date kickoffTime) {
        super.setKickoffTime(kickoffTime);
        this.kickoffTimestamp = DateUtils.convertDateToTimestamp(kickoffTime);
    }

    @Override
    /* (non-Javadoc)
     * @see org.apache.oozie.client.rest.JsonBundleJob#getPauseTime()
     */
    public Date getPauseTime() {
        return DateUtils.toDate(pauseTimestamp);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.client.rest.JsonBundleJob#setPauseTime(java.util.Date)
     */
    @Override
    public void setPauseTime(Date pauseTime) {
        super.setPauseTime(pauseTime);
        this.pauseTimestamp = DateUtils.convertDateToTimestamp(pauseTime);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.client.rest.JsonBundleJob#setStatus(org.apache.oozie.client.BundleJob.Status)
     */
    @Override
    public void setStatus(org.apache.oozie.client.BundleJob.Status val) {
        super.setStatus(val);
        this.status = val.toString();
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.client.rest.JsonBundleJob#setTimeUnit(org.apache.oozie.client.BundleJob.Timeunit)
     */
    @Override
    public void setTimeUnit(Timeunit timeUnit) {
        super.setTimeUnit(timeUnit);
        this.timeUnitStr = timeUnit.toString();
    }

    /**
     * @param return the suspendTime
     */
    public void setSuspendedTime(Date suspendTime) {
        this.suspendedTimestamp = DateUtils.convertDateToTimestamp(suspendTime);
    }

}
