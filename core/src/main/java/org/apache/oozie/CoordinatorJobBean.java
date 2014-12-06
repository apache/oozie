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

import org.apache.hadoop.io.Writable;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.JsonUtils;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.WritableUtils;
import org.apache.openjpa.persistence.jdbc.Index;
import org.apache.openjpa.persistence.jdbc.Strategy;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.NamedNativeQueries;
import javax.persistence.NamedNativeQuery;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.persistence.Transient;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Entity
@NamedQueries( {
        @NamedQuery(name = "UPDATE_COORD_JOB", query = "update CoordinatorJobBean w set w.appName = :appName, w.appPath = :appPath,w.concurrency = :concurrency, w.conf = :conf, w.externalId = :externalId, w.frequency = :frequency, w.lastActionNumber = :lastActionNumber, w.timeOut = :timeOut, w.timeZone = :timeZone, w.createdTimestamp = :createdTime, w.endTimestamp = :endTime, w.execution = :execution, w.jobXml = :jobXml, w.lastActionTimestamp = :lastAction, w.lastModifiedTimestamp = :lastModifiedTime, w.nextMaterializedTimestamp = :nextMaterializedTime, w.origJobXml = :origJobXml, w.slaXml=:slaXml, w.startTimestamp = :startTime, w.statusStr = :status, w.timeUnitStr = :timeUnit, w.appNamespace = :appNamespace, w.bundleId = :bundleId, w.matThrottling = :matThrottling  where w.id = :id"),

        @NamedQuery(name = "UPDATE_COORD_JOB_STATUS", query = "update CoordinatorJobBean w set w.statusStr =:status, w.lastModifiedTimestamp = :lastModifiedTime where w.id = :id"),

        @NamedQuery(name = "UPDATE_COORD_JOB_PENDING", query = "update CoordinatorJobBean w set w.pending = :pending, w.lastModifiedTimestamp = :lastModifiedTime where w.id = :id"),

        @NamedQuery(name = "UPDATE_COORD_JOB_BUNDLEID", query = "update CoordinatorJobBean w set w.bundleId = :bundleId where w.id = :id"),

        @NamedQuery(name = "UPDATE_COORD_JOB_APPNAMESPACE", query = "update CoordinatorJobBean w set w.appNamespace = :appNamespace where w.id = :id"),

        @NamedQuery(name = "UPDATE_COORD_JOB_STATUS_PENDING", query = "update CoordinatorJobBean w set w.statusStr = :status, w.pending = :pending where w.id = :id"),

        @NamedQuery(name = "UPDATE_COORD_JOB_BUNDLEID_APPNAMESPACE_PAUSETIME", query = "update CoordinatorJobBean w set w.bundleId = :bundleId, w.appNamespace = :appNamespace, w.pauseTimestamp = :pauseTime where w.id = :id"),

        @NamedQuery(name = "UPDATE_COORD_JOB_STATUS_MODTIME", query = "update CoordinatorJobBean w set w.statusStr = :status, w.lastModifiedTimestamp = :lastModifiedTime where w.id = :id"),

        @NamedQuery(name = "UPDATE_COORD_JOB_STATUS_PENDING_MODTIME", query = "update CoordinatorJobBean w set w.statusStr = :status, w.lastModifiedTimestamp = :lastModifiedTime, w.pending = :pending where w.id = :id"),

        @NamedQuery(name = "UPDATE_COORD_JOB_LAST_MODIFIED_TIME", query = "update CoordinatorJobBean w set w.lastModifiedTimestamp = :lastModifiedTime where w.id = :id"),

        @NamedQuery(name = "UPDATE_COORD_JOB_STATUS_PENDING_TIME", query = "update CoordinatorJobBean w set w.statusStr = :status, w.pending = :pending, w.doneMaterialization = :doneMaterialization, w.lastModifiedTimestamp = :lastModifiedTime, w.suspendedTimestamp = :suspendedTime where w.id = :id"),

        @NamedQuery(name = "UPDATE_COORD_JOB_MATERIALIZE", query = "update CoordinatorJobBean w set w.statusStr = :status, w.pending = :pending, w.doneMaterialization = :doneMaterialization, w.lastActionTimestamp = :lastActionTime, w.lastActionNumber = :lastActionNumber, w.nextMaterializedTimestamp = :nextMatdTime where w.id = :id"),

        @NamedQuery(name = "UPDATE_COORD_JOB_CHANGE", query = "update CoordinatorJobBean w set w.endTimestamp = :endTime, w.statusStr = :status, w.pending = :pending, w.doneMaterialization = :doneMaterialization, w.concurrency = :concurrency, w.pauseTimestamp = :pauseTime, w.lastActionNumber = :lastActionNumber, w.lastActionTimestamp = :lastActionTime, w.nextMaterializedTimestamp = :nextMatdTime, w.lastModifiedTimestamp = :lastModifiedTime where w.id = :id"),

        @NamedQuery(name = "DELETE_COORD_JOB", query = "delete from CoordinatorJobBean w where w.id IN (:id)"),

        @NamedQuery(name = "GET_COORD_JOBS", query = "select OBJECT(w) from CoordinatorJobBean w"),

        @NamedQuery(name = "GET_COORD_JOB", query = "select OBJECT(w) from CoordinatorJobBean w where w.id = :id"),

        @NamedQuery(name = "GET_COORD_JOB_USER_APPNAME", query = "select w.user, w.appName from CoordinatorJobBean w where w.id = :id"),

        @NamedQuery(name = "GET_COORD_JOB_INPUT_CHECK", query = "select w.user, w.appName, w.statusStr, w.appNamespace, w.execution, w.frequency, w.timeUnitStr, w.timeZone, w.endTimestamp from CoordinatorJobBean w where w.id = :id"),

        @NamedQuery(name = "GET_COORD_JOB_ACTION_READY", query = "select w.id, w.user, w.group, w.appName, w.statusStr, w.execution, w.concurrency from CoordinatorJobBean w where w.id = :id"),

        @NamedQuery(name = "GET_COORD_JOB_ACTION_KILL", query = "select w.id, w.user, w.group, w.appName, w.statusStr from CoordinatorJobBean w where w.id = :id"),

        @NamedQuery(name = "GET_COORD_JOB_MATERIALIZE", query = "select w.id, w.user, w.group, w.appName, w.statusStr, w.frequency, w.matThrottling, w.timeOut, w.timeZone, w.startTimestamp, w.endTimestamp, w.pauseTimestamp, w.nextMaterializedTimestamp, w.lastActionTimestamp, w.lastActionNumber, w.doneMaterialization, w.bundleId, w.conf, w.jobXml, w.appNamespace, w.timeUnitStr, w.execution from CoordinatorJobBean w where w.id = :id"),

        @NamedQuery(name = "GET_COORD_JOB_SUSPEND_KILL", query = "select w.id, w.user, w.group, w.appName, w.statusStr, w.bundleId, w.appNamespace, w.doneMaterialization from CoordinatorJobBean w where w.id = :id"),

        @NamedQuery(name = "GET_COORD_JOBS_PENDING", query = "select OBJECT(w) from CoordinatorJobBean w where w.pending = 1 order by w.lastModifiedTimestamp"),

        @NamedQuery(name = "GET_COORD_JOBS_CHANGED", query = "select OBJECT(w) from CoordinatorJobBean w where w.pending = 1 AND w.doneMaterialization = 1 AND w.lastModifiedTimestamp >= :lastModifiedTime"),

        @NamedQuery(name = "GET_COORD_JOBS_COUNT", query = "select count(w) from CoordinatorJobBean w"),

        @NamedQuery(name = "GET_COORD_JOBS_COLUMNS", query = "select w.id, w.appName, w.statusStr, w.user, w.group, w.startTimestamp, w.endTimestamp, w.appPath, w.concurrency, w.frequency, w.lastActionTimestamp, w.nextMaterializedTimestamp, w.createdTimestamp, w.timeUnitStr, w.timeZone, w.timeOut from CoordinatorJobBean w order by w.createdTimestamp desc"),

        //TODO need to remove.
        @NamedQuery(name = "GET_COORD_JOBS_OLDER_THAN", query = "select OBJECT(w) from CoordinatorJobBean w where w.startTimestamp <= :matTime AND (w.statusStr = 'PREP' OR w.statusStr = 'RUNNING' or w.statusStr = 'RUNNINGWITHERROR') AND (w.nextMaterializedTimestamp < :matTime OR w.nextMaterializedTimestamp IS NULL) AND (w.nextMaterializedTimestamp IS NULL OR (w.endTimestamp > w.nextMaterializedTimestamp AND (w.pauseTimestamp IS NULL OR w.pauseTimestamp > w.nextMaterializedTimestamp))) order by w.lastModifiedTimestamp"),

        @NamedQuery(name = "GET_COORD_JOBS_OLDER_FOR_MATERILZATION", query = "select w.id from CoordinatorJobBean w where w.startTimestamp <= :matTime AND (w.statusStr = 'PREP' OR w.statusStr = 'RUNNING' or w.statusStr = 'RUNNINGWITHERROR') AND (w.nextMaterializedTimestamp < :matTime OR w.nextMaterializedTimestamp IS NULL) AND (w.nextMaterializedTimestamp IS NULL OR (w.endTimestamp > w.nextMaterializedTimestamp AND (w.pauseTimestamp IS NULL OR w.pauseTimestamp > w.nextMaterializedTimestamp))) and w.matThrottling > ( select count(a.jobId) from CoordinatorActionBean a where a.jobId = w.id and a.statusStr = 'WAITING') order by w.lastModifiedTimestamp"),

        @NamedQuery(name = "GET_COORD_JOBS_OLDER_THAN_STATUS", query = "select OBJECT(w) from CoordinatorJobBean w where w.statusStr = :status AND w.lastModifiedTimestamp <= :lastModTime order by w.lastModifiedTimestamp"),

        @NamedQuery(name = "GET_COMPLETED_COORD_JOBS_OLDER_THAN_STATUS", query = "select OBJECT(w) from CoordinatorJobBean w where ( w.statusStr = 'SUCCEEDED' OR w.statusStr = 'FAILED' or w.statusStr = 'KILLED') AND w.lastModifiedTimestamp <= :lastModTime order by w.lastModifiedTimestamp"),

        @NamedQuery(name = "GET_COMPLETED_COORD_JOBS_WITH_NO_PARENT_OLDER_THAN_STATUS", query = "select w.id from CoordinatorJobBean w where ( w.statusStr = 'SUCCEEDED' OR w.statusStr = 'FAILED' or w.statusStr = 'KILLED' or w.statusStr = 'DONEWITHERROR') AND w.lastModifiedTimestamp <= :lastModTime and w.bundleId is null order by w.lastModifiedTimestamp"),

        @NamedQuery(name = "GET_COORD_JOBS_UNPAUSED", query = "select OBJECT(w) from CoordinatorJobBean w where w.statusStr = 'RUNNING' OR w.statusStr = 'RUNNINGWITHERROR' OR w.statusStr = 'PREP' order by w.lastModifiedTimestamp"),

        @NamedQuery(name = "GET_COORD_JOBS_PAUSED", query = "select OBJECT(w) from CoordinatorJobBean w where w.statusStr = 'PAUSED' OR w.statusStr = 'PAUSEDWITHERROR' OR w.statusStr = 'PREPPAUSED' order by w.lastModifiedTimestamp"),

        @NamedQuery(name = "GET_COORD_JOBS_FOR_BUNDLE", query = "select OBJECT(w) from CoordinatorJobBean w where w.bundleId = :bundleId order by w.lastModifiedTimestamp"),

        @NamedQuery(name = "GET_COORD_JOBS_WITH_PARENT_ID", query = "select w.id from CoordinatorJobBean w where w.bundleId = :parentId"),

        @NamedQuery(name = "GET_COORD_COUNT_WITH_PARENT_ID_NOT_READY_FOR_PURGE", query = "select count(w) from CoordinatorJobBean w where w.bundleId = :parentId and (w.statusStr NOT IN ('SUCCEEDED', 'FAILED', 'KILLED', 'DONEWITHERROR') OR w.lastModifiedTimestamp >= :lastModTime)"),

        @NamedQuery(name = "GET_COORD_JOB_FOR_USER_APPNAME", query = "select w.user, w.appName from CoordinatorJobBean w where w.id = :id"),

        @NamedQuery(name = "GET_COORD_JOB_FOR_USER", query = "select w.user from CoordinatorJobBean w where w.id = :id"),

        @NamedQuery(name = "GET_COORD_JOB_STATUS", query = "select w.statusStr from CoordinatorJobBean w where w.id = :id"),

        @NamedQuery(name = "GET_COORD_JOB_STATUS_PARENTID", query = "select w.statusStr, w.bundleId from CoordinatorJobBean w where w.id = :id"),

        @NamedQuery(name = "GET_COORD_IDS_FOR_STATUS_TRANSIT", query = "select DISTINCT w.id from CoordinatorActionBean a, CoordinatorJobBean w where w.id = a.jobId and a.lastModifiedTimestamp >= :lastModifiedTime and (w.statusStr IN ('PAUSED', 'RUNNING', 'RUNNINGWITHERROR', 'PAUSEDWITHERROR') or w.pending = 1)  and w.statusStr <> 'IGNORED'")

})
@NamedNativeQueries({
        @NamedNativeQuery(name = "GET_COORD_FOR_ABANDONEDCHECK", query = "select w.id, w.USER_NAME, w.group_name, w.APP_NAME from coord_jobs w where ( w.STATUS = 'RUNNING' or w.STATUS = 'RUNNINGWITHERROR' ) and w.start_time < ?2 and w.created_time < ?2 and w.id in (select failedJobs.job_id from (select a.job_id from coord_actions a where ( a.STATUS = 'FAILED' or a.STATUS = 'TIMEDOUT'  or a.STATUS = 'SUSPENDED') group by a.job_id having count(*) >= ?1 ) failedJobs LEFT OUTER JOIN (select b.job_id from coord_actions b where b.STATUS = 'SUCCEEDED' group by b.job_id having count(*) > 0 ) successJobs   on  failedJobs.job_id = successJobs.job_id where successJobs.job_id is null )")

})
@Table(name = "COORD_JOBS")
public class CoordinatorJobBean implements Writable, CoordinatorJob, JsonBean {

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
    private StringBlob conf = null;

    @Basic
    @Column(name = "frequency")
    private String frequency = "0";

    @Basic
    @Column(name = "time_zone")
    private String timeZone = null;

    @Basic
    @Column(name = "concurrency")
    private int concurrency = 0;

    @Basic
    @Column(name = "mat_throttling")
    private int matThrottling = 0;

    @Basic
    @Column(name = "time_out")
    private int timeOut = 0;

    @Basic
    @Column(name = "last_action_number")
    private int lastActionNumber;

    @Basic
    @Column(name = "user_name")
    private String user = null;

    @Basic
    @Column(name = "group_name")
    private String group = null;

    @Basic
    @Index
    @Column(name = "bundle_id")
    private String bundleId = null;

    @Transient
    private String consoleUrl;

    @Transient
    private List<CoordinatorActionBean> actions;

    @Transient
    private int numActions = 0;

    @Basic
    @Index
    @Column(name = "status")
    private String statusStr = CoordinatorJob.Status.PREP.toString();

    @Basic
    @Column(name = "start_time")
    private java.sql.Timestamp startTimestamp = null;

    @Basic
    @Index
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
    private String timeUnitStr = CoordinatorJob.Timeunit.NONE.toString();

    @Basic
    @Column(name = "execution")
    private String execution = CoordinatorJob.Execution.FIFO.toString();

    @Basic
    @Column(name = "last_action")
    private java.sql.Timestamp lastActionTimestamp = null;

    @Basic
    @Index
    @Column(name = "next_matd_time")
    private java.sql.Timestamp nextMaterializedTimestamp = null;

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
    private StringBlob jobXml = null;

    @Basic
    @Column(name = "orig_job_xml")
    @Lob
    @Strategy("org.apache.oozie.executor.jpa.StringBlobValueHandler")
    private StringBlob origJobXml = null;

    @Basic
    @Column(name = "sla_xml")
    @Lob
    @Strategy("org.apache.oozie.executor.jpa.StringBlobValueHandler")
    private StringBlob slaXml = null;

    @Basic
    @Column(name = "pending")
    private int pending = 0;

    @Basic
    @Column(name = "done_materialization")
    private int doneMaterialization = 0;

    @Basic
    @Column(name = "app_namespace")
    private String appNamespace = null;

    /**
     * Get start timestamp
     *
     * @return start timestamp
     */
    public java.sql.Timestamp getStartTimestamp() {
        return startTimestamp;
    }

    /**
     * Set start timestamp
     *
     * @param startTimestamp start timestamp
     */
    public void setStartTimestamp(java.sql.Timestamp startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    /**
     * Get end timestamp
     *
     * @return end timestamp
     */
    public java.sql.Timestamp getEndTimestamp() {
        return endTimestamp;
    }

    /**
     * Set end timestamp
     *
     * @param endTimestamp end timestamp
     */
    public void setEndTimestamp(java.sql.Timestamp endTimestamp) {
        this.endTimestamp = endTimestamp;
    }

    /**
     * Get next materialized timestamp
     *
     * @return next materialized timestamp
     */
    public Timestamp getNextMaterializedTimestamp() {
        return nextMaterializedTimestamp;
    }

    /**
     * Set next materialized timestamp
     *
     * @param nextMaterializedTimestamp next materialized timestamp
     */
    public void setNextMaterializedTimestamp(java.sql.Timestamp nextMaterializedTimestamp) {
        this.nextMaterializedTimestamp = nextMaterializedTimestamp;
    }

    /**
     * Get last modified timestamp
     *
     * @return last modified timestamp
     */
    public Timestamp getLastModifiedTimestamp() {
        return lastModifiedTimestamp;
    }

    /**
     * Set last modified timestamp
     *
     * @param lastModifiedTimestamp last modified timestamp
     */
    public void setLastModifiedTimestamp(java.sql.Timestamp lastModifiedTimestamp) {
        this.lastModifiedTimestamp = lastModifiedTimestamp;
    }

    /**
     * Get suspended timestamp
     *
     * @return suspended timestamp
     */
    public Timestamp getSuspendedTimestamp() {
        return suspendedTimestamp;
    }

    /**
     * Set suspended timestamp
     *
     * @param suspendedTimestamp suspended timestamp
     */
    public void setSuspendedTimestamp(java.sql.Timestamp suspendedTimestamp) {
        this.suspendedTimestamp = suspendedTimestamp;
    }

    /**
     * Get job xml
     *
     * @return job xml
     */
    public String getJobXml() {
        return jobXml == null ? null : jobXml.getString();
    }

    /**
     * Set job xml
     *
     * @param jobXml job xml
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
     * Get original job xml
     *
     * @return original job xml
     */
    public String getOrigJobXml() {
        return origJobXml == null ? null : origJobXml.getString();
    }

    /**
     * Set original job xml
     *
     * @param origJobXml
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
     * Get sla xml
     *
     * @return sla xml
     */
    public String getSlaXml() {
        return slaXml == null ? null : slaXml.getString();
    }

    /**
     * Set sla xml
     *
     * @param slaXml sla xml
     */
    public void setSlaXml(String slaXml) {
        if (this.slaXml == null) {
            this.slaXml = new StringBlob(slaXml);
        }
        else {
            this.slaXml.setString(slaXml);
        }
    }

    public void setSlaXmlBlob(StringBlob slaXml) {
        this.slaXml = slaXml;
    }

    public StringBlob getSlaXmlBlob() {
        return slaXml;
    }



    /**
     * Set last action timestamp
     *
     * @param lastActionTimestamp last action timestamp
     */
    public void setLastActionTimestamp(java.sql.Timestamp lastActionTimestamp) {
        this.lastActionTimestamp = lastActionTimestamp;
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
     * Set doneMaterialization to true
     */
    public void setDoneMaterialization() {
        this.doneMaterialization = 1;
    }

    /**
     * Set doneMaterialization
     */
    public void setDoneMaterialization(int i) {
        this.doneMaterialization = i;
    }

    /**
     * Set doneMaterialization to false
     */
    public void resetDoneMaterialization() {
        this.doneMaterialization = 0;
    }

    /**
     * Return if the action is done with materialization
     *
     * @return if the action is done with materialization
     */
    public boolean isDoneMaterialization() {
        return doneMaterialization == 1 ? true : false;
    }


    /**
     * Get app namespce
     *
     * @return app namespce
     */
    public String getAppNamespace() {
        return appNamespace;
    }

    /**
     * Set app namespce
     *
     * @param appNamespace the app namespce to set
     */
    public void setAppNamespace(String appNamespace) {
        this.appNamespace = appNamespace;
    }

    public CoordinatorJobBean() {
        actions = new ArrayList<CoordinatorActionBean>();
    }

    /*
     * Serialize the coordinator bean to a data output. @param dataOutput data
     * output. @throws IOException thrown if the coordinator bean could not be
     * serialized.
     */
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeStr(dataOutput, getAppPath());
        WritableUtils.writeStr(dataOutput, getAppName());
        WritableUtils.writeStr(dataOutput, getId());
        WritableUtils.writeStr(dataOutput, getConf());
        WritableUtils.writeStr(dataOutput, getStatusStr());
        WritableUtils.writeStr(dataOutput, getFrequency());
        WritableUtils.writeStr(dataOutput, getTimeUnit().toString());
        WritableUtils.writeStr(dataOutput, getTimeZone());
        dataOutput.writeInt(getConcurrency());
        WritableUtils.writeStr(dataOutput, getExecutionOrder().toString());
        dataOutput.writeLong((getLastActionTime() != null) ? getLastActionTime().getTime() : -1);
        dataOutput.writeLong((getNextMaterializedTime() != null) ? getNextMaterializedTime().getTime() : -1);
        dataOutput.writeLong((getStartTime() != null) ? getStartTime().getTime() : -1);
        dataOutput.writeLong((getEndTime() != null) ? getEndTime().getTime() : -1);
        WritableUtils.writeStr(dataOutput, getUser());
        WritableUtils.writeStr(dataOutput, getGroup());
        WritableUtils.writeStr(dataOutput, getExternalId());
        dataOutput.writeInt(getTimeout());
        dataOutput.writeInt(getMatThrottling());
        if (isPending()) {
            dataOutput.writeInt(1);
        } else {
            dataOutput.writeInt(0);
        }
        if (isDoneMaterialization()) {
            dataOutput.writeInt(1);
        } else {
            dataOutput.writeInt(0);
        }
        WritableUtils.writeStr(dataOutput, getAppNamespace());
    }

    /**
     * Deserialize a coordinator bean from a data input.
     *
     * @param dataInput data input.
     * @throws IOException thrown if the workflow bean could not be deserialized.
     */
    public void readFields(DataInput dataInput) throws IOException {
        setAppPath(WritableUtils.readStr(dataInput));
        setAppName(WritableUtils.readStr(dataInput));
        setId(WritableUtils.readStr(dataInput));
        setConf(WritableUtils.readStr(dataInput));
        setStatus(CoordinatorJob.Status.valueOf(WritableUtils.readStr(dataInput)));
        setFrequency(WritableUtils.readStr(dataInput));
        setTimeUnit(CoordinatorJob.Timeunit.valueOf(WritableUtils.readStr(dataInput)));
        setTimeZone(WritableUtils.readStr(dataInput));
        setConcurrency(dataInput.readInt());
        setExecutionOrder(Execution.valueOf(WritableUtils.readStr(dataInput)));

        long d = dataInput.readLong();
        if (d != -1) {
            setLastActionTime(new Date(d));
        }
        d = dataInput.readLong();
        if (d != -1) {
            setNextMaterializedTime(new Date(d));
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
        setTimeout(dataInput.readInt());
        setMatThrottling(dataInput.readInt());

        d = dataInput.readInt();
        if (d == 1) {
            setPending();
        }

        d = dataInput.readInt();
        if (d == 1) {
            setDoneMaterialization();
        }

        setAppNamespace(WritableUtils.readStr(dataInput));
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
            case IGNORED:
                isTerminal = true;
                break;
            default:
                isTerminal = false;
                break;
        }
        return isTerminal;
    }

    @Override
    public Status getStatus() {
        return Status.valueOf(this.statusStr);
    }

    /**
     * Get status
     *
     * @return status
     */
    public String getStatusStr() {
        return statusStr;
    }

    /**
     * Get status
     *
     * @return status
     */
    public void setStatusStr(String status) {
        this.statusStr = status;
    }

    @Override
    public void setStatus(Status val) {
        this.statusStr = val.toString();
    }

    /**
     * Get time unit
     *
     * @return time unit
     */
    public String getTimeUnitStr() {
        return timeUnitStr;
    }

    /**
     * Set time unit
     *
     */
    public void setTimeUnitStr(String timeunit) {
        this.timeUnitStr = timeunit;
    }

    public void setTimeUnit(Timeunit timeUnit) {
        this.timeUnitStr = timeUnit.toString();
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.client.rest.JsonCoordinatorJob#getTimeUnit()
     */
    @Override
    public Timeunit getTimeUnit() {
        return Timeunit.valueOf(this.timeUnitStr);
    }

    /**
     * Set order
     *
     * @param order
     */
    public void setExecutionOrder(Execution order) {
        this.execution = order.toString();
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.client.rest.JsonCoordinatorJob#getExecutionOrder()
     */
    @Override
    public Execution getExecutionOrder() {
        return Execution.valueOf(this.execution);
    }

    /**
     * Get execution
     *
     * @return execution
     */
    public void setExecution(String order) {
        this.execution = order;
    }

    /**
     * Get execution
     *
     * @return execution
     */
    public String getExecution() {
        return execution;
    }

    public void setLastActionTime(Date lastAction) {
        this.lastActionTimestamp = DateUtils.convertDateToTimestamp(lastAction);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.client.rest.JsonCoordinatorJob#getLastActionTime()
     */
    @Override
    public Date getLastActionTime() {
        return DateUtils.toDate(lastActionTimestamp);
    }

    /**
     * Get last action timestamp
     *
     * @return last action timestamp
     */
    public Timestamp getLastActionTimestamp() {
        return lastActionTimestamp;
    }

    public void setNextMaterializedTime(Date nextMaterializedTime) {
        this.nextMaterializedTimestamp = DateUtils.convertDateToTimestamp(nextMaterializedTime);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.client.rest.JsonCoordinatorJob#getNextMaterializedTime()
     */
    @Override
    public Date getNextMaterializedTime() {
        return DateUtils.toDate(nextMaterializedTimestamp);
    }

    /**
     * Set last modified time
     *
     * @param lastModifiedTime last modified time
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

    /**
     * Set suspended time
     *
     * @param suspendedTime suspended time
     */
    public void setSuspendedTime(Date suspendedTime) {
        this.suspendedTimestamp = DateUtils.convertDateToTimestamp(suspendedTime);
    }

    /**
     * Get suspended time
     *
     * @return suspended time
     */
    public Date getSuspendedTime() {
        return DateUtils.toDate(suspendedTimestamp);
    }

    public void setStartTime(Date startTime) {
        this.startTimestamp = DateUtils.convertDateToTimestamp(startTime);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.client.rest.JsonCoordinatorJob#getStartTime()
     */
    @Override
    public Date getStartTime() {
        return DateUtils.toDate(startTimestamp);
    }

    public void setEndTime(Date endTime) {
        this.endTimestamp = DateUtils.convertDateToTimestamp(endTime);
    }

    public void setPauseTime(Date pauseTime) {
        this.pauseTimestamp = DateUtils.convertDateToTimestamp(pauseTime);
    }

    @Override
    public Date getEndTime() {
        return DateUtils.toDate(endTimestamp);
    }

    @Override
    public Date getPauseTime() {
        return DateUtils.toDate(pauseTimestamp);
    }

    public Timestamp getPauseTimestamp() {
        return pauseTimestamp;
    }

    /**
     * Set created time
     *
     * @param createTime created time
     */
    public void setCreatedTime(Date createTime) {
        this.createdTimestamp = DateUtils.convertDateToTimestamp(createTime);
    }

    /**
     * Get created time
     *
     * @return created time
     */
    public Date getCreatedTime() {
        return DateUtils.toDate(createdTimestamp);
    }

    /**
     * Get created timestamp
     *
     * @return created timestamp
     */
    public Timestamp getCreatedTimestamp() {
        return createdTimestamp;
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
        return conf == null ? null : conf.getString();
    }

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

    public void setFrequency(String frequency) {
        this.frequency = frequency;
    }

    public String getFrequency() {
        return frequency;
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

    public void setTimeout(int timeOut) {
        this.timeOut = timeOut;
    }

    public int getTimeout() {
        return timeOut;
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
        return MessageFormat.format("Coordinator application id[{0}] status[{1}]", getId(), getStatus());
    }

    public void setActions(List<CoordinatorActionBean> nodes) {
        this.actions = (nodes != null) ? nodes : new ArrayList<CoordinatorActionBean>();
    }

    @SuppressWarnings("unchecked")
    public List<CoordinatorAction> getActions() {
        return (List) actions;
    }

    /**
     * Convert a coordinator application list into a JSONArray.
     *
     * @param applications list.
     * @param timeZoneId time zone to use for dates in the JSON array.
     * @return the corresponding JSON array.
     */
    @SuppressWarnings("unchecked")
    public static JSONArray toJSONArray(List<CoordinatorJobBean> applications, String timeZoneId) {
        JSONArray array = new JSONArray();
        if (applications != null) {
            for (CoordinatorJobBean application : applications) {
                array.add(application.toJSONObject(timeZoneId));
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
     */
    public void setPending() {
        this.pending = 1;
    }

    /**
     * Set pending to false
     */
    public void resetPending() {
        this.pending = 0;
    }

    public int getNumActions() {
        return numActions;
    }

    public void setNumActions(int numAction) {
        this.numActions = numAction;
    }

    @SuppressWarnings("unchecked")
    public JSONObject toJSONObject() {
        return toJSONObject("GMT");
    }

    @SuppressWarnings("unchecked")
    public JSONObject toJSONObject(String timeZoneId) {
        JSONObject json = new JSONObject();
        json.put(JsonTags.COORDINATOR_JOB_PATH, getAppPath());
        json.put(JsonTags.COORDINATOR_JOB_NAME, getAppName());
        json.put(JsonTags.COORDINATOR_JOB_ID, getId());
        json.put(JsonTags.COORDINATOR_JOB_EXTERNAL_ID, getExternalId());
        json.put(JsonTags.COORDINATOR_JOB_BUNDLE_ID, getBundleId());
        json.put(JsonTags.COORDINATOR_JOB_CONF, getConf());
        json.put(JsonTags.COORDINATOR_JOB_STATUS, getStatus().toString());
        json.put(JsonTags.COORDINATOR_JOB_EXECUTIONPOLICY, getExecutionOrder().toString());
        json.put(JsonTags.COORDINATOR_JOB_FREQUENCY, getFrequency());
        json.put(JsonTags.COORDINATOR_JOB_TIMEUNIT, getTimeUnit().toString());
        json.put(JsonTags.COORDINATOR_JOB_TIMEZONE, getTimeZone());
        json.put(JsonTags.COORDINATOR_JOB_CONCURRENCY, getConcurrency());
        json.put(JsonTags.COORDINATOR_JOB_TIMEOUT, getTimeout());
        json.put(JsonTags.COORDINATOR_JOB_LAST_ACTION_TIME, JsonUtils.formatDateRfc822(getLastActionTime(), timeZoneId));
        json.put(JsonTags.COORDINATOR_JOB_NEXT_MATERIALIZED_TIME,
                JsonUtils.formatDateRfc822(getNextMaterializedTime(), timeZoneId));
        json.put(JsonTags.COORDINATOR_JOB_START_TIME, JsonUtils.formatDateRfc822(getStartTime(), timeZoneId));
        json.put(JsonTags.COORDINATOR_JOB_END_TIME, JsonUtils.formatDateRfc822(getEndTime(), timeZoneId));
        json.put(JsonTags.COORDINATOR_JOB_PAUSE_TIME, JsonUtils.formatDateRfc822(getPauseTime(), timeZoneId));
        json.put(JsonTags.COORDINATOR_JOB_USER, getUser());
        json.put(JsonTags.COORDINATOR_JOB_GROUP, getGroup());
        json.put(JsonTags.COORDINATOR_JOB_ACL, getAcl());
        json.put(JsonTags.COORDINATOR_JOB_CONSOLE_URL, getConsoleUrl());
        json.put(JsonTags.COORDINATOR_JOB_MAT_THROTTLING, getMatThrottling());
        json.put(JsonTags.COORDINATOR_ACTIONS, CoordinatorActionBean.toJSONArray(actions, timeZoneId));
        json.put(JsonTags.TO_STRING,toString());
        json.put(JsonTags.COORDINATOR_JOB_NUM_ACTION, numActions);

        return json;
    }

}
