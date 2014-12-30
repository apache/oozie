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
import javax.persistence.ColumnResult;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.NamedNativeQueries;
import javax.persistence.NamedNativeQuery;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.SqlResultSetMapping;
import javax.persistence.Table;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.Date;
import java.util.List;

@Entity
@NamedQueries({

        @NamedQuery(name = "UPDATE_COORD_ACTION", query = "update CoordinatorActionBean w set w.actionNumber = :actionNumber, w.actionXml = :actionXml, w.consoleUrl = :consoleUrl, w.createdConf = :createdConf, w.errorCode = :errorCode, w.errorMessage = :errorMessage, w.externalStatus = :externalStatus, w.missingDependencies = :missingDependencies, w.runConf = :runConf, w.timeOut = :timeOut, w.trackerUri = :trackerUri, w.type = :type, w.createdTimestamp = :createdTime, w.externalId = :externalId, w.jobId = :jobId, w.lastModifiedTimestamp = :lastModifiedTime, w.nominalTimestamp = :nominalTime, w.slaXml = :slaXml, w.statusStr = :status where w.id = :id"),

        @NamedQuery(name = "UPDATE_COORD_ACTION_MIN", query = "update CoordinatorActionBean w set w.actionXml = :actionXml, w.missingDependencies = :missingDependencies, w.lastModifiedTimestamp = :lastModifiedTime, w.statusStr = :status where w.id = :id"),
        // Query to update the action status, pending status and last modified time stamp of a Coordinator action
        @NamedQuery(name = "UPDATE_COORD_ACTION_STATUS_PENDING_TIME", query = "update CoordinatorActionBean w set w.statusStr =:status, w.pending =:pending, w.lastModifiedTimestamp = :lastModifiedTime where w.id = :id"),
        // Update query for InputCheck
        @NamedQuery(name = "UPDATE_COORD_ACTION_FOR_INPUTCHECK", query = "update CoordinatorActionBean w set w.statusStr = :status, w.lastModifiedTimestamp = :lastModifiedTime, w.actionXml = :actionXml, w.missingDependencies = :missingDependencies where w.id = :id"),
        // Update query for Push-based missing dependency check
        @NamedQuery(name = "UPDATE_COORD_ACTION_FOR_PUSH_INPUTCHECK", query = "update CoordinatorActionBean w set w.statusStr = :status, w.lastModifiedTimestamp = :lastModifiedTime,  w.actionXml = :actionXml, w.pushMissingDependencies = :pushMissingDependencies where w.id = :id"),
        // Update query for Push-based missing dependency check
        @NamedQuery(name = "UPDATE_COORD_ACTION_DEPENDENCIES", query = "update CoordinatorActionBean w set w.missingDependencies = :missingDependencies, w.pushMissingDependencies = :pushMissingDependencies where w.id = :id"),
        // Update query for Start
        @NamedQuery(name = "UPDATE_COORD_ACTION_FOR_START", query = "update CoordinatorActionBean w set w.statusStr =:status, w.lastModifiedTimestamp = :lastModifiedTime, w.runConf = :runConf, w.externalId = :externalId, w.pending = :pending, w.errorCode = :errorCode, w.errorMessage = :errorMessage  where w.id = :id"),

        @NamedQuery(name = "UPDATE_COORD_ACTION_FOR_MODIFIED_DATE", query = "update CoordinatorActionBean w set w.lastModifiedTimestamp = :lastModifiedTime where w.id = :id"),

        @NamedQuery(name = "UPDATE_COORD_ACTION_RERUN", query = "update CoordinatorActionBean w set w.actionXml =:actionXml, w.statusStr = :status, w.externalId = :externalId, w.externalStatus = :externalStatus, w.rerunTimestamp = :rerunTime, w.lastModifiedTimestamp = :lastModifiedTime, w.createdTimestamp = :createdTime, w.createdConf = :createdConf, w.runConf = :runConf, w.missingDependencies = :missingDependencies, w.pushMissingDependencies = :pushMissingDependencies, w.errorCode = :errorCode, w.errorMessage = :errorMessage where w.id = :id"),

        @NamedQuery(name = "DELETE_COMPLETED_ACTIONS_FOR_COORDINATOR", query = "delete from CoordinatorActionBean a where a.jobId = :jobId and (a.statusStr = 'SUCCEEDED' OR a.statusStr = 'FAILED' OR a.statusStr= 'KILLED')"),

        @NamedQuery(name = "DELETE_ACTIONS_FOR_LONG_RUNNING_COORDINATOR", query = "delete from CoordinatorActionBean a where a.id IN (:actionId)"),

        @NamedQuery(name = "DELETE_UNSCHEDULED_ACTION", query = "delete from CoordinatorActionBean a where a.id = :id and (a.statusStr = 'WAITING' OR a.statusStr = 'READY')"),

        @NamedQuery(name = "GET_COORD_ACTIONS_FOR_COORDINATOR", query = "select a.id from CoordinatorActionBean a where a.jobId = :jobId"),

        // Query used by XTestcase to setup tables
        @NamedQuery(name = "GET_COORD_ACTIONS", query = "select OBJECT(w) from CoordinatorActionBean w"),
        // Select query used only by test cases
        @NamedQuery(name = "GET_COORD_ACTION", query = "select OBJECT(a) from CoordinatorActionBean a where a.id = :id"),

        // Select query used by SLAService on restart
        @NamedQuery(name = "GET_COORD_ACTION_FOR_SLA", query = "select a.id, a.jobId, a.statusStr, a.externalId, a.lastModifiedTimestamp from CoordinatorActionBean a where a.id = :id"),
        // Select query used by ActionInfo command
        @NamedQuery(name = "GET_COORD_ACTION_FOR_INFO", query = "select a.id, a.jobId, a.actionNumber, a.consoleUrl, a.errorCode, a.errorMessage, a.externalId, a.externalStatus, a.trackerUri, a.createdTimestamp, a.nominalTimestamp, a.statusStr, a.lastModifiedTimestamp, a.missingDependencies, a.pushMissingDependencies from CoordinatorActionBean a where a.id = :id"),
        // Select Query used by Timeout and skip commands
        @NamedQuery(name = "GET_COORD_ACTION_FOR_TIMEOUT", query = "select a.id, a.jobId, a.statusStr, a.runConf, a.pending, a.nominalTimestamp, a.createdTimestamp from CoordinatorActionBean a where a.id = :id"),
        // Select query used by InputCheck command
        @NamedQuery(name = "GET_COORD_ACTION_FOR_INPUTCHECK", query = "select a.id, a.jobId, a.statusStr, a.runConf, a.nominalTimestamp, a.createdTimestamp, a.actionXml, a.missingDependencies, a.pushMissingDependencies, a.timeOut, a.externalId from CoordinatorActionBean a where a.id = :id"),
        // Select query used by CoordActionUpdate command
        @NamedQuery(name = "GET_COORD_ACTION_FOR_EXTERNALID", query = "select a.id, a.jobId, a.statusStr, a.pending, a.externalId, a.lastModifiedTimestamp, a.slaXml, a.nominalTimestamp, a.createdTimestamp from CoordinatorActionBean a where a.externalId = :externalId"),
        // Select query used by Check command
        @NamedQuery(name = "GET_COORD_ACTION_FOR_CHECK", query = "select a.id, a.jobId, a.statusStr, a.pending, a.externalId, a.lastModifiedTimestamp, a.slaXml, a.nominalTimestamp, a.createdTimestamp from CoordinatorActionBean a where a.id = :id"),
        // Select query used by Start command
        @NamedQuery(name = "GET_COORD_ACTION_FOR_START", query = "select a.id, a.jobId, a.statusStr, a.pending, a.createdConf, a.slaXml, a.actionXml, a.externalId, a.errorMessage, a.errorCode, a.nominalTimestamp, a.createdTimestamp from CoordinatorActionBean a where a.id = :id"),

        @NamedQuery(name = "GET_COORD_ACTIONS_FOR_JOB_FIFO", query = "select a.id, a.jobId, a.statusStr, a.pending, a.nominalTimestamp, a.createdTimestamp from CoordinatorActionBean a where a.jobId = :jobId AND a.statusStr = 'READY' order by a.nominalTimestamp"),

        @NamedQuery(name = "GET_COORD_ACTIONS_FOR_JOB_LIFO", query = "select a.id, a.jobId, a.statusStr, a.pending, a.nominalTimestamp, a.createdTimestamp from CoordinatorActionBean a where a.jobId = :jobId AND a.statusStr = 'READY' order by a.nominalTimestamp desc"),

        @NamedQuery(name = "GET_COORD_RUNNING_ACTIONS_COUNT", query = "select count(a) from CoordinatorActionBean a where a.jobId = :jobId AND (a.statusStr = 'RUNNING' OR a.statusStr='SUBMITTED')"),

        @NamedQuery(name = "GET_COORD_ACTIONS_COUNT_BY_JOBID", query = "select count(a) from CoordinatorActionBean a where a.jobId = :jobId"),

        @NamedQuery(name = "GET_COORD_ACTIVE_ACTIONS_COUNT_BY_JOBID", query = "select count(a) from CoordinatorActionBean a where a.jobId = :jobId AND a.statusStr = 'WAITING'"),

        @NamedQuery(name = "GET_COORD_ACTIONS_PENDING_FALSE_COUNT", query = "select count(a) from CoordinatorActionBean a where a.jobId = :jobId AND a.pending = 0 AND (a.statusStr = 'SUSPENDED' OR a.statusStr = 'TIMEDOUT' OR a.statusStr = 'SUCCEEDED' OR a.statusStr = 'KILLED' OR a.statusStr = 'FAILED')"),

        @NamedQuery(name = "GET_COORD_ACTIONS_PENDING_FALSE_STATUS_COUNT", query = "select count(a) from CoordinatorActionBean a where a.jobId = :jobId AND a.pending = 0 AND a.statusStr = :status"),

        @NamedQuery(name = "GET_ACTIONS_FOR_COORD_JOB", query = "select count(a) from CoordinatorActionBean a where a.jobId = :jobId"),
        // Query to retrieve Coordinator actions sorted by nominal time
        @NamedQuery(name = "GET_ACTIONS_FOR_COORD_JOB_ORDER_BY_NOMINAL_TIME", query = "select a.id, a.actionNumber, a.consoleUrl, a.errorCode, a.errorMessage, a.externalId, a.externalStatus, a.jobId, a.trackerUri, a.createdTimestamp, a.nominalTimestamp, a.statusStr, a.lastModifiedTimestamp, a.missingDependencies, a.pushMissingDependencies, a.timeOut from CoordinatorActionBean a where a.jobId = :jobId order by a.nominalTimestamp"),
        // Query to maintain backward compatibility for coord job info command
        @NamedQuery(name = "GET_ALL_COLS_FOR_ACTIONS_FOR_COORD_JOB_ORDER_BY_NOMINAL_TIME", query = "select OBJECT(a) from CoordinatorActionBean a where a.jobId = :jobId order by a.nominalTimestamp"),
        // Query to retrieve action id, action status, pending status and external Id of not completed Coordinator actions
        @NamedQuery(name = "GET_COORD_ACTIONS_NOT_COMPLETED", query = "select a.id, a.statusStr, a.pending, a.externalId, a.pushMissingDependencies, a.nominalTimestamp, a.createdTimestamp, a.jobId from CoordinatorActionBean a where a.jobId = :jobId AND a.statusStr <> 'FAILED' AND a.statusStr <> 'TIMEDOUT' AND a.statusStr <> 'SUCCEEDED' AND a.statusStr <> 'KILLED' AND a.statusStr <> 'IGNORED'"),

        // Query to retrieve action id, action status, pending status and external Id of running Coordinator actions
        @NamedQuery(name = "GET_COORD_ACTIONS_RUNNING", query = "select a.id, a.statusStr, a.pending, a.externalId, a.nominalTimestamp, a.createdTimestamp from CoordinatorActionBean a where a.jobId = :jobId and a.statusStr = 'RUNNING'"),

        // Query to retrieve action id, action status, pending status and external Id of suspended Coordinator actions
        @NamedQuery(name = "GET_COORD_ACTIONS_SUSPENDED", query = "select a.id, a.statusStr, a.pending, a.externalId, a.nominalTimestamp, a.createdTimestamp from CoordinatorActionBean a where a.jobId = :jobId and a.statusStr = 'SUSPENDED'"),

        // Query to retrieve count of Coordinator actions which are pending
        @NamedQuery(name = "GET_COORD_ACTIONS_PENDING_COUNT", query = "select count(a) from CoordinatorActionBean a where a.jobId = :jobId AND a.pending > 0"),

        // Query to retrieve status of Coordinator actions
        @NamedQuery(name = "GET_COORD_ACTIONS_STATUS_UNIGNORED", query = "select a.statusStr, a.pending from CoordinatorActionBean a where a.jobId = :jobId AND a.statusStr <> 'IGNORED'"),

        // Query to retrieve status of Coordinator actions
        @NamedQuery(name = "GET_COORD_ACTION_STATUS", query = "select a.statusStr from CoordinatorActionBean a where a.id = :id"),

        @NamedQuery(name = "GET_COORD_ACTION_FOR_COORD_JOB_BY_ACTION_NUMBER", query = "select a.id from CoordinatorActionBean a where a.jobId = :jobId AND a.actionNumber = :actionNumber"),

        @NamedQuery(name = "GET_COORD_ACTIONS_BY_LAST_MODIFIED_TIME", query = "select a.jobId from CoordinatorActionBean a where a.lastModifiedTimestamp >= :lastModifiedTime"),

        //Used by coordinator store only
        @NamedQuery(name = "GET_RUNNING_ACTIONS_FOR_COORD_JOB", query = "select OBJECT(a) from CoordinatorActionBean a where a.jobId = :jobId AND a.statusStr = 'RUNNING'"),

        @NamedQuery(name = "GET_RUNNING_ACTIONS_OLDER_THAN", query = "select a.id from CoordinatorActionBean a where a.statusStr = 'RUNNING' AND a.lastModifiedTimestamp <= :lastModifiedTime"),

        @NamedQuery(name = "GET_COORD_ACTIONS_WAITING_SUBMITTED_OLDER_THAN", query = "select a.id, a.jobId, a.statusStr, a.externalId, a.pushMissingDependencies from CoordinatorActionBean a where (a.statusStr = 'WAITING' OR a.statusStr = 'SUBMITTED') AND a.lastModifiedTimestamp <= :lastModifiedTime"),

        @NamedQuery(name = "GET_COORD_ACTIONS_FOR_RECOVERY_OLDER_THAN", query = "select a.id, a.jobId, a.statusStr, a.externalId, a.pending from CoordinatorActionBean a where a.pending > 0 AND (a.statusStr = 'SUSPENDED' OR a.statusStr = 'KILLED' OR a.statusStr = 'RUNNING') AND a.lastModifiedTimestamp <= :lastModifiedTime"),
        // Select query used by rerun, requires almost all columns so select * is used
        @NamedQuery(name = "GET_ACTIONS_FOR_DATES", query = "select OBJECT(a) from CoordinatorActionBean a where a.jobId = :jobId AND (a.statusStr = 'TIMEDOUT' OR a.statusStr = 'SUCCEEDED' OR a.statusStr = 'KILLED' OR a.statusStr = 'FAILED' OR a.statusStr = 'IGNORED') AND a.nominalTimestamp >= :startTime AND a.nominalTimestamp <= :endTime"),
        // Select query used by log
        @NamedQuery(name = "GET_ACTION_IDS_FOR_DATES", query = "select a.id from CoordinatorActionBean a where a.jobId = :jobId AND (a.statusStr = 'TIMEDOUT' OR a.statusStr = 'SUCCEEDED' OR a.statusStr = 'KILLED' OR a.statusStr = 'FAILED') AND a.nominalTimestamp >= :startTime AND a.nominalTimestamp <= :endTime"),
        // Select query used by rerun, requires almost all columns so select * is used
        @NamedQuery(name = "GET_ACTION_FOR_NOMINALTIME", query = "select OBJECT(a) from CoordinatorActionBean a where a.jobId = :jobId AND a.nominalTimestamp = :nominalTime"),

        @NamedQuery(name = "GET_ACTIONS_BY_DATES_FOR_KILL", query = "select a.id, a.jobId, a.statusStr, a.externalId, a.pending, a.nominalTimestamp, a.createdTimestamp from CoordinatorActionBean a where a.jobId = :jobId AND (a.statusStr <> 'FAILED' AND a.statusStr <> 'KILLED' AND a.statusStr <> 'SUCCEEDED' AND a.statusStr <> 'TIMEDOUT') AND a.nominalTimestamp >= :startTime AND a.nominalTimestamp <= :endTime"),

        @NamedQuery(name = "GET_COORD_ACTIONS_COUNT", query = "select count(w) from CoordinatorActionBean w"),

        @NamedQuery(name = "GET_COORD_ACTIONS_COUNT_RUNNING_FOR_RANGE", query = "select count(w) from CoordinatorActionBean w where w.statusStr = 'RUNNING' and w.jobId= :jobId and w.id >= :startAction AND w.id <= :endAction"),

        @NamedQuery(name = "GET_COORD_ACTIONS_MAX_MODIFIED_DATE_FOR_RANGE", query = "select max(w.lastModifiedTimestamp) from CoordinatorActionBean w where w.jobId= :jobId and w.id >= :startAction AND w.id <= :endAction"),

        @NamedQuery(name = "GET_READY_ACTIONS_GROUP_BY_JOBID", query = "select a.jobId, min(a.lastModifiedTimestamp) from CoordinatorActionBean a where a.statusStr = 'READY' group by a.jobId having min(a.lastModifiedTimestamp) < :lastModifiedTime")})

@Table(name = "COORD_ACTIONS")
public class CoordinatorActionBean implements
        Writable,CoordinatorAction,JsonBean {

    @Id
    private String id;

    @Basic
    @Index
    @Column(name = "job_id")
    private String jobId;

    @Basic
    @Index
    @Column(name = "status")
    private String statusStr = CoordinatorAction.Status.WAITING.toString();

    @Basic
    @Index
    @Column(name = "nominal_time")
    private java.sql.Timestamp nominalTimestamp = null;

    @Basic
    @Index
    @Column(name = "last_modified_time")
    private java.sql.Timestamp lastModifiedTimestamp = null;

    @Basic
    @Index
    @Column(name = "created_time")
    private java.sql.Timestamp createdTimestamp = null;

    @Basic
    @Index
    @Column(name = "rerun_time")
    private java.sql.Timestamp rerunTimestamp = null;

    @Basic
    @Index
    @Column(name = "external_id")
    private String externalId;

    @Basic
    @Column(name = "sla_xml")
    @Lob
    @Strategy("org.apache.oozie.executor.jpa.StringBlobValueHandler")
    private StringBlob slaXml = null;

    @Basic
    @Column(name = "pending")
    private int pending = 0;

    @Basic
    @Column(name = "job_type")
    private String type;

    @Basic
    @Column(name = "action_number")
    private int actionNumber;

    @Basic
    @Column(name = "created_conf")
    @Lob
    @Strategy("org.apache.oozie.executor.jpa.StringBlobValueHandler")
    private StringBlob createdConf;

    @Basic
    @Column(name = "time_out")
    private int timeOut = 0;

    @Basic
    @Column(name = "run_conf")
    @Lob
    @Strategy("org.apache.oozie.executor.jpa.StringBlobValueHandler")
    private StringBlob runConf;

    @Basic
    @Column(name = "action_xml")
    @Lob
    @Strategy("org.apache.oozie.executor.jpa.StringBlobValueHandler")
    private StringBlob actionXml;

    @Basic
    @Column(name = "missing_dependencies")
    @Lob
    @Strategy("org.apache.oozie.executor.jpa.StringBlobValueHandler")
    private StringBlob missingDependencies;

    @Basic
    @Column(name = "push_missing_dependencies")
    @Lob
    @Strategy("org.apache.oozie.executor.jpa.StringBlobValueHandler")
    private StringBlob pushMissingDependencies;

    @Basic
    @Column(name = "external_status")
    private String externalStatus;

    @Basic
    @Column(name = "tracker_uri")
    private String trackerUri;

    @Basic
    @Column(name = "console_url")
    private String consoleUrl;

    @Basic
    @Column(name = "error_code")
    private String errorCode;

    @Basic
    @Column(name = "error_message")
    private String errorMessage;

    @SuppressWarnings("unchecked")
    public JSONObject toJSONObject() {
        return toJSONObject("GMT");
    }

    public CoordinatorActionBean() {
    }

    /**
     * Serialize the coordinator bean to a data output.
     *
     * @param dataOutput data output.
     * @throws IOException thrown if the coordinator bean could not be
     *         serialized.
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeStr(dataOutput, getJobId());
        WritableUtils.writeStr(dataOutput, getType());
        WritableUtils.writeStr(dataOutput, getId());
        WritableUtils.writeStr(dataOutput, getCreatedConf());
        WritableUtils.writeStr(dataOutput, getStatus().toString());
        dataOutput.writeInt(getActionNumber());
        WritableUtils.writeStr(dataOutput, getRunConf());
        WritableUtils.writeStr(dataOutput, getExternalStatus());
        WritableUtils.writeStr(dataOutput, getTrackerUri());
        WritableUtils.writeStr(dataOutput, getConsoleUrl());
        WritableUtils.writeStr(dataOutput, getErrorCode());
        WritableUtils.writeStr(dataOutput, getErrorMessage());
        dataOutput.writeLong((getCreatedTime() != null) ? getCreatedTime().getTime() : -1);
        dataOutput.writeLong((getLastModifiedTime() != null) ? getLastModifiedTime().getTime() : -1);
    }

    /**
     * Deserialize a coordinator bean from a data input.
     *
     * @param dataInput data input.
     * @throws IOException thrown if the workflow bean could not be
     *         deserialized.
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        setJobId(WritableUtils.readStr(dataInput));
        setType(WritableUtils.readStr(dataInput));
        setId(WritableUtils.readStr(dataInput));
        setCreatedConf(WritableUtils.readStr(dataInput));
        setStatus(CoordinatorAction.Status.valueOf(WritableUtils.readStr(dataInput)));
        setActionNumber(dataInput.readInt());
        setRunConf(WritableUtils.readStr(dataInput));
        setExternalStatus(WritableUtils.readStr(dataInput));
        setTrackerUri(WritableUtils.readStr(dataInput));
        setConsoleUrl(WritableUtils.readStr(dataInput));
        setErrorCode(WritableUtils.readStr(dataInput));
        setErrorMessage(WritableUtils.readStr(dataInput));
        long d = dataInput.readLong();
        if (d != -1) {
            setCreatedTime(new Date(d));
        }
        d = dataInput.readLong();
        if (d != -1) {
            setLastModifiedTime(new Date(d));
        }
    }

    @Override
    public String getJobId() {
        return this.jobId;
    }

    public void setJobId(String id) {
        this.jobId = id;
    }

    @Override
    public Status getStatus() {
        return Status.valueOf(statusStr);
    }

    /**
     * Return the status in string
     * @return
     */
    public String getStatusStr() {
        return statusStr;
    }

    public void setStatus(Status status) {
        this.statusStr = status.toString();
    }

    public void setStatusStr(String statusStr) {
        this.statusStr = statusStr;
    }

    public void setCreatedTime(Date createdTime) {
        this.createdTimestamp = DateUtils.convertDateToTimestamp(createdTime);
    }

    public void setRerunTime(Date rerunTime) {
        this.rerunTimestamp = DateUtils.convertDateToTimestamp(rerunTime);
    }

    public void setNominalTime(Date nominalTime) {
        this.nominalTimestamp = DateUtils.convertDateToTimestamp(nominalTime);
    }

    public void setLastModifiedTime(Date lastModifiedTime) {
        this.lastModifiedTimestamp = DateUtils.convertDateToTimestamp(lastModifiedTime);
    }

    public Date getCreatedTime() {
        return DateUtils.toDate(createdTimestamp);
    }

    public Timestamp getCreatedTimestamp() {
        return createdTimestamp;
    }

    public Date getRerunTime() {
        return DateUtils.toDate(rerunTimestamp);
    }

    public Timestamp getRerunTimestamp() {
        return rerunTimestamp;
    }

    @Override
    public Date getLastModifiedTime() {
        return DateUtils.toDate(lastModifiedTimestamp);
    }

    public Timestamp getLastModifiedTimestamp() {
        return lastModifiedTimestamp;
    }

    @Override
    public Date getNominalTime() {
        return DateUtils.toDate(nominalTimestamp);
    }

    public Timestamp getNominalTimestamp() {
        return nominalTimestamp;
    }

    @Override
    public String getExternalId() {
        return externalId;
    }

    public void setExternalId(String externalId) {
        this.externalId = externalId;
    }

    public StringBlob getSlaXmlBlob() {
        return slaXml;
    }

    public void setSlaXmlBlob(StringBlob slaXml) {
        this.slaXml = slaXml;
    }

    public String getSlaXml() {
        return slaXml == null ? null : slaXml.getString();
    }

    public void setSlaXml(String slaXml) {
        if (this.slaXml == null) {
            this.slaXml = new StringBlob(slaXml);
        }
        else {
            this.slaXml.setString(slaXml);
        }
    }

    /**
     * @return true if in terminal status
     */
    public boolean isTerminalStatus() {
        boolean isTerminal = true;
        switch (getStatus()) {
            case WAITING:
            case READY:
            case SUBMITTED:
            case RUNNING:
            case SUSPENDED:
                isTerminal = false;
                break;
            default:
                isTerminal = true;
                break;
        }
        return isTerminal;
    }

    /**
     * Return if the action is complete with failure.
     *
     * @return if the action is complete with failure.
     */
    public boolean isTerminalWithFailure() {
        boolean result = false;
        switch (getStatus()) {
            case FAILED:
            case KILLED:
            case TIMEDOUT:
                result = true;
        }
        return result;
    }

    /**
     * Set some actions are in progress for particular coordinator action.
     *
     * @param pending set pending to true
     */
    public void setPending(int pending) {
        this.pending = pending;
    }

    /**
     * increment pending and return it
     *
     * @return pending
     */
    public int incrementAndGetPending() {
        this.pending++;
        return pending;
    }

    /**
     * decrement pending and return it
     *
     * @return pending
     */
    public int decrementAndGetPending() {
        this.pending = Math.max(this.pending - 1, 0);
        return pending;
    }

    /**
     * Get some actions are in progress for particular bundle action.
     *
     * @return pending
     */
    public int getPending() {
        return this.pending;
    }

    /**
     * Return if the action is pending.
     *
     * @return if the action is pending.
     */
    public boolean isPending() {
        return pending > 0 ? true : false;
    }

    @Override
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setActionNumber(int actionNumber) {
        this.actionNumber = actionNumber;
    }

    @Override
    public int getActionNumber() {
        return actionNumber;
    }

    @Override
    public String getCreatedConf() {
        return createdConf == null ? null : createdConf.getString();
    }

    public void setCreatedConf(String createdConf) {
        if (this.createdConf == null) {
            this.createdConf = new StringBlob(createdConf);
        }
        else {
            this.createdConf.setString(createdConf);
        }
    }

    public void setCreatedConfBlob(StringBlob createdConf) {
        this.createdConf = createdConf;
    }

    public StringBlob getCreatedConfBlob() {
        return createdConf;
    }

    public void setRunConf(String runConf) {
        if (this.runConf == null) {
            this.runConf = new StringBlob(runConf);
        }
        else {
            this.runConf.setString(runConf);
        }
    }

    @Override
    public String getRunConf() {
        return runConf == null ? null : runConf.getString();
    }

    public void setRunConfBlob(StringBlob runConf) {
        this.runConf = runConf;
    }

    public StringBlob getRunConfBlob() {
        return runConf;
    }


    public void setMissingDependencies(String missingDependencies) {
        if (this.missingDependencies == null) {
            this.missingDependencies = new StringBlob(missingDependencies);
        }
        else {
            this.missingDependencies.setString(missingDependencies);
        }
    }

    @Override
    public String getMissingDependencies() {
        return missingDependencies == null ? null : missingDependencies.getString();
    }

    public void setMissingDependenciesBlob(StringBlob missingDependencies) {
        this.missingDependencies = missingDependencies;
    }

    public StringBlob getMissingDependenciesBlob() {
        return missingDependencies;
    }

    @Override
    public String getPushMissingDependencies() {
        return pushMissingDependencies == null ? null : pushMissingDependencies.getString();
    }

    public void setPushMissingDependencies(String pushMissingDependencies) {
        if (this.pushMissingDependencies == null) {
            this.pushMissingDependencies = new StringBlob(pushMissingDependencies);
        }
        else {
            this.pushMissingDependencies.setString(pushMissingDependencies);
        }
    }

    public void setPushMissingDependenciesBlob(StringBlob pushMissingDependencies) {
        this.pushMissingDependencies = pushMissingDependencies;
    }

    public StringBlob getPushMissingDependenciesBlob() {
        return pushMissingDependencies;
    }

    public String getExternalStatus() {
        return externalStatus;
    }

    public void setExternalStatus(String externalStatus) {
        this.externalStatus = externalStatus;
    }

    @Override
    public String getTrackerUri() {
        return trackerUri;
    }

    public void setTrackerUri(String trackerUri) {
        this.trackerUri = trackerUri;
    }

    @Override
    public String getConsoleUrl() {
        return consoleUrl;
    }

    public void setConsoleUrl(String consoleUrl) {
        this.consoleUrl = consoleUrl;
    }

    @Override
    public String getErrorCode() {
        return errorCode;
    }

    @Override
    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorInfo(String errorCode, String errorMessage) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    public String getActionXml() {
        return actionXml == null ? null : actionXml.getString();
    }

    public void setActionXml(String actionXml) {
        if (this.actionXml == null) {
            this.actionXml = new StringBlob(actionXml);
        }
        else {
            this.actionXml.setString(actionXml);
        }
    }

    public void setActionXmlBlob(StringBlob actionXml) {
        this.actionXml = actionXml;
    }

    public StringBlob getActionXmlBlob() {
        return actionXml;
    }

    @Override
    public String toString() {
        return MessageFormat.format("CoordinatorAction name[{0}] status[{1}]",
                                    getId(), getStatus());
    }

    public int getTimeOut() {
        return timeOut;
    }

    public void setTimeOut(int timeOut) {
        this.timeOut = timeOut;
    }


    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @SuppressWarnings("unchecked")
    public JSONObject toJSONObject(String timeZoneId) {
        JSONObject json = new JSONObject();
        json.put(JsonTags.COORDINATOR_ACTION_ID, id);
        json.put(JsonTags.COORDINATOR_JOB_ID, jobId);
        json.put(JsonTags.COORDINATOR_ACTION_TYPE, type);
        json.put(JsonTags.COORDINATOR_ACTION_NUMBER, actionNumber);
        json.put(JsonTags.COORDINATOR_ACTION_CREATED_CONF, getCreatedConf());
        json.put(JsonTags.COORDINATOR_ACTION_CREATED_TIME, JsonUtils
                .formatDateRfc822(getCreatedTime(), timeZoneId));
        json.put(JsonTags.COORDINATOR_ACTION_NOMINAL_TIME, JsonUtils
                .formatDateRfc822(getNominalTime(), timeZoneId));
        json.put(JsonTags.COORDINATOR_ACTION_EXTERNALID, externalId);
        // json.put(JsonTags.COORDINATOR_ACTION_START_TIME, JsonUtils
        // .formatDateRfc822(startTime), timeZoneId);
        json.put(JsonTags.COORDINATOR_ACTION_STATUS, statusStr);
        json.put(JsonTags.COORDINATOR_ACTION_RUNTIME_CONF, getRunConf());
        json.put(JsonTags.COORDINATOR_ACTION_LAST_MODIFIED_TIME, JsonUtils
                .formatDateRfc822(getLastModifiedTime(), timeZoneId));
        // json.put(JsonTags.COORDINATOR_ACTION_START_TIME, JsonUtils
        // .formatDateRfc822(startTime), timeZoneId);
        // json.put(JsonTags.COORDINATOR_ACTION_END_TIME, JsonUtils
        // .formatDateRfc822(endTime), timeZoneId);
        json.put(JsonTags.COORDINATOR_ACTION_MISSING_DEPS, getMissingDependencies());
        json.put(JsonTags.COORDINATOR_ACTION_PUSH_MISSING_DEPS, getPushMissingDependencies());
        json.put(JsonTags.COORDINATOR_ACTION_EXTERNAL_STATUS, externalStatus);
        json.put(JsonTags.COORDINATOR_ACTION_TRACKER_URI, trackerUri);
        json.put(JsonTags.COORDINATOR_ACTION_CONSOLE_URL, consoleUrl);
        json.put(JsonTags.COORDINATOR_ACTION_ERROR_CODE, errorCode);
        json.put(JsonTags.COORDINATOR_ACTION_ERROR_MESSAGE, errorMessage);
        json.put(JsonTags.TO_STRING, toString());
        return json;
    }

    /**
     * Convert a nodes list into a JSONArray.
     *
     * @param actions nodes list.
     * @param timeZoneId time zone to use for dates in the JSON array.
     * @return the corresponding JSON array.
     */
    @SuppressWarnings("unchecked")
    public static JSONArray toJSONArray(List<CoordinatorActionBean> actions, String timeZoneId) {
        JSONArray array = new JSONArray();
        for (CoordinatorActionBean action : actions) {
            array.add(action.toJSONObject(timeZoneId));
        }
        return array;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        CoordinatorActionBean other = (CoordinatorActionBean) obj;
        if (id == null) {
            if (other.id != null) {
                return false;
            }
        }
        else if (!id.equals(other.id)) {
            return false;
        }
        return true;
    }


}
