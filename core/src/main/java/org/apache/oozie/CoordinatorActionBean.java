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
import javax.persistence.ColumnResult;
import javax.persistence.Entity;
import javax.persistence.Lob;
import javax.persistence.NamedNativeQueries;
import javax.persistence.NamedNativeQuery;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.SqlResultSetMapping;

import org.apache.hadoop.io.Writable;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.rest.JsonCoordinatorAction;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.WritableUtils;
import org.apache.openjpa.persistence.jdbc.Index;

@SqlResultSetMapping(
        name = "CoordActionJobIdLmt",
        columns = {@ColumnResult(name = "job_id"),
            @ColumnResult(name = "min_lmt")})

@Entity
@NamedQueries({

        @NamedQuery(name = "UPDATE_COORD_ACTION", query = "update CoordinatorActionBean w set w.actionNumber = :actionNumber, w.actionXml = :actionXml, w.consoleUrl = :consoleUrl, w.createdConf = :createdConf, w.errorCode = :errorCode, w.errorMessage = :errorMessage, w.externalStatus = :externalStatus, w.missingDependencies = :missingDependencies, w.runConf = :runConf, w.timeOut = :timeOut, w.trackerUri = :trackerUri, w.type = :type, w.createdTimestamp = :createdTime, w.externalId = :externalId, w.jobId = :jobId, w.lastModifiedTimestamp = :lastModifiedTime, w.nominalTimestamp = :nominalTime, w.slaXml = :slaXml, w.status = :status where w.id = :id"),

        @NamedQuery(name = "UPDATE_COORD_ACTION_MIN", query = "update CoordinatorActionBean w set w.actionXml = :actionXml, w.missingDependencies = :missingDependencies, w.lastModifiedTimestamp = :lastModifiedTime, w.status = :status where w.id = :id"),
        // Query to update the action status, pending status and last modified time stamp of a Coordinator action
        @NamedQuery(name = "UPDATE_COORD_ACTION_STATUS_PENDING_TIME", query = "update CoordinatorActionBean w set w.status =:status, w.pending =:pending, w.lastModifiedTimestamp = :lastModifiedTime where w.id = :id"),
        // Update query for InputCheck
        @NamedQuery(name = "UPDATE_COORD_ACTION_FOR_INPUTCHECK", query = "update CoordinatorActionBean w set w.status = :status, w.lastModifiedTimestamp = :lastModifiedTime, w.actionXml = :actionXml, w.missingDependencies = :missingDependencies where w.id = :id"),
        // Update query for Start
        @NamedQuery(name = "UPDATE_COORD_ACTION_FOR_START", query = "update CoordinatorActionBean w set w.status =:status, w.lastModifiedTimestamp = :lastModifiedTime, w.runConf = :runConf, w.externalId = :externalId, w.pending = :pending  where w.id = :id"),

        @NamedQuery(name = "DELETE_COMPLETED_ACTIONS_FOR_COORDINATOR", query = "delete from CoordinatorActionBean a where a.jobId = :jobId and (a.status = 'SUCCEEDED' OR a.status = 'FAILED' OR a.status= 'KILLED')"),

        @NamedQuery(name = "DELETE_UNSCHEDULED_ACTION", query = "delete from CoordinatorActionBean a where a.id = :id and (a.status = 'WAITING' OR a.status = 'READY')"),

        // Query used by XTestcase to setup tables
        @NamedQuery(name = "GET_COORD_ACTIONS", query = "select OBJECT(w) from CoordinatorActionBean w"),
        // Select query used only by test cases
        @NamedQuery(name = "GET_COORD_ACTION", query = "select OBJECT(a) from CoordinatorActionBean a where a.id = :id"),

        // Select query used by ActionInfo command
        @NamedQuery(name = "GET_COORD_ACTION_FOR_INFO", query = "select a.id, a.jobId, a.actionNumber, a.consoleUrl, a.errorCode, a.errorMessage, a.externalId, a.externalStatus, a.trackerUri, a.createdTimestamp, a.nominalTimestamp, a.status, a.lastModifiedTimestamp, a.missingDependencies from CoordinatorActionBean a where a.id = :id"),
        // Select Query used by Timeout command
        @NamedQuery(name = "GET_COORD_ACTION_FOR_TIMEOUT", query = "select a.id, a.jobId, a.status, a.runConf, a.pending from CoordinatorActionBean a where a.id = :id"),
        // Select query used by InputCheck command
        @NamedQuery(name = "GET_COORD_ACTION_FOR_INPUTCHECK", query = "select a.id, a.jobId, a.status, a.runConf, a.nominalTimestamp, a.createdTimestamp, a.actionXml, a.missingDependencies, a.timeOut from CoordinatorActionBean a where a.id = :id"),
        // Select query used by CoordActionUpdate command
        @NamedQuery(name = "GET_COORD_ACTION_FOR_EXTERNALID", query = "select a.id, a.jobId, a.status, a.pending, a.externalId, a.lastModifiedTimestamp, a.slaXml from CoordinatorActionBean a where a.externalId = :externalId"),
        // Select query used by Check command
        @NamedQuery(name = "GET_COORD_ACTION_FOR_CHECK", query = "select a.id, a.jobId, a.status, a.pending, a.externalId, a.lastModifiedTimestamp, a.slaXml from CoordinatorActionBean a where a.id = :id"),
        // Select query used by Start command
        @NamedQuery(name = "GET_COORD_ACTION_FOR_START", query = "select a.id, a.jobId, a.status, a.pending, a.createdConf, a.slaXml, a.actionXml, a.externalId, a.errorMessage, a.errorCode from CoordinatorActionBean a where a.id = :id"),

        @NamedQuery(name = "GET_COORD_ACTIONS_FOR_JOB_FIFO", query = "select a.id, a.jobId, a.status, a.pending from CoordinatorActionBean a where a.jobId = :jobId AND a.status = 'READY' order by a.nominalTimestamp"),

        @NamedQuery(name = "GET_COORD_ACTIONS_FOR_JOB_LIFO", query = "select a.id, a.jobId, a.status, a.pending from CoordinatorActionBean a where a.jobId = :jobId AND a.status = 'READY' order by a.nominalTimestamp desc"),

        @NamedQuery(name = "GET_COORD_RUNNING_ACTIONS_COUNT", query = "select count(a) from CoordinatorActionBean a where a.jobId = :jobId AND (a.status = 'RUNNING' OR a.status='SUBMITTED')"),

        @NamedQuery(name = "GET_COORD_ACTIONS_COUNT_BY_JOBID", query = "select count(a) from CoordinatorActionBean a where a.jobId = :jobId"),

        @NamedQuery(name = "GET_COORD_ACTIVE_ACTIONS_COUNT_BY_JOBID", query = "select count(a) from CoordinatorActionBean a where a.jobId = :jobId AND a.status = 'WAITING'"),

        @NamedQuery(name = "GET_COORD_ACTIONS_PENDING_FALSE_COUNT", query = "select count(a) from CoordinatorActionBean a where a.jobId = :jobId AND a.pending = 0 AND (a.status = 'SUSPENDED' OR a.status = 'TIMEDOUT' OR a.status = 'SUCCEEDED' OR a.status = 'KILLED' OR a.status = 'FAILED')"),

        @NamedQuery(name = "GET_COORD_ACTIONS_PENDING_FALSE_STATUS_COUNT", query = "select count(a) from CoordinatorActionBean a where a.jobId = :jobId AND a.pending = 0 AND a.status = :status"),

        @NamedQuery(name = "GET_ACTIONS_FOR_COORD_JOB", query = "select count(a) from CoordinatorActionBean a where a.jobId = :jobId"),
        // Query to retrieve Coordinator actions sorted by nominal time
        @NamedQuery(name = "GET_ACTIONS_FOR_COORD_JOB_ORDER_BY_NOMINAL_TIME", query = "select a.id, a.actionNumber, a.consoleUrl, a.errorCode, a.errorMessage, a.externalId, a.externalStatus, a.jobId, a.trackerUri, a.createdTimestamp, a.nominalTimestamp, a.status, a.lastModifiedTimestamp, a.missingDependencies, a.timeOut from CoordinatorActionBean a where a.jobId = :jobId order by a.nominalTimestamp"),
        // Query to maintain backward compatibility for coord job info command
        @NamedQuery(name = "GET_ALL_COLS_FOR_ACTIONS_FOR_COORD_JOB_ORDER_BY_NOMINAL_TIME", query = "select OBJECT(a) from CoordinatorActionBean a where a.jobId = :jobId order by a.nominalTimestamp"),
        // Query to retrieve action id, action status, pending status and external Id of not completed Coordinator actions
        @NamedQuery(name = "GET_COORD_ACTIONS_NOT_COMPLETED", query = "select a.id, a.status, a.pending, a.externalId from CoordinatorActionBean a where a.jobId = :jobId AND a.status <> 'FAILED' AND a.status <> 'TIMEDOUT' AND a.status <> 'SUCCEEDED' AND a.status <> 'KILLED'"),

        // Query to retrieve action id, action status, pending status and external Id of running Coordinator actions
        @NamedQuery(name = "GET_COORD_ACTIONS_RUNNING", query = "select a.id, a.status, a.pending, a.externalId from CoordinatorActionBean a where a.jobId = :jobId and a.status = 'RUNNING'"),

        // Query to retrieve action id, action status, pending status and external Id of suspended Coordinator actions
        @NamedQuery(name = "GET_COORD_ACTIONS_SUSPENDED", query = "select a.id, a.status, a.pending, a.externalId from CoordinatorActionBean a where a.jobId = :jobId and a.status = 'SUSPENDED'"),

        // Query to retrieve count of Coordinator actions which are pending
        @NamedQuery(name = "GET_COORD_ACTIONS_PENDING_COUNT", query = "select count(a) from CoordinatorActionBean a where a.jobId = :jobId AND a.pending > 0"),

        // Query to retrieve status of Coordinator actions which are not pending
        @NamedQuery(name = "GET_COORD_ACTIONS_STATUS_BY_PENDING_FALSE", query = "select a.status from CoordinatorActionBean a where a.jobId = :jobId AND a.pending = 0"),

        @NamedQuery(name = "GET_COORD_ACTION_FOR_COORD_JOB_BY_ACTION_NUMBER", query = "select a.id from CoordinatorActionBean a where a.jobId = :jobId AND a.actionNumber = :actionNumber"),

        @NamedQuery(name = "GET_COORD_ACTIONS_BY_LAST_MODIFIED_TIME", query = "select a.jobId from CoordinatorActionBean a where a.lastModifiedTimestamp >= :lastModifiedTime"),

        //Used by coordinator store only
        @NamedQuery(name = "GET_RUNNING_ACTIONS_FOR_COORD_JOB", query = "select OBJECT(a) from CoordinatorActionBean a where a.jobId = :jobId AND a.status = 'RUNNING'"),

        @NamedQuery(name = "GET_RUNNING_ACTIONS_OLDER_THAN", query = "select a.id from CoordinatorActionBean a where a.status = 'RUNNING' AND a.lastModifiedTimestamp <= :lastModifiedTime"),

        @NamedQuery(name = "GET_COORD_ACTIONS_WAITING_SUBMITTED_OLDER_THAN", query = "select a.id, a.jobId, a.status, a.externalId from CoordinatorActionBean a where (a.status = 'WAITING' OR a.status = 'SUBMITTED') AND a.lastModifiedTimestamp <= :lastModifiedTime"),

        @NamedQuery(name = "GET_COORD_ACTIONS_FOR_RECOVERY_OLDER_THAN", query = "select a.id, a.jobId, a.status, a.externalId from CoordinatorActionBean a where a.pending > 0 AND (a.status = 'SUSPENDED' OR a.status = 'KILLED' OR a.status = 'RUNNING') AND a.lastModifiedTimestamp <= :lastModifiedTime"),
        // Select query used by rerun, requires almost all columns so select * is used
        @NamedQuery(name = "GET_ACTIONS_FOR_DATES", query = "select OBJECT(a) from CoordinatorActionBean a where a.jobId = :jobId AND (a.status = 'TIMEDOUT' OR a.status = 'SUCCEEDED' OR a.status = 'KILLED' OR a.status = 'FAILED') AND a.nominalTimestamp >= :startTime AND a.nominalTimestamp <= :endTime"),
        // Select query used by log
        @NamedQuery(name = "GET_ACTION_IDS_FOR_DATES", query = "select a.id from CoordinatorActionBean a where a.jobId = :jobId AND (a.status = 'TIMEDOUT' OR a.status = 'SUCCEEDED' OR a.status = 'KILLED' OR a.status = 'FAILED') AND a.nominalTimestamp >= :startTime AND a.nominalTimestamp <= :endTime"),
        // Select query used by rerun, requires almost all columns so select * is used
        @NamedQuery(name = "GET_ACTION_FOR_NOMINALTIME", query = "select OBJECT(a) from CoordinatorActionBean a where a.jobId = :jobId AND a.nominalTimestamp = :nominalTime"),

        @NamedQuery(name = "GET_COORD_ACTIONS_COUNT", query = "select count(w) from CoordinatorActionBean w")})

@NamedNativeQueries({

    @NamedNativeQuery(name = "GET_READY_ACTIONS_GROUP_BY_JOBID", query = "select a.job_id as job_id, MIN(a.last_modified_time) as min_lmt from COORD_ACTIONS a where a.status = 'READY' GROUP BY a.job_id HAVING MIN(a.last_modified_time) < ?", resultSetMapping = "CoordActionJobIdLmt")
        })
public class CoordinatorActionBean extends JsonCoordinatorAction implements
        Writable {
    @Basic
    @Index
    @Column(name = "job_id")
    private String jobId;

    @Basic
    @Index
    @Column(name = "status")
    private String status = null;

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

    @Column(name = "sla_xml")
    @Lob
    private String slaXml = null;

    @Basic
    @Column(name = "pending")
    private int pending = 0;

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

    @Override
    public void setJobId(String id) {
        super.setJobId(id);
        this.jobId = id;
    }

    @Override
    public Status getStatus() {
        return Status.valueOf(status);
    }

    @Override
    public void setStatus(Status status) {
        super.setStatus(status);
        this.status = status.toString();
    }

    @Override
    public void setCreatedTime(Date createdTime) {
        this.createdTimestamp = DateUtils.convertDateToTimestamp(createdTime);
        super.setCreatedTime(createdTime);
    }

    public void setRerunTime(Date rerunTime) {
        this.rerunTimestamp = DateUtils.convertDateToTimestamp(rerunTime);
    }

    @Override
    public void setNominalTime(Date nominalTime) {
        this.nominalTimestamp = DateUtils.convertDateToTimestamp(nominalTime);
        super.setNominalTime(nominalTime);
    }

    @Override
    public void setLastModifiedTime(Date lastModifiedTime) {
        this.lastModifiedTimestamp = DateUtils.convertDateToTimestamp(lastModifiedTime);
        super.setLastModifiedTime(lastModifiedTime);
    }

    @Override
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

    @Override
    public void setExternalId(String externalId) {
        super.setExternalId(externalId);
        this.externalId = externalId;
    }

    public String getSlaXml() {
        return slaXml;
    }

    public void setSlaXml(String slaXml) {
        this.slaXml = slaXml;
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
}
