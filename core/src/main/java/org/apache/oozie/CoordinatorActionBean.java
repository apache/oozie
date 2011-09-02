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

import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.rest.JsonCoordinatorAction;

import java.util.Date;

import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.WritableUtils;
import org.apache.openjpa.persistence.jdbc.Index;
import org.apache.hadoop.io.Writable;

import java.io.DataOutput;
import java.io.IOException;
import java.io.DataInput;

import javax.persistence.Entity;
import javax.persistence.Column;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.NamedNativeQuery;
import javax.persistence.NamedNativeQueries;
import javax.persistence.SqlResultSetMapping;
import javax.persistence.ColumnResult;
import javax.persistence.Basic;
import javax.persistence.Lob;

import java.sql.Timestamp;

@SqlResultSetMapping(
        name = "CoordActionJobIdLmt",
        columns = {@ColumnResult(name = "job_id"),
            @ColumnResult(name = "min_lmt")})

@Entity
@NamedQueries({

    @NamedQuery(name = "UPDATE_COORD_ACTION", query = "update CoordinatorActionBean w set w.actionNumber = :actionNumber, w.actionXml = :actionXml, w.consoleUrl = :consoleUrl, w.createdConf = :createdConf, w.errorCode = :errorCode, w.errorMessage = :errorMessage, w.externalStatus = :externalStatus, w.missingDependencies = :missingDependencies, w.runConf = :runConf, w.timeOut = :timeOut, w.trackerUri = :trackerUri, w.type = :type, w.createdTimestamp = :createdTime, w.externalId = :externalId, w.jobId = :jobId, w.lastModifiedTimestamp = :lastModifiedTime, w.nominalTimestamp = :nominalTime, w.slaXml = :slaXml, w.status = :status where w.id = :id"),

    @NamedQuery(name = "DELETE_COMPLETED_COORD_ACTIONS", query = "delete from CoordinatorActionBean a where a.id = :id and (a.status = 'SUCCEEDED' OR a.status = 'FAILED' OR a.status = 'KILLED')"),

    @NamedQuery(name = "GET_COORD_ACTIONS", query = "select OBJECT(w) from CoordinatorActionBean w"),

    @NamedQuery(name = "GET_COMPLETED_ACTIONS_OLDER_THAN", query = "select OBJECT(a) from CoordinatorActionBean a where a.createdTimestamp < :createdTime and (a.status = 'SUCCEEDED' OR a.status = 'FAILED' OR a.status = 'KILLED')"),

    @NamedQuery(name = "GET_COORD_ACTION", query = "select OBJECT(a) from CoordinatorActionBean a where a.id = :id"),

    @NamedQuery(name = "GET_COORD_ACTION_FOR_EXTERNALID", query = "select OBJECT(a) from CoordinatorActionBean a where a.externalId = :externalId"),

    @NamedQuery(name = "GET_COORD_ACTIONS_FOR_JOB_FIFO", query = "select OBJECT(a) from CoordinatorActionBean a where a.jobId = :jobId AND a.status = 'READY' order by a.nominalTimestamp"),

    @NamedQuery(name = "GET_COORD_ACTIONS_FOR_JOB_LIFO", query = "select OBJECT(a) from CoordinatorActionBean a where a.jobId = :jobId AND a.status = 'READY' order by a.nominalTimestamp desc"),

    @NamedQuery(name = "GET_COORD_RUNNING_ACTIONS_COUNT", query = "select count(a) from CoordinatorActionBean a where a.jobId = :jobId AND (a.status = 'RUNNING' OR a.status='SUBMITTED')"),

    @NamedQuery(name = "GET_COORD_ACTIONS_COUNT_BY_JOBID", query = "select count(a) from CoordinatorActionBean a where a.jobId = :jobId"),

    @NamedQuery(name = "GET_ACTIONS_FOR_COORD_JOB", query = "select OBJECT(a) from CoordinatorActionBean a where a.jobId = :jobId"),

    @NamedQuery(name = "GET_RUNNING_ACTIONS_FOR_COORD_JOB", query = "select OBJECT(a) from CoordinatorActionBean a where a.jobId = :jobId AND a.status = 'RUNNING'"),

    @NamedQuery(name = "GET_RUNNING_ACTIONS_OLDER_THAN", query = "select OBJECT(a) from CoordinatorActionBean a where a.status = 'RUNNING' AND a.lastModifiedTimestamp <= :lastModifiedTime"),

    @NamedQuery(name = "GET_WAITING_SUBMITTED_ACTIONS_OLDER_THAN", query = "select OBJECT(a) from CoordinatorActionBean a where (a.status = 'WAITING' OR a.status = 'SUBMITTED') AND a.lastModifiedTimestamp <= :lastModifiedTime"),

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
    @Column(name = "external_id")
    private String externalId;

    @Column(name = "sla_xml")
    @Lob
    private String slaXml = null;

    public CoordinatorActionBean() {
    }

    /**
     * Serialize the coordinator bean to a data output.
     *
     * @param dataOutput data output.
     * @throws IOException thrown if the coordinator bean could not be serialized.
     */
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
        WritableUtils.writeStr(dataOutput, getErrorCode());
        WritableUtils.writeStr(dataOutput, getErrorMessage());
    }

    /**
     * Deserialize a coordinator bean from a data input.
     *
     * @param dataInput data input.
     * @throws IOException thrown if the workflow bean could not be deserialized.
     */
    public void readFields(DataInput dataInput) throws IOException {
        setJobId(WritableUtils.readStr(dataInput));
        setType(WritableUtils.readStr(dataInput));
        setId(WritableUtils.readStr(dataInput));
        setCreatedConf(WritableUtils.readStr(dataInput));
        setStatus(CoordinatorAction.Status.valueOf(WritableUtils
                .readStr(dataInput)));
        setRunConf(WritableUtils.readStr(dataInput));
        setExternalStatus(WritableUtils.readStr(dataInput));
        setTrackerUri(WritableUtils.readStr(dataInput));
        setConsoleUrl(WritableUtils.readStr(dataInput));
        long d = dataInput.readLong();
        if (d != -1) {
            setCreatedTime(new Date(d));
        }
        d = dataInput.readLong();
        if (d != -1) {
            setLastModifiedTime(new Date(d));
        }
        d = dataInput.readLong();
        d = dataInput.readLong();
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

}
