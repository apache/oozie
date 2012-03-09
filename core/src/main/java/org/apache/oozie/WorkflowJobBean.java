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

import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.lite.LiteWorkflowInstance;
import org.apache.oozie.client.rest.JsonWorkflowJob;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.WritableUtils;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.IOException;
import java.io.DataOutput;
import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.Column;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Basic;
import javax.persistence.Lob;

import java.sql.Timestamp;

import org.apache.openjpa.persistence.jdbc.Index;

@Entity
@NamedQueries({

    @NamedQuery(name = "UPDATE_WORKFLOW", query = "update WorkflowJobBean w set w.appName = :appName, w.appPath = :appPath, w.conf = :conf, w.group = :groupName, w.run = :run, w.user = :user, w.authToken = :authToken, w.createdTimestamp = :createdTime, w.endTimestamp = :endTime, w.externalId = :externalId, w.lastModifiedTimestamp = :lastModTime, w.logToken = :logToken, w.protoActionConf = :protoActionConf, w.slaXml =:slaXml, w.startTimestamp = :startTime, w.status = :status, w.wfInstance = :wfInstance where w.id = :id"),

    @NamedQuery(name = "DELETE_WORKFLOW", query = "delete from WorkflowJobBean w where w.id = :id"),

    @NamedQuery(name = "GET_WORKFLOWS", query = "select OBJECT(w) from WorkflowJobBean w order by w.startTimestamp desc"),

    @NamedQuery(name = "GET_WORKFLOWS_COLUMNS", query = "select w.id, w.appName, w.status, w.run, w.user, w.group, w.createdTimestamp, "
            + "w.startTimestamp, w.lastModifiedTimestamp, w.endTimestamp, w.externalId from WorkflowJobBean w order by w.createdTimestamp desc"),

    @NamedQuery(name = "GET_WORKFLOWS_COUNT", query = "select count(w) from WorkflowJobBean w"),

    @NamedQuery(name = "GET_COMPLETED_WORKFLOWS_OLDER_THAN", query = "select w from WorkflowJobBean w where w.endTimestamp < :endTime"),

    @NamedQuery(name = "GET_WORKFLOW", query = "select OBJECT(w) from WorkflowJobBean w where w.id = :id"),

    @NamedQuery(name = "GET_WORKFLOW_FOR_UPDATE", query = "select OBJECT(w) from WorkflowJobBean w where w.id = :id"),

    @NamedQuery(name = "GET_WORKFLOW_ID_FOR_EXTERNAL_ID", query = "select  w.id from WorkflowJobBean w where w.externalId = :externalId"),

    @NamedQuery(name = "GET_WORKFLOWS_COUNT_WITH_STATUS", query = "select count(w) from WorkflowJobBean w where w.status = :status"),

    @NamedQuery(name = "GET_WORKFLOWS_COUNT_WITH_STATUS_IN_LAST_N_SECS", query = "select count(w) from WorkflowJobBean w where w.status = :status and w.lastModifiedTimestamp > :lastModTime")

        })
public class WorkflowJobBean extends JsonWorkflowJob implements Writable {

    @Column(name = "proto_action_conf")
    @Lob
    private String protoActionConf = null;

    @Basic
    @Column(name = "log_token")
    private String logToken = null;

    @Basic
    @Index
    @Column(name = "external_id")
    private String externalId = null;

    @Basic
    @Index
    @Column(name = "status")
    private String status = WorkflowJob.Status.PREP.toString();

    @Basic
    @Column(name = "created_time")
    private java.sql.Timestamp createdTimestamp = null;

    @Basic
    @Column(name = "start_time")
    private java.sql.Timestamp startTimestamp = null;

    @Basic
    @Index
    @Column(name = "end_time")
    private java.sql.Timestamp endTimestamp = null;

    @Column(name = "auth_token")
    @Lob
    private String authToken = null;

    @Basic
    @Index
    @Column(name = "last_modified_time")
    private java.sql.Timestamp lastModifiedTimestamp = null;

    // @Basic(fetch = FetchType.LAZY)
    // @Column(name="wfinstance",columnDefinition="blob")
    @Column(name = "wf_instance")
    @Lob
    private byte[] wfInstance = null;

    @Column(name = "sla_xml")
    @Lob
    private String slaXml = null;

    /**
     * Default constructor.
     */
    public WorkflowJobBean() {
    }

    /**
     * Serialize the workflow bean to a data output.
     *
     * @param dataOutput data output.
     * @throws IOException thrown if the workflow bean could not be serialized.
     */
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeStr(dataOutput, getAppPath());
        WritableUtils.writeStr(dataOutput, getAppName());
        WritableUtils.writeStr(dataOutput, getId());
        WritableUtils.writeStr(dataOutput, getParentId());
        WritableUtils.writeStr(dataOutput, getConf());
        WritableUtils.writeStr(dataOutput, getStatusStr());
        dataOutput.writeLong((getCreatedTime() != null) ? getCreatedTime().getTime() : -1);
        dataOutput.writeLong((getStartTime() != null) ? getStartTime().getTime() : -1);
        dataOutput.writeLong((getLastModifiedTime() != null) ? getLastModifiedTime().getTime() : -1);
        dataOutput.writeLong((getEndTime() != null) ? getEndTime().getTime() : -1);
        WritableUtils.writeStr(dataOutput, getUser());
        WritableUtils.writeStr(dataOutput, getGroup());
        dataOutput.writeInt(getRun());
        WritableUtils.writeStr(dataOutput, authToken);
        WritableUtils.writeStr(dataOutput, logToken);
        WritableUtils.writeStr(dataOutput, protoActionConf);
    }

    /**
     * Deserialize a workflow bean from a data input.
     *
     * @param dataInput data input.
     * @throws IOException thrown if the workflow bean could not be deserialized.
     */
    public void readFields(DataInput dataInput) throws IOException {
        setAppPath(WritableUtils.readStr(dataInput));
        setAppName(WritableUtils.readStr(dataInput));
        setId(WritableUtils.readStr(dataInput));
        setParentId(WritableUtils.readStr(dataInput));
        setConf(WritableUtils.readStr(dataInput));
        setStatus(WorkflowJob.Status.valueOf(WritableUtils.readStr(dataInput)));
        // setStatus(WritableUtils.readStr(dataInput));
        long d = dataInput.readLong();
        if (d != -1) {
            setCreatedTime(new Date(d));
        }
        d = dataInput.readLong();
        if (d != -1) {
        }
        setStartTime(new Date(d));
        d = dataInput.readLong();
        if (d != -1) {
            setLastModifiedTime(new Date(d));
        }
        d = dataInput.readLong();
        if (d != -1) {
            setEndTime(new Date(d));
        }
        setUser(WritableUtils.readStr(dataInput));
        setGroup(WritableUtils.readStr(dataInput));
        setRun(dataInput.readInt());
        authToken = WritableUtils.readStr(dataInput);
        logToken = WritableUtils.readStr(dataInput);
        protoActionConf = WritableUtils.readStr(dataInput);
        setExternalId(getExternalId());
        setProtoActionConf(protoActionConf);
    }

    public String getAuthToken() {
        return authToken;
    }

    public void setAuthToken(String authToken) {
        this.authToken = authToken;
    }

    public String getLogToken() {
        return logToken;
    }

    public void setLogToken(String logToken) {
        this.logToken = logToken;
    }

    public String getSlaXml() {
        return slaXml;
    }

    public void setSlaXml(String slaXml) {
        this.slaXml = slaXml;
    }

    public WorkflowInstance getWorkflowInstance() {
        return get(this.wfInstance);
    }

    public byte[] getWfInstance() {
        return wfInstance;
    }

    public void setWorkflowInstance(WorkflowInstance workflowInstance) {
        setWfInstance(workflowInstance);
    }

    public void setWfInstance(byte[] wfInstance) {
        this.wfInstance = wfInstance;
    }

    public void setWfInstance(WorkflowInstance wfInstance) {
        this.wfInstance = WritableUtils.toByteArray((LiteWorkflowInstance) wfInstance);
    }

    public String getProtoActionConf() {
        return protoActionConf;
    }

    public void setProtoActionConf(String protoActionConf) {
        this.protoActionConf = protoActionConf;
    }

    public String getprotoActionConf() {
        return protoActionConf;
    }

    public String getlogToken() {
        return logToken;
    }

    public String getStatusStr() {
        return status;
    }

    public Timestamp getLastModifiedTimestamp() {
        return lastModifiedTimestamp;
    }

    public Timestamp getStartTimestamp() {
        return startTimestamp;
    }

    public Timestamp getCreatedTimestamp() {
        return createdTimestamp;
    }

    public Timestamp getEndTimestamp() {
        return endTimestamp;
    }

    @Override
    public void setAppName(String val) {
        super.setAppName(val);
    }

    @Override
    public void setAppPath(String val) {
        super.setAppPath(val);
    }

    @Override
    public void setConf(String val) {
        super.setConf(val);
    }

    @Override
    public void setStatus(Status val) {
        super.setStatus(val);
        this.status = val.toString();
    }

    @Override
    public Status getStatus() {
        return Status.valueOf(this.status);
    }

    @Override
    public void setExternalId(String externalId) {
        super.setExternalId(externalId);
        this.externalId = externalId;
    }

    @Override
    public String getExternalId() {
        return externalId;
    }

    @Override
    public void setLastModifiedTime(Date lastModifiedTime) {
        super.setLastModifiedTime(lastModifiedTime);
        this.lastModifiedTimestamp = DateUtils.convertDateToTimestamp(lastModifiedTime);
    }

    @Override
    public Date getLastModifiedTime() {
        return DateUtils.toDate(lastModifiedTimestamp);
    }

    @Override
    public Date getCreatedTime() {
        return DateUtils.toDate(createdTimestamp);
    }

    @Override
    public void setCreatedTime(Date createdTime) {
        super.setCreatedTime(createdTime);
        this.createdTimestamp = DateUtils.convertDateToTimestamp(createdTime);
    }

    @Override
    public Date getStartTime() {
        return DateUtils.toDate(startTimestamp);
    }

    @Override
    public void setStartTime(Date startTime) {
        super.setStartTime(startTime);
        this.startTimestamp = DateUtils.convertDateToTimestamp(startTime);
    }

    @Override
    public Date getEndTime() {
        return DateUtils.toDate(endTimestamp);
    }

    @Override
    public void setEndTime(Date endTime) {
        super.setEndTime(endTime);
        this.endTimestamp = DateUtils.convertDateToTimestamp(endTime);
    }

    private WorkflowInstance get(byte[] array) {
        LiteWorkflowInstance pInstance = WritableUtils.fromByteArray(array, LiteWorkflowInstance.class);
        return pInstance;
    }

}
