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
import java.util.Properties;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Lob;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Transient;

import org.apache.hadoop.io.Writable;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.rest.JsonWorkflowAction;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.PropertiesUtils;
import org.apache.oozie.util.WritableUtils;
import org.apache.openjpa.persistence.jdbc.Index;

/**
 * Bean that contains all the information to start an action for a workflow node.
 */
@Entity
@NamedQueries({

    @NamedQuery(name = "UPDATE_ACTION", query = "update WorkflowActionBean a set a.conf = :conf, a.consoleUrl = :consoleUrl, a.data = :data, a.stats = :stats, a.externalChildIDs = :externalChildIDs, a.errorCode = :errorCode, a.errorMessage = :errorMessage, a.externalId = :externalId, a.externalStatus = :externalStatus, a.name = :name, a.cred = :cred , a.retries = :retries, a.trackerUri = :trackerUri, a.transition = :transition, a.type = :type, a.endTimestamp = :endTime, a.executionPath = :executionPath, a.lastCheckTimestamp = :lastCheckTime, a.logToken = :logToken, a.pending = :pending, a.pendingAgeTimestamp = :pendingAge, a.signalValue = :signalValue, a.slaXml = :slaXml, a.startTimestamp = :startTime, a.status = :status, a.wfId=:wfId where a.id = :id"),

    @NamedQuery(name = "DELETE_ACTION", query = "delete from WorkflowActionBean a where a.id = :id"),

    @NamedQuery(name = "DELETE_ACTIONS_FOR_WORKFLOW", query = "delete from WorkflowActionBean a where a.wfId = :wfId"),

    @NamedQuery(name = "GET_ACTIONS", query = "select OBJECT(a) from WorkflowActionBean a"),

    @NamedQuery(name = "GET_ACTION", query = "select OBJECT(a) from WorkflowActionBean a where a.id = :id"),

    @NamedQuery(name = "GET_ACTION_FOR_UPDATE", query = "select OBJECT(a) from WorkflowActionBean a where a.id = :id"),

    @NamedQuery(name = "GET_ACTIONS_FOR_WORKFLOW", query = "select OBJECT(a) from WorkflowActionBean a where a.wfId = :wfId order by a.startTimestamp"),

    @NamedQuery(name = "GET_ACTIONS_OF_WORKFLOW_FOR_UPDATE", query = "select OBJECT(a) from WorkflowActionBean a where a.wfId = :wfId order by a.startTimestamp"),

    @NamedQuery(name = "GET_PENDING_ACTIONS", query = "select OBJECT(a) from WorkflowActionBean a where a.pending = 1 AND a.pendingAgeTimestamp < :pendingAge AND a.status <> 'RUNNING'"),

    @NamedQuery(name = "GET_RUNNING_ACTIONS", query = "select OBJECT(a) from WorkflowActionBean a where a.pending = 1 AND a.status = 'RUNNING' AND a.lastCheckTimestamp < :lastCheckTime"),

    @NamedQuery(name = "GET_RETRY_MANUAL_ACTIONS", query = "select OBJECT(a) from WorkflowActionBean a where a.wfId = :wfId AND (a.status = 'START_RETRY' OR a.status = 'START_MANUAL' OR a.status = 'END_RETRY' OR a.status = 'END_MANUAL')") })

public class WorkflowActionBean extends JsonWorkflowAction implements Writable {

    @Basic
    @Index
    @Column(name = "wf_id")
    private String wfId = null;

    @Basic
    @Index
    @Column(name = "status")
    private String status = WorkflowAction.Status.PREP.toString();

    @Basic
    @Column(name = "last_check_time")
    private java.sql.Timestamp lastCheckTimestamp;

    @Basic
    @Column(name = "end_time")
    private java.sql.Timestamp endTimestamp = null;

    @Basic
    @Column(name = "start_time")
    private java.sql.Timestamp startTimestamp = null;

    @Basic
    @Column(name = "execution_path", length = 1024)
    private String executionPath = null;

    @Basic
    @Column(name = "pending")
    private int pending = 0;

    // @Temporal(TemporalType.TIME)
    // @Column(name="pending_age",columnDefinition="timestamp default '0000-00-00 00:00:00'")
    @Basic
    @Index
    @Column(name = "pending_age")
    private java.sql.Timestamp pendingAgeTimestamp = null;

    @Basic
    @Column(name = "signal_value")
    private String signalValue = null;

    @Basic
    @Column(name = "log_token")
    private String logToken = null;

    @Transient
    private Date pendingAge;

    @Column(name = "sla_xml")
    @Lob
    private String slaXml = null;

    /**
     * Default constructor.
     */
    public WorkflowActionBean() {
    }

    /**
     * Serialize the action bean to a data output.
     *
     * @param dataOutput data output.
     * @throws IOException thrown if the action bean could not be serialized.
     */

    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeStr(dataOutput, getId());
        WritableUtils.writeStr(dataOutput, getName());
        WritableUtils.writeStr(dataOutput, getCred());
        WritableUtils.writeStr(dataOutput, getType());
        WritableUtils.writeStr(dataOutput, getConf());
        WritableUtils.writeStr(dataOutput, getStatusStr());
        dataOutput.writeInt(getRetries());
        dataOutput.writeLong((getStartTime() != null) ? getStartTime().getTime() : -1);
        dataOutput.writeLong((getEndTime() != null) ? getEndTime().getTime() : -1);
        dataOutput.writeLong((getLastCheckTime() != null) ? getLastCheckTime().getTime() : -1);
        WritableUtils.writeStr(dataOutput, getTransition());
        WritableUtils.writeStr(dataOutput, getData());
        WritableUtils.writeStr(dataOutput, getStats());
        WritableUtils.writeStr(dataOutput, getExternalChildIDs());
        WritableUtils.writeStr(dataOutput, getExternalId());
        WritableUtils.writeStr(dataOutput, getExternalStatus());
        WritableUtils.writeStr(dataOutput, getTrackerUri());
        WritableUtils.writeStr(dataOutput, getConsoleUrl());
        WritableUtils.writeStr(dataOutput, getErrorCode());
        WritableUtils.writeStr(dataOutput, getErrorMessage());
        WritableUtils.writeStr(dataOutput, wfId);
        WritableUtils.writeStr(dataOutput, executionPath);
        dataOutput.writeInt(pending);
        dataOutput.writeLong((pendingAge != null) ? pendingAge.getTime() : -1);
        WritableUtils.writeStr(dataOutput, signalValue);
        WritableUtils.writeStr(dataOutput, logToken);
        dataOutput.writeInt(getUserRetryCount());
        dataOutput.writeInt(getUserRetryInterval());
        dataOutput.writeInt(getUserRetryMax());
    }

    /**
     * Deserialize an action bean from a data input.
     *
     * @param dataInput data input.
     * @throws IOException thrown if the action bean could not be deserialized.
     */
    public void readFields(DataInput dataInput) throws IOException {
        setId(WritableUtils.readStr(dataInput));
        setName(WritableUtils.readStr(dataInput));
        setCred(WritableUtils.readStr(dataInput));
        setType(WritableUtils.readStr(dataInput));
        setConf(WritableUtils.readStr(dataInput));
        setStatus(WorkflowAction.Status.valueOf(WritableUtils.readStr(dataInput)));
        setRetries(dataInput.readInt());
        long d = dataInput.readLong();
        if (d != -1) {
            setStartTime(new Date(d));
        }
        d = dataInput.readLong();
        if (d != -1) {
            setEndTime(new Date(d));
        }
        d = dataInput.readLong();
        if (d != -1) {
            setLastCheckTime(new Date(d));
        }
        setTransition(WritableUtils.readStr(dataInput));
        setData(WritableUtils.readStr(dataInput));
        setStats(WritableUtils.readStr(dataInput));
        setExternalChildIDs(WritableUtils.readStr(dataInput));
        setExternalId(WritableUtils.readStr(dataInput));
        setExternalStatus(WritableUtils.readStr(dataInput));
        setTrackerUri(WritableUtils.readStr(dataInput));
        setConsoleUrl(WritableUtils.readStr(dataInput));
        setErrorInfo(WritableUtils.readStr(dataInput), WritableUtils.readStr(dataInput));
        wfId = WritableUtils.readStr(dataInput);
        executionPath = WritableUtils.readStr(dataInput);
        pending = dataInput.readInt();
        d = dataInput.readLong();
        if (d != -1) {
            pendingAge = new Date(d);
            pendingAgeTimestamp = DateUtils.convertDateToTimestamp(pendingAge);
        }
        signalValue = WritableUtils.readStr(dataInput);
        logToken = WritableUtils.readStr(dataInput);
        setUserRetryCount(dataInput.readInt());
        setUserRetryInterval(dataInput.readInt());
        setUserRetryMax(dataInput.readInt());
    }

    /**
     * Return if the action execution is complete.
     *
     * @return if the action start is complete.
     */
    public boolean isExecutionComplete() {
        return getStatus() == WorkflowAction.Status.DONE;
    }

    /**
     * Return if the action is START_RETRY or START_MANUAL or END_RETRY or
     * END_MANUAL.
     *
     * @return boolean true if status is START_RETRY or START_MANUAL or END_RETRY or
     *         END_MANUAL
     */
    public boolean isRetryOrManual() {
        return (getStatus() == WorkflowAction.Status.START_RETRY || getStatus() == WorkflowAction.Status.START_MANUAL
                || getStatus() == WorkflowAction.Status.END_RETRY || getStatus() == WorkflowAction.Status.END_MANUAL);
    }
    
    /**
     * Return true if the action is USER_RETRY
     *
     * @return boolean true if status is USER_RETRY
     */
    public boolean isUserRetry() {
        return (getStatus() == WorkflowAction.Status.USER_RETRY);
    }

    /**
     * Return if the action is complete.
     *
     * @return if the action is complete.
     */
    public boolean isComplete() {
        return getStatus() == WorkflowAction.Status.OK || getStatus() == WorkflowAction.Status.KILLED ||
                getStatus() == WorkflowAction.Status.ERROR;
    }

    /**
     * Set the action pending flag to true.
     */
    public void setPendingOnly() {
        pending = 1;
    }

    /**
     * Set the action as pending and the current time as pending.
     */
    public void setPending() {
        pending = 1;
        pendingAge = new Date();
        pendingAgeTimestamp = DateUtils.convertDateToTimestamp(pendingAge);
    }

    /**
     * Set a time when the action will be pending, normally a time in the future.
     *
     * @param pendingAge the time when the action will be pending.
     */
    public void setPendingAge(Date pendingAge) {
        this.pendingAge = pendingAge;
        this.pendingAgeTimestamp = DateUtils.convertDateToTimestamp(pendingAge);
    }

    /**
     * Return the pending age of the action.
     *
     * @return the pending age of the action, <code>null</code> if the action is not pending.
     */
    public Date getPendingAge() {
        return DateUtils.toDate(pendingAgeTimestamp);
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
     * Removes the pending flag and pendingAge from the action.
     */
    public void resetPending() {
        pending = 0;
        pendingAge = null;
        pendingAgeTimestamp = null;
    }

    /**
     * Removes the pending flag from the action.
     */
    public void resetPendingOnly() {
        pending = 0;
    }

    /**
     * Increments the number of retries for the action.
     */
    public void incRetries() {
        setRetries(getRetries() + 1);
    }

    /**
     * Set a tracking information for an action, and set the action status to {@link Action.Status#DONE}
     *
     * @param externalId external ID for the action.
     * @param trackerUri tracker URI for the action.
     * @param consoleUrl console URL for the action.
     */
    public void setStartData(String externalId, String trackerUri, String consoleUrl) {
        setExternalId(ParamChecker.notEmpty(externalId, "externalId"));
        setTrackerUri(ParamChecker.notEmpty(trackerUri, "trackerUri"));
        setConsoleUrl(ParamChecker.notEmpty(consoleUrl, "consoleUrl"));
        Date now = new Date();
        if (this.startTimestamp == null) {
            setStartTime(now);
        }
        setLastCheckTime(now);
        setStatus(Status.RUNNING);
    }

    /**
     * Set the completion information for an action start. Sets the Action status to {@link Action.Status#DONE}
     *
     * @param externalStatus action external end status.
     * @param actionData action output data, <code>null</code> if there is no action output data.
     */
    public void setExecutionData(String externalStatus, Properties actionData) {
        setStatus(Status.DONE);
        setExternalStatus(ParamChecker.notEmpty(externalStatus, "externalStatus"));
        if (actionData != null) {
            setData(PropertiesUtils.propertiesToString(actionData));
        }
    }

    /**
     * Return the action statistics info.
     *
     * @return Json representation of the stats.
     */
    public String getExecutionStats() {
        return getStats();
    }

    /**
     * Set the action statistics info for the workflow action.
     *
     * @param Json representation of the stats.
     */
    public void setExecutionStats(String jsonStats) {
        setStats(jsonStats);
    }

    /**
     * Return the external child IDs.
     *
     * @return externalChildIDs as a string.
     */
    public String getExternalChildIDs() {
        return super.getExternalChildIDs();
    }

    /**
     * Set the external child IDs for the workflow action.
     *
     * @param externalChildIDs as a string.
     */
    public void setExternalChildIDs(String externalChildIDs) {
        super.setExternalChildIDs(externalChildIDs);
    }

    /**
     * Set the completion information for an action end.
     *
     * @param status action status, {@link Action.Status#OK} or {@link Action.Status#ERROR} or {@link
     * Action.Status#KILLED}
     * @param signalValue the signal value. In most cases, the value should be OK or ERROR.
     */
    public void setEndData(Status status, String signalValue) {
        if (status == null || (status != Status.OK && status != Status.ERROR && status != Status.KILLED)) {
            throw new IllegalArgumentException("Action status must be OK, ERROR or KILLED. Received ["
                    + status.toString() + "]");
        }
        if (status == Status.OK) {
            setErrorInfo(null, null);
        }
        setStatus(status);
        setSignalValue(ParamChecker.notEmpty(signalValue, "signalValue"));
    }


    /**
     * Return the job Id.
     *
     * @return the job Id.
     */
    public String getJobId() {
        return wfId;
    }

    /**
     * Return the job Id.
     *
     * @return the job Id.
     */
    public String getWfId() {
        return wfId;
    }

    /**
     * Set the job id.
     *
     * @param id jobId;
     */
    public void setJobId(String id) {
        this.wfId = id;
    }

    public String getSlaXml() {
        return slaXml;
    }

    public void setSlaXml(String slaXml) {
        this.slaXml = slaXml;
    }

    @Override
    public void setStatus(Status val) {
        this.status = val.toString();
        super.setStatus(val);
    }

    public String getStatusStr() {
        return status;
    }

    @Override
    public Status getStatus() {
        return Status.valueOf(this.status);
    }

    /**
     * Return the node execution path.
     *
     * @return the node execution path.
     */
    public String getExecutionPath() {
        return executionPath;
    }

    /**
     * Set the node execution path.
     *
     * @param executionPath the node execution path.
     */
    public void setExecutionPath(String executionPath) {
        this.executionPath = executionPath;
    }

    /**
     * Return the signal value for the action. <p/> For decision nodes it is the choosen transition, for actions it is
     * OK or ERROR.
     *
     * @return the action signal value.
     */
    public String getSignalValue() {
        return signalValue;
    }

    /**
     * Set the signal value for the action. <p/> For decision nodes it is the choosen transition, for actions it is OK
     * or ERROR.
     *
     * @param signalValue the action signal value.
     */
    public void setSignalValue(String signalValue) {
        this.signalValue = signalValue;
    }

    /**
     * Return the job log token.
     *
     * @return the job log token.
     */
    public String getLogToken() {
        return logToken;
    }

    /**
     * Set the job log token.
     *
     * @param logToken the job log token.
     */
    public void setLogToken(String logToken) {
        this.logToken = logToken;
    }

    /**
     * Return the action last check time
     *
     * @return the last check time
     */
    public Date getLastCheckTime() {
        return DateUtils.toDate(lastCheckTimestamp);
    }

    /**
     * Return the action last check time
     *
     * @return the last check time
     */
    public Timestamp getLastCheckTimestamp() {
        return lastCheckTimestamp;
    }

    /**
     * Return the action last check time
     *
     * @return the last check time
     */
    public Timestamp getStartTimestamp() {
        return startTimestamp;
    }

    /**
     * Return the action last check time
     *
     * @return the last check time
     */
    public Timestamp getEndTimestamp() {
        return endTimestamp;
    }


    /**
     * Return the action last check time
     *
     * @return the last check time
     */
    public Timestamp getPendingAgeTimestamp() {
        return pendingAgeTimestamp;
    }

    /**
     * Sets the action last check time
     *
     * @param lastCheckTime the last check time to set.
     */
    public void setLastCheckTime(Date lastCheckTime) {
        this.lastCheckTimestamp = DateUtils.convertDateToTimestamp(lastCheckTime);
    }

    public boolean getPending() {
        return this.pending == 1 ? true : false;
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

}
