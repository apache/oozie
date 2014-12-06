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
import java.util.Date;
import java.util.List;
import java.util.Properties;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;

import org.apache.hadoop.io.Writable;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.JsonUtils;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.PropertiesUtils;
import org.apache.oozie.util.WritableUtils;
import org.apache.openjpa.persistence.jdbc.Index;
import org.apache.openjpa.persistence.jdbc.Strategy;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * Bean that contains all the information to start an action for a workflow
 * node.
 */
@Entity
@NamedQueries({

    @NamedQuery(name = "UPDATE_ACTION", query = "update WorkflowActionBean a set a.conf = :conf, a.consoleUrl = :consoleUrl, a.data = :data, a.stats = :stats, a.externalChildIDs = :externalChildIDs, a.errorCode = :errorCode, a.errorMessage = :errorMessage, a.externalId = :externalId, a.externalStatus = :externalStatus, a.name = :name, a.cred = :cred , a.retries = :retries, a.trackerUri = :trackerUri, a.transition = :transition, a.type = :type, a.endTimestamp = :endTime, a.executionPath = :executionPath, a.lastCheckTimestamp = :lastCheckTime, a.logToken = :logToken, a.pending = :pending, a.pendingAgeTimestamp = :pendingAge, a.signalValue = :signalValue, a.slaXml = :slaXml, a.startTimestamp = :startTime, a.statusStr = :status, a.wfId=:wfId where a.id = :id"),

    @NamedQuery(name = "UPDATE_ACTION_FOR_LAST_CHECKED_TIME", query = "update WorkflowActionBean a set a.lastCheckTimestamp = :lastCheckTime where a.id = :id"),

    @NamedQuery(name = "UPDATE_ACTION_START", query = "update WorkflowActionBean a set a.startTimestamp = :startTime, a.externalChildIDs = :externalChildIDs, a.conf = :conf, a.errorCode = :errorCode, a.errorMessage = :errorMessage, a.startTimestamp = :startTime, a.externalId = :externalId, a.trackerUri = :trackerUri, a.consoleUrl = :consoleUrl, a.lastCheckTimestamp = :lastCheckTime, a.statusStr = :status, a.externalStatus = :externalStatus, a.data = :data, a.retries = :retries, a.pending = :pending, a.pendingAgeTimestamp = :pendingAge, a.userRetryCount = :userRetryCount where a.id = :id"),

    @NamedQuery(name = "UPDATE_ACTION_CHECK", query = "update WorkflowActionBean a set a.userRetryCount = :userRetryCount, a.stats = :stats, a.externalChildIDs = :externalChildIDs, a.externalStatus = :externalStatus, a.statusStr = :status, a.data = :data, a.pending = :pending, a.errorCode = :errorCode, a.errorMessage = :errorMessage, a.lastCheckTimestamp = :lastCheckTime, a.retries = :retries, a.pendingAgeTimestamp = :pendingAge, a.startTimestamp = :startTime where a.id = :id"),

    @NamedQuery(name = "UPDATE_ACTION_END", query = "update WorkflowActionBean a set a.stats = :stats, a.errorCode = :errorCode, a.errorMessage = :errorMessage, a.retries = :retries, a.endTimestamp = :endTime, a.statusStr = :status, a.pending = :pending, a.pendingAgeTimestamp = :pendingAge, a.signalValue = :signalValue, a.userRetryCount = :userRetryCount, a.externalStatus = :externalStatus where a.id = :id"),

    @NamedQuery(name = "UPDATE_ACTION_PENDING", query = "update WorkflowActionBean a set a.pending = :pending, a.pendingAgeTimestamp = :pendingAge where a.id = :id"),

    @NamedQuery(name = "UPDATE_ACTION_STATUS_PENDING", query = "update WorkflowActionBean a set a.statusStr = :status, a.pending = :pending, a.pendingAgeTimestamp = :pendingAge where a.id = :id"),

    @NamedQuery(name = "UPDATE_ACTION_PENDING_TRANS", query = "update WorkflowActionBean a set a.pending = :pending, a.pendingAgeTimestamp = :pendingAge, a.transition = :transition where a.id = :id"),

    @NamedQuery(name = "UPDATE_ACTION_PENDING_TRANS_ERROR", query = "update WorkflowActionBean a set a.pending = :pending, a.pendingAgeTimestamp = :pendingAge, a.transition = :transition, a.errorCode = :errorCode, a.errorMessage = :errorMessage where a.id = :id"),

    @NamedQuery(name = "DELETE_ACTION", query = "delete from WorkflowActionBean a where a.id = :id"),

    @NamedQuery(name = "DELETE_ACTIONS_FOR_WORKFLOW", query = "delete from WorkflowActionBean a where a.wfId = :wfId"),

    @NamedQuery(name = "GET_ACTIONS", query = "select OBJECT(a) from WorkflowActionBean a"),

    @NamedQuery(name = "GET_ACTION", query = "select OBJECT(a) from WorkflowActionBean a where a.id = :id"),

    @NamedQuery(name = "GET_ACTION_ID_TYPE_LASTCHECK", query = "select a.id, a.type, a.lastCheckTimestamp from WorkflowActionBean a where a.id = :id"),

    @NamedQuery(name = "GET_ACTION_FAIL", query = "select a.id, a.wfId, a.name, a.statusStr, a.pending, a.type, a.logToken, a.transition, a.errorCode, a.errorMessage from WorkflowActionBean a where a.id = :id"),

    @NamedQuery(name = "GET_ACTION_SIGNAL", query = "select a.id, a.wfId, a.name, a.statusStr, a.pending, a.pendingAgeTimestamp, a.type, a.logToken, a.transition, a.errorCode, a.errorMessage, a.executionPath, a.signalValue, a.slaXml from WorkflowActionBean a where a.id = :id"),

    @NamedQuery(name = "GET_ACTION_CHECK", query = "select a.id, a.wfId, a.name, a.statusStr, a.pending, a.pendingAgeTimestamp, a.type, a.logToken, a.transition, a.retries, a.userRetryCount, a.userRetryMax, a.userRetryInterval, a.trackerUri, a.startTimestamp, a.endTimestamp, a.lastCheckTimestamp, a.errorCode, a.errorMessage, a.externalId, a.externalStatus, a.externalChildIDs, a.conf from WorkflowActionBean a where a.id = :id"),

    @NamedQuery(name = "GET_ACTION_END", query = "select a.id, a.wfId, a.name, a.statusStr, a.pending, a.pendingAgeTimestamp, a.type, a.logToken, a.transition, a.retries, a.trackerUri, a.userRetryCount, a.userRetryMax, a.userRetryInterval, a.startTimestamp, a.endTimestamp, a.errorCode, a.errorMessage, a.externalId, a.externalStatus, a.externalChildIDs, a.conf, a.data, a.stats from WorkflowActionBean a where a.id = :id"),

    @NamedQuery(name = "GET_ACTION_COMPLETED", query = "select a.id, a.wfId, a.statusStr, a.type, a.logToken from WorkflowActionBean a where a.id = :id"),

    @NamedQuery(name = "GET_ACTION_FOR_UPDATE", query = "select OBJECT(a) from WorkflowActionBean a where a.id = :id"),

    @NamedQuery(name = "GET_ACTION_FOR_SLA", query = "select a.id, a.statusStr, a.startTimestamp, a.endTimestamp from WorkflowActionBean a where a.id = :id"),

    @NamedQuery(name = "GET_ACTIONS_FOR_WORKFLOW", query = "select OBJECT(a) from WorkflowActionBean a where a.wfId = :wfId order by a.startTimestamp"),

    @NamedQuery(name = "GET_ACTIONS_OF_WORKFLOW_FOR_UPDATE", query = "select OBJECT(a) from WorkflowActionBean a where a.wfId = :wfId order by a.startTimestamp"),

    @NamedQuery(name = "GET_PENDING_ACTIONS", query = "select a.id, a.wfId, a.statusStr, a.type, a.pendingAgeTimestamp from WorkflowActionBean a where a.pending = 1 AND a.pendingAgeTimestamp < :pendingAge AND a.statusStr <> 'RUNNING'"),

    @NamedQuery(name = "GET_RUNNING_ACTIONS", query = "select a.id from WorkflowActionBean a where a.pending = 1 AND a.statusStr = 'RUNNING' AND a.lastCheckTimestamp < :lastCheckTime"),

    @NamedQuery(name = "GET_RETRY_MANUAL_ACTIONS", query = "select OBJECT(a) from WorkflowActionBean a where a.wfId = :wfId AND (a.statusStr = 'START_RETRY' OR a.statusStr = 'START_MANUAL' OR a.statusStr = 'END_RETRY' OR a.statusStr = 'END_MANUAL')"),

    @NamedQuery(name = "GET_ACTIONS_FOR_WORKFLOW_RERUN", query = "select a.id, a.name, a.statusStr from WorkflowActionBean a where a.wfId = :wfId order by a.startTimestamp") })
@Table(name = "WF_ACTIONS")
public class WorkflowActionBean implements Writable, WorkflowAction, JsonBean {
    @Id
    private String id;

    @Basic
    @Index
    @Column(name = "wf_id")
    private String wfId = null;

    @Basic
    @Column(name = "created_time")
    private Timestamp createdTimeTS = null;

    @Basic
    @Index
    @Column(name = "status")
    private String statusStr = WorkflowAction.Status.PREP.toString();

    @Basic
    @Column(name = "last_check_time")
    private Timestamp lastCheckTimestamp;

    @Basic
    @Column(name = "end_time")
    private Timestamp endTimestamp = null;

    @Basic
    @Column(name = "start_time")
    private Timestamp startTimestamp = null;

    @Basic
    @Column(name = "execution_path", length = 1024)
    private String executionPath = null;

    @Basic
    @Column(name = "pending")
    private int pending = 0;

    @Basic
    @Index
    @Column(name = "pending_age")
    private Timestamp pendingAgeTimestamp = null;

    @Basic
    @Column(name = "signal_value")
    private String signalValue = null;

    @Basic
    @Column(name = "log_token")
    private String logToken = null;

    @Basic
    @Column(name = "sla_xml")
    @Lob
    @Strategy("org.apache.oozie.executor.jpa.StringBlobValueHandler")
    private StringBlob slaXml;

    @Basic
    @Column(name = "name")
    private String name = null;

    @Basic
    @Column(name = "cred")
    private String cred = null;

    @Basic
    @Column(name = "type")
    private String type = null;

    @Basic
    @Column(name = "conf")
    @Lob
    @Strategy("org.apache.oozie.executor.jpa.StringBlobValueHandler")
    private StringBlob conf;

    @Basic
    @Column(name = "retries")
    private int retries;

    @Basic
    @Column(name = "user_retry_count")
    private int userRetryCount;

    @Basic
    @Column(name = "user_retry_max")
    private int userRetryMax;

    @Basic
    @Column(name = "user_retry_interval")
    private int userRetryInterval;

    @Basic
    @Column(name = "transition")
    private String transition = null;

    @Basic
    @Column(name = "data")
    @Lob
    @Strategy("org.apache.oozie.executor.jpa.StringBlobValueHandler")
    private StringBlob data;

    @Basic
    @Column(name = "stats")
    @Lob
    @Strategy("org.apache.oozie.executor.jpa.StringBlobValueHandler")
    private StringBlob stats;

    @Basic
    @Column(name = "external_child_ids")
    @Lob
    @Strategy("org.apache.oozie.executor.jpa.StringBlobValueHandler")
    private StringBlob externalChildIDs;

    @Basic
    @Column(name = "external_id")
    private String externalId = null;

    @Basic
    @Column(name = "external_status")
    private String externalStatus = null;

    @Basic
    @Column(name = "tracker_uri")
    private String trackerUri = null;

    @Basic
    @Column(name = "console_url")
    private String consoleUrl = null;

    @Basic
    @Column(name = "error_code")
    private String errorCode = null;

    @Column(name = "error_message", length = 500)
    private String errorMessage = null;

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
        dataOutput.writeLong((getPendingAge() != null) ? getPendingAge().getTime() : -1);
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
            pendingAgeTimestamp = DateUtils.convertDateToTimestamp(new Date(d));
        }
        signalValue = WritableUtils.readStr(dataInput);
        logToken = WritableUtils.readStr(dataInput);
        setUserRetryCount(dataInput.readInt());
        setUserRetryInterval(dataInput.readInt());
        setUserRetryMax(dataInput.readInt());
    }

    /**
     * Return whether workflow action in terminal state or not
     *
     * @return
     */
    public boolean inTerminalState() {
        boolean isTerminalState = false;
        switch (WorkflowAction.Status.valueOf(statusStr)) {
            case ERROR:
            case FAILED:
            case KILLED:
            case OK:
                isTerminalState = true;
                break;
            default:
                break;
        }
        return isTerminalState;
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
     * @return boolean true if status is START_RETRY or START_MANUAL or
     *         END_RETRY or END_MANUAL
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
        return getStatus() == WorkflowAction.Status.OK || getStatus() == WorkflowAction.Status.KILLED
                || getStatus() == WorkflowAction.Status.ERROR;
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
            case ERROR:
                result = true;
        }
        return result;
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
        pendingAgeTimestamp = DateUtils.convertDateToTimestamp(new Date());
    }

    /**
     * Set pending flag
     */
    public void setPending(int i) {
        pending = i;
    }

    /**
     * Set a time when the action will be pending, normally a time in the
     * future.
     *
     * @param pendingAge the time when the action will be pending.
     */
    public void setPendingAge(Date pendingAge) {
        this.pendingAgeTimestamp = DateUtils.convertDateToTimestamp(pendingAge);
    }

    /**
     * Return the pending age of the action.
     *
     * @return the pending age of the action, <code>null</code> if the action is
     *         not pending.
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
     * Set a tracking information for an action, and set the action status to
     * {@link Action.Status#DONE}
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
     * Set the completion information for an action start. Sets the Action
     * status to {@link Action.Status#DONE}
     *
     * @param externalStatus action external end status.
     * @param actionData action output data, <code>null</code> if there is no
     *        action output data.
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
    @Override
    public String getExternalChildIDs() {
        return externalChildIDs == null ? null : externalChildIDs.getString();
    }

    /**
     * Set the external child IDs for the workflow action.
     *
     * @param externalChildIDs as a string.
     */
    public void setExternalChildIDs(String externalChildIDs) {
        if (this.externalChildIDs == null) {
            this.externalChildIDs = new StringBlob(externalChildIDs);
        }
        else {
            this.externalChildIDs.setString(externalChildIDs);
        }
    }

    /**
     * Set external child ids
     *
     * @param externalChildIds
     */
    public void setExternalChildIDsBlob(StringBlob externalChildIDs) {
        this.externalChildIDs = externalChildIDs;
    }

    /**
     * Get external ChildIds
     *
     * @return
     */
    public StringBlob getExternalChildIDsBlob() {
        return externalChildIDs;
    }

    /**
     * Set the completion information for an action end.
     *
     * @param status action status, {@link Action.Status#OK} or
     *        {@link Action.Status#ERROR} or {@link Action.Status#KILLED}
     * @param signalValue the signal value. In most cases, the value should be
     *        OK or ERROR.
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

    public void setSlaXml(String slaXmlStr) {
        if (this.slaXml == null) {
            this.slaXml = new StringBlob(slaXmlStr);
        }
        else {
            this.slaXml.setString(slaXmlStr);
        }
    }

    public String getSlaXml() {
        return slaXml == null ? null : slaXml.getString();
    }

    public void setSlaXmlBlob(StringBlob slaXml) {
        this.slaXml = slaXml;
    }

    public StringBlob getSlaXmlBlob() {
        return slaXml;
    }

    /**
     * Set status of job
     *
     * @param val
     */
    public void setStatus(Status val) {
        this.statusStr = val.toString();
    }

    @Override
    public Status getStatus() {
        return Status.valueOf(this.statusStr);
    }

    /**
     * Set status
     *
     * @param statusStr
     */
    public void setStatusStr(String statusStr) {
        this.statusStr = statusStr;
    }

    /**
     * Get status
     *
     * @return
     */
    public String getStatusStr() {
        return statusStr;
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
     * Return the signal value for the action.
     * <p/>
     * For decision nodes it is the choosen transition, for actions it is OK or
     * ERROR.
     *
     * @return the action signal value.
     */
    public String getSignalValue() {
        return signalValue;
    }

    /**
     * Set the signal value for the action.
     * <p/>
     * For decision nodes it is the choosen transition, for actions it is OK or
     * ERROR.
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

    public int getPending() {
        return this.pending;
    }

    @Override
    public Date getStartTime() {
        return DateUtils.toDate(startTimestamp);
    }

    /**
     * Set start time
     *
     * @param startTime
     */
    public void setStartTime(Date startTime) {
        this.startTimestamp = DateUtils.convertDateToTimestamp(startTime);
    }

    @Override
    public Date getEndTime() {
        return DateUtils.toDate(endTimestamp);
    }

    /**
     * Set end time
     *
     * @param endTime
     */
    public void setEndTime(Date endTime) {
        this.endTimestamp = DateUtils.convertDateToTimestamp(endTime);
    }

    @SuppressWarnings("unchecked")
    public JSONObject toJSONObject() {
        return toJSONObject("GMT");
    }

    @SuppressWarnings("unchecked")
    public JSONObject toJSONObject(String timeZoneId) {
        JSONObject json = new JSONObject();
        json.put(JsonTags.WORKFLOW_ACTION_ID, id);
        json.put(JsonTags.WORKFLOW_ACTION_NAME, name);
        json.put(JsonTags.WORKFLOW_ACTION_AUTH, cred);
        json.put(JsonTags.WORKFLOW_ACTION_TYPE, type);
        json.put(JsonTags.WORKFLOW_ACTION_CONF, getConf());
        json.put(JsonTags.WORKFLOW_ACTION_STATUS, statusStr);
        json.put(JsonTags.WORKFLOW_ACTION_RETRIES, (long) retries);
        json.put(JsonTags.WORKFLOW_ACTION_START_TIME, JsonUtils.formatDateRfc822(getStartTime(), timeZoneId));
        json.put(JsonTags.WORKFLOW_ACTION_END_TIME, JsonUtils.formatDateRfc822(getEndTime(), timeZoneId));
        json.put(JsonTags.WORKFLOW_ACTION_TRANSITION, transition);
        json.put(JsonTags.WORKFLOW_ACTION_DATA, getData());
        json.put(JsonTags.WORKFLOW_ACTION_STATS, getStats());
        json.put(JsonTags.WORKFLOW_ACTION_EXTERNAL_CHILD_IDS, getExternalChildIDs());
        json.put(JsonTags.WORKFLOW_ACTION_EXTERNAL_ID, externalId);
        json.put(JsonTags.WORKFLOW_ACTION_EXTERNAL_STATUS, externalStatus);
        json.put(JsonTags.WORKFLOW_ACTION_TRACKER_URI, trackerUri);
        json.put(JsonTags.WORKFLOW_ACTION_CONSOLE_URL, consoleUrl);
        json.put(JsonTags.WORKFLOW_ACTION_ERROR_CODE, errorCode);
        json.put(JsonTags.WORKFLOW_ACTION_ERROR_MESSAGE, errorMessage);
        json.put(JsonTags.TO_STRING, toString());
        return json;
    }

    @Override
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Timestamp getCreatedTimestamp() {
        return createdTimeTS;
    }

    public Date getCreatedTime() {
        return DateUtils.toDate(createdTimeTS);
    }

    public void setCreatedTime(Date createdTime) {
        this.createdTimeTS = DateUtils.convertDateToTimestamp(createdTime);
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getCred() {
        return cred;
    }

    public void setCred(String cred) {
        this.cred = cred;
    }

    @Override
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
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

    @Override
    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    @Override
    public int getUserRetryCount() {
        return userRetryCount;
    }

    public void setUserRetryCount(int retryCount) {
        this.userRetryCount = retryCount;
    }

    public void incrmentUserRetryCount() {
        this.userRetryCount++;
    }

    @Override
    public int getUserRetryMax() {
        return userRetryMax;
    }

    /**
     * Set user retry max
     *
     * @param retryMax
     */
    public void setUserRetryMax(int retryMax) {
        this.userRetryMax = retryMax;
    }

    @Override
    public int getUserRetryInterval() {
        return userRetryInterval;
    }

    public void setUserRetryInterval(int retryInterval) {
        this.userRetryInterval = retryInterval;
    }

    @Override
    public String getTransition() {
        return transition;
    }

    /**
     * Set transition
     *
     * @param transition
     */
    public void setTransition(String transition) {
        this.transition = transition;
    }

    @Override
    public String getData() {
        return data == null ? null : data.getString();
    }

    /**
     * Set data
     *
     * @param data
     */
    public void setData(String data) {
        if (this.data == null) {
            this.data = new StringBlob(data);
        }
        else {
            this.data.setString(data);
        }
    }

    public void setDataBlob(StringBlob data) {
        this.data = data;
    }

    public StringBlob getDataBlob() {
        return data;
    }

    @Override
    public String getStats() {
        return stats == null ? null : stats.getString();
    }

    /**
     * Set stats
     *
     * @param stats
     */
    public void setStats(String stats) {
        if (this.stats == null) {
            this.stats = new StringBlob(stats);
        }
        else {
            this.stats.setString(stats);
        }
    }

    public void setStatsBlob(StringBlob stats) {
        this.stats = stats;
    }

    public StringBlob getStatsBlob() {
        return this.stats;
    }

    @Override
    public String getExternalId() {
        return externalId;
    }

    /**
     * Set external Id
     *
     * @param externalId
     */
    public void setExternalId(String externalId) {
        this.externalId = externalId;
    }

    @Override
    public String getExternalStatus() {
        return externalStatus;
    }

    /**
     * Set external status
     *
     * @param externalStatus
     */
    public void setExternalStatus(String externalStatus) {
        this.externalStatus = externalStatus;
    }

    @Override
    public String getTrackerUri() {
        return trackerUri;
    }

    /**
     * Set tracker uri
     *
     * @param trackerUri
     */
    public void setTrackerUri(String trackerUri) {
        this.trackerUri = trackerUri;
    }

    @Override
    public String getConsoleUrl() {
        return consoleUrl;
    }

    /**
     * Set console URL
     *
     * @param consoleUrl
     */
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

    /**
     * Set the error Info
     *
     * @param errorCode
     * @param errorMessage
     */
    public void setErrorInfo(String errorCode, String errorMessage) {
        this.errorCode = errorCode;
        if (errorMessage != null && errorMessage.length() > 500) {
            errorMessage = errorMessage.substring(0, 500);
        }
        this.errorMessage = errorMessage;
    }

    @Override
    public String toString() {
        return MessageFormat.format("Action name[{0}] status[{1}]", getName(), getStatus());
    }

    /**
     * Convert a nodes list into a JSONArray.
     *
     * @param nodes nodes list.
     * @param timeZoneId time zone to use for dates in the JSON array.
     * @return the corresponding JSON array.
     */
    @SuppressWarnings("unchecked")
    public static JSONArray toJSONArray(List<WorkflowActionBean> nodes, String timeZoneId) {
        JSONArray array = new JSONArray();
        for (WorkflowActionBean node : nodes) {
            array.add(node.toJSONObject(timeZoneId));
        }
        return array;
    }

}
