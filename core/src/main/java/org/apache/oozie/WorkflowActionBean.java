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

import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.rest.JsonWorkflowAction;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.PropertiesUtils;
import org.apache.oozie.util.WritableUtils;
import org.apache.hadoop.io.Writable;

import java.util.Date;
import java.util.Properties;
import java.io.DataOutput;
import java.io.IOException;
import java.io.DataInput;

/**
 * Bean that contains all the information to start an action for a workflow node.
 */
public class WorkflowActionBean extends JsonWorkflowAction implements Writable {
    private String jobId;
    private String executionPath;
    private boolean pending;
    private Date pendingAge;
    private Date lastCheckTime;
    private String signalValue;
    private String logToken;

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
        WritableUtils.writeStr(dataOutput, getType());
        WritableUtils.writeStr(dataOutput, getConf());
        WritableUtils.writeStr(dataOutput, getStatus().toString());
        dataOutput.writeInt(getRetries());
        dataOutput.writeLong((getStartTime() != null) ? getStartTime().getTime() : -1);
        dataOutput.writeLong((getEndTime() != null) ? getEndTime().getTime() : -1);
        dataOutput.writeLong((getLastCheckTime() != null) ? getLastCheckTime().getTime() : -1);
        WritableUtils.writeStr(dataOutput, getTransition());
        WritableUtils.writeStr(dataOutput, getData());
        WritableUtils.writeStr(dataOutput, getExternalId());
        WritableUtils.writeStr(dataOutput, getExternalStatus());
        WritableUtils.writeStr(dataOutput, getTrackerUri());
        WritableUtils.writeStr(dataOutput, getConsoleUrl());
        WritableUtils.writeStr(dataOutput, getErrorCode());
        WritableUtils.writeStr(dataOutput, getErrorMessage());
        WritableUtils.writeStr(dataOutput, jobId);
        WritableUtils.writeStr(dataOutput, executionPath);
        dataOutput.writeBoolean(pending);
        dataOutput.writeLong((pendingAge != null) ? pendingAge.getTime() : -1);
        WritableUtils.writeStr(dataOutput, signalValue);
        WritableUtils.writeStr(dataOutput, logToken);
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
        setExternalId(WritableUtils.readStr(dataInput));
        setExternalStatus(WritableUtils.readStr(dataInput));
        setTrackerUri(WritableUtils.readStr(dataInput));
        setConsoleUrl(WritableUtils.readStr(dataInput));
        setErrorInfo(WritableUtils.readStr(dataInput), WritableUtils.readStr(dataInput));
        jobId = WritableUtils.readStr(dataInput);
        executionPath = WritableUtils.readStr(dataInput);
        pending = dataInput.readBoolean();
        d = dataInput.readLong();
        if (d != -1) {
            pendingAge = new Date(d);
        }
        signalValue = WritableUtils.readStr(dataInput);
        logToken = WritableUtils.readStr(dataInput);
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
     * Return if the action is complete.
     *
     * @return if the action is complete.
     */
    public boolean isComplete() {
        return getStatus() == WorkflowAction.Status.OK || getStatus() == WorkflowAction.Status.KILLED ||
               getStatus() == WorkflowAction.Status.ERROR;
    }

    /**
     * Set the action as pending and the current time as pending.
     */
    public void setPending() {
        pending = true;
        pendingAge = new Date();
    }

    /**
     * Set a time when the action will be pending, normally a time in the future.
     *
     * @param pendingAge the time when the action will be pending.
     */
    public void setPendingAge(Date pendingAge) {
        this.pendingAge = pendingAge;
    }

    /**
     * Return the pending age of the action.
     *
     * @return the pending age of the action, <code>null</code> if the action is not pending.
     */
    public Date getPendingAge() {
        return pendingAge;
    }

    /**
     * Return if the action is pending.
     *
     * @return if the action is pending.
     */
    public boolean isPending() {
        return pending;
    }

    /**
     * Removes the pending flag from the action.
     */
    public void resetPending() {
        pending = false;
        pendingAge = null;
    }


    /**
     * Increments the number of retries for the action.
     */
    public void incRetries() {
        setRetries(getRetries() + 1);
    }

    /**
     * Set a tracking information for an action, and set the action status to {@link org.apache.oozie.client.WorkflowAction.Status#DONE}
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
        setStartTime(now);
        setLastCheckTime(now);
        setStatus(Status.RUNNING);
    }

    /**
     * Set the completion information for an action start. Sets the Action status to {@link org.apache.oozie.client.WorkflowAction.Status#DONE}
     *
     * @param externalStatus action external end status.
     * @param actionData     action output data, <code>null</code> if there is no action output data.
     */
    public void setExecutionData(String externalStatus, Properties actionData) {
        setStatus(Status.DONE);
        setExternalStatus(ParamChecker.notEmpty(externalStatus, "externalStatus"));
        if (actionData != null) {
            setData(PropertiesUtils.propertiesToString(actionData));
        }
    }

    /**
     * Set the completion information for an action end.
     *
     * @param status action status, {@link org.apache.oozie.client.WorkflowAction.Status#OK} or
     *        {@link org.apache.oozie.client.WorkflowAction.Status#ERROR} or {@link org.apache.oozie.client.WorkflowAction.Status#KILLED}
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
        return jobId;
    }

    /**
     * Set the job id.
     *
     * @param id jobId;
     */
    public void setJobId(String id) {
        this.jobId = id;
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
     * For decision nodes it is the choosen transition, for actions it is OK or ERROR.
     *
     * @return  the action signal value.
     */
    public String getSignalValue() {
        return signalValue;
    }

    /**
     * Set the signal value for the action.
     * <p/>
     * For decision nodes it is the choosen transition, for actions it is OK or ERROR.
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
        return lastCheckTime;
    }

    /**
     * Sets the action last check time
     *
     * @param lastCheckTime the last check time to set.
     */
    public void setLastCheckTime(Date lastCheckTime) {
        this.lastCheckTime = lastCheckTime;
    }
}
