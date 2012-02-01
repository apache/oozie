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
package org.apache.oozie.client.rest;

import java.text.MessageFormat;
import java.util.Date;
import java.util.List;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.DiscriminatorColumn;
import javax.persistence.DiscriminatorType;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.apache.oozie.client.CoordinatorAction;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

@Entity
@Table(name = "COORD_ACTIONS")
@DiscriminatorColumn(name = "bean_type", discriminatorType = DiscriminatorType.STRING)
public class JsonCoordinatorAction implements CoordinatorAction, JsonBean {

    @Id
    private String id;

    @Transient
    private String jobId;

    @Basic
    @Column(name = "job_type")
    private String type;

    @Transient
    private Status status = CoordinatorAction.Status.WAITING;

    @Basic
    @Column(name = "action_number")
    private int actionNumber;

    @Transient
    private Date createdTime;

    @Column(name = "created_conf")
    @Lob
    private String createdConf;

    @Transient
    private String externalId;

    @Basic
    @Column(name = "time_out")
    private int timeOut = 0;

    @Transient
    private Date lastModifiedTime;

    @Transient
    private Date nominalTime;

    @Column(name = "run_conf")
    @Lob
    private String runConf;

    @Column(name = "action_xml")
    @Lob
    private String actionXml;

    @Column(name = "missing_dependencies")
    @Lob
    private String missingDependencies;

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

    public JsonCoordinatorAction() {

    }

    @SuppressWarnings("unchecked")
    public JSONObject toJSONObject() {
        JSONObject json = new JSONObject();
        json.put(JsonTags.COORDINATOR_ACTION_ID, id);
        json.put(JsonTags.COORDINATOR_JOB_ID, jobId);
        json.put(JsonTags.COORDINATOR_ACTION_TYPE, type);
        json.put(JsonTags.COORDINATOR_ACTION_NUMBER, actionNumber);
        json.put(JsonTags.COORDINATOR_ACTION_CREATED_CONF, createdConf);
        json.put(JsonTags.COORDINATOR_ACTION_CREATED_TIME, JsonUtils
                .formatDateRfc822(createdTime));
        json.put(JsonTags.COORDINATOR_ACTION_NOMINAL_TIME, JsonUtils
                .formatDateRfc822(nominalTime));
        json.put(JsonTags.COORDINATOR_ACTION_EXTERNALID, externalId);
        // json.put(JsonTags.COORDINATOR_ACTION_START_TIME, JsonUtils
        // .formatDateRfc822(startTime));
        json.put(JsonTags.COORDINATOR_ACTION_STATUS, status.toString());
        json.put(JsonTags.COORDINATOR_ACTION_RUNTIME_CONF, runConf);
        json.put(JsonTags.COORDINATOR_ACTION_LAST_MODIFIED_TIME, JsonUtils
                .formatDateRfc822(lastModifiedTime));
        // json.put(JsonTags.COORDINATOR_ACTION_START_TIME, JsonUtils
        // .formatDateRfc822(startTime));
        // json.put(JsonTags.COORDINATOR_ACTION_END_TIME, JsonUtils
        // .formatDateRfc822(endTime));
        json.put(JsonTags.COORDINATOR_ACTION_MISSING_DEPS, missingDependencies);
        json.put(JsonTags.COORDINATOR_ACTION_EXTERNAL_STATUS, externalStatus);
        json.put(JsonTags.COORDINATOR_ACTION_TRACKER_URI, trackerUri);
        json.put(JsonTags.COORDINATOR_ACTION_CONSOLE_URL, consoleUrl);
        json.put(JsonTags.COORDINATOR_ACTION_ERROR_CODE, errorCode);
        json.put(JsonTags.COORDINATOR_ACTION_ERROR_MESSAGE, errorMessage);
        json.put(JsonTags.TO_STRING, toString());
        return json;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String id) {
        this.jobId = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getExternalId() {
        return externalId;
    }

    public void setExternalId(String extId) {
        this.externalId = extId;
    }


    public void setActionNumber(int actionNumber) {
        this.actionNumber = actionNumber;
    }

    public int getActionNumber() {
        return actionNumber;
    }

    public String getCreatedConf() {
        return createdConf;
    }

    public void setCreatedConf(String createdConf) {
        this.createdConf = createdConf;
    }

    public void setCreatedTime(Date createdTime) {
        this.createdTime = createdTime;
    }

    public Date getCreatedTime() {
        return createdTime;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public void setLastModifiedTime(Date lastModifiedTime) {
        this.lastModifiedTime = lastModifiedTime;
    }

    public Date getLastModifiedTime() {
        return lastModifiedTime;
    }

    public void setRunConf(String runConf) {
        this.runConf = runConf;
    }

    public String getRunConf() {
        return runConf;
    }

    public void setMissingDependencies(String missingDependencies) {
        this.missingDependencies = missingDependencies;
    }

    public String getMissingDependencies() {
        return missingDependencies;
    }

    public String getExternalStatus() {
        return externalStatus;
    }

    public void setExternalStatus(String externalStatus) {
        this.externalStatus = externalStatus;
    }

    public String getTrackerUri() {
        return trackerUri;
    }

    public void setTrackerUri(String trackerUri) {
        this.trackerUri = trackerUri;
    }

    public String getConsoleUrl() {
        return consoleUrl;
    }

    public void setConsoleUrl(String consoleUrl) {
        this.consoleUrl = consoleUrl;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorInfo(String errorCode, String errorMessage) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    public String getActionXml() {
        return actionXml;
    }

    public void setActionXml(String actionXml) {
        this.actionXml = actionXml;
    }

    @Override
    public String toString() {
        return MessageFormat.format("WorkflowAction name[{0}] status[{1}]",
                                    getId(), getStatus());
    }

    public Date getNominalTime() {
        return nominalTime;
    }

    public void setNominalTime(Date nominalTime) {
        this.nominalTime = nominalTime;
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

    /**
     * Convert a nodes list into a JSONArray.
     *
     * @param actions nodes list.
     * @return the corresponding JSON array.
     */
    @SuppressWarnings("unchecked")
    public static JSONArray toJSONArray(List<? extends JsonCoordinatorAction> actions) {
        JSONArray array = new JSONArray();
        for (JsonCoordinatorAction action : actions) {
            array.add(action.toJSONObject());
        }
        return array;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#equals(java.lang.Object)
     */
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
        JsonCoordinatorAction other = (JsonCoordinatorAction) obj;
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
