/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.client.rest;

import org.apache.oozie.client.WorkflowAction;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.persistence.*;

/**
 * Json Bean that represents an Oozie workflow node.
 */
@Entity
@Table(name = "WF_ACTIONS")
@DiscriminatorColumn(name = "bean_type", discriminatorType = DiscriminatorType.STRING)

public class JsonWorkflowAction implements WorkflowAction, JsonBean {
    @Id
    private String id;

    @Basic
    @Column(name = "name")
    private String name = null;

    @Basic
    @Column(name = "type")
    private String type = null;

    @Basic
    @Column(name = "conf")
    @Lob
    private String conf = null;

    @Transient
    private Status status = WorkflowAction.Status.PREP;

    @Basic
    @Column(name = "retries")
    private int retries;

    @Transient
    private Date startTime;

    @Transient
    private Date endTime;

    @Basic
    @Column(name = "transition")
    private String transition = null;

    @Column(name = "data")
    @Lob
    private String data = null;

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

    @Column(name = "error_message")
    @Lob
    private String errorMessage = null;

    public JsonWorkflowAction() {
    }

    public JsonWorkflowAction(JSONObject jsonObject) {
        id = (String) jsonObject.get(JsonTags.WORKFLOW_ACTION_ID);
        name = (String) jsonObject.get(JsonTags.WORKFLOW_ACTION_NAME);
        type = (String) jsonObject.get(JsonTags.WORKFLOW_ACTION_TYPE);
        conf = (String) jsonObject.get(JsonTags.WORKFLOW_ACTION_CONF);
        status = Status.valueOf((String) jsonObject.get(JsonTags.WORKFLOW_ACTION_STATUS));
        retries = (int) JsonUtils.getLongValue(jsonObject, JsonTags.WORKFLOW_ACTION_RETRIES);
        startTime = JsonUtils.parseDateRfc822((String) jsonObject.get(JsonTags.WORKFLOW_ACTION_START_TIME));
        endTime = JsonUtils.parseDateRfc822((String) jsonObject.get(JsonTags.WORKFLOW_ACTION_END_TIME));
        transition = (String) jsonObject.get(JsonTags.WORKFLOW_ACTION_TRANSITION);
        data = (String) jsonObject.get(JsonTags.WORKFLOW_ACTION_DATA);
        externalId = (String) jsonObject.get(JsonTags.WORKFLOW_ACTION_EXTERNAL_ID);
        externalStatus = (String) jsonObject.get(JsonTags.WORKFLOW_ACTION_EXTERNAL_STATUS);
        trackerUri = (String) jsonObject.get(JsonTags.WORKFLOW_ACTION_TRACKER_URI);
        consoleUrl = (String) jsonObject.get(JsonTags.WORKFLOW_ACTION_CONSOLE_URL);
        errorCode = (String) jsonObject.get(JsonTags.WORKFLOW_ACTION_ERROR_CODE);
        errorMessage = (String) jsonObject.get(JsonTags.WORKFLOW_ACTION_ERROR_MESSAGE);
    }

    @SuppressWarnings("unchecked")
    public JSONObject toJSONObject() {
        JSONObject json = new JSONObject();
        json.put(JsonTags.WORKFLOW_ACTION_ID, id);
        json.put(JsonTags.WORKFLOW_ACTION_NAME, name);
        json.put(JsonTags.WORKFLOW_ACTION_TYPE, type);
        json.put(JsonTags.WORKFLOW_ACTION_CONF, conf);
        json.put(JsonTags.WORKFLOW_ACTION_START_TIME, JsonUtils.formatDateRfc822(startTime));
        json.put(JsonTags.WORKFLOW_ACTION_STATUS, status.toString());
        json.put(JsonTags.WORKFLOW_ACTION_RETRIES, (long) retries);
        json.put(JsonTags.WORKFLOW_ACTION_START_TIME, JsonUtils.formatDateRfc822(startTime));
        json.put(JsonTags.WORKFLOW_ACTION_END_TIME, JsonUtils.formatDateRfc822(endTime));
        json.put(JsonTags.WORKFLOW_ACTION_TRANSITION, transition);
        json.put(JsonTags.WORKFLOW_ACTION_DATA, data);
        json.put(JsonTags.WORKFLOW_ACTION_EXTERNAL_ID, externalId);
        json.put(JsonTags.WORKFLOW_ACTION_EXTERNAL_STATUS, externalStatus);
        json.put(JsonTags.WORKFLOW_ACTION_TRACKER_URI, trackerUri);
        json.put(JsonTags.WORKFLOW_ACTION_CONSOLE_URL, consoleUrl);
        json.put(JsonTags.WORKFLOW_ACTION_ERROR_CODE, errorCode);
        json.put(JsonTags.WORKFLOW_ACTION_ERROR_MESSAGE, errorMessage);
        return json;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getConf() {
        return conf;
    }

    public void setConf(String conf) {
        this.conf = conf;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public String getTransition() {
        return transition;
    }

    public void setTransition(String transition) {
        this.transition = transition;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getExternalId() {
        return externalId;
    }

    public void setExternalId(String externalId) {
        this.externalId = externalId;
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

    public String toString() {
        return MessageFormat.format("Action name[{0}] status[{1}]", getName(), getStatus());
    }

    /**
     * Convert a nodes list into a JSONArray.
     *
     * @param nodes nodes list.
     * @return the corresponding JSON array.
     */
    @SuppressWarnings("unchecked")
    public static JSONArray toJSONArray(List<? extends JsonWorkflowAction> nodes) {
        JSONArray array = new JSONArray();
        for (JsonWorkflowAction node : nodes) {
            array.add(node.toJSONObject());
        }
        return array;
    }

    /**
     * Convert a JSONArray into a nodes list.
     *
     * @param array JSON array.
     * @return the corresponding nodes list.
     */
    @SuppressWarnings("unchecked")
    public static List<JsonWorkflowAction> fromJSONArray(JSONArray array) {
        List<JsonWorkflowAction> list = new ArrayList<JsonWorkflowAction>();
        for (Object obj : array) {
            list.add(new JsonWorkflowAction((JSONObject) obj));
        }
        return list;
    }

}
