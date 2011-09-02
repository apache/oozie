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
package org.apache.oozie.client.rest;

import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.persistence.*;

/**
 * Json Bean that represents an Oozie workflow job.
 */

@Entity
@Table(name = "WF_JOBS")
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
@DiscriminatorColumn(name = "bean_type", discriminatorType = DiscriminatorType.STRING)
public class JsonWorkflowJob implements WorkflowJob, JsonBean {

    @Id
    private String id;

    @Basic
    @Column(name = "app_name")
    private String appName = null;

    @Basic
    @Column(name = "app_path")
    private String appPath = null;

    @Transient
    private String externalId = null;

    @Column(name = "conf")
    @Lob
    private String conf = null;

    @Transient
    private Status status = WorkflowJob.Status.PREP;

    @Transient
    private Date createdTime;

    @Transient
    private Date startTime;

    @Transient
    private Date endTime;

    @Transient
    private Date lastModifiedTime;

    @Basic
    @Column(name = "user_name")
    private String user = null;

    @Basic
    @Column(name = "group_name")
    private String group;

    @Basic
    @Column(name = "run")
    private int run = 1;

    @Transient
    private String consoleUrl;

    @Transient
    private List<? extends JsonWorkflowAction> actions;

    public JsonWorkflowJob() {
        actions = new ArrayList<JsonWorkflowAction>();
    }

    @SuppressWarnings("unchecked")
    public JsonWorkflowJob(JSONObject json) {
        appPath = (String) json.get(JsonTags.WORKFLOW_APP_PATH);
        appName = (String) json.get(JsonTags.WORKFLOW_APP_NAME);
        id = (String) json.get(JsonTags.WORKFLOW_ID);
        externalId = (String) json.get(JsonTags.WORKFLOW_EXTERNAL_ID);
        conf = (String) json.get(JsonTags.WORKFLOW_CONF);
        status = Status.valueOf((String) json.get(JsonTags.WORKFLOW_STATUS));
        lastModifiedTime = JsonUtils.parseDateRfc822((String) json.get(JsonTags.WORKFLOW_LAST_MOD_TIME));
        createdTime = JsonUtils.parseDateRfc822((String) json.get(JsonTags.WORKFLOW_CREATED_TIME));
        startTime = JsonUtils.parseDateRfc822((String) json.get(JsonTags.WORKFLOW_START_TIME));
        endTime = JsonUtils.parseDateRfc822((String) json.get(JsonTags.WORKFLOW_END_TIME));
        user = (String) json.get(JsonTags.WORKFLOW_USER);
        group = (String) json.get(JsonTags.WORKFLOW_GROUP);
        run = (int) JsonUtils.getLongValue(json, JsonTags.WORKFLOW_RUN);
        consoleUrl = (String) json.get(JsonTags.WORKFLOW_CONSOLE_URL);
        actions = JsonWorkflowAction.fromJSONArray((JSONArray) json.get(JsonTags.WORKFLOW_ACTIONS));
    }

    @SuppressWarnings("unchecked")
    public JSONObject toJSONObject() {
        JSONObject json = new JSONObject();
        json.put(JsonTags.WORKFLOW_APP_PATH, appPath);
        json.put(JsonTags.WORKFLOW_APP_NAME, appName);
        json.put(JsonTags.WORKFLOW_ID, id);
        json.put(JsonTags.WORKFLOW_EXTERNAL_ID, externalId);
        json.put(JsonTags.WORKFLOW_CONF, conf);
        json.put(JsonTags.WORKFLOW_STATUS, status.toString());
        json.put(JsonTags.WORKFLOW_LAST_MOD_TIME, JsonUtils.formatDateRfc822(lastModifiedTime));
        json.put(JsonTags.WORKFLOW_CREATED_TIME, JsonUtils.formatDateRfc822(createdTime));
        json.put(JsonTags.WORKFLOW_START_TIME, JsonUtils.formatDateRfc822(startTime));
        json.put(JsonTags.WORKFLOW_END_TIME, JsonUtils.formatDateRfc822(endTime));
        json.put(JsonTags.WORKFLOW_USER, user);
        json.put(JsonTags.WORKFLOW_GROUP, group);
        json.put(JsonTags.WORKFLOW_RUN, (long) run);
        json.put(JsonTags.WORKFLOW_CONSOLE_URL, consoleUrl);
        json.put(JsonTags.WORKFLOW_ACTIONS, JsonWorkflowAction.toJSONArray(actions));
        return json;
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

    public Date getLastModifiedTime() {
        return lastModifiedTime;
    }

    public void setLastModifiedTime(Date lastModTime) {
        this.lastModifiedTime = lastModTime;
    }

    public Date getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(Date createdTime) {
        this.createdTime = createdTime;
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

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public int getRun() {
        return run;
    }

    public void setRun(int run) {
        this.run = run;
    }

    /**
     * Return the workflow job console URL.
     *
     * @return the workflow job console URL.
     */
    public String getConsoleUrl() {
        return consoleUrl;
    }

    /**
     * Set the workflow job console URL.
     *
     * @param consoleUrl the workflow job console URL.
     */
    public void setConsoleUrl(String consoleUrl) {
        this.consoleUrl = consoleUrl;
    }

    @SuppressWarnings("unchecked")
    public List<WorkflowAction> getActions() {
        return (List) actions;
    }

    public void setActions(List<? extends JsonWorkflowAction> nodes) {
        this.actions = (nodes != null) ? nodes : new ArrayList<JsonWorkflowAction>();
    }

    public String toString() {
        return MessageFormat.format("Workflow id[{0}] status[{1}]", getId(), getStatus());
    }

    /**
     * Convert a workflows list into a JSONArray.
     *
     * @param workflows workflows list.
     * @return the corresponding JSON array.
     */
    @SuppressWarnings("unchecked")
    public static JSONArray toJSONArray(List<? extends JsonWorkflowJob> workflows) {
        JSONArray array = new JSONArray();
        if (workflows != null) {
            for (JsonWorkflowJob node : workflows) {
                array.add(node.toJSONObject());
            }
        }
        return array;
    }

    /**
     * Convert a JSONArray into a workflows list.
     *
     * @param array JSON array.
     * @return the corresponding workflows list.
     */
    @SuppressWarnings("unchecked")
    public static List<WorkflowJob> fromJSONArray(JSONArray array) {
        List<WorkflowJob> list = new ArrayList<WorkflowJob>();
        for (Object obj : array) {
            list.add(new JsonWorkflowJob((JSONObject) obj));
        }
        return list;
    }

}
