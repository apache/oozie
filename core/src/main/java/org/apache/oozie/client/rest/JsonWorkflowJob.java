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

    @Basic
    @Column(name = "parent_id")
    private String parentId;

    @Transient
    private String consoleUrl;

    @Transient
    private List<? extends JsonWorkflowAction> actions;

    public JsonWorkflowJob() {
        actions = new ArrayList<JsonWorkflowAction>();
    }

    @SuppressWarnings("unchecked")
    public JSONObject toJSONObject() {
        JSONObject json = new JSONObject();
        json.put(JsonTags.WORKFLOW_APP_PATH, getAppPath());
        json.put(JsonTags.WORKFLOW_APP_NAME, getAppName());
        json.put(JsonTags.WORKFLOW_ID, getId());
        json.put(JsonTags.WORKFLOW_EXTERNAL_ID, getExternalId());
        json.put(JsonTags.WORKFLOW_PARENT_ID, getParentId());
        json.put(JsonTags.WORKFLOW_CONF, getConf());
        json.put(JsonTags.WORKFLOW_STATUS, getStatus().toString());
        json.put(JsonTags.WORKFLOW_LAST_MOD_TIME, JsonUtils.formatDateRfc822(getLastModifiedTime()));
        json.put(JsonTags.WORKFLOW_CREATED_TIME, JsonUtils.formatDateRfc822(getCreatedTime()));
        json.put(JsonTags.WORKFLOW_START_TIME, JsonUtils.formatDateRfc822(getStartTime()));
        json.put(JsonTags.WORKFLOW_END_TIME, JsonUtils.formatDateRfc822(getEndTime()));
        json.put(JsonTags.WORKFLOW_USER, getUser());
        json.put(JsonTags.WORKFLOW_GROUP, getGroup());
        json.put(JsonTags.WORKFLOW_ACL, getAcl());
        json.put(JsonTags.WORKFLOW_RUN, (long) getRun());
        json.put(JsonTags.WORKFLOW_CONSOLE_URL, getConsoleUrl());
        json.put(JsonTags.WORKFLOW_ACTIONS, JsonWorkflowAction.toJSONArray(actions));
        json.put(JsonTags.TO_STRING, toString());
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

    @Override
    public String getAcl() {
        return getGroup();
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
     * Return the corresponding Action ID, if any.
     *
     * @return the coordinator Action Id.
     */
    public String getParentId() {
        return parentId;
    }

    /**
     * Set coordinator action id
     *
     * @param parentId : coordinator action id
     */
    public void setParentId(String parentId) {
        this.parentId = parentId;
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

    @Override
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

}
