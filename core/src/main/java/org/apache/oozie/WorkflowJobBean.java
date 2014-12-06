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

import org.apache.hadoop.io.Writable;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.JsonUtils;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.WritableUtils;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.lite.LiteWorkflowInstance;
import org.apache.openjpa.persistence.jdbc.Index;
import org.apache.openjpa.persistence.jdbc.Strategy;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.persistence.Transient;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Entity

@NamedQueries({

    @NamedQuery(name = "UPDATE_WORKFLOW", query = "update WorkflowJobBean w set w.appName = :appName, w.appPath = :appPath, w.conf = :conf, w.group = :groupName, w.run = :run, w.user = :user, w.createdTimestamp = :createdTime, w.endTimestamp = :endTime, w.externalId = :externalId, w.lastModifiedTimestamp = :lastModTime,w.logToken = :logToken, w.protoActionConf = :protoActionConf, w.slaXml =:slaXml, w.startTimestamp = :startTime, w.statusStr = :status, w.wfInstance = :wfInstance where w.id = :id"),

    @NamedQuery(name = "UPDATE_WORKFLOW_MODTIME", query = "update WorkflowJobBean w set w.lastModifiedTimestamp = :lastModTime where w.id = :id"),

    @NamedQuery(name = "UPDATE_WORKFLOW_STATUS_MODTIME", query = "update WorkflowJobBean w set w.statusStr = :status, w.lastModifiedTimestamp = :lastModTime where w.id = :id"),

    @NamedQuery(name = "UPDATE_WORKFLOW_PARENT_MODIFIED", query = "update WorkflowJobBean w set w.parentId = :parentId, w.lastModifiedTimestamp = :lastModTime where w.id = :id"),

    @NamedQuery(name = "UPDATE_WORKFLOW_STATUS_INSTANCE_MODIFIED", query = "update WorkflowJobBean w set w.statusStr = :status, w.wfInstance = :wfInstance, w.lastModifiedTimestamp = :lastModTime where w.id = :id"),

    @NamedQuery(name = "UPDATE_WORKFLOW_STATUS_INSTANCE_MOD_END", query = "update WorkflowJobBean w set w.statusStr = :status, w.wfInstance = :wfInstance, w.lastModifiedTimestamp = :lastModTime, w.endTimestamp = :endTime where w.id = :id"),

    @NamedQuery(name = "UPDATE_WORKFLOW_STATUS_INSTANCE_MOD_START_END", query = "update WorkflowJobBean w set w.statusStr = :status, w.wfInstance = :wfInstance, w.lastModifiedTimestamp = :lastModTime, w.startTimestamp = :startTime, w.endTimestamp = :endTime where w.id = :id"),

    @NamedQuery(name = "UPDATE_WORKFLOW_RERUN", query = "update WorkflowJobBean w set w.appName = :appName, w.protoActionConf = :protoActionConf, w.appPath = :appPath, w.conf = :conf, w.logToken = :logToken, w.user = :user, w.group = :group, w.externalId = :externalId, w.endTimestamp = :endTime, w.run = :run, w.statusStr = :status, w.wfInstance = :wfInstance, w.lastModifiedTimestamp = :lastModTime where w.id = :id"),

    @NamedQuery(name = "DELETE_WORKFLOW", query = "delete from WorkflowJobBean w where w.id IN (:id)"),

    @NamedQuery(name = "GET_WORKFLOWS", query = "select OBJECT(w) from WorkflowJobBean w order by w.startTimestamp desc"),

    @NamedQuery(name = "GET_WORKFLOWS_COLUMNS", query = "select w.id, w.appName, w.statusStr, w.run, w.user, w.group, w.createdTimestamp, w.startTimestamp, w.lastModifiedTimestamp, w.endTimestamp, w.externalId, w.parentId from WorkflowJobBean w order by w.createdTimestamp desc"),

    @NamedQuery(name = "GET_WORKFLOWS_COUNT", query = "select count(w) from WorkflowJobBean w"),

    @NamedQuery(name = "GET_COMPLETED_WORKFLOWS_OLDER_THAN", query = "select w from WorkflowJobBean w where w.endTimestamp < :endTime"),

    @NamedQuery(name = "GET_COMPLETED_WORKFLOWS_WITH_NO_PARENT_OLDER_THAN", query = "select w.id from WorkflowJobBean w where w.endTimestamp < :endTime and w.parentId is null"),

    @NamedQuery(name = "GET_COMPLETED_COORD_WORKFLOWS_OLDER_THAN", query = "select w.id, w.parentId from WorkflowJobBean w where w.endTimestamp < :endTime and w.parentId like '%C@%'"),

    @NamedQuery(name = "GET_WORKFLOW", query = "select OBJECT(w) from WorkflowJobBean w where w.id = :id"),

    @NamedQuery(name = "GET_WORKFLOW_STARTTIME", query = "select w.id, w.startTimestamp from WorkflowJobBean w where w.id = :id"),

    @NamedQuery(name = "GET_WORKFLOW_START_END_TIME", query = "select w.id, w.startTimestamp, w.endTimestamp from WorkflowJobBean w where w.id = :id"),

    @NamedQuery(name = "GET_WORKFLOW_USER_GROUP", query = "select w.user, w.group from WorkflowJobBean w where w.id = :id"),

    @NamedQuery(name = "GET_WORKFLOW_SUSPEND", query = "select w.id, w.user, w.group, w.appName, w.statusStr, w.parentId, w.startTimestamp, w.endTimestamp, w.logToken, w.wfInstance  from WorkflowJobBean w where w.id = :id"),

    @NamedQuery(name = "GET_WORKFLOW_RERUN", query = "select w.id, w.user, w.group, w.appName, w.statusStr, w.run, w.logToken, w.wfInstance, w.parentId from WorkflowJobBean w where w.id = :id"),

    @NamedQuery(name = "GET_WORKFLOW_DEFINITION", query = "select w.id, w.user, w.group, w.appName, w.logToken, w.wfInstance from WorkflowJobBean w where w.id = :id"),

    @NamedQuery(name = "GET_WORKFLOW_ACTION_OP", query = "select w.id, w.user, w.group, w.appName, w.appPath, w.statusStr, w.run, w.parentId, w.logToken, w.wfInstance, w.protoActionConf from WorkflowJobBean w where w.id = :id"),

    @NamedQuery(name = "GET_WORKFLOW_KILL", query = "select w.id, w.user, w.group, w.appName, w.appPath, w.statusStr, w.parentId, w.startTimestamp, w.endTimestamp, w.logToken, w.wfInstance, w.slaXml from WorkflowJobBean w where w.id = :id"),

    @NamedQuery(name = "GET_WORKFLOW_RESUME", query = "select w.id, w.user, w.group, w.appName, w.appPath, w.statusStr, w.parentId, w.startTimestamp, w.endTimestamp, w.logToken, w.wfInstance, w.protoActionConf from WorkflowJobBean w where w.id = :id"),

    @NamedQuery(name = "GET_WORKFLOW_FOR_UPDATE", query = "select OBJECT(w) from WorkflowJobBean w where w.id = :id"),

    @NamedQuery(name = "GET_WORKFLOW_FOR_SLA", query = "select w.id, w.statusStr, w.startTimestamp, w.endTimestamp from WorkflowJobBean w where w.id = :id"),

    @NamedQuery(name = "GET_WORKFLOW_ID_FOR_EXTERNAL_ID", query = "select  w.id from WorkflowJobBean w where w.externalId = :externalId"),

    @NamedQuery(name = "GET_WORKFLOWS_COUNT_WITH_STATUS", query = "select count(w) from WorkflowJobBean w where w.statusStr = :status"),

    @NamedQuery(name = "GET_WORKFLOWS_COUNT_WITH_STATUS_IN_LAST_N_SECS", query = "select count(w) from WorkflowJobBean w where w.statusStr = :status and w.lastModifiedTimestamp > :lastModTime"),

    @NamedQuery(name = "GET_WORKFLOWS_WITH_WORKFLOW_PARENT_ID", query = "select w.id from WorkflowJobBean w where w.parentId = :parentId"),

    @NamedQuery(name = "GET_WORKFLOWS_WITH_COORD_PARENT_ID", query = "select w.id from WorkflowJobBean w where w.parentId like :parentId"), // when setting parentId parameter, make sure to append a '%' (percent symbol) at the end (e.g. 0000004-130709155224435-oozie-rkan-C%")

    @NamedQuery(name = "GET_WORKFLOWS_BASIC_INFO_BY_PARENT_ID", query = "select w.id, w.statusStr, w.endTimestamp from WorkflowJobBean w where w.parentId = :parentId"),

    @NamedQuery(name = "GET_WORKFLOWS_BASIC_INFO_BY_COORD_PARENT_ID", query = "select w.id,  w.statusStr, w.endTimestamp from WorkflowJobBean w where w.parentId like :parentId"),

    @NamedQuery(name = "GET_WORKFLOW_FOR_USER", query = "select w.user from WorkflowJobBean w where w.id = :id"),

    @NamedQuery(name = "GET_WORKFLOW_STATUS", query = "select w.statusStr from WorkflowJobBean w where w.id = :id"),

    @NamedQuery(name = "GET_WORKFLOWS_PARENT_COORD_RERUN", query = "select w.id, w.statusStr, w.startTimestamp, w.endTimestamp "
            + "from WorkflowJobBean w where w.parentId = :parentId order by w.createdTimestamp")})
@Table(name = "WF_JOBS")
public class WorkflowJobBean implements Writable, WorkflowJob, JsonBean {

    @Id
    private String id;

    @Basic
    @Column(name = "proto_action_conf")
    @Lob
    @Strategy("org.apache.oozie.executor.jpa.StringBlobValueHandler")
    private StringBlob protoActionConf;

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
    private String statusStr = WorkflowJob.Status.PREP.toString();

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

    @Basic
    @Index
    @Column(name = "last_modified_time")
    private java.sql.Timestamp lastModifiedTimestamp = null;

    @Basic
    @Column(name = "wf_instance")
    @Lob
    @Strategy("org.apache.oozie.executor.jpa.BinaryBlobValueHandler")
    private BinaryBlob wfInstance ;

    @Basic
    @Column(name = "sla_xml")
    @Lob
    @Strategy("org.apache.oozie.executor.jpa.StringBlobValueHandler")
    private StringBlob slaXml;


    @Basic
    @Column(name = "app_name")
    private String appName = null;

    @Basic
    @Column(name = "app_path")
    private String appPath = null;

    @Basic
    @Column(name = "conf")
    @Lob
    @Strategy("org.apache.oozie.executor.jpa.StringBlobValueHandler")
    private StringBlob conf;

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
    @Index
    @Column(name = "parent_id")
    private String parentId;

    @Transient
    private String consoleUrl;

    @Transient
    private List<WorkflowActionBean> actions;


    /**
     * Default constructor.
     */
    public WorkflowJobBean() {
        actions = new ArrayList<WorkflowActionBean>();
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
        WritableUtils.writeStr(dataOutput, logToken);
        WritableUtils.writeStr(dataOutput, getProtoActionConf());
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
        logToken = WritableUtils.readStr(dataInput);
        setProtoActionConf(WritableUtils.readStr(dataInput));
        setExternalId(getExternalId());
    }

    public boolean inTerminalState() {
        boolean inTerminalState = false;
        switch (WorkflowJob.Status.valueOf(statusStr)) {
            case FAILED:
            case KILLED:
            case SUCCEEDED:
                inTerminalState = true;
                break;
            default:
                break;
        }
        return inTerminalState;
    }

    public String getLogToken() {
        return logToken;
    }

    public void setLogToken(String logToken) {
        this.logToken = logToken;
    }

    public String getSlaXml() {
        return slaXml == null ? null : slaXml.getString();
    }

    public void setSlaXml(String slaXml) {
        if (this.slaXml == null) {
            this.slaXml = new StringBlob(slaXml);
        }
        else {
            this.slaXml.setString(slaXml);
        }
    }

    public void setSlaXmlBlob(StringBlob slaXml) {
        this.slaXml = slaXml;
    }

    public StringBlob getSlaXmlBlob() {
        return this.slaXml;
    }

    public WorkflowInstance getWorkflowInstance() {
        return wfInstance == null ? null : get(wfInstance.getBytes());
    }

    public BinaryBlob getWfInstanceBlob() {
        return this.wfInstance;
    }

    public void setWorkflowInstance(WorkflowInstance workflowInstance) {
        if (this.wfInstance == null) {
            this.wfInstance = new BinaryBlob(WritableUtils.toByteArray((LiteWorkflowInstance) workflowInstance), true);
        }
        else {
            this.wfInstance.setBytes(WritableUtils.toByteArray((LiteWorkflowInstance) workflowInstance));
        }
    }

    public void setWfInstanceBlob(BinaryBlob wfInstance) {
        this.wfInstance = wfInstance;
    }

    public String getProtoActionConf() {
        return protoActionConf == null ? null : protoActionConf.getString();
    }

    public void setProtoActionConf(String protoActionConf) {
        if (this.protoActionConf == null) {
            this.protoActionConf = new StringBlob(protoActionConf);
        }
        else {
            this.protoActionConf.setString(protoActionConf);
        }
    }

    public void setProtoActionConfBlob (StringBlob protoBytes) {
        this.protoActionConf = protoBytes;
    }

    public StringBlob getProtoActionConfBlob() {
        return this.protoActionConf;
    }

    public String getlogToken() {
        return logToken;
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

    public void setStatusStr (String statusStr) {
        this.statusStr = statusStr;
    }

    public void setStatus(Status val) {
        this.statusStr = val.toString();
    }

    @Override
    public Status getStatus() {
        return Status.valueOf(statusStr);
    }

    public String getStatusStr() {
        return statusStr;
    }

    public void setExternalId(String externalId) {
        this.externalId = externalId;
    }

    @Override
    public String getExternalId() {
        return externalId;
    }

    public void setLastModifiedTime(Date lastModifiedTime) {
        this.lastModifiedTimestamp = DateUtils.convertDateToTimestamp(lastModifiedTime);
    }

    public Date getLastModifiedTime() {
        return DateUtils.toDate(lastModifiedTimestamp);
    }

    public Date getCreatedTime() {
        return DateUtils.toDate(createdTimestamp);
    }

    public void setCreatedTime(Date createdTime) {
        this.createdTimestamp = DateUtils.convertDateToTimestamp(createdTime);
    }

    @Override
    public Date getStartTime() {
        return DateUtils.toDate(startTimestamp);
    }

    public void setStartTime(Date startTime) {
        this.startTimestamp = DateUtils.convertDateToTimestamp(startTime);
    }

    public Date getEndTime() {
        return DateUtils.toDate(endTimestamp);
    }

    public void setEndTime(Date endTime) {
        this.endTimestamp = DateUtils.convertDateToTimestamp(endTime);
    }

    private WorkflowInstance get(byte[] array) {
        LiteWorkflowInstance pInstance = WritableUtils.fromByteArray(array, LiteWorkflowInstance.class);
        return pInstance;
    }

    @SuppressWarnings("unchecked")
    public JSONObject toJSONObject() {
        return toJSONObject("GMT");
    }

    @SuppressWarnings("unchecked")
    public JSONObject toJSONObject(String timeZoneId) {
        JSONObject json = new JSONObject();
        json.put(JsonTags.WORKFLOW_APP_PATH, getAppPath());
        json.put(JsonTags.WORKFLOW_APP_NAME, getAppName());
        json.put(JsonTags.WORKFLOW_ID, getId());
        json.put(JsonTags.WORKFLOW_EXTERNAL_ID, getExternalId());
        json.put(JsonTags.WORKFLOW_PARENT_ID, getParentId());
        json.put(JsonTags.WORKFLOW_CONF, getConf());
        json.put(JsonTags.WORKFLOW_STATUS, getStatus().toString());
        json.put(JsonTags.WORKFLOW_LAST_MOD_TIME, JsonUtils.formatDateRfc822(getLastModifiedTime(), timeZoneId));
        json.put(JsonTags.WORKFLOW_CREATED_TIME, JsonUtils.formatDateRfc822(getCreatedTime(), timeZoneId));
        json.put(JsonTags.WORKFLOW_START_TIME, JsonUtils.formatDateRfc822(getStartTime(), timeZoneId));
        json.put(JsonTags.WORKFLOW_END_TIME, JsonUtils.formatDateRfc822(getEndTime(), timeZoneId));
        json.put(JsonTags.WORKFLOW_USER, getUser());
        json.put(JsonTags.WORKFLOW_GROUP, getGroup());
        json.put(JsonTags.WORKFLOW_ACL, getAcl());
        json.put(JsonTags.WORKFLOW_RUN, (long) getRun());
        json.put(JsonTags.WORKFLOW_CONSOLE_URL, getConsoleUrl());
        json.put(JsonTags.WORKFLOW_ACTIONS, WorkflowActionBean.toJSONArray(actions, timeZoneId));
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
        return this.conf;
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

    public void setActions(List<WorkflowActionBean> nodes) {
        this.actions = (nodes != null) ? nodes : new ArrayList<WorkflowActionBean>();
    }

    @Override
    public String toString() {
        return MessageFormat.format("Workflow id[{0}] status[{1}]", getId(), getStatus());
    }

    /**
     * Convert a workflows list into a JSONArray.
     *
     * @param workflows workflows list.
     * @param timeZoneId time zone to use for dates in the JSON array.
     * @return the corresponding JSON array.
     */
    @SuppressWarnings("unchecked")
    public static JSONArray toJSONArray(List<WorkflowJobBean> workflows, String timeZoneId) {
        JSONArray array = new JSONArray();
        if (workflows != null) {
            for (WorkflowJobBean node : workflows) {
                array.add(node.toJSONObject(timeZoneId));
            }
        }
        return array;
    }



}
