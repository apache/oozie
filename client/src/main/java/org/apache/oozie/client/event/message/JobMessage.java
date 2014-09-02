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

package org.apache.oozie.client.event.message;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.oozie.AppType;
import org.apache.oozie.client.event.Event.MessageType;
import org.apache.oozie.client.event.JobEvent.EventStatus;
import org.apache.oozie.client.event.JobEvent;
import org.apache.oozie.client.event.jms.JMSHeaderConstants;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;


/**
 * Class holding attributes related to a job message
 *
 */
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class JobMessage extends EventMessage {

    @JsonProperty
    private String id;
    @JsonProperty
    private String parentId;
    @JsonProperty
    private Date startTime;
    @JsonProperty
    private Date endTime;

    private Map<String, String> jmsMessageProperties = new HashMap<String, String>();
    private JobEvent.EventStatus eventStatus;
    private String appName;
    private String user;

    /**
     * Default constructor
     */
    public JobMessage() {
        // Default constructor for jackson
    }

    /**
     * Constructs a job message
     *
     * @param eventStatus the event status
     * @param appType the appType for event
     * @param id the id of job
     * @param parentId the parent id of job
     * @param startTime the start time of job
     * @param endTime the end time of job
     * @param user user of the job
     * @param appName appName for job
     */
    public JobMessage(JobEvent.EventStatus eventStatus, AppType appType, String id, String parentId, Date startTime,
            Date endTime, String user, String appName) {
        super(MessageType.JOB, appType);
        this.eventStatus = eventStatus;
        this.id = id;
        this.parentId = parentId;
        this.startTime = startTime;
        this.appName = appName;
        this.user = user;
        this.endTime = endTime;

        jmsMessageProperties = new HashMap<String, String>();
        jmsMessageProperties.put(JMSHeaderConstants.APP_TYPE, appType.toString());
        jmsMessageProperties.put(JMSHeaderConstants.MESSAGE_TYPE, MessageType.JOB.toString());
        jmsMessageProperties.put(JMSHeaderConstants.EVENT_STATUS, eventStatus.toString());
        jmsMessageProperties.put(JMSHeaderConstants.APP_NAME, appName);
        jmsMessageProperties.put(JMSHeaderConstants.USER, user);
    }

    /**
     * Sets the Job id for message
     *
     * @param id the job id
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Gets the job id
     *
     * @return the Job id
     */
    public String getId() {
        return id;
    }

    /**
     * Sets the parent job id for message
     *
     * @param parentId the parent job id
     */
    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    /**
     * Gets the parent job id
     *
     * @return the parentId
     */
    public String getParentId() {
        return parentId;
    }

    /**
     * Sets the job start time for message
     *
     * @param startTime
     */
    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    /**
     * Gets the job start time
     *
     * @return the start time
     */
    public Date getStartTime() {
        return startTime;
    }

    /**
     * Sets the job end time for message
     *
     * @param endTime
     */
    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    /**
     * Gets the job end time
     *
     * @return the end time
     */
    public Date getEndTime() {
        return endTime;
    }

    /**
     * Sets the job's app name for message
     *
     * @param appName
     */
    public void setAppName(String appName) {
        this.appName = appName;
    }

    /**
     * Gets the job's app name
     *
     * @return the app name
     */
    @JsonIgnore
    public String getAppName() {
        return appName;
    }

    /**
     * Sets the JMS selectors for message
     *
     * @param properties the jms selector key value pair
     */
    void setMessageProperties(Map<String, String> properties) {
        jmsMessageProperties = properties;
    }

    /**
     * Gets the message properties
     *
     * @return the message properties
     */
    @JsonIgnore
    public Map<String, String> getMessageProperties() {
        return jmsMessageProperties;
    }

    /**
     * sets the job user for the msg
     *
     * @param user
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * Gets the job user
     *
     * @return the user
     */
    @JsonIgnore
    public String getUser() {
        return user;
    }

    /**
     * Sets the event status
     *
     * @param eventStatus
     */
    public void setEventStatus(JobEvent.EventStatus eventStatus) {
        this.eventStatus = eventStatus;
    }

    /**
     * Gets the event status
     *
     * @return the event status
     */
    @JsonIgnore
    public JobEvent.EventStatus getEventStatus() {
        return eventStatus;
    }

    @Override
    public void setProperties(Message message) throws JMSException {
        super.setProperties(message);
        setEventStatus(EventStatus.valueOf(message.getStringProperty(JMSHeaderConstants.EVENT_STATUS)));
        setAppName(message.getStringProperty(JMSHeaderConstants.APP_NAME));
        setUser(message.getStringProperty(JMSHeaderConstants.USER));
    }
}
