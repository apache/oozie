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

import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.oozie.AppType;
import org.apache.oozie.client.event.Event.MessageType;
import org.apache.oozie.client.event.SLAEvent;
import org.apache.oozie.client.event.jms.JMSHeaderConstants;
import org.apache.oozie.client.event.message.EventMessage;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class SLAMessage extends EventMessage {

    @JsonProperty
    private String id;
    @JsonProperty
    private String parentId;
    @JsonProperty
    private Date nominalTime;
    @JsonProperty
    private Date expectedStartTime;
    @JsonProperty
    private Date actualStartTime;
    @JsonProperty
    private Date expectedEndTime;
    @JsonProperty
    private Date actualEndTime;
    @JsonProperty
    private long expectedDuration;
    @JsonProperty
    private long actualDuration;
    @JsonProperty
    private String notificationMessage;
    @JsonProperty
    private String upstreamApps;

    private String user;
    private String appName;
    private SLAEvent.EventStatus eventStatus;
    private SLAEvent.SLAStatus slaStatus;

    public SLAMessage(){
        // Dummy constructor for JSON
    }

    public SLAMessage(SLAEvent.EventStatus eventStatus, SLAEvent.SLAStatus slaStatus, AppType appType, String appName,
            String user, String jobId, String parentJobId, Date nominalTime, Date expectedStartTime,
            Date actualStartTime, Date expectedEndTime, Date actualEndTime, long expectedDuration, long actualDuration,
            String notificationMessage, String upstreamApps) {

        super(MessageType.SLA, appType);
        this.eventStatus = eventStatus;
        this.slaStatus = slaStatus;
        this.appName = appName;
        this.user = user;
        this.id = jobId;
        this.parentId = parentJobId;
        this.nominalTime = nominalTime;
        this.expectedStartTime = expectedStartTime;
        this.actualStartTime = actualStartTime;
        this.expectedEndTime = expectedEndTime;
        this.actualEndTime = actualEndTime;
        this.expectedDuration = expectedDuration;
        this.actualDuration = actualDuration;
        this.notificationMessage = notificationMessage;
        this.upstreamApps = upstreamApps;
    }

    /**
     * Get the job Id
     *
     * @return id the job id
     */
    public String getId() {
        return id;
    }

    /**
     * Set the job Id for message
     *
     * @param id the job id
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Gets the parent job id
     *
     * @return the parent job Id
     */
    public String getParentId() {
        return parentId;
    }

    /**
     * Set the parent job Id for message
     *
     * @param parentId the parent job Id
     */
    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    /**
     * Get nominal time
     *
     * @return nominal time
     */
    public Date getNominalTime() {
        return nominalTime;
    }

    /**
     * Set nominal time for message
     *
     * @param nominalTime
     */
    public void setNominalTime(Date nominalTime) {
        this.nominalTime = nominalTime;
    }

    /**
     * Get expected start time
     *
     * @return the expected start time
     */
    public Date getExpectedStartTime() {
        return expectedStartTime;
    }

    /**
     * Set expected start time for message
     *
     * @param expectedStartTime
     */
    public void setExpectedStartTime(Date expectedStartTime) {
        this.expectedStartTime = expectedStartTime;
    }

    /**
     * Get actual start time
     *
     * @return actual start time
     */
    public Date getActualStartTime() {
        return actualStartTime;
    }

    /**
     * Set actual start time for message
     *
     * @param actualStartTime
     */
    public void setActualStartTime(Date actualStartTime) {
        this.actualStartTime = actualStartTime;
    }

    /**
     * Get expected end time
     *
     * @return expectedEndTime
     */
    public Date getExpectedEndTime() {
        return expectedEndTime;
    }

    /**
     * Set expected end time for message
     *
     * @param expectedEndTime
     */
    public void setExpectedEndTime(Date expectedEndTime) {
        this.expectedEndTime = expectedEndTime;
    }

    /**
     * Get actual end time
     *
     * @return actual end time
     */
    public Date getActualEndTime() {
        return actualEndTime;
    }

    /**
     * Set actual end time for message
     *
     * @param actualEndTime
     */
    public void setActualEndTime(Date actualEndTime) {
        this.actualEndTime = actualEndTime;
    }

    /**
     * Get expected duration time (in milliseconds)
     *
     * @return expectedDuration (in milliseconds)
     */
    public long getExpectedDuration() {
        return expectedDuration;
    }

    /**
     * Set expected duration (in milliseconds) for message
     *
     * @param expectedDuration (in milliseconds)
     */
    public void setExpectedDuration(long expectedDuration) {
        this.expectedDuration = expectedDuration;
    }

    /**
     * Get actual duration (in milliseconds)
     *
     * @return actual duration (in milliseconds)
     */
    public long getActualDuration() {
        return actualDuration;
    }

    /**
     * Set actual duration (in milliseconds) for message
     *
     * @param actualDuration (in milliseconds)
     */
    public void setActualDuration(long actualDuration) {
        this.actualDuration = actualDuration;
    }

    /**
     * Get notification message
     *
     * @return notification message
     */
    public String getNotificationMessage() {
        return notificationMessage;
    }

    /**
     * Set notification message
     *
     * @param notificationMessage
     */
    public void setNotificationMessage(String notificationMessage) {
        this.notificationMessage = notificationMessage;
    }

    /**
     * Get upstream app names
     *
     * @return upstreamApps
     */
    public String getUpstreamApps() {
        return upstreamApps;
    }

    /**
     * Set upstream app names
     *
     * @param upstreamApps
     */
    public void setUpstreamApps(String upstreamApps) {
        this.upstreamApps = upstreamApps;
    }

    /**
     * Get user name
     *
     * @return user name
     */
    @JsonIgnore
    public String getUser() {
        return user;
    }

    /**
     * Set user name for message
     *
     * @param user
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * Get application name
     *
     * @return application name
     */
    @JsonIgnore
    public String getAppName() {
        return appName;
    }

    /**
     * Set application name for message
     *
     * @param appName
     */
    public void setAppName(String appName) {
        this.appName = appName;
    }

    /**
     * Get event status
     *
     * @return event status
     */
    @JsonIgnore
    public SLAEvent.EventStatus getEventStatus() {
        return eventStatus;
    }

    /**
     * Set event status
     *
     * @param eventStatus
     */
    public void setEventStatus(SLAEvent.EventStatus eventStatus){
        this.eventStatus = eventStatus;
    }

    /**
     * Get SLA status
     *
     * @return sla status
     */
    @JsonIgnore
    public SLAEvent.SLAStatus getSLAStatus() {
        return slaStatus;
    }

    /**
     * Set SLA status for message
     *
     * @param slaStatus
     */
    public void setSLAStatus(SLAEvent.SLAStatus slaStatus) {
        this.slaStatus = slaStatus;
    }

    /**
     * Set the JMS properties for SLA message
     *
     * @param message the JMS message
     * @throws JMSException
     */
    @Override
    @JsonIgnore
    public void setProperties(Message message) throws JMSException {
        super.setProperties(message);
        setEventStatus(SLAEvent.EventStatus.valueOf(message.getStringProperty(JMSHeaderConstants.EVENT_STATUS)));
        setSLAStatus(SLAEvent.SLAStatus.valueOf(message.getStringProperty(JMSHeaderConstants.SLA_STATUS)));
        setAppName(message.getStringProperty(JMSHeaderConstants.APP_NAME));
        setUser(message.getStringProperty(JMSHeaderConstants.USER));
    }
}
