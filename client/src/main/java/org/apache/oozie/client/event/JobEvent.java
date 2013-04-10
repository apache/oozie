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
package org.apache.oozie.client.event;

import java.io.Serializable;
import java.util.Date;

import org.apache.oozie.client.event.Event;

/**
 * An abstract implementation of the Event interface, related to
 * notification events after job status changes
 */
public abstract class JobEvent implements Event, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Coarse-grained status of the job event
     *
     */
    public static enum EventStatus {
        WAITING, STARTED, SUCCESS, SUSPEND, FAILURE
    }

    private AppType appType;
    private MessageType msgType;
    private String id;
    private String parentId;
    private EventStatus eventStatus;
    private String appName;
    private String user;
    private Date startTime;
    private Date endTime;

    public JobEvent(AppType appType, String id, String parentId, String user, String appName) {
        this.appType = appType;
        this.msgType = MessageType.JOB;
        this.id = id;
        this.parentId = parentId;
        this.user = user;
        this.appName = appName;
    }

    @Override
    public AppType getAppType() {
        return appType;
    }

    public void setAppType(AppType type) {
        appType = type;
    }

    @Override
    public MessageType getMsgType() {
        return msgType;
    }

    public void setMsgType(MessageType type) {
        msgType = type;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getParentId() {
        return parentId;
    }

    public void setParentId(String id) {
        parentId = id;
    }

    public EventStatus getEventStatus() {
        return eventStatus;
    }

    public void setEventStatus(EventStatus eStatus) {
        eventStatus = eStatus;
    }

    @Override
    public String getAppName() {
        return appName;
    }

    public void setAppName(String name) {
        appName = name;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String euser) {
        user = euser;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date time) {
        startTime = time;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date time) {
        endTime = time;
    }

    @Override
    public String toString() {
        return "ID: " + getId() + ", AppType: " + getAppType() + ", Appname: " + getAppName() + ", Status: "
                + getEventStatus();
    }

}
